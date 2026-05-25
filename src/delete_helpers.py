"""历史数据清理：分批删除、审计、DEBUG 模式（仅记 SQL 不删）等公共能力。"""
from __future__ import annotations

import datetime
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Iterable, Sequence

import pymysql
from loguru import logger


def placeholders(n: int) -> str:
    if n <= 0:
        raise ValueError("placeholders count must be positive")
    return ",".join(["%s"] * n)


def chunk_list(items: Sequence[Any], size: int) -> list[list[Any]]:
    return [list(items[i : i + size]) for i in range(0, len(items), size)]


def _is_mutating_sql(sql: str) -> bool:
    head = sql.lstrip()[:20].upper()
    return head.startswith("DELETE") or head.startswith("UPDATE")


class DeleteSqlDebugger:
    """DEBUG 模式下记录 DELETE/UPDATE（参数已展开，便于 EXPLAIN）。"""

    def __init__(self, enabled: bool, log_dir: str | None = None) -> None:
        self.enabled = enabled
        self._lock = threading.Lock()
        self._file_path: str | None = None
        if enabled:
            base = log_dir or os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "log"
            )
            os.makedirs(base, exist_ok=True)
            today = datetime.date.today().isoformat()
            self._file_path = os.path.join(base, f"delete_sql_debug_{today}.log")

    def log(self, handler_name: str, sql: str, params: Sequence[Any] | None = None) -> None:
        if not self.enabled:
            return
        rendered = self._render(sql, params)
        line = (
            f"{datetime.datetime.now().isoformat(timespec='seconds')} "
            f"| {handler_name} | {rendered}\n"
        )
        with self._lock:
            logger.info(f"[DEBUG] [{handler_name}] {rendered}")
            if self._file_path:
                with open(self._file_path, "a", encoding="utf-8") as fh:
                    fh.write(line)

    @staticmethod
    def _render(sql: str, params: Sequence[Any] | None) -> str:
        if not params:
            return sql.strip()
        out = sql
        for p in params:
            if p is None:
                val = "NULL"
            elif isinstance(p, (int, float)):
                val = str(p)
            else:
                val = "'" + str(p).replace("'", "''") + "'"
            out = out.replace("%s", val, 1)
        return out.strip()


class CleanupAuditWriter:
    """写入 cleanup_delete_audit（DEBUG 模式下跳过）。"""

    def __init__(
        self, conn_kwargs: dict, handler_name: str, *, dry_run: bool = False
    ) -> None:
        self.conn_kwargs = conn_kwargs
        self.handler_name = handler_name
        self.dry_run = dry_run
        self._batch_no = 0
        self._run_date = datetime.date.today()

    def next_batch_no(self) -> int:
        self._batch_no += 1
        return self._batch_no

    def record(
        self,
        table_name: str,
        deleted_count: int,
        *,
        service_provider_code: str | None = None,
        min_work_order_date: datetime.date | None = None,
        max_work_order_date: datetime.date | None = None,
        batch_no: int | None = None,
        remark: str | None = None,
    ) -> None:
        if self.dry_run or deleted_count <= 0:
            return
        sql = """
            INSERT INTO cleanup_delete_audit
              (RunDate, HandlerName, TableName, DeletedCount,
               ServiceProviderCode, MinWorkOrderDate, MaxWorkOrderDate, BatchNo, Remark)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        params = (
            self._run_date,
            self.handler_name,
            table_name,
            deleted_count,
            service_provider_code,
            min_work_order_date,
            max_work_order_date,
            batch_no,
            remark,
        )
        try:
            with pymysql.connect(**self.conn_kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params)
                conn.commit()
        except Exception as exc:
            logger.warning(f"写入审计表失败({table_name}): {exc}")


class BatchDeleter:
    """单表 IN 删除；dry_run 时只记 SQL 不执行 DELETE/UPDATE。"""

    def __init__(
        self,
        conn_kwargs: dict,
        debugger: DeleteSqlDebugger,
        handler_name: str,
        *,
        dry_run: bool = False,
        in_chunk_size: int = 500,
    ) -> None:
        self.conn_kwargs = {**conn_kwargs, "cursorclass": pymysql.cursors.DictCursor}
        self.debugger = debugger
        self.handler_name = handler_name
        self.dry_run = dry_run
        self.in_chunk_size = in_chunk_size

    def _connect(self):
        return pymysql.connect(**self.conn_kwargs)

    def delete_by_ids(
        self,
        table: str,
        id_column: str,
        ids: Sequence[Any],
        *,
        extra_where: str = "",
        disable_fk: bool = False,
    ) -> int:
        if not ids:
            return 0
        total = 0
        for chunk in chunk_list(list(ids), self.in_chunk_size):
            ph = placeholders(len(chunk))
            sql = f"DELETE FROM `{table}` WHERE `{id_column}` IN ({ph})"
            if extra_where:
                sql += f" AND ({extra_where})"
            self.debugger.log(self.handler_name, sql, chunk)
            if self.dry_run:
                total += len(chunk)
                continue
            with self._connect() as conn:
                try:
                    with conn.cursor() as cur:
                        if disable_fk:
                            cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                        deleted = cur.execute(sql, chunk)
                        if disable_fk:
                            cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                    conn.commit()
                    total += deleted
                except Exception:
                    conn.rollback()
                    raise
        return total

    def execute_sql(
        self,
        sql: str,
        params: Sequence[Any] | None = None,
        *,
        disable_fk: bool = False,
    ) -> int:
        if _is_mutating_sql(sql):
            self.debugger.log(self.handler_name, sql, params)
        if self.dry_run and _is_mutating_sql(sql):
            return len(params) if params else 0
        with self._connect() as conn:
            try:
                with conn.cursor() as cur:
                    if disable_fk:
                        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
                    affected = cur.execute(sql, params or ())
                    if disable_fk:
                        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
                conn.commit()
                return affected
            except Exception:
                conn.rollback()
                raise

    def fetch_ids(self, sql: str, params: Sequence[Any] | None = None) -> list[Any]:
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params or ())
                rows = cur.fetchall()
        if not rows:
            return []
        key = next(iter(rows[0].keys()))
        return [row[key] for row in rows]

    def parallel_delete_tables(
        self,
        jobs: Iterable[tuple[str, str, Sequence[Any], dict]],
        worker_threads: int,
    ) -> dict[str, int]:
        results: dict[str, int] = {}

        def _job(item: tuple[str, str, Sequence[Any], dict]) -> tuple[str, int]:
            table, col, ids, kw = item
            deleter = BatchDeleter(
                {k: v for k, v in self.conn_kwargs.items() if k != "cursorclass"},
                self.debugger,
                self.handler_name,
                dry_run=self.dry_run,
                in_chunk_size=self.in_chunk_size,
            )
            return table, deleter.delete_by_ids(table, col, ids, **kw)

        with ThreadPoolExecutor(max_workers=max(1, worker_threads)) as pool:
            futures = {pool.submit(_job, j): j[0] for j in jobs if j[2]}
            for fut in as_completed(futures):
                table, count = fut.result()
                results[table] = count
        return results


def sleep_between_batches(seconds: float) -> None:
    if seconds > 0:
        logger.info(f"批间休眠 {seconds}s，降低对业务库压力…")
        time.sleep(seconds)
