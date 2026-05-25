"""按工单服务商筛选，分批删除 tb_workresourceinfo 与 basic_resourceitem。

筛选入口::

    SELECT b.Id, b.ResourceId
    FROM tb_workorderinfo a
    INNER JOIN tb_workresourceinfo b ON b.WorkOrderId = a.Id
    WHERE a.ServiceProviderCode = ? ...
"""
from __future__ import annotations

import datetime
from typing import Any, Sequence

import pymysql
from loguru import logger

from base_handler import BaseHandler
from delete_helpers import (
    BatchDeleter,
    CleanupAuditWriter,
    DeleteSqlDebugger,
    placeholders,
    sleep_between_batches,
)


class DeleteWorkorderResourceHandler(BaseHandler):
    HANDLER_NAME = "DeleteWorkorderResourceHandler"

    def __init__(
        self,
        connection_kwargs: dict,
        service_provider_code: str = "1002",
        batch_size: int = 100,
        worker_threads: int = 2,
        batch_sleep_seconds: float = 30,
        order_by: str = "b.Id",
        dry_run: bool = False,
        sql_log_dir: str | None = None,
        in_chunk_size: int = 500,
        cut_off_time: datetime.time | None = None,
    ) -> None:
        if cut_off_time is None:
            cut_off_time = datetime.time(hour=6, minute=0, second=0)
        super().__init__(cut_off_time)

        self.service_provider_code = service_provider_code
        self.batch_size = batch_size
        self.worker_threads = max(1, worker_threads)
        self.batch_sleep_seconds = batch_sleep_seconds
        self.order_by = order_by
        self.dry_run = dry_run

        self._conn_kwargs = dict(connection_kwargs)
        debugger = DeleteSqlDebugger(dry_run, sql_log_dir)
        self._deleter = BatchDeleter(
            self._conn_kwargs,
            debugger,
            self.HANDLER_NAME,
            dry_run=dry_run,
            in_chunk_size=in_chunk_size,
        )
        self._audit = CleanupAuditWriter(
            self._conn_kwargs, self.HANDLER_NAME, dry_run=dry_run
        )

    def _process_once(self) -> bool:
        rows = self._fetch_resource_rows()
        if not rows:
            logger.info("资源清理：今日无更多待删附件记录")
            return True

        batch_no = self._audit.next_batch_no()
        work_ids = [r["Id"] for r in rows]
        resource_ids = list({r["ResourceId"] for r in rows if r.get("ResourceId")})
        wo_dates = self._fetch_workorder_date_range(work_ids)

        work_deleted = self._deleter.delete_by_ids("tb_workresourceinfo", "Id", work_ids)
        self._audit.record(
            "tb_workresourceinfo",
            work_deleted,
            service_provider_code=self.service_provider_code,
            min_work_order_date=wo_dates[0],
            max_work_order_date=wo_dates[1],
            batch_no=batch_no,
        )

        if resource_ids:
            if self.worker_threads > 1:
                results = self._deleter.parallel_delete_tables(
                    [("basic_resourceitem", "Id", resource_ids, {})],
                    1,
                )
                res_deleted = results.get("basic_resourceitem", 0)
            else:
                res_deleted = self._deleter.delete_by_ids(
                    "basic_resourceitem", "Id", resource_ids
                )
            self._audit.record(
                "basic_resourceitem",
                res_deleted,
                service_provider_code=self.service_provider_code,
                min_work_order_date=wo_dates[0],
                max_work_order_date=wo_dates[1],
                batch_no=batch_no,
            )
            logger.info(
                f"资源清理本批: workresource={work_deleted}, resourceitem={res_deleted}"
            )
        else:
            logger.info(f"资源清理本批: workresource={work_deleted}, 无 ResourceId")

        sleep_between_batches(self.batch_sleep_seconds)
        return False

    def _fetch_resource_rows(self) -> list[dict[str, Any]]:
        sql = f"""
            SELECT b.Id, b.ResourceId
            FROM tb_workorderinfo a
            INNER JOIN tb_workresourceinfo b ON b.WorkOrderId = a.Id
            WHERE a.ServiceProviderCode = %s
            ORDER BY {self.order_by}
            LIMIT %s
        """
        with pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor, **self._conn_kwargs
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (self.service_provider_code, self.batch_size))
                return cur.fetchall()

    def _fetch_workorder_date_range(
        self, work_resource_ids: Sequence[int]
    ) -> tuple[datetime.date | None, datetime.date | None]:
        ph = placeholders(len(work_resource_ids))
        sql = f"""
            SELECT MIN(a.CreatedAt) AS mn, MAX(a.CreatedAt) AS mx
            FROM tb_workorderinfo a
            INNER JOIN tb_workresourceinfo b ON b.WorkOrderId = a.Id
            WHERE b.Id IN ({ph})
        """
        with pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor, **self._conn_kwargs
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, work_resource_ids)
                row = cur.fetchone()
        if not row or row["mn"] is None:
            return None, None
        mn, mx = row["mn"], row["mx"]
        return (
            mn.date() if hasattr(mn, "date") else mn,
            mx.date() if hasattr(mx, "date") else mx,
        )
