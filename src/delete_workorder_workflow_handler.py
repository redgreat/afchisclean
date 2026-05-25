"""按工单服务商筛选，分批删除 workflow 运行时/完成态整套表数据。

筛选入口（仅 SELECT 可多表 JOIN；DELETE 均为单表 WHERE Id/外键 IN）::

    SELECT b.Id FROM tb_workorderinfo a
    INNER JOIN workflowruntimeitems b ON b.TargetEntityId = a.Id
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


class DeleteWorkorderWorkflowHandler(BaseHandler):
    HANDLER_NAME = "DeleteWorkorderWorkflowHandler"

    def __init__(
        self,
        connection_kwargs: dict,
        service_provider_code: str = "1002",
        batch_size: int = 50,
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
        runtime_ids = self._fetch_runtime_item_ids()
        if runtime_ids:
            batch_no = self._audit.next_batch_no()
            wo_dates = self._fetch_workorder_date_range_for_runtime(runtime_ids)
            self._purge_runtime_branch(runtime_ids, batch_no, wo_dates)
            sleep_between_batches(self.batch_sleep_seconds)
            return False

        complete_ids = self._fetch_complete_item_ids()
        if complete_ids:
            batch_no = self._audit.next_batch_no()
            wo_dates = self._fetch_workorder_date_range_for_complete(complete_ids)
            self._purge_complete_branch(complete_ids, batch_no, wo_dates)
            sleep_between_batches(self.batch_sleep_seconds)
            return False

        logger.info("工作流清理：今日无更多待删 runtime/complete 记录")
        return True

    def _fetch_runtime_item_ids(self) -> list[str]:
        sql = f"""
            SELECT b.Id
            FROM tb_workorderinfo a
            INNER JOIN workflowruntimeitems b ON b.TargetEntityId = a.Id
            WHERE a.ServiceProviderCode = %s
            ORDER BY {self.order_by}
            LIMIT %s
        """
        return self._deleter.fetch_ids(
            sql, (self.service_provider_code, self.batch_size)
        )

    def _fetch_complete_item_ids(self) -> list[str]:
        order = self.order_by.replace("b.", "c.")
        sql = f"""
            SELECT c.Id
            FROM tb_workorderinfo a
            INNER JOIN workflowcompleteitems c ON c.TargetEntityId = a.Id
            WHERE a.ServiceProviderCode = %s
            ORDER BY {order}
            LIMIT %s
        """
        return self._deleter.fetch_ids(
            sql, (self.service_provider_code, self.batch_size)
        )

    def _fetch_workorder_date_range_for_runtime(
        self, item_ids: list[str]
    ) -> tuple[datetime.date | None, datetime.date | None]:
        ph = placeholders(len(item_ids))
        sql = f"""
            SELECT MIN(a.CreatedAt) AS mn, MAX(a.CreatedAt) AS mx
            FROM tb_workorderinfo a
            INNER JOIN workflowruntimeitems b ON b.TargetEntityId = a.Id
            WHERE b.Id IN ({ph})
        """
        return self._parse_date_range(sql, item_ids)

    def _fetch_workorder_date_range_for_complete(
        self, item_ids: list[str]
    ) -> tuple[datetime.date | None, datetime.date | None]:
        ph = placeholders(len(item_ids))
        sql = f"""
            SELECT MIN(a.CreatedAt) AS mn, MAX(a.CreatedAt) AS mx
            FROM tb_workorderinfo a
            INNER JOIN workflowcompleteitems c ON c.TargetEntityId = a.Id
            WHERE c.Id IN ({ph})
        """
        return self._parse_date_range(sql, item_ids)

    def _parse_date_range(
        self, sql: str, params: Sequence[Any]
    ) -> tuple[datetime.date | None, datetime.date | None]:
        with pymysql.connect(
            cursorclass=pymysql.cursors.DictCursor, **self._conn_kwargs
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        if not row or row["mn"] is None:
            return None, None
        mn, mx = row["mn"], row["mx"]
        return (
            mn.date() if hasattr(mn, "date") else mn,
            mx.date() if hasattr(mx, "date") else mx,
        )

    def _audit_table(
        self,
        table: str,
        count: int,
        batch_no: int,
        wo_dates: tuple[datetime.date | None, datetime.date | None],
    ) -> None:
        self._audit.record(
            table,
            count,
            service_provider_code=self.service_provider_code,
            min_work_order_date=wo_dates[0],
            max_work_order_date=wo_dates[1],
            batch_no=batch_no,
        )

    def _ids_by_in(
        self, table: str, filter_column: str, parent_ids: Sequence[Any]
    ) -> list[Any]:
        if not parent_ids:
            return []
        sql = (
            f"SELECT Id FROM `{table}` WHERE `{filter_column}` IN "
            f"({placeholders(len(parent_ids))})"
        )
        return self._deleter.fetch_ids(sql, parent_ids)

    def _delete_and_audit(
        self,
        table: str,
        id_column: str,
        ids: Sequence[Any],
        batch_no: int,
        wo_dates: tuple[datetime.date | None, datetime.date | None],
        *,
        disable_fk: bool = False,
    ) -> int:
        if not ids:
            return 0
        count = self._deleter.delete_by_ids(
            table, id_column, ids, disable_fk=disable_fk
        )
        self._audit_table(table, count, batch_no, wo_dates)
        return count

    def _purge_runtime_branch(
        self,
        item_ids: list[str],
        batch_no: int,
        wo_dates: tuple[datetime.date | None, datetime.date | None],
    ) -> None:
        logger.info(f"清理 workflowruntime 分支，本批 {len(item_ids)} 个 RuntimeItem")
        step_ids = self._ids_by_in("workflowruntimesteps", "RuntimeItemId", item_ids)
        actor_ids = (
            self._ids_by_in("workflowruntimeactors", "RuntimeStepId", step_ids)
            if step_ids
            else []
        )
        comment_ids = (
            self._ids_by_in("workflowruntimecomments", "RuntimeActorId", actor_ids)
            if actor_ids
            else []
        )

        leaf_parallel: list[tuple[str, str, list[Any], dict]] = []
        if comment_ids:
            att_ids = self._ids_by_in(
                "workflowruntimeattachments", "RuntimeCommentId", comment_ids
            )
            leaf_parallel.append(("workflowruntimeattachments", "Id", att_ids, {}))
            leaf_parallel.append(("workflowruntimecomments", "Id", comment_ids, {}))
        if actor_ids:
            leaf_parallel.append(("workflowruntimeactors", "Id", actor_ids, {}))

        if leaf_parallel and self.worker_threads > 1:
            results = self._deleter.parallel_delete_tables(
                leaf_parallel, self.worker_threads
            )
            for table, cnt in results.items():
                self._audit_table(table, cnt, batch_no, wo_dates)
        else:
            for table, col, ids, kw in leaf_parallel:
                self._delete_and_audit(table, col, ids, batch_no, wo_dates, **kw)

        if step_ids:
            self._delete_and_audit(
                "workflowruntimerelatedactors",
                "RuntimeStepId",
                step_ids,
                batch_no,
                wo_dates,
            )

        self._delete_and_audit(
            "workflowruntimeactivities", "RuntimeItemId", item_ids, batch_no, wo_dates
        )
        if step_ids:
            self._delete_and_audit(
                "workflowruntimeactivities",
                "RuntimeStepId",
                step_ids,
                batch_no,
                wo_dates,
            )
        self._delete_and_audit(
            "workflowruntimereminderlogs",
            "RuntimeItemId",
            item_ids,
            batch_no,
            wo_dates,
        )

        ph = placeholders(len(item_ids))
        self._deleter.execute_sql(
            f"UPDATE workflowruntimeitems SET CurrentStepId = NULL WHERE Id IN ({ph})",
            item_ids,
        )
        if step_ids:
            self._delete_and_audit(
                "workflowruntimesteps", "Id", step_ids, batch_no, wo_dates, disable_fk=True
            )
        self._delete_and_audit(
            "workflowruntimestatus", "RuntimeItemId", item_ids, batch_no, wo_dates
        )
        items_deleted = self._delete_and_audit(
            "workflowruntimeitems", "Id", item_ids, batch_no, wo_dates, disable_fk=True
        )
        logger.info(f"workflowruntime 分支本批完成，删除 items={items_deleted}")

    def _purge_complete_branch(
        self,
        item_ids: list[str],
        batch_no: int,
        wo_dates: tuple[datetime.date | None, datetime.date | None],
    ) -> None:
        logger.info(f"清理 workflowcomplete 分支，本批 {len(item_ids)} 个 CompleteItem")
        step_ids = self._ids_by_in("workflowcompletesteps", "RuntimeItemId", item_ids)
        actor_ids = (
            self._ids_by_in("workflowcompleteactors", "RuntimeStepId", step_ids)
            if step_ids
            else []
        )
        comment_ids = (
            self._ids_by_in("workflowcompletecomments", "RuntimeActorId", actor_ids)
            if actor_ids
            else []
        )

        if comment_ids:
            att_ids = self._ids_by_in(
                "workflowcompleteattachments", "RuntimeCommentId", comment_ids
            )
            self._delete_and_audit(
                "workflowcompleteattachments", "Id", att_ids, batch_no, wo_dates
            )
            self._delete_and_audit(
                "workflowcompletecomments", "Id", comment_ids, batch_no, wo_dates
            )
        if actor_ids:
            self._delete_and_audit(
                "workflowcompleteactors", "Id", actor_ids, batch_no, wo_dates
            )
        if step_ids:
            self._delete_and_audit(
                "workflowcompleterelatedactors",
                "RuntimeStepId",
                step_ids,
                batch_no,
                wo_dates,
            )

        self._delete_and_audit(
            "workflowcompleteactivities", "RuntimeItemId", item_ids, batch_no, wo_dates
        )
        if step_ids:
            self._delete_and_audit(
                "workflowcompleteactivities",
                "RuntimeStepId",
                step_ids,
                batch_no,
                wo_dates,
            )

        ph = placeholders(len(item_ids))
        self._deleter.execute_sql(
            f"UPDATE workflowcompleteitems SET CurrentStepId = NULL WHERE Id IN ({ph})",
            item_ids,
        )
        if step_ids:
            self._delete_and_audit(
                "workflowcompletesteps", "Id", step_ids, batch_no, wo_dates, disable_fk=True
            )
        self._delete_and_audit(
            "workflowcompleteitems", "Id", item_ids, batch_no, wo_dates, disable_fk=True
        )
