"""Schema演进服务：演进记录 + 演进批次确认记录（DDL批次发布确认的唯一产物）。"""
from __future__ import annotations

from typing import Any

from app.services import schema_registry
from app.services.base import BaseModuleService
from app.store import store


class SchemaEvoService(BaseModuleService):
    MODULE = "schema_evo"
    ENTITY = "演进记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["记录编号", "数据流", "变更列", "列变更前", "列变更后", "兼容模式", "下游影响", "演进时间"]
    REQUIRED_FIELDS = ["记录编号", "数据流", "变更列"]
    STATUS_ORDER = ["已捕获", "兼容通过", "存在风险", "已阻断"]
    ACTION_RULES = {"提交校验": "兼容通过", "标记风险": "存在风险", "阻断演进": "已阻断"}
    NEGATIVE_ACTIONS = []

    def confirmations(self) -> list[dict[str, Any]]:
        """演进批次确认记录：批次发布后生成的确认凭据。"""
        return schema_registry.confirmation_records()

    def confirmation_for_batch(self, batch_no: str) -> dict[str, Any]:
        record = schema_registry.confirmation_for_batch(batch_no)
        if record is None:
            return {
                "batch_no": batch_no,
                "found": False,
                "columns": {},
                "message": "该批次尚无演进批次确认记录，解析只能使用旧映射",
            }
        mapping = record.get("字段映射") or {}
        columns: dict[str, Any] = {}
        for table, table_mapping in mapping.items():
            for name, meta in (table_mapping.get("columns") or {}).items():
                columns[f"{table}.{name}"] = meta
        return {
            "batch_no": batch_no,
            "found": True,
            "record_no": record.get("记录编号"),
            "record_id": record.get("id"),
            "confirmed_positions": record.get("确认位点"),
            "confirmed_changes": record.get("已确认变更数"),
            "pending_changes": record.get("待发布变更数"),
            "columns": columns,
            "mapping": {table: schema_registry.consumable_mapping(table)
                        for table in mapping},
            "updated_at": record.get("更新时间"),
        }
