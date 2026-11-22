"""Binlog解析服务：启动解析后会生产事件并推动下游event_trans转换管道。"""
from __future__ import annotations

from typing import Any

from app.services import schema_registry
from app.services.base import BaseModuleService
from app.services.event_trans import EventTransService
from app.store import store

event_trans = EventTransService()


class BinlogParseService(BaseModuleService):
    MODULE = "binlog_parse"
    ENTITY = "解析规则"
    OPERATION_TERM = "解析"
    LIST_FIELDS = ["规则编号", "源端", "日志格式", "事件过滤", "字段映射", "DDL处理", "大事务拆分", "解析速率"]
    REQUIRED_FIELDS = ["规则编号", "源端", "日志格式"]
    STATUS_ORDER = ["草稿", "解析中", "速率下降", "解析中断"]
    ACTION_RULES = {"启动解析": "解析中", "调整速率": "速率下降", "标记中断": "解析中断"}
    NEGATIVE_ACTIONS = []

    # ---------- 批次确认记录驱动的解析 / 转换入口 ----------

    def mapping_from_confirmation(self, batch_no: str,
                                  table: str | None = None) -> dict[str, Any]:
        """从演进批次确认记录选取可消费字段映射；无确认记录时回落到基线映射。"""
        record = schema_registry.confirmation_for_batch(batch_no)
        tables = [table] if table else list(schema_registry.BASE_SCHEMAS.keys())
        mappings: dict[str, Any] = {}
        for name in tables:
            if record is None:
                mappings[name] = schema_registry.base_schema(name)["columns"]
                continue
            table_mapping = (record.get("字段映射") or {}).get(name)
            if table_mapping:
                # 确认记录里只收录已确认列；基线列仍按旧版本解释。
                merged = {
                    column: {"type": meta["type"], "version": meta.get("version"),
                             "position": meta.get("position")}
                    for column, meta in schema_registry.base_schema(name)["columns"].items()
                }
                merged.update(table_mapping.get("columns") or {})
                mappings[name] = merged
            else:
                mappings[name] = schema_registry.base_schema(name)["columns"]
        return {
            "batch_no": batch_no,
            "confirmation": record,
            "tables": mappings,
            "source": "演进批次确认记录" if record else "基线Schema（本批尚无确认记录）",
            "constraint": "只用确认记录中已生效的列版本；未确认列不参与转换",
        }

    def parse_and_convert(
        self,
        *,
        batch_no: str,
        table: str,
        data: dict[str, Any],
        event_id: str | None = None,
        event_time: str | None = None,
        pipeline_id: int | None = None,
        at: str | None = None,
    ) -> dict[str, Any]:
        """解析入口：按确认记录选映射，再送入事件转换保存列版本与实际产出行。"""
        mapping = self.mapping_from_confirmation(batch_no, table)
        record = mapping["confirmation"]
        rule = self._rule_for(table)
        result = event_trans.convert_event(
            table=table,
            data=data,
            event_id=event_id,
            event_time=event_time,
            pipeline_id=pipeline_id,
            at=at,
        )
        result["解析批次"] = batch_no
        result["解析规则"] = (rule or {}).get("规则编号")
        result["映射来源"] = mapping["source"]
        result["确认记录"] = (record or {}).get("记录编号")
        result["确认位点"] = ((record or {}).get("确认位点") or {}).get(table)
        return result

    def _rule_for(self, table: str) -> dict[str, Any] | None:
        stream = schema_registry.base_schema(table).get("data_stream")
        rows = store.rows(self.MODULE)
        matched = [r for r in rows if stream and stream.startswith(str(r.get("源端", "")))]
        return (matched or rows or [None])[0]

    # ---------- 既有编排 ----------

    def trigger_downstream_event_trans(self, entry_id: int) -> dict[str, Any] | None:
        """binlog解析变为「解析中」后，级联创建 event_trans 转换管道。

        数据流链路: binlog_parse -> event_trans
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "解析中":
            return None

        trans_rows = store.rows("event_trans")
        existing = [r for r in trans_rows if str(r.get("源端", "")) == str(entry.get("源端", ""))]
        if existing:
            existing[0]["status"] = "转换中"
            existing[0]["pending"] = True
            existing[0]["abnormal"] = False
            existing[0]["最近吞吐"] = "12.5k eps"
            return existing[0]

        new_id = max((int(r.get("id", 0)) for r in trans_rows), default=0) + 1
        downstream = {
            "id": new_id,
            "管道编号": f"EVEN-{new_id:04d}",
            "源端": str(entry.get("源端", "")),
            "事件类型": str(entry.get("事件过滤", "INSERT,UPDATE,DELETE")),
            "转换函数": "flatten_nested_json, cast_timestamp, normalize_pk",
            "输出格式": "Avro",
            "失败策略": "写入死信队列",
            "转换顺序": "先扁平后类型转换",
            "最近吞吐": "12.5k eps",
            "status": "转换中",
            "pending": True,
            "abnormal": False,
        }
        trans_rows.append(downstream)
        return downstream

    def propagate_rate_adjustment(self) -> dict[str, Any]:
        """解析速率下降时，将上游速率事件传播到下游。

        数据流链路: binlog_parse(速率下降) -> event_trans(过载) -> lag_monitor(延迟上升)
        """
        rows = store.rows(self.MODULE)
        slow_rows = [r for r in rows if r.get("status") == "速率下降"]
        result = {"affected_downstream": 0}

        for slow in slow_rows:
            tag = str(slow.get("源端", ""))
            # 传播到事件转换
            for tr in store.rows("event_trans"):
                if str(tr.get("源端", "")) == tag and tr.get("status") == "转换中":
                    tr["status"] = "转换过载"
                    tr["pending"] = True
                    tr["abnormal"] = True
                    result["affected_downstream"] += 1
            # 传播到延迟监控
            for lm in store.rows("lag_monitor"):
                if str(lm.get("数据流", "")) == tag and lm.get("status") == "正常":
                    lm["status"] = "延迟偏高"
                    lm["pending"] = True
                    lm["abnormal"] = True
                    result["affected_downstream"] += 1

        return result