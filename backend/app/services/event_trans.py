"""事件转换服务：转换完成后触发维表关联和Dedup去重。"""
from __future__ import annotations

from typing import Any

from app.services import schema_registry
from app.services.base import BaseModuleService
from app.store import store

RESULT = "event_trans_result"
PENDING = "event_trans_pending"


class EventTransService(BaseModuleService):
    MODULE = "event_trans"
    ENTITY = "转换管道"
    OPERATION_TERM = "转换"
    LIST_FIELDS = ["管道编号", "源端", "事件类型", "转换函数", "输出格式", "失败策略", "转换顺序", "最近吞吐"]
    REQUIRED_FIELDS = ["管道编号", "源端", "事件类型"]
    STATUS_ORDER = ["草稿", "转换中", "转换过载", "已停止"]
    ACTION_RULES = {"启动转换": "转换中", "扩容处理": "转换过载", "停止管道": "已停止"}
    NEGATIVE_ACTIONS = []

    # ---------- 事件转换：保存列版本和实际产出行 ----------

    def convert_event(
        self,
        *,
        table: str,
        data: dict[str, Any],
        event_id: str | None = None,
        event_time: str | None = None,
        pipeline_id: int | None = None,
        at: str | None = None,
    ) -> dict[str, Any]:
        """按演进批次确认记录选取可消费字段映射，转换一条事件。

        - 事件带确认记录之外的列（如尚未发布的 region）：产出空行，登记待发布记录，
          不消费字段、不推进列版本。
        - 事件只带基线列：继续用旧映射和旧列版本解释。
        """
        at = at or schema_registry.now_text()
        mapping = schema_registry.consumable_mapping(table)
        base_columns = set(mapping["base_columns"])
        mapped_columns = set(mapping["columns"].keys())
        data_keys = {str(key) for key in data.keys()}

        declared_unmapped = sorted(data_keys - mapped_columns)
        pending_columns = sorted(
            column for column in declared_unmapped
            if schema_registry.proposed_changes_for_column(table, column)
        )
        old_mapping = schema_registry.uses_old_mapping(table, data)
        pipeline = self._pipeline_for(table, pipeline_id)
        applied_version = (
            str(mapping["base_version"]) if old_mapping else str(mapping["version"])
        )

        if pending_columns:
            row: dict[str, Any] = {}
            outcome = "pending_publish"
            note = (
                f"列 {','.join(pending_columns)} 尚无演进批次确认记录，"
                "字段映射不可消费，事件保持待发布"
            )
        else:
            unknown = sorted(data_keys - mapped_columns)
            row = {}
            for name, value in data.items():
                if name in mapped_columns:
                    row[name] = value
                else:
                    row[name] = value  # 未知列原样保留，但不受确认映射背书
            outcome = "converted"
            note = (
                "旧事件仅带基线列，沿用旧映射与旧列版本"
                if old_mapping
                else "按演进批次确认记录中的可消费字段映射完成转换"
            )
            if unknown:
                note += f"；未登记列原样保留：{','.join(unknown)}"

        event_key = event_id or f"{table}-{len(store.aux_rows(RESULT)) + 1}"
        reuse_pending = None
        if pending_columns:
            for existing in store.aux_rows(PENDING):
                if (str(existing.get("表")) == table
                        and str(existing.get("事件键")) == str(event_key)
                        and str(existing.get("状态")) == "待发布"):
                    reuse_pending = existing
                    break

        result_id = (reuse_pending or {}).get("结果ID") or self._next_id(RESULT)
        result = {
            "id": int(result_id),
            "结果编号": f"ETR-{int(result_id):04d}",
            "事件键": str(event_key),
            "表": table,
            "数据流": mapping.get("data_stream"),
            "事件时间": event_time or "",
            "管道": pipeline.get("管道编号"),
            "管道ID": pipeline.get("id"),
            "所用列版本": applied_version,
            "所用映射": "旧映射（基线 v1）" if old_mapping else "批次确认映射",
            "确认记录": (
                mapping["confirmation_sources"][-1]["记录编号"]
                if mapping.get("confirmation_sources") else None
            ),
            "可消费列": sorted(mapped_columns),
            "转换结果": outcome,
            "实际产出行": row,
            "未消费列": pending_columns,
            "可恢复": bool(pending_columns),
            "说明": note,
            "转换时间": at,
            "status": "待发布" if pending_columns else "已转换",
            "pending": bool(pending_columns),
            "abnormal": False,
        }
        rows = store.aux_rows(RESULT)
        existing_row = next(
            (item for item in rows if int(item.get("id", 0)) == result["id"]), None)
        if existing_row is None:
            rows.append(result)
        else:
            existing_row.clear()
            existing_row.update(result)

        if pending_columns:
            waiting_batch = ""
            for pending_column in pending_columns:
                proposals = schema_registry.proposed_changes_for_column(table, pending_column)
                if proposals:
                    waiting_batch = str(proposals[0].get("批次号") or "")
                    break
            pending_record = reuse_pending or {
                "id": self._next_id(PENDING),
                "待发布编号": f"ETP-{self._next_id(PENDING):04d}",
                "事件键": str(event_key),
                "表": table,
                "数据流": mapping.get("data_stream"),
                "待发布列": pending_columns,
                "等待确认批次": waiting_batch,
                "原始数据": dict(data),
                "结果ID": result["id"],
                "结果编号": result["结果编号"],
                "登记时间": at,
                "最近查询时间": at,
                "查询次数": 1,
                "首次可消费时间": "",
                "首次可消费版本": "",
                "status": "待发布",
                "pending": True,
                "abnormal": False,
            }
            if reuse_pending is None:
                store.aux_rows(PENDING).append(pending_record)
            else:
                pending_record["最近查询时间"] = at
                pending_record["查询次数"] = int(pending_record.get("查询次数", 0)) + 1
                pending_record["待发布列"] = pending_columns
                pending_record["等待确认批次"] = waiting_batch
            result["待发布记录"] = pending_record.get("待发布编号")
        return result

    def reflow_pending(self, table: str, at: str | None = None) -> list[dict[str, Any]]:
        """批次确认后重新消费待发布事件：只有映射真正可用了才转成已转换。"""
        at = at or schema_registry.now_text()
        mapping = schema_registry.consumable_mapping(table)
        mapped_columns = set(mapping["columns"].keys())
        reflowed: list[dict[str, Any]] = []
        for record in store.aux_rows(PENDING):
            if str(record.get("表")) != table or record.get("status") != "待发布":
                continue
            pending_columns = [str(item) for item in record.get("待发布列", [])]
            if any(column not in mapped_columns for column in pending_columns):
                continue
            rows = store.aux_rows(RESULT)
            result = next(
                (item for item in rows
                 if int(item.get("id", 0)) == int(record.get("结果ID", 0))), None)
            if result is not None:
                result["所用列版本"] = str(mapping["version"])
                result["所用映射"] = "批次确认映射"
                result["确认记录"] = (
                    mapping["confirmation_sources"][-1]["记录编号"]
                    if mapping.get("confirmation_sources") else None
                )
                result["可消费列"] = sorted(mapped_columns)
                result["转换结果"] = "converted"
                result["实际产出行"] = dict(record.get("原始数据") or {})
                result["未消费列"] = []
                result["可恢复"] = False
                result["说明"] = "批次确认后按可消费字段映射补做转换"
                result["转换时间"] = at
                result["status"] = "已转换"
                result["pending"] = False
            record["首次可消费时间"] = at
            record["首次可消费版本"] = str(mapping["version"])
            record["status"] = "已消费"
            record["pending"] = False
            record["消费结果ID"] = (result or {}).get("id")
            reflowed.append(record)
        return reflowed

    def _pipeline_for(self, table: str, pipeline_id: int | None) -> dict[str, Any]:
        if pipeline_id is not None and self.get_entry(pipeline_id):
            return self.get_entry(pipeline_id)  # type: ignore[return-value]
        data_stream = schema_registry.base_schema(table).get("data_stream", table)
        for row in store.rows(self.MODULE):
            if str(row.get("源端", "")) == "PROD-MYSQL-01":
                return row
        rows = store.rows(self.MODULE)
        return rows[0] if rows else {"管道编号": "", "id": None, "源端": data_stream}

    @staticmethod
    def _next_id(module: str) -> int:
        rows = store.rows(module) if module == EventTransService.MODULE \
            else store.aux_rows(module)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    # ---------- 既有编排 ----------

    def trigger_dim_join_and_dedup(self, entry_id: int) -> dict[str, Any]:
        """事件转换管道启动后，级联创建维表关联和去重窗口。

        数据流链路: event_trans -> dim_join + dedup_stream
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "转换中":
            return None

        tag = str(entry.get("源端", ""))
        results: dict[str, Any] = {"dim_join": None, "dedup": None}

        # 创建/恢复 dim_join 维表关联
        dim_rows = store.rows("dim_join")
        has_dim = [r for r in dim_rows if str(r.get("事实表", "")) == tag]
        if has_dim:
            has_dim[0]["status"] = "关联中"
            has_dim[0]["pending"] = True
            has_dim[0]["abnormal"] = False
            has_dim[0]["命中率"] = "87.3%"
            results["dim_join"] = has_dim[0]
        else:
            new_dim_id = max((int(r.get("id", 0)) for r in dim_rows), default=0) + 1
            dim_config = {
                "id": new_dim_id,
                "配置编号": f"DIM_-{new_dim_id:04d}",
                "事实表": tag,
                "维度表": "dim_customer_profile",
                "关联键": "customer_id",
                "关联类型": "LEFT JOIN",
                "缺失策略": "填充默认值",
                "缓存TTL": "600s",
                "命中率": "87.3%",
                "status": "关联中",
                "pending": True,
                "abnormal": False,
            }
            dim_rows.append(dim_config)
            results["dim_join"] = dim_config

        # 创建/恢复 dedup_stream 去重窗口
        dedup_rows = store.rows("dedup_stream")
        has_dedup = [r for r in dedup_rows if str(r.get("数据流", "")) == tag]
        if has_dedup:
            has_dedup[0]["status"] = "去重中"
            has_dedup[0]["pending"] = True
            has_dedup[0]["abnormal"] = False
            results["dedup"] = has_dedup[0]
        else:
            new_dedup_id = max((int(r.get("id", 0)) for r in dedup_rows), default=0) + 1
            dedup_config = {
                "id": new_dedup_id,
                "窗口编号": f"DEDU-{new_dedup_id:04d}",
                "数据流": tag,
                "去重键": "event_id+timestamp",
                "窗口宽度": "10min",
                "去重算法": "BloomFilter",
                "误杀率": "0.01%",
                "内存占用": "128MB",
                "最近命中": "1420次/h",
                "status": "去重中",
                "pending": True,
                "abnormal": False,
            }
            dedup_rows.append(dedup_config)
            results["dedup"] = dedup_config

        return results