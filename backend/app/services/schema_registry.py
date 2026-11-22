"""Schema 注册表：基线列、列版本账本和批次确认记录驱动的可消费字段映射。

这里把「批次确认前只能按旧映射解释、确认后按新映射解释」落在数据层：
- ``column_version``：每张表的列版本账本，发布成功一次追加一个版本。
- ``schema_evo`` 中 ``记录类型=演进批次确认`` 的演进记录：记录本批已确认的位点、
  列类型和可消费字段映射，日志解析只从这里取已确认的映射。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store


def now_text() -> str:
    """统一的时间文本，便于接口回显和审计比对。"""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _columns(raw: dict[str, str], version: str) -> dict[str, dict[str, Any]]:
    return {
        name: {"type": type_name, "version": version, "position": None}
        for name, type_name in raw.items()
    }


# 批次发生前的基线 Schema。历史事件只依赖这里的列解释，不随新版本迁移。
BASE_SCHEMAS: dict[str, dict[str, Any]] = {
    "order_events": {
        "data_stream": "order_events_cdc",
        "version": "v1",
        "columns": _columns(
            {
                "order_id": "BIGINT",
                "user_id": "BIGINT",
                "amount": "DECIMAL(12,2)",
                "status": "VARCHAR(16)",
                "event_time": "DATETIME",
            },
            "v1",
        ),
    },
    "customer_events": {
        "data_stream": "customer_events_cdc",
        "version": "v1",
        "columns": _columns(
            {
                "customer_id": "BIGINT",
                "customer_name": "VARCHAR(64)",
                "event_time": "DATETIME",
            },
            "v1",
        ),
    },
}


def known_table(table: str) -> bool:
    return table in BASE_SCHEMAS


def base_schema(table: str) -> dict[str, Any]:
    schema = BASE_SCHEMAS.get(table)
    if schema is None:
        return {"data_stream": table, "version": "v1", "columns": {}}
    return schema


def base_column_names(table: str) -> set[str]:
    return set(base_schema(table)["columns"])


def current_column_version(table: str) -> str:
    """读取列版本账本中该表最新的生效版本；没有账本时回落到基线版本。"""
    rows = [r for r in store.aux_rows("column_version") if str(r.get("表", "")) == table]
    if not rows:
        return str(base_schema(table)["version"])
    latest = max(rows, key=lambda row: int(row.get("id", 0)))
    return str(latest.get("列版本") or base_schema(table)["version"])


def next_column_version(table: str) -> str:
    current = current_column_version(table)
    number = 1
    if current.startswith("v") and current[1:].isdigit():
        number = int(current[1:]) + 1
    return f"v{number}"


def column_version_at(table: str, position: int) -> dict[str, Any] | None:
    """按 (表, 日志位点) 找已记账的列版本，用于幂等判断。"""
    for row in store.aux_rows("column_version"):
        if str(row.get("表", "")) == table and int(row.get("日志位点") or 0) == int(position):
            return row
    return None


def record_column_version(
    table: str,
    *,
    batch_no: str,
    ddl_type: str,
    column: str,
    position: int,
    change_no: str,
    at: str | None = None,
) -> str:
    """发布成功后追加一个列版本，并把同表旧版本标记为历史（不改列版本号本身）。"""
    rows = store.aux_rows("column_version")
    version = next_column_version(table)
    for row in rows:
        if str(row.get("表", "")) == table:
            row["当前"] = False
    new_row = {
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "表": table,
        "列版本": version,
        "变更类型": ddl_type,
        "变更列": column,
        "日志位点": position,
        "批次号": batch_no,
        "变更编号": change_no,
        "生效时间": at or now_text(),
        "当前": True,
        "status": "生效",
        "pending": False,
        "abnormal": False,
    }
    rows.append(new_row)
    return version


def confirmation_records() -> list[dict[str, Any]]:
    rows = [
        row for row in store.rows("schema_evo")
        if str(row.get("记录类型", "")) == "演进批次确认"
    ]
    return sorted(rows, key=lambda row: int(row.get("id", 0)))


def confirmation_for_batch(batch_no: str) -> dict[str, Any] | None:
    for row in confirmation_records():
        if str(row.get("批次号", "")) == batch_no:
            return row
    return None


def consumable_mapping(table: str) -> dict[str, Any]:
    """从演进批次确认记录汇总该表当前可消费的字段映射。

    确认记录外的列一律不可消费；确认记录里最新位点的同名列覆盖旧类型。
    """
    base = base_schema(table)
    mapping: dict[str, Any] = {
        "table": table,
        "data_stream": base["data_stream"],
        "version": current_column_version(table),
        "base_version": base["version"],
        "base_columns": sorted(base["columns"].keys()),
        "columns": {
            name: {"type": meta["type"], "version": meta.get("version", base["version"]),
                   "position": meta.get("position")}
            for name, meta in base["columns"].items()
        },
        "confirmation_sources": [],
    }
    for record in confirmation_records():
        table_mapping = (record.get("字段映射") or {}).get(table)
        if not table_mapping:
            continue
        for name, meta in (table_mapping.get("columns") or {}).items():
            known = mapping["columns"].get(name)
            if known is not None and int(meta.get("position") or 0) < int(
                    known.get("position") or -1):
                # 更早位点的确认记录不覆盖后来已生效的列版本。
                continue
            mapping["columns"][name] = {
                "type": meta.get("type", ""),
                "version": meta.get("version", mapping["version"]),
                "position": meta.get("position"),
                "ddl_type": meta.get("ddl_type"),
                "batch_no": meta.get("batch_no") or record.get("批次号"),
            }
        mapping["confirmation_sources"].append({
            "批次号": record.get("批次号"),
            "记录编号": record.get("记录编号"),
            "确认位点": (record.get("确认位点") or {}).get(table),
            "更新时间": record.get("更新时间") or record.get("演进时间"),
        })
    return mapping


def proposed_changes_for_column(table: str, column: str) -> list[dict[str, Any]]:
    """找出仍未被确认的 DDL 变更，用于解释某列为什么还不能消费。"""
    matched: list[dict[str, Any]] = []
    for batch in store.rows("ddl_batch"):
        for change in batch.get("changes", []):
            if str(change.get("表", "")) != table:
                continue
            if str(change.get("列", "")) != column:
                continue
            if str(change.get("状态", "")) == "已确认":
                continue
            matched.append({
                "批次号": batch.get("批次编号"),
                "位点": change.get("日志位点"),
                "状态": change.get("状态"),
                "DDL类型": change.get("DDL类型"),
            })
    matched.sort(key=lambda item: int(item.get("位点") or 0))
    return matched


def uses_old_mapping(table: str, data: dict[str, Any]) -> bool:
    """判断事件是否只带基线列：这类旧事件继续用旧映射和旧列版本解释。"""
    base = base_schema(table)
    return set(data.keys()).issubset(set(base["columns"].keys()))
