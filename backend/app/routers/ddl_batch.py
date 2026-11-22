"""DDL 批次的校验 / 发布 / 补做 / 受理判定，以及解析转换和确认记录查询端点。

单独成文件是为了避免 /api/{module} 的动态路由抢先匹配 /api/ddl-batch/*，
同时不改变既有资源路由的注册顺序。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from app.schemas import ActionResult
from app.services import schema_registry, service_for
from app.store import store

router = APIRouter(prefix="/api", tags=["DDL批次"])


def _batch_service():
    service = service_for("ddl_batch")
    if service is None:  # pragma: no cover - 注册表缺项时才会发生
        raise HTTPException(status_code=500, detail="DDL批次服务未注册")
    return service


def _resolve_batch(service, batch_no: str | None, batch_id: int | None):
    if batch_no:
        batch = service.find_by_no(batch_no)
    elif batch_id is not None:
        batch = service.get_entry(batch_id)
    else:
        batch = None
    if batch is None:
        raise HTTPException(status_code=404, detail="DDL批次不存在")
    return batch


def _parse_positions(raw: str | None) -> list[int] | None:
    if not raw:
        return None
    values: list[int] = []
    for item in raw.replace("，", ",").split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError as exc:
            raise HTTPException(status_code=400,
                                detail=f"位点必须是整数：{item}") from exc
    return values or None


def _batch_payload(batch: dict[str, Any]) -> dict[str, Any]:
    return {
        "batch": batch,
        "acceptance": _batch_service().acceptance(batch),
        "confirmation": schema_registry.confirmation_for_batch(
            str(batch.get("批次编号", ""))),
        "column_versions": [
            row for row in store.aux_rows("column_version")
        ],
    }


@router.post("/ddl-batch/submit")
def submit_ddl_batch(payload: dict[str, Any]) -> dict[str, Any]:
    """一次提交带日志位点的 DDL 批次，返回批量校验结果（不自动发布）。"""
    service = _batch_service()
    batch, message = service.submit_batch(payload)
    if batch is None:
        raise HTTPException(status_code=400, detail=message)
    return {"ok": True, "message": message, **_batch_payload(batch)}


@router.post("/ddl-batch/validate")
def validate_ddl_batch(batch_no: str | None = None,
                       batch_id: int | None = None) -> dict[str, Any]:
    """对批次重新执行批量校验（不改已确认的发布状态）。"""
    service = _batch_service()
    batch = _resolve_batch(service, batch_no, batch_id)
    service.validate_batch(batch)
    return {"ok": True, **_batch_payload(batch)}


@router.post("/ddl-batch/publish")
def publish_ddl_batch(batch_no: str | None = None, batch_id: int | None = None,
                      positions: str | None = None) -> dict[str, Any]:
    """按位点顺序发布；同表前序未确认时拒绝跳级，故障位点中断后续发布。"""
    service = _batch_service()
    batch = _resolve_batch(service, batch_no, batch_id)
    result = service.publish_batch(batch, positions=_parse_positions(positions))
    return {
        "ok": True,
        "message": (
            f"发布中断于位点 {result['stopped_at']}" if result["stopped_at"] is not None
            else f"发布完成，进度 {batch.get('发布进度')}"
        ),
        "attempted": result["attempted"],
        "stopped_at": result["stopped_at"],
        **_batch_payload(batch),
    }


@router.post("/ddl-batch/redo")
def redo_ddl_batch(batch_no: str | None = None, batch_id: int | None = None,
                   positions: str | None = None) -> dict[str, Any]:
    """故障恢复后补做：重新校验失败位点，再按位点顺序继续发布。"""
    service = _batch_service()
    batch = _resolve_batch(service, batch_no, batch_id)
    result = service.redo_batch(batch, _parse_positions(positions))
    return {
        "ok": True,
        "message": result.get("message", ""),
        "attempted": result["attempted"],
        "redo_attempted": result.get("redo_attempted"),
        "stopped_at": result["stopped_at"],
        **_batch_payload(batch),
    }


@router.post("/ddl-batch/recover")
def recover_ddl_batch(batch_no: str | None = None, batch_id: int | None = None,
                      positions: str | None = None) -> dict[str, Any]:
    """恢复发布故障位点（只恢复故障标记，不自动发布）。"""
    service = _batch_service()
    batch = _resolve_batch(service, batch_no, batch_id)
    result = service.recover_batch(batch, _parse_positions(positions))
    return {
        "ok": True,
        "recovered": result["recovered"],
        "remaining_faults": result["remaining_faults"],
        **_batch_payload(batch),
    }


@router.get("/ddl-batch/acceptance")
def ddl_batch_acceptance(batch_no: str | None = None,
                         batch_id: int | None = None) -> dict[str, Any]:
    """只读受理判定：故障窗口里客户表能否独立受理、订单表是否仍被阻断。"""
    service = _batch_service()
    batch = _resolve_batch(service, batch_no, batch_id)
    return {"ok": True, **_batch_payload(batch)}


@router.get("/ddl-batch/column-versions")
def ddl_batch_column_versions(table: str | None = None,
                              batch_no: str | None = None) -> dict[str, Any]:
    """列版本账本，按 (表, 日志位点) 幂等记账。"""
    rows = list(store.aux_rows("column_version"))
    if table:
        rows = [row for row in rows if str(row.get("表", "")) == table]
    if batch_no:
        rows = [row for row in rows if str(row.get("批次号", "")) == batch_no]
    return {
        "ok": True,
        "items": rows,
        "total": len(rows),
        "versions_by_table": {
            name: schema_registry.current_column_version(name)
            for name in schema_registry.BASE_SCHEMAS
        },
    }


@router.get("/ddl-batch/{entry_id}/logs")
def ddl_batch_logs(entry_id: int) -> dict[str, Any]:
    """批次的变更日志（提交/校验/发布/补做全部留痕）。"""
    service = _batch_service()
    batch = service.get_entry(entry_id)
    if batch is None:
        raise HTTPException(status_code=404, detail="DDL批次不存在")
    return {"ok": True, "logs": batch.get("变更日志", []), "batch": batch}


@router.post("/ddl-batch/{entry_id}/actions", response_model=ActionResult)
def ddl_batch_action(entry_id: int, payload: dict[str, Any]) -> ActionResult:
    """批次动作入口：提交校验 / 发布 / 补做发布 / 恢复故障。"""
    service = _batch_service()
    entry, message = service.run_action(entry_id, str(payload.get("action", "")))
    return ActionResult(ok=entry is not None, message=message, entry=entry)


# ---- 解析转换入口（现有 binlog_parse -> event_trans 链路的批次驱动版） ----

@router.post("/events/parse-convert")
def parse_and_convert(payload: dict[str, Any]) -> dict[str, Any]:
    """按演进批次确认记录选映射，事件转换保存所用列版本和实际产出的数据行。"""
    from app.services.binlog_parse import BinlogParseService

    service = BinlogParseService()
    batch_no = str(payload.get("batch_no") or "")
    table = str(payload.get("table") or "")
    data = payload.get("data")
    if not batch_no or not table or not isinstance(data, dict):
        raise HTTPException(status_code=400,
                            detail="需要 batch_no、table 和 data（对象）")
    result = service.parse_and_convert(
        batch_no=batch_no,
        table=table,
        data=data,
        event_id=payload.get("event_id"),
        event_time=payload.get("event_time"),
        pipeline_id=payload.get("pipeline_id"),
    )
    return {"ok": True, "result": result}


@router.post("/events/reflow")
def reflow_pending(payload: dict[str, Any]) -> dict[str, Any]:
    """可续处理：按当前确认映射重新消费待发布记录（时间可回填，便于核对首次可消费时间）。"""
    from app.services.event_trans import EventTransService

    service = EventTransService()
    table = str(payload.get("table") or "")
    if not table:
        raise HTTPException(status_code=400, detail="需要 table")
    reflowed = service.reflow_pending(table, at=payload.get("at"))
    return {
        "ok": True,
        "table": table,
        "reflowed": reflowed,
        "total": len(reflowed),
        "mapping": schema_registry.consumable_mapping(table),
    }


@router.get("/events/results")
def event_results(table: str | None = None, event_id: str | None = None,
                  status: str | None = None) -> dict[str, Any]:
    """事件转换结果：含所用列版本和实际产出的数据行。"""
    from app.services.event_trans import RESULT

    rows = list(store.aux_rows(RESULT))
    if table:
        rows = [row for row in rows if str(row.get("表", "")) == table]
    if event_id:
        rows = [row for row in rows if str(row.get("事件键", "")) == event_id]
    if status:
        rows = [row for row in rows if str(row.get("status", "")) == status]
    return {"ok": True, "items": rows, "total": len(rows)}


@router.get("/events/pending")
def event_pending(table: str | None = None, event_id: str | None = None,
                  status: str | None = None) -> dict[str, Any]:
    """待发布事件记录：映射未确认时仍可查询、可续处理。"""
    from app.services.event_trans import PENDING

    rows = list(store.aux_rows(PENDING))
    if table:
        rows = [row for row in rows if str(row.get("表", "")) == table]
    if event_id:
        rows = [row for row in rows if str(row.get("事件键", "")) == event_id]
    if status:
        rows = [row for row in rows if str(row.get("status", "")) == status]
    return {"ok": True, "items": rows, "total": len(rows)}


# ---- 演进批次确认记录与可消费映射 ----

@router.get("/schema_evo/confirmations")
def schema_evo_confirmations() -> dict[str, Any]:
    """演进批次确认记录列表（发布确认生成的唯一凭据）。"""
    from app.services.schema_evo import SchemaEvoService

    service = SchemaEvoService()
    items = service.confirmations()
    return {"ok": True, "items": items, "total": len(items)}


@router.get("/schema_evo/confirmations/{batch_no}")
def schema_evo_confirmation(batch_no: str) -> dict[str, Any]:
    """按批次查确认记录、确认位点和可消费字段映射。"""
    from app.services.schema_evo import SchemaEvoService

    service = SchemaEvoService()
    return {"ok": True, "confirmation": service.confirmation_for_batch(batch_no)}


@router.get("/schema_evo/consumable-mapping")
def schema_evo_consumable_mapping(table: str | None = None) -> dict[str, Any]:
    """可消费字段映射：只来自演进批次确认记录。"""
    tables = [table] if table else list(schema_registry.BASE_SCHEMAS.keys())
    return {
        "ok": True,
        "mappings": {name: schema_registry.consumable_mapping(name) for name in tables},
    }
