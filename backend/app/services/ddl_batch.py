"""DDL批次服务：日志位点批次的一次提交、批量校验、按位点发布和演进批次确认。

业务约束（与需求一一对应）：
- 一个批次可以一次提交多条带日志位点的 DDL 变更，先做批量校验，再按位点顺序发布。
- 同一张表的发布不允许跳级：前序位点未确认时，后面的位点只登记「不可受理」，不改列版本。
- 部分发布失败时按位点中断，后面变更停留在「校验通过（待发布）」，但独立表仍可判定受理。
- 发布会生成「演进批次确认记录」（schema_evo 中记录类型=演进批次确认），日志解析只消费这里的映射。
- 已确认的变更按 (表, 日志位点) 幂等：补做、重复提交原批都只记一次列版本。
"""
from __future__ import annotations

from typing import Any

from app.services import schema_registry
from app.services.base import BaseModuleService
from app.services.event_trans import EventTransService
from app.store import store

ADD_COLUMN = "ADD COLUMN"
MODIFY_COLUMN = "MODIFY COLUMN"
DROP_COLUMN = "DROP COLUMN"

STATUS_VALIDATED = "校验通过"
STATUS_VALIDATION_FAILED = "校验失败"
STATUS_PENDING = "待校验"
STATUS_CONFIRMED = "已确认"
STATUS_PUBLISH_FAILED = "发布失败"
STATUS_PUBLISH_REJECTED = "发布拒绝"

BATCH_PENDING = "待校验"
BATCH_VALIDATED = "已校验"
BATCH_PARTIAL = "部分发布"
BATCH_CONFIRMED = "已确认"
BATCH_FAILED = "发布失败"
BATCH_VALIDATION_FAILED = "校验未通过"

_TABLE_KEYS = ("表", "业务表", "影响表", "table", "table_name")
_COLUMN_KEYS = ("列", "列名", "字段", "column", "column_name", "field")
_POSITION_KEYS = ("日志位点", "位点", "binlog位点", "position", "pos",
                  "binlog_position", "log_position")
_DDL_KEYS = ("DDL类型", "ddl_type", "ddl", "操作", "operation")
_TYPE_KEYS = ("列类型", "新列类型", "column_type", "data_type", "new_type")
_BEFORE_KEYS = ("变更前类型", "原类型", "before", "before_type", "old_type")
_AFTER_KEYS = ("变更后类型", "新类型", "after", "after_type", "new_type", "列类型", "type")
_BATCH_NO_KEYS = ("批次编号", "batch_no", "批次号", "batch")
_SOURCE_KEYS = ("源端", "source", "source_id")
_CHANGES_KEYS = ("changes", "变更列表", "ddl_changes", "变更明细", "items", "ddl_list")
_FAULT_KEYS = ("发布故障位点", "fail_on_publish", "inject_publish_failure",
               "fault_positions", "inject_failure")
_VALIDATION_FAULT_KEYS = ("校验故障位点", "fail_on_validate", "validation_fault_positions")

event_trans = EventTransService()


def _pick(raw: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if key in raw and raw[key] not in (None, ""):
            return raw[key]
    return None


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def normalize_ddl_type(raw: Any, before: str, after: str) -> str:
    text = str(raw or "").strip()
    upper = text.upper()
    if "DROP" in upper or "删除" in text:
        return DROP_COLUMN
    if "ADD" in upper or "新增" in text or "添加" in text:
        return ADD_COLUMN
    if "MODIFY" in upper or "CHANGE" in upper or "ALTER" in upper or "修改" in text:
        return MODIFY_COLUMN
    if not text:
        if after and before:
            return MODIFY_COLUMN
        if after:
            return ADD_COLUMN
    return text or ADD_COLUMN


def _normalize_type(value: Any) -> str:
    return str(value or "").strip()


def normalize_change(raw: dict[str, Any], seq: int) -> dict[str, Any] | None:
    """把外部提交的 DDL 变更整理成统一结构；缺表或缺列返回 None。"""
    if not isinstance(raw, dict):
        return None
    table = str(_pick(raw, _TABLE_KEYS) or "").strip()
    column = str(_pick(raw, _COLUMN_KEYS) or "").strip()
    position = _as_int(_pick(raw, _POSITION_KEYS))
    if position is None:
        # 兼容 position 写在 sql/remark 里的情况：不做猜测，直接视为不合法。
        return None
    if not table or not column:
        return None
    before = _normalize_type(_pick(raw, _BEFORE_KEYS))
    after = _normalize_type(_pick(raw, _AFTER_KEYS))
    ddl_type = normalize_ddl_type(_pick(raw, _DDL_KEYS), before, after)
    if ddl_type == ADD_COLUMN and not after:
        after = "VARCHAR(64)"
    return {
        "变更编号": str(_pick(raw, ("变更编号", "change_no")) or f"CHG-{seq:04d}"),
        "表": table,
        "列": column,
        "日志位点": position,
        "DDL类型": ddl_type,
        "变更前类型": before,
        "变更后类型": after,
        "列版本": str(raw.get("列版本") or ""),
        "兼容策略": str(_pick(raw, ("兼容策略", "compat")) or "向后兼容+默认值填充"),
        "独立性": str(raw.get("独立性") or ""),
        "状态": STATUS_PENDING,
        "校验": "待校验",
        "校验说明": "",
        "受理判定": "待校验",
        "受理说明": "",
        "失败原因": "",
        "首次可消费时间": "",
        "确认时间": "",
        "确认批次": "",
    }


class DdlBatchService(BaseModuleService):
    MODULE = "ddl_batch"
    ENTITY = "DDL批次"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["批次编号", "源端", "DDL条数", "变更位点", "校验结果",
                   "发布进度", "部分发布", "确认记录", "最近提交时间"]
    REQUIRED_FIELDS = ["批次编号", "源端"]
    STATUS_ORDER = [BATCH_PENDING, BATCH_VALIDATED, BATCH_PARTIAL,
                    BATCH_CONFIRMED, BATCH_FAILED, BATCH_VALIDATION_FAILED]
    ACTION_RULES = {}
    NEGATIVE_ACTIONS = []

    # ---------- 查询 ----------

    def find_by_no(self, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if str(row.get("批次编号", "")) == str(batch_no):
                return row
        return None

    def _next_batch_no(self) -> str:
        rows = store.rows(self.MODULE)
        number = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        return f"DDL-BATCH-{number:04d}"

    def _next_change_seq(self) -> int:
        number = 0
        for batch in store.rows(self.MODULE):
            number += len(batch.get("changes", []))
        return number + 1

    # ---------- 提交 ----------

    def submit_batch(self, payload: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """一次提交带日志位点的 DDL 批次，并立即做批量校验（不自动发布）。

        同批次编号重复提交时按原批合并：已确认的变更保持原列版本与首次可消费时间。
        """
        raw_changes = _pick(payload, _CHANGES_KEYS)
        if isinstance(raw_changes, dict):
            raw_changes = [raw_changes]
        if not isinstance(raw_changes, list) or not raw_changes:
            single = normalize_change(payload, self._next_change_seq())
            raw_changes = [single] if single else []
        if not raw_changes:
            return None, "批次未包含合法的 DDL 变更（需要 表、列、日志位点）"

        batch_no = str(_pick(payload, _BATCH_NO_KEYS) or "").strip()
        existing = self.find_by_no(batch_no) if batch_no else None
        resubmitted = existing is not None
        batch = existing or self._new_batch(batch_no or self._next_batch_no(), payload)

        incoming: list[dict[str, Any]] = []
        for raw in raw_changes:
            change = normalize_change(raw, self._next_change_seq() + len(incoming))
            if change is None:
                return None, "存在缺少 表/列/日志位点 的 DDL 变更，整批拒绝提交"
            incoming.append(change)

        self._merge_changes(batch, incoming, resubmitted)
        batch["提交次序号"] = int(batch.get("提交次序号", 1)) + (1 if resubmitted else 0)
        batch["最近提交时间"] = schema_registry.now_text()
        source = _pick(payload, _SOURCE_KEYS)
        if source:
            batch["源端"] = str(source)
        confirmed_positions = {
            int(c.get("日志位点") or 0) for c in batch.get("changes", [])
            if c.get("状态") == STATUS_CONFIRMED
        }
        faults = _pick(payload, _FAULT_KEYS)
        if faults:
            self._set_faults(batch, "发布故障位点", faults, exclude=confirmed_positions)
        validation_faults = _pick(payload, _VALIDATION_FAULT_KEYS)
        if validation_faults:
            self._set_faults(batch, "校验故障位点", validation_faults,
                             exclude=confirmed_positions)

        self.validate_batch(batch)
        self._append_log(
            batch,
            "提交校验",
            f"批次{batch['批次编号']}第{batch['提交次序号']}次提交，"
            f"{len(batch['changes'])}条变更完成批量校验",
        )
        if resubmitted:
            message = (f"批次{batch['批次编号']}为重复提交，已按 (表,位点) 合并，"
                       f"已确认的 {batch.get('已确认数', 0)} 条变更未重复记账")
        else:
            message = (f"DDL批次{batch['批次编号']}已提交，"
                       f"{len(batch['changes'])}条变更完成批量校验")
        return batch, message

    def _new_batch(self, batch_no: str, payload: dict[str, Any]) -> dict[str, Any]:
        rows = store.rows(self.MODULE)
        batch = {
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "批次编号": batch_no,
            "源端": str(_pick(payload, _SOURCE_KEYS) or "PROD-MYSQL-01"),
            "DDL条数": 0,
            "变更位点": "",
            "起始位点": None,
            "结束位点": None,
            "校验结果": "待校验",
            "发布进度": "0/0",
            "成功条数": 0,
            "失败条数": 0,
            "已确认数": 0,
            "待发布数": 0,
            "部分发布": "否",
            "确认记录": "",
            "确认位点": {},
            "发布顺序": [],
            "发布故障位点": [],
            "校验故障位点": [],
            "提交次序号": 1,
            "最近提交时间": schema_registry.now_text(),
            "changes": [],
            "变更日志": [],
            "status": BATCH_PENDING,
            "pending": True,
            "abnormal": False,
        }
        rows.append(batch)
        return batch

    def _merge_changes(self, batch: dict[str, Any], incoming: list[dict[str, Any]],
                       resubmitted: bool) -> None:
        changes: list[dict[str, Any]] = batch.setdefault("changes", [])
        for raw in incoming:
            target = self._match_change(changes, raw)
            if target is None:
                raw["变更编号"] = raw["变更编号"] or f"CHG-{self._next_change_seq():04d}"
                changes.append(raw)
                continue
            # 已确认/已失败的变更保留状态、列版本和首次可消费时间，只补齐描述信息。
            for field in ("DDL类型", "变更前类型", "变更后类型", "兼容策略"):
                if raw.get(field) and not target.get(field):
                    target[field] = raw[field]
            if target.get("状态") == STATUS_PENDING and raw.get("变更后类型"):
                target["变更后类型"] = raw["变更后类型"]
            if resubmitted:
                target.setdefault("重复提交次数", 0)
                target["重复提交次数"] = int(target["重复提交次数"]) + 1
        changes.sort(key=lambda item: int(item.get("日志位点") or 0))
        self._refresh_batch_fields(batch)

    @staticmethod
    def _match_change(changes: list[dict[str, Any]],
                      raw: dict[str, Any]) -> dict[str, Any] | None:
        for change in changes:
            if (str(change.get("表")) == raw["表"]
                    and int(change.get("日志位点") or 0) == raw["日志位点"]):
                return change
        # 位置换了但仍是同一条 DDL：视为重复提交，不再生成新变更。
        for change in changes:
            if (str(change.get("表")) == raw["表"]
                    and str(change.get("列")) == raw["列"]
                    and str(change.get("DDL类型")) == raw["DDL类型"]
                    and str(change.get("变更后类型") or "") == str(raw.get("变更后类型") or "")):
                change.setdefault("关联位点", []).append(raw["日志位点"])
                return change
        return None

    def _refresh_batch_fields(self, batch: dict[str, Any]) -> None:
        changes = batch.get("changes", [])
        positions = sorted(int(change.get("日志位点") or 0) for change in changes)
        batch["DDL条数"] = len(changes)
        batch["变更位点"] = ",".join(str(item) for item in positions)
        batch["起始位点"] = positions[0] if positions else None
        batch["结束位点"] = positions[-1] if positions else None
        confirmed = [c for c in changes if c.get("状态") == STATUS_CONFIRMED]
        failed = [c for c in changes if c.get("状态") == STATUS_PUBLISH_FAILED]
        pending = [c for c in changes if c.get("状态") == STATUS_VALIDATED]
        rejected = [c for c in changes if c.get("状态") == STATUS_PUBLISH_REJECTED]
        batch["已确认数"] = len(confirmed)
        batch["成功条数"] = len(confirmed)
        batch["失败条数"] = len(failed) + len(rejected)
        batch["待发布数"] = len(pending)
        batch["发布进度"] = f"{len(confirmed)}/{len(changes)}"
        batch["确认位点"] = {
            str(change.get("表")): int(change.get("日志位点") or 0) for change in confirmed
        }
        batch["部分发布"] = "是" if confirmed and (len(confirmed) < len(changes)) else "否"

    # ---------- 批量校验 ----------

    def validate_batch(self, batch: dict[str, Any]) -> dict[str, Any]:
        for change in batch.get("changes", []):
            if change.get("状态") == STATUS_CONFIRMED:
                change["校验"] = "通过"
                change["受理判定"] = "已受理"
                continue
            if change.get("状态") in (STATUS_PUBLISH_FAILED, STATUS_PUBLISH_REJECTED):
                # 发布失败/拒绝的变更不因重复提交改判校验结论，只能走补做。
                continue
            self._validate_change(batch, change)
        validated = [c for c in batch.get("changes", []) if c.get("状态") == STATUS_VALIDATED]
        failed = [c for c in batch.get("changes", [])
                  if c.get("状态") == STATUS_VALIDATION_FAILED]
        unchanged = [c for c in batch.get("changes", [])
                     if c.get("状态") == STATUS_PUBLISH_FAILED]
        total = len(batch.get("changes", []))
        batch["校验结果"] = f"{total - len(failed) - len(unchanged)}/{total} 通过"
        confirmed = [c for c in batch.get("changes", [])
                     if c.get("状态") == STATUS_CONFIRMED]
        if confirmed and not validated and not failed:
            # 整批都是幂等命中：直接算已确认，不新列发布计划。
            batch["status"] = BATCH_CONFIRMED
            batch["pending"] = False
            batch["abnormal"] = False
        elif failed:
            batch["status"] = BATCH_VALIDATION_FAILED
            batch["abnormal"] = True
        elif not batch.get("确认记录"):
            batch["status"] = BATCH_VALIDATED if validated else batch.get("status", BATCH_PENDING)
        elif batch.get("已确认数"):
            # 已有确认记录但仍有权变更待处理：属于部分发布，不能退回「校验未通过」。
            batch["status"] = BATCH_PARTIAL
            batch["abnormal"] = False
        self._refresh_batch_fields(batch)
        return batch

    def _validate_change(self, batch: dict[str, Any], change: dict[str, Any]) -> None:
        table = str(change.get("表"))
        column = str(change.get("列"))
        position = int(change.get("日志位点") or 0)
        ddl_type = str(change.get("DDL类型"))
        errors: list[str] = []

        # (表,位点) 已在列版本账本里：这是重复提交，直接按已确认处理，不再重复校验。
        existing_version = schema_registry.column_version_at(table, position)
        if existing_version is not None:
            change["状态"] = STATUS_CONFIRMED
            change["校验"] = "通过"
            change["校验说明"] = "该(表,位点)已确认，按幂等跳过重复校验"
            change["列版本"] = str(existing_version.get("列版本"))
            change["首次可消费时间"] = str(existing_version.get("生效时间") or "")
            change["确认时间"] = str(existing_version.get("生效时间") or "")
            change["确认批次"] = str(existing_version.get("批次号") or "")
            change["受理判定"] = "已受理"
            change["受理说明"] = f"列版本 {change['列版本']} 已生效（幂等）"
            change["重复确认"] = True
            self._mark_independence(batch, change)
            return

        if not schema_registry.known_table(table):
            errors.append(f"业务表 {table} 未登记基线Schema")
        if position <= 0:
            errors.append("日志位点必须为正整数")

        if position in batch.get("校验故障位点", []):
            errors.append(f"位点 {position} 校验环境故障，兼容性无法判定")

        if schema_registry.known_table(table):
            base_columns = schema_registry.base_column_names(table)
            available = set(base_columns)
            available |= set(schema_registry.consumable_mapping(table)["columns"].keys())
            # 同批更早位点里新增的列，对后续 MODIFY 可见。
            for earlier in batch.get("changes", []):
                if earlier is change:
                    continue
                if str(earlier.get("表")) != table:
                    continue
                if int(earlier.get("日志位点") or 0) >= position:
                    continue
                if str(earlier.get("DDL类型")) == ADD_COLUMN:
                    available.add(str(earlier.get("列")))

            if ddl_type == ADD_COLUMN:
                if column in available:
                    errors.append(f"列 {column} 已存在，ADD COLUMN 不兼容")
                if not str(change.get("变更后类型") or "").strip():
                    errors.append("ADD COLUMN 需要给出列类型")
            elif ddl_type == MODIFY_COLUMN:
                if column not in available:
                    errors.append(f"列 {column} 不存在，无法修改类型（同表发布不允许跳级）")
                if not str(change.get("变更后类型") or "").strip():
                    errors.append("MODIFY COLUMN 需要给出变更后类型")
                earlier_same_column = [
                    c for c in batch.get("changes", [])
                    if c is not change
                    and str(c.get("表")) == table
                    and str(c.get("列")) == column
                    and int(c.get("日志位点") or 0) < position
                    and str(c.get("DDL类型")) == ADD_COLUMN
                ]
                if earlier_same_column and str(
                        earlier_same_column[-1].get("变更后类型") or "") == str(
                            change.get("变更后类型") or ""):
                    errors.append("修改前后类型一致，无需发布")
            elif ddl_type == DROP_COLUMN:
                if column not in available:
                    errors.append(f"列 {column} 不存在，无法删除")
            else:
                errors.append(f"未知 DDL 类型：{ddl_type}")

        self._mark_independence(batch, change)
        if errors:
            change["校验"] = "失败"
            change["校验说明"] = "；".join(errors)
            change["状态"] = STATUS_VALIDATION_FAILED
            change["受理判定"] = "不可受理"
            change["受理说明"] = "校验未通过：" + "；".join(errors)
        else:
            change["校验"] = "通过"
            change["校验说明"] = "独立通过批量校验" if change["独立性"].startswith("独立") \
                else "批量校验通过（依赖同批前序位点）"
            change["状态"] = STATUS_VALIDATED
            change["受理判定"] = "可受理，待发布"
            change["受理说明"] = change["校验说明"]

    def _mark_independence(self, batch: dict[str, Any], change: dict[str, Any]) -> None:
        table = str(change.get("表"))
        position = int(change.get("日志位点") or 0)
        earlier = [
            c for c in batch.get("changes", [])
            if c is not change
            and str(c.get("表")) == table
            and int(c.get("日志位点") or 0) < position
            and c.get("状态") != STATUS_CONFIRMED
        ]
        if earlier:
            change["独立性"] = f"依赖同表前序位点 {earlier[-1].get('日志位点')}"
        else:
            change["独立性"] = "独立表变更，不受其他表位点故障影响"

    # ---------- 发布 ----------

    def publish_batch(self, batch: dict[str, Any], *,
                      positions: list[int] | None = None,
                      include_failed: bool = False) -> dict[str, Any]:
        """按位点顺序发布；同一张表前序未确认时拒绝跳级，故障位点中断后续发布。"""
        at = schema_registry.now_text()
        attempted: list[dict[str, Any]] = []
        confirmed_now: list[dict[str, Any]] = []
        stopped_at: int | None = None
        faults = set(batch.get("发布故障位点", []))
        wanted = set(positions) if positions else None

        for change in sorted(batch.get("changes", []),
                             key=lambda item: int(item.get("日志位点") or 0)):
            position = int(change.get("日志位点") or 0)
            if wanted is not None and position not in wanted:
                continue
            if change.get("状态") == STATUS_CONFIRMED:
                attempted.append({"位点": position, "表": change.get("表"),
                                  "结果": "已确认，跳过（幂等，不重复记账）"})
                continue
            if change.get("状态") == STATUS_VALIDATION_FAILED:
                attempted.append({"位点": position, "表": change.get("表"),
                                  "结果": "校验未通过，不发布"})
                continue
            if change.get("状态") == STATUS_PUBLISH_FAILED and not include_failed and \
                    position not in faults:
                attempted.append({"位点": position, "表": change.get("表"),
                                  "结果": "历史发布失败，需补做"})
                continue

            blockers = self._same_table_blockers(change)
            if blockers:
                change["状态"] = STATUS_PUBLISH_REJECTED
                change["受理判定"] = "不可受理"
                change["受理说明"] = (
                    f"同一张表不允许跳级：位点 {blockers[0]['位点']} 尚未确认"
                    f"（{blockers[0]['批次']}）"
                )
                change["失败原因"] = change["受理说明"]
                attempted.append({"位点": position, "表": change.get("表"),
                                  "结果": change["受理说明"]})
                continue

            if position in faults:
                change["状态"] = STATUS_PUBLISH_FAILED
                change["受理判定"] = "不可受理"
                change["受理说明"] = f"发布中断：位点 {position} 目标表发布故障"
                change["失败原因"] = f"位点 {position} 发布故障（目标表写入中断）"
                attempted.append({"位点": position, "表": change.get("表"),
                                  "结果": change["失败原因"]})
                stopped_at = position
                break

            version = self._confirm_change(batch, change, at)
            if version:
                confirmed_now.append(change)
                attempted.append({"位点": position, "表": change.get("表"),
                                  "结果": f"发布成功，列版本 {change.get('列版本')}"})

        record = self._upsert_confirmation(batch, at)
        if confirmed_now:
            for change in confirmed_now:
                event_trans.reflow_pending(str(change.get("表")), at)
                self._append_log(
                    batch, "发布",
                    f"{change.get('表')}@{change.get('日志位点')} 发布成功 -> "
                    f"列版本 {change.get('列版本')}",
                )
                batch.setdefault("发布顺序", []).append(
                    f"{change.get('表')}#{change.get('日志位点')}->{change.get('列版本')}"
                )
        if stopped_at is not None:
            self._append_log(batch, "发布中断",
                             f"位点 {stopped_at} 发布失败，后续变更未开始发布")
        if attempted:
            self._append_log(
                batch, "发布结果",
                "；".join(f"{item['位点']}:{item['结果']}" for item in attempted),
            )

        self._refresh_batch_fields(batch)
        all_confirmed = all(c.get("状态") == STATUS_CONFIRMED for c in batch.get("changes", []))
        any_failed = any(c.get("状态") == STATUS_PUBLISH_FAILED for c in batch.get("changes", []))
        if all_confirmed and batch.get("changes"):
            batch["status"] = BATCH_CONFIRMED
            batch["pending"] = False
            batch["abnormal"] = False
        elif any_failed:
            batch["status"] = BATCH_PARTIAL if batch.get("已确认数") else BATCH_FAILED
            batch["pending"] = True
            batch["abnormal"] = True
        elif batch.get("确认记录"):
            batch["status"] = BATCH_PARTIAL
            batch["pending"] = True
            batch["abnormal"] = False
        else:
            batch["pending"] = True
            batch["abnormal"] = False

        return {
            "batch": batch,
            "attempted": attempted,
            "confirmed_now": confirmed_now,
            "confirmation": record,
            "stopped_at": stopped_at,
        }

    def _same_table_blockers(self, change: dict[str, Any]) -> list[dict[str, Any]]:
        """扫描所有批次，找出同表更小位点尚未确认的变更，阻止跳级发布。"""
        table = str(change.get("表"))
        position = int(change.get("日志位点") or 0)
        blockers: list[dict[str, Any]] = []
        for batch in store.rows(self.MODULE):
            for other in batch.get("changes", []):
                if other is change:
                    continue
                if str(other.get("表")) != table:
                    continue
                if int(other.get("日志位点") or 0) >= position:
                    continue
                if other.get("状态") == STATUS_CONFIRMED:
                    continue
                blockers.append({"批次": batch.get("批次编号"),
                                 "位点": int(other.get("日志位点") or 0),
                                 "状态": other.get("状态")})
        blockers.sort(key=lambda item: item["位点"])
        return blockers

    def _confirm_change(self, batch: dict[str, Any], change: dict[str, Any],
                        at: str) -> str:
        """确认一条变更：幂等写列版本账本，记录首次可消费时间。"""
        table = str(change.get("表"))
        position = int(change.get("日志位点") or 0)
        existing = schema_registry.column_version_at(table, position)
        if existing is not None:
            # 同一 (表,位点) 已在别的批次确认过：只做关联，不重复记列版本。
            version = str(existing.get("列版本"))
            first_at = str(existing.get("生效时间") or at)
            change["重复确认"] = True
        else:
            version = schema_registry.record_column_version(
                table,
                batch_no=str(batch.get("批次编号")),
                ddl_type=str(change.get("DDL类型")),
                column=str(change.get("列")),
                position=position,
                change_no=str(change.get("变更编号")),
                at=at,
            )
            first_at = at
            change["重复确认"] = False
        change["列版本"] = version
        change["状态"] = STATUS_CONFIRMED
        change["校验"] = "通过"
        change["受理判定"] = "已受理"
        change["受理说明"] = f"列版本 {version} 已生效"
        change["失败原因"] = ""
        if position in batch.get("发布故障位点", []):
            batch["发布故障位点"] = [
                item for item in batch.get("发布故障位点", []) if item != position]
        change["首次可消费时间"] = first_at
        change["确认时间"] = at
        change["确认批次"] = str(batch.get("批次编号"))
        return version

    def _upsert_confirmation(self, batch: dict[str, Any],
                             at: str) -> dict[str, Any] | None:
        """生成/更新演进批次确认记录：只收录已确认变更，按位点稳定排序。"""
        confirmed = sorted(
            [c for c in batch.get("changes", []) if c.get("状态") == STATUS_CONFIRMED],
            key=lambda item: int(item.get("日志位点") or 0),
        )
        if not confirmed:
            return None
        rows = store.rows("schema_evo")
        record = schema_registry.confirmation_for_batch(str(batch.get("批次编号")))
        mapping: dict[str, Any] = {}
        for change in confirmed:
            table = str(change.get("表"))
            table_mapping = mapping.setdefault(
                table, {"columns": {}, "确认位点": {}, "首个位点": change.get("日志位点")}
            )
            table_mapping["columns"][str(change.get("列"))] = {
                "type": str(change.get("变更后类型") or ""),
                "version": str(change.get("列版本") or ""),
                "position": int(change.get("日志位点") or 0),
                "ddl_type": str(change.get("DDL类型")),
                "batch_no": str(change.get("确认批次") or batch.get("批次编号")),
                "first_consumable_at": str(change.get("首次可消费时间") or at),
            }
            table_mapping["确认位点"][str(change.get("变更编号"))] = int(
                change.get("日志位点") or 0)

        if record is None:
            new_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
            record = {
                "id": new_id,
                "记录编号": f"SCHE-{new_id:04d}",
                "记录类型": "演进批次确认",
                "数据流": str(batch.get("源端", "")),
                "变更列": "",
                "列变更前": "",
                "列变更后": "",
                "兼容模式": "向前兼容",
                "下游影响": "",
                "演进时间": at,
                "更新时间": at,
                "status": "兼容通过",
                "pending": False,
                "abnormal": False,
            }
            rows.append(record)
        else:
            # 合并旧映射，但绝不回退已确认列的版本与首次可消费时间。
            old_mapping = record.get("字段映射") or {}
            for table, table_mapping in old_mapping.items():
                target = mapping.setdefault(table, {"columns": {}, "确认位点": {},
                                                     "首个位点": None})
                for name, meta in (table_mapping.get("columns") or {}).items():
                    target["columns"].setdefault(name, meta)
                target["确认位点"].update(table_mapping.get("确认位点") or {})
            record["更新时间"] = at

        record["批次号"] = str(batch.get("批次编号"))
        # 每张表只记最后确认到的位点：同表位点必须按序推进，前面的都已确认。
        record["确认位点"] = {
            table: max(meta["确认位点"].values()) if meta["确认位点"] else None
            for table, meta in mapping.items()
        }
        record["字段映射"] = mapping
        record["已确认变更数"] = sum(
            len(meta.get("确认位点") or {}) for meta in mapping.values())
        record["待发布变更数"] = sum(
            1 for c in batch.get("changes", []) if c.get("状态") != STATUS_CONFIRMED)
        record["批次总数"] = int(record.get("批次总数", 0)) + (
            0 if record.get("关联批次数") else 1)
        record["关联批次数"] = len({
            meta.get("batch_no") for table_mapping in mapping.values()
            for meta in (table_mapping.get("columns") or {}).values()
            if meta.get("batch_no")
        })
        record["状态说明"] = "本批已确认变更的字段映射，日志解析只从这里取可消费列"
        record["变更列"] = "；".join(
            f"{c.get('表')}.{c.get('列')}@{c.get('日志位点')}->{c.get('列版本')}"
            for c in confirmed
        )
        record["列变更前"] = "；".join(
            f"{c.get('表')}.{c.get('列')}:{c.get('变更前类型') or '无'}" for c in confirmed)
        record["列变更后"] = "；".join(
            f"{c.get('表')}.{c.get('列')}:{c.get('变更后类型')}" for c in confirmed)
        record["下游影响"] = "；".join(
            sorted({f"{c.get('表')}及下游消费" for c in confirmed}))
        batch["确认记录"] = str(record.get("记录编号"))
        batch["确认记录ID"] = int(record.get("id", 0))
        return record

    # ---------- 补做 / 故障恢复 ----------

    def redo_batch(self, batch: dict[str, Any],
                   positions: list[int] | None = None) -> dict[str, Any]:
        """补做：重新校验失败/待发布变更，然后按位点顺序继续发布。

        故障位点未恢复时补做只会重新确认失败状态，不会推进列版本。
        """
        at = schema_registry.now_text()
        faults = set(batch.get("发布故障位点", []))
        wanted = set(positions) if positions else None
        retried: list[dict[str, Any]] = []
        for change in sorted(batch.get("changes", []),
                             key=lambda item: int(item.get("日志位点") or 0)):
            position = int(change.get("日志位点") or 0)
            if wanted is not None and position not in wanted:
                continue
            if change.get("状态") == STATUS_CONFIRMED:
                continue
            if change.get("状态") == STATUS_PUBLISH_FAILED:
                if position in faults:
                    retried.append({"位点": position, "结果": "故障未恢复，补做仍失败"})
                    change["失败原因"] = f"位点 {position} 发布故障未恢复，补做失败"
                    change["受理说明"] = change["失败原因"]
                    continue
                # 故障已恢复：重新校验后回到可发布状态。
                self._validate_change(batch, change)
                if change.get("状态") == STATUS_VALIDATION_FAILED:
                    retried.append({"位点": position, "结果": "补做校验未通过"})
                    continue
                retried.append({"位点": position, "结果": "故障已恢复，重新校验通过"})
            elif change.get("状态") == STATUS_PUBLISH_REJECTED:
                self._validate_change(batch, change)
                retried.append({"位点": position, "结果": "重新校验后待发布"})
        self._append_log(
            batch, "补做",
            "；".join(f"{item['位点']}:{item['结果']}" for item in retried) or "无待补做变更",
        )
        result = self.publish_batch(batch, positions=positions, include_failed=True)
        result["message"] = (
            f"补做中断于位点 {result['stopped_at']}" if result["stopped_at"] is not None
            else f"补做完成，发布进度 {batch.get('发布进度')}"
        )
        result["redo_attempted"] = retried
        result["redo_at"] = at
        return result

    def recover_batch(self, batch: dict[str, Any],
                      positions: list[int] | None = None) -> dict[str, Any]:
        """恢复发布故障：清空故障位点，但不会自动发布，需再执行补做。"""
        before = list(batch.get("发布故障位点", []))
        if positions:
            keep = [item for item in before if item not in set(positions)]
            batch["发布故障位点"] = keep
            cleared = [item for item in before if item in set(positions)]
        else:
            batch["发布故障位点"] = []
            cleared = before
        for change in batch.get("changes", []):
            if int(change.get("日志位点") or 0) in cleared:
                change["失败原因"] = ""
                if change.get("状态") == STATUS_PUBLISH_FAILED:
                    change["受理说明"] = "故障已恢复，等待补做发布"
        self._append_log(batch, "恢复故障",
                         f"已恢复位点 {','.join(str(item) for item in cleared) or '无'}")
        self._refresh_batch_fields(batch)
        return {
            "batch": batch,
            "recovered": cleared,
            "remaining_faults": list(batch.get("发布故障位点", [])),
        }

    def _set_faults(self, batch: dict[str, Any], field: str, raw: Any,
                    exclude: set[int] | None = None) -> None:
        if isinstance(raw, (int, str)):
            values = [item for item in str(raw).replace("，", ",").split(",") if item.strip()]
        elif isinstance(raw, list):
            values = raw
        else:
            values = []
        positions = []
        for item in values:
            position = _as_int(item)
            if position and position not in (exclude or set()):
                positions.append(position)
        batch[field] = sorted(set(positions))

    # ---------- 受理判定 ----------

    def acceptance(self, batch: dict[str, Any]) -> dict[str, Any]:
        """不改任何状态，只判定每条变更此刻是否受理（供故障窗口查询）。"""
        faults = set(batch.get("发布故障位点", []))
        items: list[dict[str, Any]] = []
        for change in sorted(batch.get("changes", []),
                             key=lambda item: int(item.get("日志位点") or 0)):
            position = int(change.get("日志位点") or 0)
            table = str(change.get("表"))
            status = str(change.get("状态"))
            blockers = self._same_table_blockers(change)
            if status == STATUS_CONFIRMED:
                accepted, reason = True, f"已受理，列版本 {change.get('列版本')}"
            elif status == STATUS_VALIDATION_FAILED:
                accepted, reason = False, change.get("校验说明") or "校验未通过"
            elif position in faults:
                accepted, reason = False, f"发布故障未恢复（位点 {position}）"
            elif status == STATUS_PUBLISH_FAILED:
                accepted, reason = False, change.get("失败原因") or "发布失败待补做"
            elif blockers:
                accepted, reason = False, \
                    f"同一张表不允许跳级：位点 {blockers[0]['位点']} 尚未确认"
            elif status == STATUS_VALIDATED:
                accepted, reason = True, "校验通过，可受理待发布"
            else:
                accepted, reason = False, "尚未校验"
            items.append({
                "位点": position,
                "表": table,
                "列": change.get("列"),
                "DDL类型": change.get("DDL类型"),
                "状态": status,
                "受理": "可受理" if accepted else "不可受理",
                "原因": reason,
                "独立性": change.get("独立性"),
                "同表前序": blockers[0] if blockers else None,
            })

        order_items = [item for item in items if item["表"] == "order_events"]
        customer_items = [item for item in items if item["表"] == "customer_events"]
        order_blocked = any(item["受理"] == "不可受理" for item in order_items)
        customer_accepted = all(item["受理"] == "可受理" for item in customer_items) \
            if customer_items else False
        return {
            "batch_no": batch.get("批次编号"),
            "batch_status": batch.get("status"),
            "items": items,
            "order_blocked": order_blocked,
            "customer_accepted": customer_accepted,
            "结论": (
                "订单表存在故障/未确认位点，但客户表位点相互独立，可独立判定受理"
                if order_blocked and customer_accepted
                else "按位点逐条判定"
            ),
        }

    # ---------- 动作分派 ----------

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        batch = self.get_entry(entry_id)
        if batch is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        name = action.strip()
        if name in ("发布", "批量发布", "补做发布", "补做", "补做失败变更", "继续发布") \
                and not batch.get("changes"):
            return None, f"批次{batch.get('批次编号')}没有DDL变更，无法发布"
        if name in ("提交校验", "批量校验", "重新校验"):
            self.validate_batch(batch)
            self._append_log(batch, "提交校验", "重新执行批量校验")
            return batch, f"批次{batch.get('批次编号')}已重新校验：{batch.get('校验结果')}"
        if name in ("发布", "批量发布"):
            result = self.publish_batch(batch)
            if result["stopped_at"] is not None:
                return batch, (f"批次{batch.get('批次编号')}发布中断于位点"
                               f"{result['stopped_at']}，已确认{batch.get('已确认数')}条")
            return batch, f"批次{batch.get('批次编号')}发布完成，进度{batch.get('发布进度')}"
        if name in ("补做发布", "补做", "补做失败变更", "继续发布"):
            result = self.redo_batch(batch)
            return batch, (f"批次{batch.get('批次编号')}补做完成，"
                           f"发布进度{batch.get('发布进度')}")
        if name in ("恢复故障", "恢复发布", "解除故障"):
            result = self.recover_batch(batch)
            return batch, (f"批次{batch.get('批次编号')}已恢复位点"
                           f"{','.join(str(item) for item in result['recovered']) or '无'}，"
                           f"可继续补做")
        return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

    def _append_log(self, batch: dict[str, Any], action: str, detail: str) -> None:
        log = batch.setdefault("变更日志", [])
        log.append({
            "序号": len(log) + 1,
            "时间": schema_registry.now_text(),
            "动作": action,
            "说明": detail,
        })
