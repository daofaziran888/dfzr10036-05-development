"""DDL 批次、演进批次确认和事件转换的领域级测试。

直接用服务层跑，不依赖 HTTP 客户端，便于在没有 httpx 的环境里执行：

    cd backend && .venv/bin/python -m unittest discover -s tests
"""
from __future__ import annotations

import unittest

import app.seed as seed
import app.store as store_module
from app.services import schema_registry
from app.services.binlog_parse import BinlogParseService
from app.services.ddl_batch import DdlBatchService
from app.services.event_trans import EventTransService, PENDING, RESULT


def change(table: str, column: str, position: int, ddl_type: str, **extra):
    payload = {"表": table, "列": column, "日志位点": position, "DDL类型": ddl_type}
    payload.update(extra)
    return payload


class DdlBatchTest(unittest.TestCase):
    def setUp(self) -> None:
        store_module.store._tables = {
            name: [dict(row) for row in rows] for name, rows in seed.SEED_ROWS.items()
        }
        store_module.store._aux = {}
        self.batch_service = DdlBatchService()
        self.event_trans = EventTransService()
        self.parse = BinlogParseService()

    def submit(self, batch_no: str, changes, faults=None):
        payload = {"批次编号": batch_no, "源端": "PROD-MYSQL-01", "changes": changes}
        if faults:
            payload["发布故障位点"] = faults
        return self.batch_service.submit_batch(payload)

    def default_changes(self):
        return [
            change("order_events", "tier", 71, "ADD COLUMN", 变更后类型="VARCHAR(16)"),
            change("order_events", "tier", 74, "MODIFY COLUMN",
                   变更前类型="VARCHAR(16)", 变更后类型="VARCHAR(64)"),
            change("customer_events", "region", 79, "ADD COLUMN", 变更后类型="VARCHAR(32)"),
        ]

    def test_batch_submit_validates_without_publishing(self) -> None:
        batch, message = self.submit("B1", self.default_changes())
        self.assertIsNotNone(batch)
        self.assertEqual(batch["校验结果"], "3/3 通过")
        self.assertEqual(batch["发布进度"], "0/3")
        self.assertEqual(batch["status"], "已校验")
        self.assertTrue(message)

    def test_publish_stops_at_fault_and_customer_is_independent(self) -> None:
        batch, _ = self.submit("B1", self.default_changes(), faults=[74])
        result = self.batch_service.publish_batch(batch)
        self.assertEqual(result["stopped_at"], 74)
        states = {c["日志位点"]: c["状态"] for c in batch["changes"]}
        self.assertEqual(states[71], "已确认")
        self.assertEqual(states[74], "发布失败")
        self.assertEqual(states[79], "校验通过")
        self.assertEqual(batch["status"], "部分发布")

        acceptance = self.batch_service.acceptance(batch)
        by_pos = {item["位点"]: item for item in acceptance["items"]}
        self.assertEqual(by_pos[71]["受理"], "可受理")
        self.assertEqual(by_pos[74]["受理"], "不可受理")
        self.assertEqual(by_pos[79]["受理"], "可受理")
        self.assertTrue(acceptance["order_blocked"])
        self.assertTrue(acceptance["customer_accepted"])
        # 受理判定是只读的，不能推进发布
        self.assertEqual(batch["发布进度"], "1/3")

    def test_same_table_cannot_skip_levels(self) -> None:
        batch, _ = self.submit("B1", self.default_changes())
        result = self.batch_service.publish_batch(batch, positions=[74])
        self.assertEqual(batch["发布进度"], "0/3")
        self.assertIn("不允许跳级", result["attempted"][0]["结果"])
        mapping = schema_registry.consumable_mapping("order_events")
        self.assertNotIn("tier", {k for k in mapping["columns"] if k == "tier"
                                  and mapping["columns"][k].get("position") == 74})

    def test_redo_recovers_and_is_idempotent(self) -> None:
        changes = self.default_changes()
        batch, _ = self.submit("B1", changes, faults=[74])
        self.batch_service.publish_batch(batch)
        self.batch_service.recover_batch(batch)
        self.batch_service.redo_batch(batch)
        self.assertEqual(batch["发布进度"], "3/3")
        self.assertEqual(batch["发布顺序"], [
            "order_events#71->v2",
            "order_events#74->v3",
            "customer_events#79->v2",
        ])
        versions = len(store_module.store.aux_rows("column_version"))
        self.assertEqual(versions, 3)

        # 补做、重复提交都只记一次；首次可消费时间不变
        first_at = {c["日志位点"]: c["首次可消费时间"] for c in batch["changes"]}
        self.batch_service.redo_batch(batch)
        resubmitted, _ = self.submit("B1", changes)
        self.assertEqual(len(store_module.store.aux_rows("column_version")), 3)
        self.assertEqual(
            {c["日志位点"]: c["首次可消费时间"] for c in resubmitted["changes"]}, first_at)
        self.assertEqual(resubmitted["发布进度"], "3/3")

    def test_validation_failures_block_publish(self) -> None:
        batch, _ = self.submit("B1", [
            change("order_events", "ghost", 90, "MODIFY COLUMN", 变更后类型="VARCHAR(8)"),
            change("unknown_table", "c", 91, "ADD COLUMN", 变更后类型="VARCHAR(8)"),
        ])
        self.assertEqual(batch["status"], "校验未通过")
        result = self.batch_service.publish_batch(batch)
        self.assertEqual(batch["发布进度"], "0/2")
        self.assertIsNone(result["confirmation"])


class EventTransformTest(unittest.TestCase):
    def setUp(self) -> None:
        store_module.store._tables = {
            name: [dict(row) for row in rows] for name, rows in seed.SEED_ROWS.items()
        }
        store_module.store._aux = {}
        self.batch_service = DdlBatchService()
        self.parse = BinlogParseService()
        batch, _ = self.batch_service.submit_batch({
            "批次编号": "B9",
            "changes": [
                change("order_events", "tier", 71, "ADD COLUMN", 变更后类型="VARCHAR(16)"),
                change("order_events", "tier", 74, "MODIFY COLUMN",
                       变更前类型="VARCHAR(16)", 变更后类型="VARCHAR(64)"),
                change("customer_events", "region", 79, "ADD COLUMN",
                       变更后类型="VARCHAR(32)"),
            ],
            "发布故障位点": [74],
        })
        self.batch_service.publish_batch(batch)
        self.batch = batch

    def test_unconfirmed_region_stays_pending(self) -> None:
        result = self.parse.parse_and_convert(
            batch_no="B9", table="customer_events", event_id="cust-1",
            data={"customer_id": 1, "customer_name": "A", "region": "south"},
        )
        self.assertEqual(result["转换结果"], "pending_publish")
        self.assertEqual(result["实际产出行"], {})
        self.assertEqual(result["未消费列"], ["region"])
        self.assertEqual(store_module.store.aux_rows(PENDING)[0]["status"], "待发布")

    def test_old_event_uses_old_mapping(self) -> None:
        result = self.parse.parse_and_convert(
            batch_no="B9", table="customer_events", event_id="cust-2",
            data={"customer_id": 2, "customer_name": "B"},
        )
        self.assertEqual(result["转换结果"], "converted")
        self.assertEqual(result["所用列版本"], "v1")
        self.assertEqual(result["实际产出行"],
                         {"customer_id": 2, "customer_name": "B"})

    def test_pending_becomes_consumable_after_confirmation(self) -> None:
        self.parse.parse_and_convert(
            batch_no="B9", table="customer_events", event_id="cust-3",
            data={"customer_id": 3, "region": "north"},
        )
        self.batch_service.recover_batch(self.batch)
        self.batch_service.redo_batch(self.batch)
        record = store_module.store.aux_rows(PENDING)[0]
        self.assertEqual(record["status"], "已消费")
        self.assertTrue(record["首次可消费时间"])
        converted = next(row for row in store_module.store.aux_rows(RESULT)
                         if row["事件键"] == "cust-3")
        self.assertEqual(converted["转换结果"], "converted")
        self.assertEqual(converted["实际产出行"]["region"], "north")

    def test_existing_order_row_keeps_its_column_version(self) -> None:
        mid = self.parse.parse_and_convert(
            batch_no="B9", table="order_events", event_id="order-1",
            data={"order_id": 1, "tier": "silver"},
        )
        self.assertEqual(mid["所用列版本"], "v2")
        self.batch_service.recover_batch(self.batch)
        self.batch_service.redo_batch(self.batch)
        again = next(row for row in store_module.store.aux_rows(RESULT)
                     if row["事件键"] == "order-1")
        self.assertEqual(again["所用列版本"], "v2")
        self.assertEqual(again["实际产出行"], {"order_id": 1, "tier": "silver"})


if __name__ == "__main__":
    unittest.main()
