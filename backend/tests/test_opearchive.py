"""算子执行数据存档回归测试。

只依赖标准库 unittest，直接驱动 ArchiveService，覆盖：
- 批次边界生效（旧批次兼容、新批次生效后才供消费侧读取）；
- 写入项断点续传（失败项重试、已写入项不重写、未完成位置保留）；
- 同一算子并发调优只允许一个进行中批次、同批次幂等合并；
- 执行快照与调优记录必须读取同一批次；
- 冲突检测与修正只收敛一个终态；
- 空态与异常边界。

运行：cd backend && PYTHONPATH=. .venv/bin/python -m unittest tests.test_opearchive -v
"""
from __future__ import annotations

import unittest
from copy import deepcopy

from app.seed import SEED_ROWS
from app.services.opearchive import (
    ArchiveConflictError,
    ArchiveService,
    EFFECTIVE_NO,
    EFFECTIVE_OLD,
    EFFECTIVE_YES,
    ITEM_DONE,
    ITEM_FAILED,
    ITEM_TODO,
    STATUS_DONE,
    STATUS_FAILED,
    STATUS_TODO,
    STATUS_VERIFIED,
    STATUS_VOID,
    STATUS_WRITING,
)
from app.store import store

OPERATOR_NO = "OPER-0002"
RECORD_NO = "PARA-0002"


class OpearchiveTestCase(unittest.TestCase):
    def setUp(self) -> None:
        # 深拷贝：存档的写入项/同步记录是嵌套结构，浅拷贝会让就地修改污染种子。
        for module, rows in SEED_ROWS.items():
            table = store.rows(module)
            table.clear()
            table.extend(deepcopy(rows))
        self.service = ArchiveService()

    # ------------------------------------------------------------ 验收样例

    def test_acceptance_parallelism_2_to_6_by_batch_boundary(self) -> None:
        # batch-17 是当前生效快照，口径并行度 2，node-b 仍可读旧快照。
        snap17 = self.service.read_snapshot(OPERATOR_NO, "batch-17")
        self.assertEqual(snap17["批次号"], "batch-17")
        self.assertEqual(snap17["生效并行度"], 2)
        self.assertEqual(self.service.read_snapshot(OPERATOR_NO)["批次号"], "batch-17")

        # batch-18 已校验但未完成/未生效：未完成时同步先被状态守卫拦下。
        snap18 = self.service.read_snapshot(OPERATOR_NO, "batch-18")
        self.assertEqual(snap18["生效并行度"], 6)
        archive18, message = self.service.sync_consumer(
            {"存档编号": "OPEA-0002", "消费节点": "node-b"}
        )
        self.assertIsNone(archive18)
        self.assertIn("尚未校验完成", message)

        # 补写中断的写入项并收口；已完成但未生效时，同步被生效守卫拦下。
        archive, _, _ = self.service.write_items(2)
        self.assertEqual(archive["status"], STATUS_WRITING)
        archive, _ = self.service.complete_archive(2)
        self.assertEqual(archive["status"], STATUS_DONE)
        archive18, message = self.service.sync_consumer(
            {"存档编号": "OPEA-0002", "消费节点": "node-b"}
        )
        self.assertIsNone(archive18)
        self.assertIn("还未生效", message)

        archive, _ = self.service.activate_archive(2)
        self.assertEqual(archive["生效状态"], EFFECTIVE_YES)

        # 旧批次保留完整快照、仅切换为已替换；node-b 缺省快照切到新批次。
        old = self.service.get_archive(1)
        self.assertEqual(old["status"], STATUS_DONE)
        self.assertEqual(old["生效状态"], EFFECTIVE_OLD)
        self.assertEqual(old["批次号"], "batch-17")
        self.assertEqual(old["生效并行度"], 2)
        self.assertEqual(self.service.read_snapshot(OPERATOR_NO)["批次号"], "batch-18")
        self.assertEqual(
            next(o for o in store.rows("operator") if o["算子编号"] == OPERATOR_NO)["并行度"], 6
        )

        # 生效后跨系统同步成功，消费侧保留来源标识；重复同步幂等。
        archive, _ = self.service.sync_consumer({"存档编号": "OPEA-0002", "消费节点": "node-b"})
        self.assertEqual(archive["同步记录"][-1]["来源标识"], "OPEA-0002/batch-18")
        before = len(archive["同步记录"])
        self.service.sync_consumer({"存档编号": "OPEA-0002", "消费节点": "node-b"})
        self.assertEqual(len(archive["同步记录"]), before)

    # ------------------------------------------------------------ 断点续传

    def test_failed_items_retry_only_unfinished_and_keep_position(self) -> None:
        arch = self.service.get_archive(2)
        self.assertEqual(
            [(i["序号"], i["状态"]) for i in arch["写入项"]],
            [(1, ITEM_DONE), (2, ITEM_FAILED), (3, ITEM_TODO)],
        )
        self.assertEqual(arch["未完成位置"], 2)

        # 未完成时禁止收口。
        self.assertIsNone(self.service.complete_archive(2)[0])

        arch, _, _ = self.service.write_items(2, fail_positions=[2])
        self.assertEqual(arch["status"], STATUS_FAILED)
        self.assertEqual(arch["写入项"][1]["重试次数"], 2)
        self.assertEqual(arch["写入项"][0]["状态"], ITEM_DONE)  # 已写入项不重写
        self.assertEqual(arch["未完成位置"], 2)

        arch, _, _ = self.service.write_items(2)
        self.assertTrue(all(i["状态"] == ITEM_DONE for i in arch["写入项"]))
        self.assertIsNone(arch["未完成位置"])
        self.assertEqual(arch["写入项"][0]["重试次数"], 0)

        arch, _ = self.service.complete_archive(2)
        self.assertEqual(arch["status"], STATUS_DONE)
        # 终态收口幂等，只产生一个终态。
        again, _ = self.service.complete_archive(2)
        self.assertEqual(again["status"], STATUS_DONE)

    # ------------------------------------------------------------ 并发与口径

    def test_single_active_batch_per_operator_and_idempotent_create(self) -> None:
        # batch-18 仍处于进行中（种子状态为失败），新批次被并发守卫拦下。
        with self.assertRaises(ArchiveConflictError):
            self.service.create_archive(
                {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO, "批次号": "batch-19"}
            )
        # 完成后允许下一批次。
        self.service.write_items(2)
        self.service.complete_archive(2)
        archive, _, reused = self.service.create_archive(
            {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO,
             "批次号": "batch-19", "注册新批次": True}
        )
        self.assertFalse(reused)
        self.assertEqual(archive["status"], STATUS_TODO)
        # 同算子同批次重复提交幂等合并，不产生第二份存档。
        again, _, reused = self.service.create_archive(
            {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO,
             "批次号": "batch-19", "注册新批次": True}
        )
        self.assertTrue(reused)
        self.assertEqual(again["id"], archive["id"])
        # 进行中再建另一批次仍被拒。
        with self.assertRaises(ArchiveConflictError):
            self.service.create_archive(
                {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO,
                 "批次号": "batch-20", "注册新批次": True}
            )

    def test_unregistered_batch_and_third_parallelism_are_rejected(self) -> None:
        self.service.write_items(2)
        self.service.complete_archive(2)
        with self.assertRaises(ArchiveConflictError):
            self.service.create_archive(
                {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO, "批次号": "batch-404"}
            )
        with self.assertRaises(ArchiveConflictError):
            self.service.create_archive(
                {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO,
                 "批次号": "batch-21", "注册新批次": True, "生效并行度": 9}
            )
        # 调优记录与算子归属不一致直接拒绝。
        result, _, _ = self.service.create_archive(
            {"算子编号": OPERATOR_NO, "调优记录编号": "PARA-0001", "批次号": "batch-17"}
        )
        self.assertIsNone(result)

    def test_unverified_batch_cannot_be_written_or_activated(self) -> None:
        self.service.write_items(2)
        self.service.complete_archive(2)
        archive, _, _ = self.service.create_archive(
            {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO,
             "批次号": "batch-19", "注册新批次": True}
        )
        archive_id = archive["id"]
        self.assertIsNone(self.service.write_items(archive_id)[0])
        self.assertIsNone(self.service.activate_archive(archive_id)[0])
        archive, _ = self.service.verify_archive(archive_id)
        self.assertEqual(archive["批次版本校验"], "已校验")
        self.assertEqual(archive["生效并行度"], 6)

    # ------------------------------------------------------------ 快照同批次

    def test_snapshot_requires_same_batch_as_tuning_record(self) -> None:
        tuning = next(t for t in store.rows("parallel") if t["记录编号"] == RECORD_NO)
        tuning["关联批次"] = ["batch-99"]
        tuning["快照批次"] = "batch-99"
        with self.assertRaises(ArchiveConflictError):
            self.service.read_snapshot(OPERATOR_NO, "batch-17")

    def test_snapshot_empty_state_and_void_archive(self) -> None:
        with self.assertRaises(ArchiveConflictError):
            self.service.read_snapshot(OPERATOR_NO, "batch-404")
        with self.assertRaises(ArchiveConflictError):
            self.service.read_snapshot("OPER-9999")
        self.assertEqual(self.service.list_archives(operator_no="OPER-EMPTY")[1], 0)
        # 作废后的存档不能再作为快照读取。
        self.service.write_items(2)
        self.service.complete_archive(2)
        archive, _, _ = self.service.create_archive(
            {"算子编号": OPERATOR_NO, "调优记录编号": RECORD_NO,
             "批次号": "batch-19", "注册新批次": True}
        )
        self.service.void_archive(archive["id"])
        with self.assertRaises(ArchiveConflictError):
            self.service.read_snapshot(OPERATOR_NO, "batch-19")

    # ------------------------------------------------------------ 冲突修正

    def test_repair_mixed_parallelism_in_job_record(self) -> None:
        job = next(j for j in store.rows("jobops") if j["实例编号"] == "JOBO-0002")
        job["记录并行度"] = 6  # batch-17 口径为 2，制造混用
        self.assertTrue(
            any(c["类型"] == "运行记录并行度混用" for c in self.service.list_conflicts())
        )
        archive, _, fixed = self.service.repair_conflicts(1)
        self.assertIn("JOBO-0002", fixed["运行记录修正"])
        self.assertEqual(job["记录并行度"], 2)
        self.assertEqual(job["来源标识"], "OPEA-0001/batch-17")
        self.assertEqual(self.service.list_conflicts(), [])
        self.assertEqual(archive["status"], STATUS_DONE)  # 兼容已有版本，单一终态

    def test_repair_duplicate_archive_converges_to_single_terminal(self) -> None:
        rows = store.rows("opearchive")
        duplicate = dict(rows[0])
        duplicate.update({"id": 99, "存档编号": "OPEA-0099",
                          "status": STATUS_VERIFIED, "生效状态": EFFECTIVE_YES})
        rows.append(duplicate)
        self.assertTrue(
            any(c["类型"] == "批次存档重复" for c in self.service.list_conflicts())
        )
        _, _, fixed = self.service.repair_conflicts(1)
        self.assertIn("OPEA-0099", fixed["作废重复存档"])
        self.assertEqual(rows[-1]["status"], STATUS_VOID)
        self.assertEqual(rows[-1]["生效状态"], EFFECTIVE_NO)
        self.assertEqual(sum(1 for r in rows if r["生效状态"] == EFFECTIVE_YES), 1)
        self.assertEqual(self.service.list_conflicts(), [])

    def test_repair_stream_mapping_keeps_batch_parallelism(self) -> None:
        operator = next(o for o in store.rows("operator") if o["算子编号"] == OPERATOR_NO)
        operator["输入流"] = "当前输入流-修正后"
        self.assertTrue(
            any(c["类型"] == "流映射漂移" for c in self.service.list_conflicts())
        )
        for archive_id in (1, 2):
            self.service.repair_conflicts(archive_id)
        self.assertEqual(self.service.list_conflicts(), [])
        arch = self.service.get_archive(1)
        self.assertEqual(arch["输入流"], "当前输入流-修正后")
        self.assertEqual(arch["批次号"], "batch-17")
        self.assertEqual(arch["生效并行度"], 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
