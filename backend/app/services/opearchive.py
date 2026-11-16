"""算子执行数据存档业务规则。

围绕流处理算子并行度调优后的批次边界管理，所有状态流转都收口在这里：

- 新旧并行度按批次边界生效，同一运行记录只允许引用一份「已校验」的批次版本，
  不允许把两种并行度混在同一条运行记录里；
- 执行数据按写入项逐项落档，失败项可重试、中断后保留未完成位置，
  重试只能补写未完成项，已写入项不重写；
- 执行快照与调优记录必须读取同一批次，批次版本不一致按数据版本冲突处理；
- 跨系统链路同步引用同一份算子执行数据存档，消费侧保留来源标识；
- 同一算子同一时刻只允许一个进行中的批次版本，任何修复操作只收敛出一个终态。
"""
from __future__ import annotations

import threading
from datetime import date
from typing import Any

from app.store import store

MODULE = "opearchive"
OPERATOR_MODULE = "operator"
PARALLEL_MODULE = "parallel"
JOBOPS_MODULE = "jobops"

# 存档状态机：待校验 → 已校验 → 写入中 → 已完成（唯一成功终态）；
# 写入失败回到「失败」可继续重试；「已作废」是冲突合并后的唯一废弃终态。
STATUS_TODO = "待校验"
STATUS_VERIFIED = "已校验"
STATUS_WRITING = "写入中"
STATUS_DONE = "已完成"
STATUS_FAILED = "失败"
STATUS_VOID = "已作废"
TERMINAL_STATUSES = (STATUS_DONE, STATUS_VOID)
WRITABLE_STATUSES = (STATUS_VERIFIED, STATUS_WRITING, STATUS_FAILED)

EFFECTIVE_NO = "未生效"
EFFECTIVE_YES = "生效中"
EFFECTIVE_OLD = "已替换"

ITEM_TODO = "待写入"
ITEM_DONE = "已写入"
ITEM_FAILED = "失败"

REQUIRED_FIELDS = ["算子编号", "调优记录编号", "批次号"]
DEFAULT_ITEM_COUNT = 3
ARCHIVE_PREFIX = "OPEA"


class ArchiveConflictError(Exception):
    """业务冲突：批次版本、并发调优或数据版本不满足约束时抛出。"""


class ArchiveService:
    def __init__(self) -> None:
        # 同步端点跑在线程池里，串行化创建/生效等关键流转，避免同一算子并发写出两个批次。
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ 读取

    def list_archives(
        self,
        *,
        operator_no: str | None = None,
        batch_no: str | None = None,
        status: str | None = None,
        effective: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if operator_no:
            rows = [row for row in rows if row.get("算子编号") == operator_no]
        if batch_no:
            rows = [row for row in rows if row.get("批次号") == batch_no]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        if effective:
            rows = [row for row in rows if row.get("生效状态") == effective]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_archive(self, archive_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, archive_id)

    def _find_archive_by_no(self, archive_no: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if row.get("存档编号") == archive_no:
                return row
        return None

    def _find_operator(self, operator_no: str) -> dict[str, Any] | None:
        for row in store.rows(OPERATOR_MODULE):
            if row.get("算子编号") == operator_no:
                return row
        return None

    def _find_tuning(self, record_no: str) -> dict[str, Any] | None:
        for row in store.rows(PARALLEL_MODULE):
            if row.get("记录编号") == record_no:
                return row
        return None

    def _find_archive_by_batch(self, operator_no: str, batch_no: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if row.get("算子编号") == operator_no and row.get("批次号") == batch_no:
                return row
        return None

    def _effective_archive(self, operator_no: str) -> dict[str, Any] | None:
        for row in store.rows(MODULE):
            if row.get("算子编号") == operator_no and row.get("生效状态") == EFFECTIVE_YES:
                return row
        return None

    @staticmethod
    def _tuning_batches(tuning: dict[str, Any]) -> set[str]:
        batches = set(tuning.get("关联批次") or [])
        for key in ("快照批次", "生效批次"):
            value = tuning.get(key)
            if value:
                batches.add(str(value))
        return batches

    @staticmethod
    def _as_int(value: Any) -> int | None:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return None

    # ------------------------------------------------------------------ 创建

    def create_archive(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str, bool]:
        """登记一份批次存档。

        返回 (存档, 说明, 是否复用已有存档)：同一算子+批次重复提交时幂等合并，
        不产生第二份存档；同一算子存在其他进行中批次时拒绝并发调优。
        """
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, f"缺少必填字段：{'、'.join(missing)}", False

        operator_no = str(values["算子编号"]).strip()
        record_no = str(values["调优记录编号"]).strip()
        batch_no = str(values["批次号"]).strip()

        with self._lock:
            operator = self._find_operator(operator_no)
            if operator is None:
                return None, f"流处理算子 {operator_no} 不存在，无法建立执行数据存档", False
            tuning = self._find_tuning(record_no)
            if tuning is None:
                return None, f"并行度调优记录 {record_no} 不存在，批次版本无法校验", False
            if tuning.get("流算子名") != operator_no:
                return None, (
                    f"调优记录 {record_no} 归属算子 {tuning.get('流算子名')}，"
                    f"与 {operator_no} 不一致，不能混用批次版本"
                ), False

            # 幂等：同算子同批次已经存档（含已作废以外的任何状态）时直接复用。
            existing = self._find_archive_by_batch(operator_no, batch_no)
            if existing is not None and existing.get("status") != STATUS_VOID:
                return existing, f"批次 {batch_no} 的存档已存在，重复提交已合并到同一份存档", True

            # 并发约束：同一算子只允许一个进行中的批次版本。
            active = [
                row for row in store.rows(MODULE)
                if row.get("算子编号") == operator_no and row.get("status") not in TERMINAL_STATUSES
            ]
            if active:
                raise ArchiveConflictError(
                    f"算子 {operator_no} 已有进行中的批次 {active[0].get('批次号')}，"
                    "同一算子并发调优只允许一个批次版本生效"
                )

            registered = self._tuning_batches(tuning)
            register_new = bool(values.get("注册新批次"))
            if registered and batch_no not in registered and not register_new:
                raise ArchiveConflictError(
                    f"批次 {batch_no} 未登记在调优记录 {record_no} 的批次清单"
                    f"（{sorted(registered)}）中，拒绝写入未校验批次"
                )

            suggested = self._as_int(tuning.get("建议并行度"))
            current = self._as_int(tuning.get("当前并行度"))
            allowable = {value for value in (current, suggested) if value is not None}
            effective = self._as_int(values.get("生效并行度"))
            if effective is None:
                effective = suggested if (register_new or batch_no == tuning.get("生效批次")) else current
            if effective is None:
                return None, "调优记录缺少可解析的并行度数值，无法确定生效并行度", False
            # 一份运行记录只允许出现当前并行度或建议并行度，任何第三种口径都按冲突拦下。
            if allowable and effective not in allowable:
                raise ArchiveConflictError(
                    f"批次 {batch_no} 生效并行度 {effective} 不在调优记录允许口径"
                    f"（当前 {current} / 建议 {suggested}）内，拒绝混入第三种并行度"
                )
            if batch_no == tuning.get("生效批次") and suggested is not None and effective != suggested:
                raise ArchiveConflictError(
                    f"生效批次 {batch_no} 必须采用建议并行度 {suggested}，提交值 {effective} 冲突"
                )
            # 全部校验通过后才回写调优记录，保证失败操作不留下半条批次登记。
            if register_new and batch_no not in registered:
                registered.add(batch_no)
                tuning["关联批次"] = sorted(registered)

            item_count = self._as_int(values.get("写入项数")) or DEFAULT_ITEM_COUNT
            item_count = max(1, item_count)
            rows = store.rows(MODULE)
            next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
            archive_no = f"{ARCHIVE_PREFIX}-{next_id:04d}"
            source_tag = str(values.get("来源标识") or f"{archive_no}/{batch_no}").strip()
            items = [
                {
                    "序号": position,
                    "项标识": f"{archive_no}-{position}",
                    "状态": ITEM_TODO,
                    "重试次数": 0,
                    "来源标识": None,
                    "错误信息": None,
                }
                for position in range(1, item_count + 1)
            ]
            archive = {
                "id": next_id,
                "status": STATUS_TODO,
                "pending": True,
                "abnormal": False,
                "存档编号": archive_no,
                "算子编号": operator_no,
                "批次号": batch_no,
                "调优记录编号": record_no,
                "生效并行度": effective,
                "上一版本并行度": current,
                "输入流": operator.get("输入流"),
                "处理逻辑": operator.get("处理逻辑"),
                "输出流": operator.get("输出流"),
                "生效状态": EFFECTIVE_NO,
                "来源标识": source_tag,
                "批次版本校验": "未校验",
                "校验说明": None,
                "写入项": items,
                "未完成位置": 1,
                "同步记录": [],
                "创建时间": date.today().isoformat(),
            }
            rows.append(archive)
            return archive, f"批次 {batch_no} 存档已登记，等待批次版本校验", False

    # ------------------------------------------------------------------ 校验

    def verify_archive(self, archive_id: int) -> tuple[dict[str, Any] | None, str]:
        archive = store.find(MODULE, archive_id)
        if archive is None:
            return None, f"算子执行存档 {archive_id} 不存在"
        if archive["status"] in TERMINAL_STATUSES:
            return archive, f"存档已处于终态「{archive['status']}」，校验结果保持不变"
        if archive["status"] not in (STATUS_TODO, STATUS_FAILED):
            return archive, f"存档当前为「{archive['status']}」，无需重复校验"

        operator = self._find_operator(archive["算子编号"])
        tuning = self._find_tuning(archive["调优记录编号"])
        reasons: list[str] = []
        if operator is None:
            reasons.append(f"算子 {archive['算子编号']} 已不存在")
        if tuning is None:
            reasons.append(f"调优记录 {archive['调优记录编号']} 已不存在")
        if operator is not None:
            for field in ("输入流", "处理逻辑", "输出流"):
                if archive.get(field) != operator.get(field):
                    reasons.append(f"{field}映射与算子当前定义不一致")
        if tuning is not None:
            registered = self._tuning_batches(tuning)
            if registered and archive["批次号"] not in registered:
                reasons.append(
                    f"批次号 {archive['批次号']} 不在调优记录批次清单 {sorted(registered)} 中"
                )
            allowable = {
                value for value in (
                    self._as_int(tuning.get("当前并行度")),
                    self._as_int(tuning.get("建议并行度")),
                ) if value is not None
            }
            actual = self._as_int(archive.get("生效并行度"))
            if allowable and actual not in allowable:
                reasons.append(
                    f"生效并行度 {actual} 不在调优记录允许口径 {sorted(allowable)} 内"
                )
            if archive["批次号"] == tuning.get("生效批次"):
                suggested = self._as_int(tuning.get("建议并行度"))
                if suggested is not None and actual != suggested:
                    reasons.append(f"生效批次必须采用建议并行度 {suggested}，当前为 {actual}")

        if reasons:
            archive["status"] = STATUS_FAILED
            archive["abnormal"] = True
            archive["批次版本校验"] = "未通过"
            archive["校验说明"] = "；".join(reasons)
            return archive, f"批次版本校验未通过：{archive['校验说明']}"

        archive["status"] = STATUS_VERIFIED
        archive["abnormal"] = False
        archive["批次版本校验"] = "已校验"
        archive["校验说明"] = "批次版本与调优记录、输入流/处理逻辑/输出流映射一致"
        return archive, f"批次 {archive['批次号']} 版本校验通过，可写入执行数据"

    # ------------------------------------------------------------------ 写入

    def write_items(
        self,
        archive_id: int,
        *,
        fail_positions: list[int] | None = None,
        source: str | None = None,
    ) -> tuple[dict[str, Any] | None, str, dict[str, int] | None]:
        archive = store.find(MODULE, archive_id)
        if archive is None:
            return None, f"算子执行存档 {archive_id} 不存在", None
        if archive["status"] in TERMINAL_STATUSES:
            return None, f"存档已「{archive['status']}」，写入通道已关闭", None
        if archive["status"] == STATUS_TODO:
            return None, "批次版本尚未校验通过，不能写入执行数据", None

        fail_set = {int(position) for position in (fail_positions or [])}
        archive["status"] = STATUS_WRITING
        written = skipped = failed = 0
        for item in archive["写入项"]:
            # 失败重试只能补写未完成项：已写入项原样保留，绝不重写。
            if item["状态"] == ITEM_DONE:
                skipped += 1
                continue
            if item["序号"] in fail_set:
                item["状态"] = ITEM_FAILED
                item["重试次数"] = int(item.get("重试次数", 0)) + 1
                item["错误信息"] = f"第 {item['重试次数']} 次写入失败，等待重试补写"
                failed += 1
                continue
            item["状态"] = ITEM_DONE
            item["来源标识"] = source or archive["来源标识"]
            item["错误信息"] = None
            written += 1

        unfinished = [item["序号"] for item in archive["写入项"] if item["状态"] != ITEM_DONE]
        archive["未完成位置"] = unfinished[0] if unfinished else None
        summary = {
            "本次写入": written,
            "失败项": failed,
            "跳过已写入项": skipped,
            "剩余未完成": len(unfinished),
        }
        if failed:
            archive["status"] = STATUS_FAILED
            archive["abnormal"] = True
            return archive, f"存在 {failed} 个失败项，可重试补写，未完成位置保留在 {archive['未完成位置']}", summary
        archive["abnormal"] = False
        return archive, "执行数据写入完成，可执行完成批次收口", summary

    def complete_archive(self, archive_id: int) -> tuple[dict[str, Any] | None, str]:
        archive = store.find(MODULE, archive_id)
        if archive is None:
            return None, f"算子执行存档 {archive_id} 不存在"
        if archive["status"] == STATUS_DONE:
            # 终态幂等：重复收口不产生第二个终态。
            return archive, "批次已完成，重复收口已忽略"
        if archive["status"] == STATUS_VOID:
            return None, "存档已作废，不能再收口为完成"
        unfinished = [item["序号"] for item in archive["写入项"] if item["状态"] != ITEM_DONE]
        if unfinished:
            return None, f"仍有未完成写入项 {unfinished}，需重试补写后才能完成批次"
        archive["status"] = STATUS_DONE
        archive["pending"] = False
        archive["abnormal"] = False
        archive["未完成位置"] = None
        return archive, f"批次 {archive['批次号']} 已完成，成为该算子的完整批次存档"

    # ------------------------------------------------------------------ 生效

    def activate_archive(self, archive_id: int) -> tuple[dict[str, Any] | None, str]:
        with self._lock:
            archive = store.find(MODULE, archive_id)
            if archive is None:
                return None, f"算子执行存档 {archive_id} 不存在"
            if archive["status"] not in (STATUS_VERIFIED, STATUS_DONE):
                return None, (
                    f"批次当前为「{archive['status']}」，只有已校验/已完成的批次版本才能生效；"
                    "未校验版本禁止直接供消费侧读取"
                )
            previous = self._effective_archive(archive["算子编号"])
            if previous is not None and previous is not archive:
                # 旧批次保留完整快照，仅切换生效标记，作业实例仍可按旧批次号读取。
                previous["生效状态"] = EFFECTIVE_OLD
            archive["生效状态"] = EFFECTIVE_YES

            operator = self._find_operator(archive["算子编号"])
            if operator is not None:
                operator["并行度"] = archive["生效并行度"]
                operator["最近执行"] = f"{archive['批次号']}@并行度{archive['生效并行度']}"
            tuning = self._find_tuning(archive["调优记录编号"])
            if tuning is not None:
                tuning["生效批次"] = archive["批次号"]
            return archive, (
                f"批次 {archive['批次号']} 已按批次边界生效（并行度 {archive['生效并行度']}），"
                "消费侧现在可以读取该版本"
            )

    def void_archive(self, archive_id: int) -> tuple[dict[str, Any] | None, str]:
        archive = store.find(MODULE, archive_id)
        if archive is None:
            return None, f"算子执行存档 {archive_id} 不存在"
        if archive["status"] == STATUS_VOID:
            return archive, "存档已作废，重复操作已忽略"
        if archive["status"] == STATUS_DONE:
            return None, "完整批次存档不能作废，需通过新建批次调优"
        archive["status"] = STATUS_VOID
        archive["生效状态"] = EFFECTIVE_NO
        archive["pending"] = False
        archive["abnormal"] = False
        return archive, f"批次 {archive['批次号']} 已作废，收敛为单一废弃终态"

    # ------------------------------------------------------------------ 快照

    def read_snapshot(self, operator_no: str, batch_no: str | None = None) -> dict[str, Any]:
        operator = self._find_operator(operator_no)
        if operator is None:
            raise ArchiveConflictError(f"流处理算子 {operator_no} 不存在")

        if batch_no:
            archive = self._find_archive_by_batch(operator_no, batch_no)
            if archive is None:
                raise ArchiveConflictError(f"算子 {operator_no} 没有批次 {batch_no} 的执行数据存档（空态）")
            if archive["status"] == STATUS_VOID:
                raise ArchiveConflictError(f"批次 {batch_no} 的存档已作废，不能作为执行快照读取")
            if archive["status"] == STATUS_TODO:
                raise ArchiveConflictError(f"批次 {batch_no} 尚未校验，执行快照只能选择已校验的批次版本")
        else:
            archive = self._effective_archive(operator_no)
            if archive is None:
                raise ArchiveConflictError(f"算子 {operator_no} 暂无生效批次，无法读取当前执行快照")

        # 当前执行快照与调优记录必须读取同一批次。
        tuning = self._find_tuning(archive["调优记录编号"])
        if tuning is None:
            raise ArchiveConflictError(
                f"快照引用的调优记录 {archive['调优记录编号']} 缺失，数据版本无法核对"
            )
        registered = self._tuning_batches(tuning)
        if registered and archive["批次号"] not in registered:
            raise ArchiveConflictError(
                f"执行快照批次 {archive['批次号']} 与调优记录批次 {sorted(registered)} 不一致，"
                "拒绝混用两种数据版本"
            )

        items = archive["写入项"]
        return {
            "算子编号": operator_no,
            "批次号": archive["批次号"],
            "调优记录编号": archive["调优记录编号"],
            "生效并行度": archive["生效并行度"],
            "上一版本并行度": archive.get("上一版本并行度"),
            "输入流": archive.get("输入流"),
            "处理逻辑": archive.get("处理逻辑"),
            "输出流": archive.get("输出流"),
            "生效状态": archive.get("生效状态"),
            "批次版本校验": archive.get("批次版本校验"),
            "来源标识": archive.get("来源标识"),
            "写入进度": {
                "总项数": len(items),
                "已写入": sum(1 for item in items if item["状态"] == ITEM_DONE),
                "失败项": sum(1 for item in items if item["状态"] == ITEM_FAILED),
                "未完成位置": archive.get("未完成位置"),
            },
        }

    # ------------------------------------------------------------------ 冲突

    def list_conflicts(self) -> list[dict[str, Any]]:
        conflicts: list[dict[str, Any]] = []

        # 1) 同一算子同一批次出现多份非作废存档（执行快照重复）。
        seen: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for row in store.rows(MODULE):
            if row.get("status") == STATUS_VOID:
                continue
            seen.setdefault((row.get("算子编号", ""), row.get("批次号", "")), []).append(row)
        for (operator_no, batch_no), group in seen.items():
            if len(group) > 1:
                conflicts.append({
                    "类型": "批次存档重复",
                    "算子编号": operator_no,
                    "批次号": batch_no,
                    "涉及存档": [row.get("存档编号") for row in group],
                    "说明": "同一批次存在多份存档，必须合并为单一终态",
                })

        # 2) 存档的流映射与算子当前数据流漂移，或快照/调优批次口径不一致。
        for row in store.rows(MODULE):
            if row.get("status") == STATUS_VOID:
                continue
            operator = self._find_operator(row.get("算子编号", ""))
            if operator is not None:
                for field in ("输入流", "处理逻辑", "输出流"):
                    if row.get(field) != operator.get(field):
                        conflicts.append({
                            "类型": "流映射漂移",
                            "算子编号": row.get("算子编号"),
                            "批次号": row.get("批次号"),
                            "涉及存档": [row.get("存档编号")],
                            "说明": f"{field}与算子当前数据流不一致，需按当前数据流修正",
                        })
            tuning = self._find_tuning(row.get("调优记录编号", ""))
            if tuning is not None:
                registered = self._tuning_batches(tuning)
                if registered and row.get("批次号") not in registered:
                    conflicts.append({
                        "类型": "快照调优批次不一致",
                        "算子编号": row.get("算子编号"),
                        "批次号": row.get("批次号"),
                        "涉及存档": [row.get("存档编号")],
                        "说明": "执行快照与调优记录未读取同一批次",
                    })

        # 3) 作业实例运行记录把两种并行度混在同一批次里。
        # 作业实例通过「引用存档」绑定同一份算子执行数据存档；缺失时再按批次号全局匹配。
        for job in store.rows(JOBOPS_MODULE):
            batch_no = job.get("引用批次")
            if not batch_no:
                continue
            archive = next(
                (row for row in store.rows(MODULE)
                 if row.get("存档编号") == job.get("引用存档") and row.get("status") != STATUS_VOID),
                None,
            )
            if archive is None:
                candidates = [
                    row for row in store.rows(MODULE)
                    if row.get("批次号") == batch_no and row.get("status") != STATUS_VOID
                ]
                archive = candidates[0] if candidates else None
            if archive is None:
                conflicts.append({
                    "类型": "运行记录引用未知批次",
                    "算子编号": None,
                    "批次号": batch_no,
                    "涉及存档": [job.get("实例编号")],
                    "说明": "作业实例引用的批次没有已校验存档",
                })
                continue
            recorded = self._as_int(job.get("记录并行度"))
            if recorded is not None and recorded != self._as_int(archive.get("生效并行度")):
                conflicts.append({
                    "类型": "运行记录并行度混用",
                    "算子编号": archive.get("算子编号"),
                    "批次号": batch_no,
                    "涉及存档": [archive.get("存档编号"), job.get("实例编号")],
                    "说明": (
                        f"运行记录并行度 {recorded} 与批次生效并行度 "
                        f"{archive.get('生效并行度')} 混用，必须修正为单一口径"
                    ),
                })
        return conflicts

    def repair_conflicts(self, archive_id: int) -> tuple[dict[str, Any] | None, str, dict[str, Any] | None]:
        archive = store.find(MODULE, archive_id)
        if archive is None:
            return None, f"算子执行存档 {archive_id} 不存在", None

        fixed: dict[str, Any] = {"作废重复存档": [], "流映射修正": [], "运行记录修正": []}
        operator_no = archive["算子编号"]
        batch_no = archive["批次号"]

        # 1) 合并重复存档：保留最早一份，其余收敛到「已作废」终态。
        duplicates = [
            row for row in store.rows(MODULE)
            if row.get("算子编号") == operator_no
            and row.get("批次号") == batch_no
            and row is not archive
        ]
        for duplicate in duplicates:
            if duplicate.get("status") != STATUS_VOID:
                duplicate["status"] = STATUS_VOID
                duplicate["生效状态"] = EFFECTIVE_NO
                duplicate["pending"] = False
                duplicate["abnormal"] = False
                fixed["作废重复存档"].append(duplicate.get("存档编号"))

        # 2) 按当前数据流修正流映射；并行度属于批次口径，保持兼容不动。
        operator = self._find_operator(operator_no)
        if operator is not None:
            for field in ("输入流", "处理逻辑", "输出流"):
                if archive.get(field) != operator.get(field):
                    archive[field] = operator.get(field)
                    fixed["流映射修正"].append(field)

        # 3) 把引用该批次的作业实例拉回到存档的生效并行度，消除混用。
        target_parallelism = self._as_int(archive.get("生效并行度"))
        for job in store.rows(JOBOPS_MODULE):
            if job.get("引用批次") != batch_no:
                continue
            recorded = self._as_int(job.get("记录并行度"))
            if recorded != target_parallelism:
                job["记录并行度"] = target_parallelism
                fixed["运行记录修正"].append(job.get("实例编号"))
            job["引用存档"] = archive["存档编号"]
            if not job.get("来源标识"):
                job["来源标识"] = archive["来源标识"]

        archive["abnormal"] = False
        if archive["status"] not in TERMINAL_STATUSES and all(
            item["状态"] == ITEM_DONE for item in archive["写入项"]
        ):
            archive["status"] = STATUS_DONE
            archive["pending"] = False
        return archive, "冲突修正完成，所有记录已收敛到同一份已校验批次版本", fixed

    # ------------------------------------------------------------------ 同步

    def sync_consumer(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        archive_no = str(values.get("存档编号") or "").strip()
        operator_no = str(values.get("算子编号") or "").strip()
        batch_no = str(values.get("批次号") or "").strip()
        consumer = str(values.get("消费节点") or "").strip()
        system = str(values.get("来源系统") or "跨系统同步链路").strip()
        if not consumer:
            return None, "同步需要提供消费节点"
        archive = self._find_archive_by_no(archive_no) if archive_no else None
        if archive is None and operator_no and batch_no:
            archive = self._find_archive_by_batch(operator_no, batch_no)
        if archive is None:
            return None, "找不到对应算子执行数据存档，同步链路必须引用同一份存档"
        if archive["status"] not in (STATUS_VERIFIED, STATUS_DONE):
            return None, f"批次 {archive['批次号']} 尚未校验完成，不能同步给消费侧"
        if archive["生效状态"] != EFFECTIVE_YES:
            return None, (
                f"批次 {archive['批次号']} 还未生效，消费节点 {consumer} 暂不可读取；"
                "请先让批次按边界生效"
            )

        source_tag = str(archive.get("来源标识") or f"{archive['存档编号']}/{archive['批次号']}")
        for record in archive["同步记录"]:
            # 幂等：同链路同节点重复同步直接复用，不产生重复消费记录。
            if record.get("消费节点") == consumer and record.get("来源系统") == system:
                record["来源标识"] = source_tag
                return archive, f"消费节点 {consumer} 已同步该批次，重复请求已合并"
        archive["同步记录"].append({
            "来源系统": system,
            "消费节点": consumer,
            "来源标识": source_tag,
            "引用批次": archive["批次号"],
            "引用存档": archive["存档编号"],
            "同步时间": date.today().isoformat(),
        })
        return archive, f"批次 {archive['批次号']} 已同步给 {consumer}，消费侧保留来源标识 {source_tag}"
