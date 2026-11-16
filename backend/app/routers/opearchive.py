"""算子执行数据存档接口。

按批次边界维护并行度调优后的执行快照与执行数据：批次版本校验、写入项断点续传、
批次生效、执行快照读取、冲突检测修正与跨系统同步都在这里暴露，业务判断全部
下沉到 app.services.opearchive。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.opearchive import ArchiveConflictError, ArchiveService

router = APIRouter(prefix="/api/opearchive", tags=["算子执行数据存档"])

service = ArchiveService()

LIST_FIELDS = [
    "存档编号", "算子编号", "批次号", "调优记录编号", "生效并行度",
    "上一版本并行度", "生效状态", "批次版本校验", "来源标识", "未完成位置",
]


@router.get("", response_model=PageResult[dict])
def list_archives(
    operator_no: str | None = Query(default=None, description="算子编号，例如 OPER-0002"),
    batch_no: str | None = Query(default=None, description="批次号，例如 batch-17"),
    status: str | None = Query(default=None, description="待校验、已校验、写入中、失败、已完成、已作废"),
    effective: str | None = Query(default=None, description="未生效、生效中、已替换"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按算子、批次号、状态与生效标记过滤存档；空表返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_archives(
        operator_no=operator_no, batch_no=batch_no, status=status,
        effective=effective, page=page, size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/snapshot")
def read_snapshot(
    operator_no: str = Query(..., description="算子编号，例如 OPER-0002"),
    batch_no: str | None = Query(default=None, description="缺省时读取当前生效批次"),
) -> dict[str, Any]:
    """执行快照读取：只返回已校验批次，快照与调优记录必须落在同一批次。"""
    try:
        return service.read_snapshot(operator_no, batch_no)
    except ArchiveConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/conflicts")
def list_conflicts() -> dict[str, Any]:
    """列出数据版本冲突：存档重复、流映射漂移、快照/调优批次不一致、并行度混用。"""
    conflicts = service.list_conflicts()
    return {"total": len(conflicts), "items": conflicts}


@router.post("/sync")
def sync_consumer(payload: EntryPayload) -> ActionResult:
    """跨系统链路同步：消费侧引用同一份存档并保留来源标识，重复同步幂等合并。"""
    archive, message = service.sync_consumer(payload.values)
    if archive is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=archive)


@router.get("/export")
def export_archives() -> dict[str, Any]:
    """导出算子执行数据存档全量清单。静态路径必须排在 /{archive_id} 之前。"""
    items, total = service.list_archives(page=1, size=10000)
    return {"module": "opearchive", "total": total, "items": items}


@router.get("/{archive_id}", response_model=dict)
def get_archive(archive_id: int) -> dict:
    """读取单份存档明细（含写入项进度与同步记录）。"""
    archive = service.get_archive(archive_id)
    if archive is None:
        raise HTTPException(status_code=404, detail=f"算子执行存档 {archive_id} 不存在")
    return archive


@router.post("", response_model=ActionResult)
def create_archive(payload: EntryPayload) -> ActionResult:
    """按调优批次登记存档；并发批次与并行度冲突会被拦下并说明原因。"""
    try:
        archive, message, _reused = service.create_archive(payload.values)
    except ArchiveConflictError as exc:
        return ActionResult(ok=False, message=str(exc))
    if archive is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=archive)


@router.post("/{archive_id}/actions", response_model=ActionResult)
def run_action(archive_id: int, payload: EntryPayload) -> ActionResult:
    """对存档执行动作：校验批次、写入执行数据、完成批次、生效批次、作废、修正冲突。"""
    action = str(payload.values.get("action") or "").strip()
    values = payload.values
    try:
        if action == "校验批次":
            archive, message = service.verify_archive(archive_id)
        elif action == "写入执行数据":
            fail_positions = values.get("fail_positions") or values.get("失败项序号")
            if fail_positions is not None and not isinstance(fail_positions, list):
                return ActionResult(ok=False, message="失败项序号必须是数组")
            archive, message, summary = service.write_items(
                archive_id,
                fail_positions=[int(item) for item in fail_positions] if fail_positions else None,
                source=values.get("来源标识"),
            )
            if archive is None:
                return ActionResult(ok=False, message=message)
            return ActionResult(ok=True, message=message, entry=_with_summary(archive, summary))
        elif action == "完成批次":
            archive, message = service.complete_archive(archive_id)
        elif action == "生效批次":
            archive, message = service.activate_archive(archive_id)
        elif action == "作废批次":
            archive, message = service.void_archive(archive_id)
        elif action == "修正冲突":
            archive, message, fixed = service.repair_conflicts(archive_id)
            if archive is None:
                return ActionResult(ok=False, message=message)
            return ActionResult(ok=True, message=message, entry=_with_summary(archive, fixed))
        else:
            return ActionResult(
                ok=False,
                message=(
                    "动作「{0}」不属于算子执行数据存档可执行范围".format(action)
                    if action else "缺少 action 参数"
                ),
            )
    except ArchiveConflictError as exc:
        return ActionResult(ok=False, message=str(exc))
    except (TypeError, ValueError) as exc:
        return ActionResult(ok=False, message=f"动作参数格式不正确：{exc}")

    if archive is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=archive)


def _with_summary(archive: dict[str, Any], summary: dict[str, Any] | None) -> dict[str, Any]:
    """动作结果里带上本次写入/修正摘要，同时完整返回存档本身。"""
    result = dict(archive)
    if summary:
        result["本次操作摘要"] = summary
    return result
