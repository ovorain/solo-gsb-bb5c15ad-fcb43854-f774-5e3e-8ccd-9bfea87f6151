"""FastAPI 入口：胶片档案扫描整理 API。"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Query, Response, status
from sqlite3 import Connection

from . import database, schemas, services


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动即建库建表（幂等），依赖与数据库初始化都纳入启动流程
    database.init_db()
    yield


app = FastAPI(
    title="胶片档案扫描整理 API",
    description="按胶卷汇总扫描帧、去重、统计预期范围内的连续缺帧；批次导入幂等；支持误导入批次撤销、补扫任务认领与补扫完成回执；支持交付快照及相对快照的区间级差异对比。",
    version="1.5.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=schemas.HealthOut, tags=["系统"])
def health():
    return {"status": "ok", "db": str(database.get_db_path())}


@app.post(
    "/batches",
    response_model=schemas.BatchResult,
    status_code=status.HTTP_201_CREATED,
    tags=["批次"],
)
def submit_batch(
    payload: schemas.BatchIn,
    response: Response,
    conn: Connection = Depends(database.get_db),
):
    """提交一个扫描批次。相同内容重复提交返回 200 且不改变任何结果。"""
    try:
        result, code = services.submit_batch(conn, payload)
    except services.BatchIdConflictError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"batch_id '{e.batch_id}' 已被其他内容占用，请更换后重试",
        )
    response.status_code = code
    return result


@app.get("/batches/{batch_id}", response_model=schemas.BatchResult, tags=["批次"])
def get_batch(batch_id: str, conn: Connection = Depends(database.get_db)):
    result = services.get_batch(conn, batch_id)
    if result is None:
        raise HTTPException(status_code=404, detail="批次不存在")
    return result


@app.delete(
    "/batches/{batch_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["批次"],
)
def delete_batch(batch_id: str, conn: Connection = Depends(database.get_db)):
    """撤销一个误导入的批次。

    删除该批并按剩余批次原导入顺序重建预期范围、帧归属及各批新增/重复明细
    （共享帧不会丢失，批内重复保留）。批次不存在返回 404；若剩余批次为无法
    区分批内/跨批重复的旧数据，返回 409 且数据不变。
    """
    try:
        services.delete_batch(conn, batch_id.strip())
    except services.BatchNotFoundError:
        raise HTTPException(status_code=404, detail="批次不存在")
    except services.LegacyBatchDataError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "撤销被拒绝：该胶卷存在旧版格式的历史批次，缺少原始帧列表，"
                f"无法区分批内与跨批重复（相关批次：{', '.join(e.legacy_batch_ids)}）；"
                "为保证重建结果准确，本次未改动任何数据。"
            ),
        )
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/rolls", response_model=list[schemas.RollSummary], tags=["胶卷"])
def list_rolls(conn: Connection = Depends(database.get_db)):
    """列出所有胶卷及其汇总状态。"""
    return services.list_rolls(conn)


@app.get("/rolls/{roll_no}", response_model=schemas.RollState, tags=["胶卷"])
def get_roll(roll_no: str, conn: Connection = Depends(database.get_db)):
    """查询单个胶卷的合并帧号、重复帧明细与连续缺帧区间。"""
    result = services.get_roll_state(conn, roll_no.strip())
    if result is None:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    return result


@app.get(
    "/rolls/{roll_no}/frame-provenance",
    response_model=schemas.FrameProvenanceOut,
    tags=["胶卷"],
)
def get_frame_provenance(
    roll_no: str,
    start: int = Query(..., ge=0, description="帧号区间起始（含），非负整数"),
    end: int = Query(..., ge=0, description="帧号区间结束（含），非负整数，end ≥ start"),
    limit: int = Query(..., ge=1, description="每页最多返回的帧数（正整数）"),
    cursor: int | None = Query(
        None, ge=0, description="游标：上一页末帧号；续页只取帧号更大的帧"
    ),
    conn: Connection = Depends(database.get_db),
):
    """胶卷帧号扫描溯源：追查区间内每个实际存在的帧由哪些批次扫描。

    只按**实际存在的帧号**升序键集分页（不逐帧遍历区间跨度），每页最多
    ``limit`` 帧，``next_cursor`` 为下一页游标（上一页末帧号）。每帧返回：
    最早有效批次（首扫归属）、按批次**原导入顺序**排列的参与批次及各批
    **原始列表中的出现次数**（旧批次无原始列表时为 ``null``，不推断），
    以及该帧是否位于当前预期覆盖内。撤销批次后按剩余批次实时重算，
    首扫归属随之转移。参数无效（含 end < start）返回 422，胶卷不存在返回 404。
    """
    if end < start:
        raise HTTPException(status_code=422, detail="end 不能小于 start")
    try:
        return services.frame_provenance(
            conn, roll_no.strip(), start, end, limit, cursor
        )
    except services.RollNotFoundError:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    except services.ProvenanceRangeError:
        raise HTTPException(status_code=422, detail="end 不能小于 start")


@app.post(
    "/rolls/{roll_no}/snapshots",
    response_model=schemas.RollSnapshotOut,
    status_code=status.HTTP_201_CREATED,
    tags=["交付快照"],
)
def create_roll_snapshot(
    roll_no: str, conn: Connection = Depends(database.get_db)
):
    """为胶卷创建交付快照：在一致性读中固化当时的预期闭区间、实扫帧与缺帧闭区间。

    返回可长期查询的快照 ID；快照不随批次导入、撤销或补扫改变。
    胶卷不存在（尚未导入任何批次）返回 404。
    """
    try:
        return services.create_snapshot(conn, roll_no.strip())
    except services.RollNotFoundError:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")


@app.get(
    "/rolls/{roll_no}/snapshots/{snapshot_id}/diff",
    response_model=schemas.SnapshotDiffOut,
    tags=["交付快照"],
)
def get_roll_snapshot_diff(
    roll_no: str, snapshot_id: str, conn: Connection = Depends(database.get_db)
):
    """返回胶卷当前状态相对快照的差异。

    预期覆盖、实扫帧、缺帧三个维度独立比较，分别给出相对快照新增与消失的
    闭区间（按区间做集合运算）。即使胶卷最后一批已被撤销，只要快照属于该
    胶卷编号，仍返回差异（当前按空状态处理，roll_exists=false）。未知快照
    ID 或快照与胶卷编号不匹配返回 404。
    """
    try:
        return services.get_snapshot_diff(conn, roll_no.strip(), snapshot_id.strip())
    except services.SnapshotNotFoundError:
        raise HTTPException(status_code=404, detail="快照不存在或不属于该胶卷")


@app.post(
    "/rolls/{roll_no}/rescan-claims",
    response_model=schemas.RescanClaimOut,
    status_code=status.HTTP_201_CREATED,
    tags=["补扫认领"],
)
def create_rescan_claim(
    roll_no: str,
    payload: schemas.RescanClaimIn,
    conn: Connection = Depends(database.get_db),
):
    """认领一段缺帧用于补扫。

    当前缺帧区间扣除所有未到期认领后，按帧号升序分配首段不超过
    ``max_frames`` 的连续闭区间。胶卷不存在返回 404，暂无可领取帧返回 409。
    """
    try:
        return services.create_rescan_claim(conn, payload, roll_no.strip())
    except services.RollNotFoundError:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    except services.NoClaimableFramesError:
        raise HTTPException(status_code=409, detail="当前没有可领取的缺帧（可能已被其他未到期任务认领）")


@app.get(
    "/rolls/{roll_no}/rescan-claims",
    response_model=list[schemas.RescanClaimOut],
    tags=["补扫认领"],
)
def list_rescan_claims(
    roll_no: str, conn: Connection = Depends(database.get_db)
):
    """列出该胶卷全部未到期的补扫认领任务，及其帧段内仍缺失的帧段。"""
    result = services.list_rescan_claims(conn, roll_no.strip())
    if result is None:
        raise HTTPException(status_code=404, detail="胶卷不存在（尚未导入任何批次）")
    return result


@app.delete(
    "/rescan-claims/{claim_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["补扫认领"],
)
def release_rescan_claim(
    claim_id: str, conn: Connection = Depends(database.get_db)
):
    """释放（删除）一个补扫认领任务；任务不存在、已释放、已到期或已结案返回 404。"""
    if not services.release_rescan_claim(conn, claim_id.strip()):
        raise HTTPException(status_code=404, detail="补扫任务不存在、已释放、已到期或已结案")
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.post(
    "/rescan-claims/{claim_id}/receipts",
    response_model=schemas.RescanReceiptOut,
    status_code=status.HTTP_201_CREATED,
    tags=["补扫认领"],
)
def submit_rescan_receipt(
    claim_id: str,
    payload: schemas.RescanReceiptIn,
    response: Response,
    conn: Connection = Depends(database.get_db),
):
    """提交补扫完成回执：认领 ID、补扫批次号与实际扫到的非空帧列表。

    以认领闭区间作为该补扫批次的预期范围，批次导入、回执持久化与认领结案
    在同一 SQLite 写事务内完成。只接受未到期、未释放、未结案的认领；帧号须在
    认领闭区间内且提交时仍属该胶卷缺帧，否则 409 且数据不变。相同认领、批次号
    与原始帧列表的重试返回已存回执（200），不重复导入；变更内容或对已结案认领
    再提交返回 409。
    """
    try:
        result, code = services.submit_rescan_receipt(conn, claim_id, payload)
    except services.ClaimNotFoundError:
        raise HTTPException(status_code=404, detail="补扫任务不存在")
    except services.ClaimUnavailableError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except services.ReceiptConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except services.BatchIdConflictError as e:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"batch_id '{e.batch_id}' 已被其他内容占用，请更换后重试",
        )
    response.status_code = code
    return result


if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host=os.environ.get("HOST", "0.0.0.0"),
        port=int(os.environ.get("PORT", "8000")),
    )
