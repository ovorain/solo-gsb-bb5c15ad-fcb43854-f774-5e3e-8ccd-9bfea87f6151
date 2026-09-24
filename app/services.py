"""核心业务逻辑：批次导入、跨批次合并、缺帧区间计算、补扫任务认领。"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from . import schemas


# ---------- 纯函数（便于测试） ----------

def content_hash(roll_no: str, start: int, end: int, frames: list[int]) -> str:
    """同一胶卷下相同（预期范围 + 帧号集合）的内容指纹，用于幂等判定。"""
    payload = json.dumps(
        {"roll_no": roll_no, "start": start, "end": end, "frames": sorted(set(frames))},
        separators=(",", ":"),
        ensure_ascii=False,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """合并重叠或相邻的闭区间，例如 [(1,5),(5,8)] -> [(1,8)]。"""
    if not intervals:
        return []
    merged: list[tuple[int, int]] = []
    for s, e in sorted(intervals):
        if not merged or s > merged[-1][1] + 1:
            merged.append((s, e))
        else:
            prev_s, prev_e = merged[-1]
            merged[-1] = (prev_s, max(prev_e, e))
    return merged


def missing_intervals(
    expected: list[tuple[int, int]], frames: set[int]
) -> list[tuple[int, int]]:
    """返回合并后的预期范围内、未被扫描帧覆盖的连续缺帧区间。

    直接做区间集合差（预期区间 − 帧点区间），不在帧号轴上逐帧展开，
    因此预期范围跨度巨大（如 [1, 20亿]）时成本也只与区间/帧数相关。
    """
    return _subtract_merged(
        merge_intervals(expected), frames_to_ranges(frames)
    )


def out_of_range_frames(
    expected: list[tuple[int, int]], frames: set[int]
) -> list[int]:
    expected = merge_intervals(expected)
    return sorted(
        f for f in frames if not any(s <= f <= e for s, e in expected)
    )


def subtract_intervals(
    base: list[tuple[int, int]], cuts: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """从一组闭区间 base 中扣除 cuts 覆盖的部分，返回合并后的剩余闭区间。

    基于有序区间的单趟扫描，总时间 O(n+m)；base/cuts 无需预先合并
    （重叠/相邻会被规范化），输出恒为互不相交、不相邻的闭区间。
    """
    return difference_intervals(base, cuts)


def frames_to_ranges(frames) -> list[tuple[int, int]]:
    """把排序后的帧号集合压成连续闭区间，例如 [1,2,3,5,8,9] -> [(1,3),(5,5),(8,9)]。

    实扫帧以帧集合持久化，差异比较时先经此函数转成区间，避免逐帧参与集合运算。
    """
    result: list[tuple[int, int]] = []
    start: int | None = None
    prev: int | None = None
    for f in sorted(frames):
        if start is None:
            start = prev = f
        elif f == prev + 1:
            prev = f
        else:
            result.append((start, prev))
            start = prev = f
    if start is not None:
        result.append((start, prev))
    return result


def _subtract_merged(
    a: list[tuple[int, int]], b: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """A − B，要求 a、b 均为已合并（互不重叠/相邻）、按起点升序的闭区间。

    双指针单趟扫描，总时间 O(n+m)，不展开任何区间。
    """
    result: list[tuple[int, int]] = []
    j = 0
    nb = len(b)
    for s, e in a:
        # 完全落在当前段左侧的切割区间对本段及后续段都不再有用，指针单调前进
        while j < nb and b[j][1] < s:
            j += 1
        pos = s
        k = j
        while k < nb and b[k][0] <= e:
            cs, ce = b[k]
            if ce >= pos:
                if cs > pos:
                    result.append((pos, min(e, cs - 1)))
                if ce >= e:
                    pos = e + 1
                    break
                pos = ce + 1
            k += 1
        if pos <= e:
            result.append((pos, e))
    return result


def difference_intervals(
    a: list[tuple[int, int]], b: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """闭区间集合差 A − B（输入允许重叠/相邻，会先合并规范化），线性扫描。"""
    return _subtract_merged(merge_intervals(a), merge_intervals(b))


def intersect_intervals(
    a: list[tuple[int, int]], b: list[tuple[int, int]]
) -> list[tuple[int, int]]:
    """两组已合并闭区间的交集（合并后的连续闭区间）。"""
    result: list[tuple[int, int]] = []
    for as_, ae in merge_intervals(a):
        for bs, be in merge_intervals(b):
            s, e = max(as_, bs), min(ae, be)
            if s <= e:
                result.append((s, e))
    return merge_intervals(result)


def _range_diff(
    current: list[tuple[int, int]], snapshotted: list[tuple[int, int]]
) -> schemas.RangeDiff:
    """两个已合并区间集合的双向差：新增 = 当前−快照，消失 = 快照−当前。"""
    return schemas.RangeDiff(
        added=[list(x) for x in _subtract_merged(current, snapshotted)],
        removed=[list(x) for x in _subtract_merged(snapshotted, current)],
    )


# ---------- 数据访问 ----------

def _expected_ranges(conn: sqlite3.Connection, roll_no: str) -> list[tuple[int, int]]:
    rows = conn.execute(
        "SELECT start_no, end_no FROM roll_expected WHERE roll_no = ?",
        (roll_no,),
    ).fetchall()
    return merge_intervals([(r["start_no"], r["end_no"]) for r in rows])


def _all_frames(conn: sqlite3.Connection, roll_no: str) -> set[int]:
    rows = conn.execute(
        "SELECT frame_no FROM roll_frames WHERE roll_no = ?", (roll_no,)
    ).fetchall()
    return {r["frame_no"] for r in rows}


def _batch_metas(conn: sqlite3.Connection, roll_no: str) -> list[schemas.BatchMeta]:
    rows = conn.execute(
        """SELECT id, expected_start, expected_end, frames_json, raw_frames_json,
                  frames_merged_json, duplicates_json, created_at
             FROM batches WHERE roll_no = ? ORDER BY created_at, rowid""",
        (roll_no,),
    ).fetchall()
    return [
        schemas.BatchMeta(
            batch_id=r["id"],
            expected_start=r["expected_start"],
            expected_end=r["expected_end"],
            # 新批次保留原始帧列表（含批内重复、保序）；旧库迁移来的批次退回去重集合
            frames=json.loads(r["raw_frames_json"] or r["frames_json"]),
            added_frames=json.loads(r["frames_merged_json"]),
            duplicates=json.loads(r["duplicates_json"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]


def _state_from_db(conn: sqlite3.Connection, roll_no: str) -> dict:
    expected = _expected_ranges(conn, roll_no)
    frames = _all_frames(conn, roll_no)
    return {
        "expected": expected,
        "frames": frames,
        "missing": missing_intervals(expected, frames),
        "out_of_range": out_of_range_frames(expected, frames),
    }


def get_roll_state(conn: sqlite3.Connection, roll_no: str) -> schemas.RollState | None:
    if not conn.execute(
        "SELECT 1 FROM batches WHERE roll_no = ? LIMIT 1", (roll_no,)
    ).fetchone():
        return None
    st = _state_from_db(conn, roll_no)
    metas = _batch_metas(conn, roll_no)
    frames_sorted = sorted(st["frames"])
    return schemas.RollState(
        roll_no=roll_no,
        expected_ranges=[list(x) for x in st["expected"]],
        frames=frames_sorted,
        missing_ranges=[list(x) for x in st["missing"]],
        out_of_range_frames=st["out_of_range"],
        total_frames=len(frames_sorted),
        batch_count=len(metas),
        batches=metas,
    )


def list_rolls(conn: sqlite3.Connection) -> list[schemas.RollSummary]:
    rows = conn.execute(
        """SELECT roll_no,
                  (SELECT COUNT(*) FROM roll_frames f WHERE f.roll_no = b.roll_no) AS total,
                  COUNT(*) AS batch_count
             FROM batches b GROUP BY roll_no ORDER BY roll_no"""
    ).fetchall()
    out: list[schemas.RollSummary] = []
    for r in rows:
        st = _state_from_db(conn, r["roll_no"])
        out.append(
            schemas.RollSummary(
                roll_no=r["roll_no"],
                total_frames=r["total"],
                batch_count=r["batch_count"],
                missing_ranges=[list(x) for x in st["missing"]],
            )
        )
    return out


def get_batch(conn: sqlite3.Connection, batch_id: str) -> schemas.BatchResult | None:
    r = conn.execute("SELECT * FROM batches WHERE id = ?", (batch_id,)).fetchone()
    if r is None:
        return None
    st = _state_from_db(conn, r["roll_no"])
    return schemas.BatchResult(
        batch_id=r["id"],
        roll_no=r["roll_no"],
        repeated=True,
        added_frames=json.loads(r["frames_merged_json"]),
        duplicates=json.loads(r["duplicates_json"]),
        merged_frames=sorted(st["frames"]),
        expected_ranges=[list(x) for x in st["expected"]],
        missing_ranges=[list(x) for x in st["missing"]],
        out_of_range_frames=st["out_of_range"],
        created_at=r["created_at"],
    )


class BatchIdConflictError(Exception):
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"batch_id 已被其他内容占用: {batch_id}")


def _import_batch_tx(
    conn: sqlite3.Connection,
    roll_no: str,
    expected_start: int,
    expected_end: int,
    frames_in: list[int],
    batch_id: str,
) -> tuple[str, list[int], list[int]]:
    """在**已开启的写事务内**导入一个批次（批次提交与补扫回执共用）。

    调用方须先持有 BEGIN IMMEDIATE 事务。返回 (batch_id, 新增帧, 重复帧)。
    若显式 batch_id 已被其他内容占用，抛 BatchIdConflictError（由调用方回滚）。
    """
    frames = sorted(set(frames_in))
    raw_frames = list(frames_in)  # 保留原始帧列表（含批内重复、保序），供撤销时重建
    digest = content_hash(roll_no, expected_start, expected_end, frames)

    # batch_id 若已被不同内容占用则拒绝（409 在 main 中处理）
    clash = conn.execute(
        "SELECT roll_no, content_hash FROM batches WHERE id = ?", (batch_id,)
    ).fetchone()
    if clash is not None and (
        clash["roll_no"] != roll_no or clash["content_hash"] != digest
    ):
        raise BatchIdConflictError(batch_id)

    existing_frames = _all_frames(conn, roll_no)

    within_submit: set[int] = set()
    dup_seen: set[int] = set()
    for f in frames_in:  # 未去重的原始列表，检出批内重复
        if f in dup_seen:
            within_submit.add(f)
        dup_seen.add(f)
    # 批内重复 + 该胶卷此前批次已扫到的帧
    duplicates = sorted(within_submit | (set(frames) & existing_frames))
    new_frames = sorted(set(frames) - existing_frames)

    conn.execute(
        """INSERT INTO batches
           (id, roll_no, expected_start, expected_end, frames_json,
            raw_frames_json, frames_merged_json, duplicates_json, content_hash)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            batch_id,
            roll_no,
            expected_start,
            expected_end,
            json.dumps(frames),
            json.dumps(raw_frames),
            json.dumps(new_frames),
            json.dumps(duplicates),
            digest,
        ),
    )
    conn.execute(
        "INSERT OR IGNORE INTO roll_expected (roll_no, start_no, end_no) VALUES (?, ?, ?)",
        (roll_no, expected_start, expected_end),
    )
    conn.executemany(
        "INSERT OR IGNORE INTO roll_frames (roll_no, frame_no, batch_id) VALUES (?, ?, ?)",
        [(roll_no, f, batch_id) for f in new_frames],
    )
    return batch_id, new_frames, duplicates


def submit_batch(conn: sqlite3.Connection, data: schemas.BatchIn) -> tuple[schemas.BatchResult, int]:
    """导入一个批次。返回 (结果, HTTP 状态码 201新建 / 200重复)。

    幂等保证：(roll_no, 内容指纹) 唯一。相同内容重复导入不写任何数据。
    """
    frames = sorted(set(data.frames))
    digest = content_hash(
        data.roll_no, data.expected_start, data.expected_end, frames
    )

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT id FROM batches WHERE roll_no = ? AND content_hash = ?",
            (data.roll_no, digest),
        ).fetchone()
        if existing is not None:
            conn.execute("ROLLBACK")
            result = get_batch(conn, existing["id"])
            assert result is not None
            return result, 200

        batch_id = data.batch_id or f"auto-{uuid.uuid4().hex}"
        batch_id, new_frames, duplicates = _import_batch_tx(
            conn,
            data.roll_no,
            data.expected_start,
            data.expected_end,
            data.frames,
            batch_id,
        )
        conn.execute("COMMIT")
    except BatchIdConflictError:
        conn.execute("ROLLBACK")
        raise
    except Exception:
        conn.execute("ROLLBACK")
        raise

    st = _state_from_db(conn, data.roll_no)
    result = schemas.BatchResult(
        batch_id=batch_id,
        roll_no=data.roll_no,
        repeated=False,
        added_frames=new_frames,
        duplicates=duplicates,
        merged_frames=sorted(st["frames"]),
        expected_ranges=[list(x) for x in st["expected"]],
        missing_ranges=[list(x) for x in st["missing"]],
        out_of_range_frames=st["out_of_range"],
    )
    return result, 201


# ---------- 批次撤销 ----------

class BatchNotFoundError(Exception):
    def __init__(self, batch_id: str):
        self.batch_id = batch_id
        super().__init__(f"批次不存在: {batch_id}")


class LegacyBatchDataError(Exception):
    """旧版库遗留批次缺少原始帧列表，撤销后无法区分批内与跨批重复。"""

    def __init__(self, legacy_batch_ids: list[str]):
        self.legacy_batch_ids = legacy_batch_ids
        super().__init__(
            f"存在无法区分批内/跨批重复的旧批次: {', '.join(legacy_batch_ids)}"
        )


def _within_batch_duplicates(raw_frames: list[int]) -> set[int]:
    """从原始帧列表中检出批内重复出现的帧号。"""
    seen: set[int] = set()
    dups: set[int] = set()
    for f in raw_frames:
        if f in seen:
            dups.add(f)
        seen.add(f)
    return dups


def delete_batch(conn: sqlite3.Connection, batch_id: str) -> None:
    """撤销一个误导入批次，全程在同一 SQLite 写事务内完成（失败整体回滚）。

    - 批次不存在：抛 BatchNotFoundError（接口返回 404），不触碰数据。
    - 删除目标批次后，按剩余批次的**原导入顺序**重建：
      预期范围集合（roll_expected）、帧归属（roll_frames，共享帧归最早扫到它
      的剩余批次，故共享帧不会随首个批次丢失）、各批新增帧与重复帧明细
      （批内重复保留；跨批重复按重建后的前缀集合重算）。
    - 若剩余批次中存在旧版数据（raw_frames_json 为 NULL，无法区分批内与
      跨批重复），抛 LegacyBatchDataError（接口返回 409）且不改变任何数据。
    - 最后一批撤销后胶卷视为不存在：清空其预期范围与帧，并把该胶卷全部
      未释放的历史认领标记为已释放，使其不阻碍同编号胶卷重新导入后的领取。
    """
    conn.execute("BEGIN IMMEDIATE")
    try:
        target = conn.execute(
            "SELECT id, roll_no FROM batches WHERE id = ?", (batch_id,)
        ).fetchone()
        if target is None:
            raise BatchNotFoundError(batch_id)
        roll_no = target["roll_no"]

        remaining = conn.execute(
            """SELECT id, expected_start, expected_end, raw_frames_json
                 FROM batches
                WHERE roll_no = ? AND id != ?
                ORDER BY created_at, rowid""",
            (roll_no, batch_id),
        ).fetchall()

        # 旧批次只有去重后的帧集合，无法重算“批内重复 + 跨批重复”明细
        legacy = [r["id"] for r in remaining if r["raw_frames_json"] is None]
        if legacy:
            raise LegacyBatchDataError(legacy)

        # 先清空帧归属再删批次，避免外键约束；随后按剩余批次整体重建
        conn.execute(
            "DELETE FROM roll_frames WHERE roll_no = ?", (roll_no,)
        )
        conn.execute("DELETE FROM batches WHERE id = ?", (batch_id,))
        conn.execute(
            "DELETE FROM roll_expected WHERE roll_no = ?", (roll_no,)
        )

        if not remaining:
            # 胶卷下再无批次：胶卷视为不存在。历史认领即时释放，
            # 同编号胶卷重新导入后不会被旧认领占住缺帧。
            conn.execute(
                """UPDATE rescan_claims
                      SET released_at = ?
                    WHERE roll_no = ? AND released_at IS NULL
                      AND completed_at IS NULL""",
                (_now_iso(), roll_no),
            )
            conn.execute("COMMIT")
            return

        conn.executemany(
            "INSERT OR IGNORE INTO roll_expected (roll_no, start_no, end_no) VALUES (?, ?, ?)",
            [(roll_no, r["expected_start"], r["expected_end"]) for r in remaining],
        )

        owned: set[int] = set()  # 按原导入顺序，此前批次已扫到的帧
        for r in remaining:
            raw_frames = json.loads(r["raw_frames_json"])
            frame_set = set(raw_frames)
            within_dup = _within_batch_duplicates(raw_frames)  # 批内重复保留
            new_frames = sorted(frame_set - owned)
            duplicates = sorted(within_dup | (frame_set & owned))

            conn.executemany(
                """INSERT OR IGNORE INTO roll_frames
                       (roll_no, frame_no, batch_id) VALUES (?, ?, ?)""",
                [(roll_no, f, r["id"]) for f in new_frames],
            )
            conn.execute(
                """UPDATE batches
                      SET frames_merged_json = ?, duplicates_json = ?
                    WHERE id = ?""",
                (json.dumps(new_frames), json.dumps(duplicates), r["id"]),
            )
            owned |= frame_set

        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------- 补扫任务认领 ----------

class RollNotFoundError(Exception):
    pass


class NoClaimableFramesError(Exception):
    pass


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _roll_exists(conn: sqlite3.Connection, roll_no: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM batches WHERE roll_no = ? LIMIT 1", (roll_no,)
        ).fetchone()
        is not None
    )


_ACTIVE_CLAIMS_SQL = """
    SELECT id, roll_no, claimant, frame_start, frame_end, max_frames,
           ttl_seconds, created_at, expires_at, released_at
      FROM rescan_claims
     WHERE roll_no = ?
       AND released_at IS NULL
       AND completed_at IS NULL
       AND expires_at > ?
     ORDER BY frame_start, id
"""


def _active_claim_rows(conn: sqlite3.Connection, roll_no: str) -> list[sqlite3.Row]:
    return conn.execute(_ACTIVE_CLAIMS_SQL, (roll_no, _now_iso())).fetchall()


def _claim_out(conn: sqlite3.Connection, r: sqlite3.Row) -> schemas.RescanClaimOut:
    """组装响应：认领帧段与当前缺帧区间取交，即该任务仍需补扫的帧段。"""
    st = _state_from_db(conn, r["roll_no"])
    remaining = intersect_intervals(
        st["missing"], [(r["frame_start"], r["frame_end"])]
    )
    return schemas.RescanClaimOut(
        id=r["id"],
        roll_no=r["roll_no"],
        claimant=r["claimant"],
        frame_start=r["frame_start"],
        frame_end=r["frame_end"],
        frame_count=r["frame_end"] - r["frame_start"] + 1,
        max_frames=r["max_frames"],
        ttl_seconds=r["ttl_seconds"],
        created_at=r["created_at"],
        expires_at=r["expires_at"],
        remaining_missing_ranges=[list(x) for x in remaining],
    )


def create_rescan_claim(
    conn: sqlite3.Connection, data: schemas.RescanClaimIn, roll_no: str
) -> schemas.RescanClaimOut:
    """原子领取一段缺帧。

    当前缺帧区间扣除所有未到期认领区间后，按帧号升序取首段长度不超过
    max_frames 的连续闭区间；整个过程在 BEGIN IMMEDIATE 写事务内完成，
    并发领取由 SQLite 写锁串行化，帧段不会交叠。
    """
    base = datetime.now(timezone.utc)
    now = base.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    expires = (base + timedelta(seconds=data.ttl_seconds)).strftime(
        "%Y-%m-%dT%H:%M:%S.%f"
    )[:-3] + "Z"

    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _roll_exists(conn, roll_no):
            raise RollNotFoundError(roll_no)

        st = _state_from_db(conn, roll_no)
        claimed = [
            (r["frame_start"], r["frame_end"])
            for r in _active_claim_rows(conn, roll_no)
        ]
        available = subtract_intervals(st["missing"], claimed)

        chosen: tuple[int, int] | None = None
        for s, e in available:  # 已按帧号升序
            if s <= e:  # subtract 已过滤空段，这里仅为类型检查友好
                chosen = (s, min(e, s + data.max_frames - 1))
                break
        if chosen is None:
            raise NoClaimableFramesError(roll_no)

        claim_id = uuid.uuid4().hex
        frame_start, frame_end = chosen
        conn.execute(
            """INSERT INTO rescan_claims
                   (id, roll_no, claimant, frame_start, frame_end,
                    max_frames, ttl_seconds, created_at, expires_at, released_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)""",
            (
                claim_id,
                roll_no,
                data.claimant,
                frame_start,
                frame_end,
                data.max_frames,
                data.ttl_seconds,
                now,
                expires,
            ),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    row = conn.execute(
        "SELECT * FROM rescan_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    assert row is not None
    return _claim_out(conn, row)


def list_rescan_claims(
    conn: sqlite3.Connection, roll_no: str
) -> list[schemas.RescanClaimOut] | None:
    """列出某胶卷全部未到期、未释放的认领任务。胶卷不存在返回 None。"""
    if not _roll_exists(conn, roll_no):
        return None
    rows = _active_claim_rows(conn, roll_no)
    return [_claim_out(conn, r) for r in rows]


def release_rescan_claim(conn: sqlite3.Connection, claim_id: str) -> bool:
    """释放（删除）认领任务；任务不存在、已释放、已到期或已结案均返回 False。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        cur = conn.execute(
            """UPDATE rescan_claims
                  SET released_at = ?
                WHERE id = ?
                  AND released_at IS NULL
                  AND completed_at IS NULL
                  AND expires_at > ?""",
            (_now_iso(), claim_id, _now_iso()),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return cur.rowcount > 0


# ---------- 补扫完成回执 ----------

class ClaimNotFoundError(Exception):
    def __init__(self, claim_id: str):
        self.claim_id = claim_id
        super().__init__(f"补扫任务不存在: {claim_id}")


class ClaimUnavailableError(Exception):
    """认领已到期或已释放，不能提交回执。"""


class ReceiptConflictError(Exception):
    """帧段越界、帧已不缺、回执内容变更或对已结案认领重复提交等冲突。"""


def _receipt_out(
    conn: sqlite3.Connection,
    claim: sqlite3.Row,
    receipt: sqlite3.Row,
    repeated: bool,
) -> schemas.RescanReceiptOut:
    """组装回执响应：remaining 为认领帧段与当前缺帧区间的交集（结案后仍可缩短）。"""
    st = _state_from_db(conn, claim["roll_no"])
    remaining = intersect_intervals(
        st["missing"], [(claim["frame_start"], claim["frame_end"])]
    )
    raw_frames = json.loads(receipt["raw_frames_json"])
    batch = conn.execute(
        "SELECT frames_merged_json, duplicates_json FROM batches WHERE id = ?",
        (receipt["batch_id"],),
    ).fetchone()
    if batch is not None:
        added_frames = json.loads(batch["frames_merged_json"])
        duplicates = json.loads(batch["duplicates_json"])
    else:  # 回执批次后来被撤销：以回执落库时的快照为准
        added_frames = json.loads(receipt["added_frames_json"])
        duplicates = json.loads(receipt["duplicates_json"])
    return schemas.RescanReceiptOut(
        id=receipt["id"],
        claim_id=claim["id"],
        roll_no=claim["roll_no"],
        claimant=claim["claimant"],
        frame_start=claim["frame_start"],
        frame_end=claim["frame_end"],
        batch_id=receipt["batch_id"],
        frames=raw_frames,
        added_frames=added_frames,
        duplicates=duplicates,
        repeated=repeated,
        remaining_missing_ranges=[list(x) for x in remaining],
        created_at=receipt["created_at"],
    )


def submit_rescan_receipt(
    conn: sqlite3.Connection, claim_id: str, data: schemas.RescanReceiptIn
) -> tuple[schemas.RescanReceiptOut, int]:
    """提交补扫完成回执，返回 (回执, 201首次 / 200幂等重试)。

    全部动作在同一 BEGIN IMMEDIATE 写事务内完成：以认领闭区间作为补扫批次
    的预期范围导入批次、持久化回执、将认领置为已结案；任一步失败整体回滚，
    数据不变。校验规则：

    - 认领记录不存在 → ClaimNotFoundError（404）；
    - 已到期或已释放 → ClaimUnavailableError（409）；
    - 每帧必须落在认领闭区间 [frame_start, frame_end] 内，且提交时仍属于
      该胶卷缺帧 → ReceiptConflictError（409）；
    - 相同认领、batch_id、原始帧列表的重试：返回已存回执（200），不重复导入；
    - 内容变更（batch_id 或原始帧列表不同）、对已结案认领再提交新回执、
      batch_id 已被其他回执/批次占用 → ReceiptConflictError（409）。

    部分补扫时只导入实际扫到的帧，认领结案后帧段内剩余缺帧不再被其占用，
    立即可被其他人重新认领。
    """
    claim_id = claim_id.strip()
    frames_in = list(data.frames)
    batch_id = data.batch_id
    now = _now_iso()

    conn.execute("BEGIN IMMEDIATE")
    try:
        claim = conn.execute(
            "SELECT * FROM rescan_claims WHERE id = ?", (claim_id,)
        ).fetchone()
        if claim is None:
            raise ClaimNotFoundError(claim_id)

        existing_receipt = conn.execute(
            "SELECT * FROM rescan_receipts WHERE claim_id = ?", (claim_id,)
        ).fetchone()

        # 幂等优先：相同认领 + 相同 batch_id + 相同原始帧列表 → 回已存回执
        if existing_receipt is not None:
            if (
                existing_receipt["batch_id"] == batch_id
                and json.loads(existing_receipt["raw_frames_json"]) == frames_in
            ):
                conn.execute("ROLLBACK")
                out = _receipt_out(conn, claim, existing_receipt, repeated=True)
                return out, 200
            # 对已结案认领提交不同内容（含新 batch_id）→ 冲突，数据不变
            raise ReceiptConflictError("该认领已结案，回执内容不可变更")

        # 尚无回执但已到期 / 已释放 / 已结案 → 不能再提交
        if claim["released_at"] is not None or claim["completed_at"] is not None:
            raise ClaimUnavailableError("补扫任务已释放或已结案")
        if claim["expires_at"] <= now:
            raise ClaimUnavailableError("补扫任务已到期")

        # 帧号须在认领闭区间内
        out_of_claim = [
            f
            for f in set(frames_in)
            if not (claim["frame_start"] <= f <= claim["frame_end"])
        ]
        if out_of_claim:
            raise ReceiptConflictError(
                f"帧号超出认领区间 [{claim['frame_start']}, {claim['frame_end']}]: "
                + ", ".join(map(str, sorted(out_of_claim)))
            )

        # 提交时每帧仍须属于该胶卷缺帧（未被其他批次扫到）
        already = sorted(set(frames_in) & _all_frames(conn, claim["roll_no"]))
        if already:
            raise ReceiptConflictError(
                "以下帧已不属于该胶卷缺帧（已被扫到）: " + ", ".join(map(str, already))
            )

        # batch_id 已被其他回执绑定 → 冲突
        receipt_clash = conn.execute(
            "SELECT claim_id FROM rescan_receipts WHERE batch_id = ?", (batch_id,)
        ).fetchone()
        if receipt_clash is not None:
            raise ReceiptConflictError(
                f"batch_id '{batch_id}' 已用于其他补扫回执"
            )

        # 同一写事务内：以认领闭区间作为批次预期范围导入补扫批次
        _, new_frames, duplicates = _import_batch_tx(
            conn,
            claim["roll_no"],
            claim["frame_start"],
            claim["frame_end"],
            frames_in,
            batch_id,
        )

        receipt_id = uuid.uuid4().hex
        conn.execute(
            """INSERT INTO rescan_receipts
                   (id, claim_id, roll_no, batch_id, raw_frames_json,
                    added_frames_json, duplicates_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                receipt_id,
                claim_id,
                claim["roll_no"],
                batch_id,
                json.dumps(frames_in),
                json.dumps(new_frames),
                json.dumps(duplicates),
                now,
            ),
        )
        # 认领结案：帧段中剩余缺帧立即让出，可被重新认领
        conn.execute(
            "UPDATE rescan_claims SET completed_at = ? WHERE id = ?",
            (now, claim_id),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    claim = conn.execute(
        "SELECT * FROM rescan_claims WHERE id = ?", (claim_id,)
    ).fetchone()
    receipt = conn.execute(
        "SELECT * FROM rescan_receipts WHERE id = ?", (receipt_id,)
    ).fetchone()
    assert claim is not None and receipt is not None
    return _receipt_out(conn, claim, receipt, repeated=False), 201


# ---------- 交付快照 ----------

class SnapshotNotFoundError(Exception):
    """快照不存在，或快照不属于 URL 中的胶卷编号。"""

    def __init__(self, snapshot_id: str):
        self.snapshot_id = snapshot_id
        super().__init__(f"快照不存在或不属于该胶卷: {snapshot_id}")


def create_snapshot(
    conn: sqlite3.Connection, roll_no: str
) -> schemas.RollSnapshotOut:
    """在一致性读中固化胶卷当前状态（预期闭区间、实扫帧、缺帧闭区间）。

    BEGIN IMMEDIATE 取写锁使本次读取与并发导入/撤销串行化，快照内容即提交
    瞬间的一致状态。快照只新增、从不修改：后续批次导入、撤销与补扫都不会
    影响其内容。胶卷不存在（尚未导入任何批次）抛 RollNotFoundError（404）。
    """
    snapshot_id = uuid.uuid4().hex
    now = _now_iso()

    conn.execute("BEGIN IMMEDIATE")
    try:
        if not _roll_exists(conn, roll_no):
            raise RollNotFoundError(roll_no)
        st = _state_from_db(conn, roll_no)
        conn.execute(
            """INSERT INTO roll_snapshots
                   (id, roll_no, expected_json, frames_json, missing_json, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                snapshot_id,
                roll_no,
                json.dumps([list(x) for x in st["expected"]]),
                json.dumps(sorted(st["frames"])),
                json.dumps([list(x) for x in st["missing"]]),
                now,
            ),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return schemas.RollSnapshotOut(
        snapshot_id=snapshot_id,
        roll_no=roll_no,
        expected_ranges=[list(x) for x in st["expected"]],
        frames=sorted(st["frames"]),
        missing_ranges=[list(x) for x in st["missing"]],
        created_at=now,
    )


def get_snapshot_diff(
    conn: sqlite3.Connection, roll_no: str, snapshot_id: str
) -> schemas.SnapshotDiffOut:
    """计算胶卷**当前**状态相对快照的差异。

    三个维度（预期覆盖、实扫帧、缺帧）独立比较，各自给出相对快照新增
    （当前有、快照无）与消失（快照有、当前无）的闭区间，全程按区间做集合
    运算，不逐帧展开大范围。

    - 快照不存在（未知 ID）或其胶卷编号与 URL 不一致 → SnapshotNotFoundError（404）；
    - 即使该胶卷最后一批已被撤销（当前视为不存在），只要快照属于该编号，
      仍返回 200：roll_exists=false，当前状态按空集合处理，差异即“快照内容
      全部消失”。
    """
    snap = conn.execute(
        "SELECT * FROM roll_snapshots WHERE id = ?", (snapshot_id,)
    ).fetchone()
    if snap is None or snap["roll_no"] != roll_no:
        raise SnapshotNotFoundError(snapshot_id)

    snap_expected = [tuple(x) for x in json.loads(snap["expected_json"])]
    snap_frames = [tuple(x) for x in frames_to_ranges(json.loads(snap["frames_json"]))]
    snap_missing = [tuple(x) for x in json.loads(snap["missing_json"])]

    # 一致性读：差异计算期间不允许并发写入改变三个维度的相互对应关系
    conn.execute("BEGIN")
    try:
        if _roll_exists(conn, roll_no):
            roll_exists = True
            st = _state_from_db(conn, roll_no)
            cur_expected = st["expected"]
            cur_frames = frames_to_ranges(st["frames"])
            cur_missing = st["missing"]
        else:
            # 胶卷最后一批已被撤销：快照仍有效，当前按空状态比较
            roll_exists = False
            cur_expected = []
            cur_frames = []
            cur_missing = []
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return schemas.SnapshotDiffOut(
        snapshot_id=snapshot_id,
        roll_no=roll_no,
        roll_exists=roll_exists,
        snapshot_created_at=snap["created_at"],
        compared_at=_now_iso(),
        expected=_range_diff(cur_expected, snap_expected),
        frames=_range_diff(cur_frames, snap_frames),
        missing=_range_diff(cur_missing, snap_missing),
    )


# ---------- 帧号扫描溯源 ----------

class ProvenanceRangeError(Exception):
    """溯源查询区间非法：end < start。"""


def frame_provenance(
    conn: sqlite3.Connection,
    roll_no: str,
    start: int,
    end: int,
    limit: int,
    cursor: int | None,
) -> schemas.FrameProvenanceOut:
    """按帧号升序分页追溯实际存在的帧由哪些有效批次扫描。

    - 只遍历真实存在的帧（roll_frames 主键索引 + frame_no 键集分页），
      不逐帧展开 [start, end] 的跨度，故区间跨度巨大时成本只与命中帧数、
      limit 及该胶卷批次数相关；
    - 续页以 frame_no > cursor 取下界，cursor 即上一页末帧号；cursor 落后于
      start 时下界回退为 start（start 帧不会被跳过），cursor 超出 end 时
      返回空页而非逐帧遍历；
    - 每帧给出当前归属（最早有效批次）、参与批次（按批次原导入顺序）及
      各批原始列表中的出现次数（旧批次无原始列表时为 null，不推断）；
    - 撤销批次后 roll_frames 与各批原始列表均已重建，本函数实时计算，
      故首扫归属会随撤销转移；
    - end < start 抛 ProvenanceRangeError（接口返回 422）；
      胶卷不存在（无任何批次）抛 RollNotFoundError（接口返回 404）。
    """
    if end < start:
        raise ProvenanceRangeError()
    # frame_no > lower：无游标时下界为 start-1（含 start）；
    # 游标落后于 start 时同样从 start 起，不跳过 start 帧
    lower = start - 1 if cursor is None else max(cursor, start - 1)

    # 只读事务中一致性读取帧集合、帧归属、批次原始列表与预期范围
    conn.execute("BEGIN")
    try:
        if not _roll_exists(conn, roll_no):
            raise RollNotFoundError(roll_no)

        # 键集分页：取 limit+1 判定是否有下一页；仅扫描实际存在的帧
        frame_rows = conn.execute(
            """SELECT frame_no, batch_id
                 FROM roll_frames
                WHERE roll_no = ? AND frame_no > ? AND frame_no <= ?
                ORDER BY frame_no
                LIMIT ?""",
            (roll_no, lower, end, limit + 1),
        ).fetchall()

        batch_rows = conn.execute(
            """SELECT id, raw_frames_json, frames_json
                 FROM batches
                WHERE roll_no = ?
                ORDER BY created_at, rowid""",
            (roll_no,),
        ).fetchall()

        expected = _expected_ranges(conn, roll_no)
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    owner_of: dict[int, str] = {r["frame_no"]: r["batch_id"] for r in frame_rows}
    page_frames = sorted(owner_of)
    has_more = len(page_frames) > limit
    if has_more:
        page_frames = page_frames[:limit]

    # 每帧 -> 参与批次（按批次原导入顺序）；旧批次只知去重集合，计数为 None
    participation: dict[int, list[schemas.FrameBatchRef]] = {}
    wanted = set(page_frames)
    for br in batch_rows:
        raw_json = br["raw_frames_json"]
        if raw_json is not None:
            counts: dict[int, int] = {}
            for f in json.loads(raw_json):
                counts[f] = counts.get(f, 0) + 1
        else:
            counts = None
        batch_frames = (
            set(counts) if counts is not None else set(json.loads(br["frames_json"]))
        )
        for f in wanted & batch_frames:
            occurrence = counts.get(f) if counts is not None else None
            participation.setdefault(f, []).append(
                schemas.FrameBatchRef(
                    batch_id=br["id"], occurrence_count=occurrence
                )
            )

    items: list[schemas.FrameProvenanceItem] = []
    for f in page_frames:
        items.append(
            schemas.FrameProvenanceItem(
                frame_no=f,
                first_scan_batch_id=owner_of[f],
                in_expected_coverage=any(s <= f <= e for s, e in expected),
                batches=participation.get(f, []),
            )
        )

    return schemas.FrameProvenanceOut(
        roll_no=roll_no,
        frames=items,
        next_cursor=page_frames[-1] if has_more and page_frames else None,
    )
