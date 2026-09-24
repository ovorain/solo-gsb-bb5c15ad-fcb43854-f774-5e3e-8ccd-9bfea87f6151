"""SQLite 连接管理与数据库初始化（建表）。"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path


def get_db_path() -> Path:
    return Path(os.environ.get("DB_PATH", "/data/film.db"))


def _connect(db_path: Path | None = None) -> sqlite3.Connection:
    path = db_path or get_db_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(path),
        timeout=float(os.environ.get("DB_TIMEOUT", "30")),
        check_same_thread=False,
        isolation_level=None,  # 显式 BEGIN/COMMIT，避免隐式事务
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
    id              TEXT PRIMARY KEY,
    roll_no         TEXT NOT NULL,
    expected_start  INTEGER NOT NULL,
    expected_end    INTEGER NOT NULL,
    frames_json     TEXT NOT NULL,
    raw_frames_json TEXT,
    frames_merged_json TEXT NOT NULL DEFAULT '[]',
    duplicates_json TEXT NOT NULL DEFAULT '[]',
    content_hash    TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    UNIQUE (roll_no, content_hash)
);

CREATE INDEX IF NOT EXISTS idx_batches_roll ON batches (roll_no);

CREATE TABLE IF NOT EXISTS roll_frames (
    roll_no  TEXT NOT NULL,
    frame_no INTEGER NOT NULL,
    batch_id TEXT NOT NULL,
    PRIMARY KEY (roll_no, frame_no),
    FOREIGN KEY (batch_id) REFERENCES batches (id)
);

CREATE TABLE IF NOT EXISTS roll_expected (
    roll_no  TEXT NOT NULL,
    start_no INTEGER NOT NULL,
    end_no   INTEGER NOT NULL,
    PRIMARY KEY (roll_no, start_no, end_no)
);

CREATE TABLE IF NOT EXISTS rescan_claims (
    id           TEXT PRIMARY KEY,
    roll_no      TEXT NOT NULL,
    claimant     TEXT NOT NULL,
    frame_start  INTEGER NOT NULL,
    frame_end    INTEGER NOT NULL,
    max_frames   INTEGER NOT NULL,
    ttl_seconds  INTEGER NOT NULL,
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    released_at  TEXT,
    completed_at TEXT,
    CHECK (frame_end >= frame_start),
    CHECK (max_frames  > 0),
    CHECK (ttl_seconds > 0)
);

CREATE INDEX IF NOT EXISTS idx_claims_roll ON rescan_claims (roll_no);

-- 补扫完成回执：一个认领至多一条回执（结案），一个补扫批次号至多绑定一条回执
CREATE TABLE IF NOT EXISTS rescan_receipts (
    id                TEXT PRIMARY KEY,
    claim_id          TEXT NOT NULL,
    roll_no           TEXT NOT NULL,
    batch_id          TEXT NOT NULL,
    raw_frames_json   TEXT NOT NULL,
    added_frames_json TEXT NOT NULL,
    duplicates_json   TEXT NOT NULL,
    created_at        TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_claim
    ON rescan_receipts (claim_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_batch
    ON rescan_receipts (batch_id);

-- 交付快照：创建时固化该胶卷的预期闭区间、实扫帧（保序去重集合）与缺帧闭区间。
-- 此后批次导入、撤销或补扫均不修改快照内容；最后一批被撤销后快照仍可查询，
-- 此时以空状态与快照做差异对比。
CREATE TABLE IF NOT EXISTS roll_snapshots (
    id              TEXT PRIMARY KEY,
    roll_no         TEXT NOT NULL,
    expected_json   TEXT NOT NULL,
    frames_json     TEXT NOT NULL,
    missing_json    TEXT NOT NULL,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX IF NOT EXISTS idx_snapshots_roll ON roll_snapshots (roll_no);
"""


def init_db() -> None:
    """幂等建表：每次启动执行，已存在的库不受影响。

    旧版库（没有 raw_frames_json 列）通过 ALTER TABLE 平滑升级；
    旧批次该列为 NULL，撤销时据此判定能否安全重建（详见 services.delete_batch）。
    旧版 rescan_claims 没有 completed_at 列，同样幂等补齐（补扫回执结案标记）。
    """
    conn = _connect()
    try:
        conn.executescript(SCHEMA)
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(batches)").fetchall()}
        if "raw_frames_json" not in cols:
            conn.execute("ALTER TABLE batches ADD COLUMN raw_frames_json TEXT")
        claim_cols = {
            r["name"] for r in conn.execute("PRAGMA table_info(rescan_claims)").fetchall()
        }
        if "completed_at" not in claim_cols:
            conn.execute("ALTER TABLE rescan_claims ADD COLUMN completed_at TEXT")
    finally:
        conn.close()


@contextmanager
def get_conn_ctx():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def get_db():
    """FastAPI 依赖：每请求一个连接。"""
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()
