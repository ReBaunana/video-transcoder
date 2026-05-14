import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path('/data/transcoder.db')

_SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    path         TEXT    NOT NULL,
    filename     TEXT    NOT NULL,
    mount        TEXT,
    src_codec    TEXT,
    src_size     INTEGER,
    dest_size    INTEGER,
    elapsed_s    REAL,
    started_at   TEXT,
    finished_at  TEXT,
    status       TEXT CHECK(status IN ('running','done','failed','skipped')),
    error        TEXT
);
CREATE INDEX IF NOT EXISTS idx_jobs_status  ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_started ON jobs(started_at);

-- Avoids re-running ffprobe on unchanged files between scan runs.
-- Key: path + size + mtime. On a cache hit the probe is skipped entirely.
CREATE TABLE IF NOT EXISTS file_cache (
    path     TEXT    PRIMARY KEY,
    size     INTEGER NOT NULL,
    mtime    REAL    NOT NULL,
    codec    TEXT    NOT NULL,
    duration REAL    NOT NULL DEFAULT 0
);
"""


def init(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.executescript(_SCHEMA)
    conn.commit()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def record_start(conn, path, filename, src_codec, src_size, mount) -> int:
    cur = conn.execute(
        "INSERT INTO jobs (path,filename,mount,src_codec,src_size,started_at,status) VALUES (?,?,?,?,?,?,?)",
        (path, filename, mount, src_codec, src_size,
         datetime.now(timezone.utc).isoformat(), 'running'),
    )
    conn.commit()
    return cur.lastrowid


def record_finish(conn, job_id, status, dest_size, elapsed_s, error=None):
    conn.execute(
        "UPDATE jobs SET status=?,dest_size=?,elapsed_s=?,finished_at=?,error=? WHERE id=?",
        (status, dest_size, elapsed_s,
         datetime.now(timezone.utc).isoformat(), error, job_id),
    )
    conn.commit()


def get_stats(conn) -> dict:
    row = conn.execute("""
        SELECT
            SUM(CASE WHEN status='done'   THEN 1   ELSE 0 END) AS done,
            SUM(CASE WHEN status='failed' THEN 1   ELSE 0 END) AS failed,
            SUM(CASE WHEN status='done'   THEN elapsed_s  ELSE 0 END) AS total_elapsed_s,
            SUM(CASE WHEN status='done'   THEN src_size   ELSE 0 END) AS total_src_bytes,
            SUM(CASE WHEN status='done'   THEN dest_size  ELSE 0 END) AS total_dest_bytes
        FROM jobs
    """).fetchone()
    return dict(row) if row else {}


def get_codec_stats(conn) -> list:
    rows = conn.execute("""
        SELECT src_codec, COUNT(*) AS cnt
        FROM jobs WHERE status='done'
        GROUP BY src_codec ORDER BY cnt DESC
    """).fetchall()
    return [dict(r) for r in rows]


def cache_get(conn, path: str, size: int, mtime: float) -> dict | None:
    row = conn.execute(
        "SELECT codec, duration FROM file_cache WHERE path=? AND size=? AND mtime=?",
        (path, size, mtime),
    ).fetchone()
    return dict(row) if row else None


def cache_set(conn, path: str, size: int, mtime: float, codec: str, duration: float = 0.0):
    conn.execute(
        "INSERT OR REPLACE INTO file_cache (path,size,mtime,codec,duration) VALUES (?,?,?,?,?)",
        (path, size, mtime, codec, duration),
    )
    conn.commit()


def get_mount_stats(conn) -> list:
    rows = conn.execute("""
        SELECT
            mount,
            SUM(CASE WHEN status='done'   THEN 1 ELSE 0 END) AS done,
            SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status='done'   THEN src_size  ELSE 0 END) AS src_bytes,
            SUM(CASE WHEN status='done'   THEN dest_size ELSE 0 END) AS dest_bytes
        FROM jobs
        WHERE mount IS NOT NULL AND mount != ''
        GROUP BY mount ORDER BY mount
    """).fetchall()
    return [dict(r) for r in rows]


def get_recent_jobs(conn, limit: int = 50) -> list:
    rows = conn.execute("""
        SELECT id,filename,mount,src_codec,src_size,dest_size,elapsed_s,started_at,status,error
        FROM jobs ORDER BY id DESC LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in rows]
