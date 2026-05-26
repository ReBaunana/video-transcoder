# app/database_curation.py
"""Curation schema additions for the video transcoder DB.

Adds performer, file_curation, face_embedding, face_match_result,
face_recognition_job, and rename_log tables alongside the existing
jobs/file_cache tables in /data/transcoder.db.

All functions take a sqlite3.Connection as first argument and assume
the caller manages transactions unless documented otherwise.
"""

from __future__ import annotations

import re
import sqlite3
import unicodedata
from typing import Any


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_CURATION_SCHEMA = """
CREATE TABLE IF NOT EXISTS performer (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    canonical_name TEXT NOT NULL UNIQUE,
    slug TEXT NOT NULL UNIQUE,
    embedding_count INTEGER NOT NULL DEFAULT 0,
    is_reference_ready INTEGER NOT NULL DEFAULT 0,
    profile_thumb TEXT,
    gender TEXT NOT NULL DEFAULT 'unknown',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS performer_alias (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    performer_id INTEGER NOT NULL REFERENCES performer(id) ON DELETE CASCADE,
    alias TEXT NOT NULL,
    alias_slug TEXT NOT NULL UNIQUE
);

CREATE INDEX IF NOT EXISTS idx_alias_slug ON performer_alias(alias_slug);

CREATE TABLE IF NOT EXISTS file_curation (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL UNIQUE,
    mount TEXT,
    studio TEXT,
    title TEXT,
    release_date TEXT,
    resolution TEXT,
    extraction_method TEXT,
    extraction_confidence REAL NOT NULL DEFAULT 0.0,
    status TEXT NOT NULL DEFAULT 'pending',
    proposed_filename TEXT,
    user_notes TEXT,
    reviewed_at TEXT,
    renamed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fc_status ON file_curation(status);
CREATE INDEX IF NOT EXISTS idx_fc_path ON file_curation(path);
CREATE INDEX IF NOT EXISTS idx_fc_mount ON file_curation(mount);

CREATE TABLE IF NOT EXISTS file_performer (
    file_curation_id INTEGER NOT NULL REFERENCES file_curation(id) ON DELETE CASCADE,
    performer_id INTEGER NOT NULL REFERENCES performer(id) ON DELETE CASCADE,
    position INTEGER NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT 'auto',
    PRIMARY KEY (file_curation_id, performer_id)
);

CREATE INDEX IF NOT EXISTS idx_fp_performer ON file_performer(performer_id);
CREATE INDEX IF NOT EXISTS idx_fp_file ON file_performer(file_curation_id);

CREATE TABLE IF NOT EXISTS face_embedding (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    performer_id INTEGER REFERENCES performer(id) ON DELETE CASCADE,
    file_curation_id INTEGER REFERENCES file_curation(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    embedding BLOB NOT NULL,
    det_score REAL NOT NULL,
    bbox TEXT NOT NULL,
    frame_time_sec REAL,
    thumbnail_path TEXT,
    quality_score REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_fe_performer
    ON face_embedding(performer_id) WHERE performer_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_fe_file ON face_embedding(file_curation_id);

CREATE TABLE IF NOT EXISTS face_match_result (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_curation_id INTEGER NOT NULL REFERENCES file_curation(id) ON DELETE CASCADE,
    performer_id INTEGER NOT NULL REFERENCES performer(id) ON DELETE CASCADE,
    similarity REAL NOT NULL,
    match_count INTEGER NOT NULL,
    total_faces INTEGER NOT NULL DEFAULT 0,
    rank INTEGER NOT NULL DEFAULT 1,
    status TEXT NOT NULL DEFAULT 'pending',
    sample_thumb_id INTEGER REFERENCES face_embedding(id),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT,
    UNIQUE(file_curation_id, performer_id)
);

CREATE INDEX IF NOT EXISTS idx_fmr_file ON face_match_result(file_curation_id, rank);
CREATE INDEX IF NOT EXISTS idx_fmr_status ON face_match_result(status);

CREATE TABLE IF NOT EXISTS face_recognition_job (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_curation_id INTEGER NOT NULL UNIQUE REFERENCES file_curation(id) ON DELETE CASCADE,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    priority INTEGER NOT NULL DEFAULT 100,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    enqueued_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_frj_status
    ON face_recognition_job(status, priority, enqueued_at);

CREATE TABLE IF NOT EXISTS rename_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_curation_id INTEGER NOT NULL REFERENCES file_curation(id),
    from_path TEXT NOT NULL,
    to_path TEXT NOT NULL,
    executed_at TEXT NOT NULL DEFAULT (datetime('now')),
    success INTEGER NOT NULL DEFAULT 0,
    error_message TEXT,
    rolled_back_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_rl_file ON rename_log(file_curation_id);
"""


# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

def init_curation(conn: sqlite3.Connection) -> None:
    """Apply curation schema. Idempotent."""
    conn.execute("PRAGMA foreign_keys = ON")
    conn.executescript(_CURATION_SCHEMA)
    # Migrations for existing DBs.
    try:
        conn.execute("ALTER TABLE performer ADD COLUMN gender TEXT NOT NULL DEFAULT 'unknown'")
        conn.commit()
    except Exception:
        pass  # column already exists
    conn.commit()


# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------

_SLUG_STRIP_RE = re.compile(r"[^a-z0-9]+")


def to_slug(name: str) -> str:
    """Normalize a display name to a kebab-case slug.

    'Riley Reid' -> 'riley-reid'
    'Angela_White' -> 'angela-white'
    'Juniper-Ren' -> 'juniper-ren'
    'Renée Pérez' -> 'renee-perez'
    """
    if not name:
        return ""
    # Strip accents
    decomposed = unicodedata.normalize("NFKD", name)
    ascii_only = "".join(c for c in decomposed if not unicodedata.combining(c))
    lowered = ascii_only.lower()
    slug = _SLUG_STRIP_RE.sub("-", lowered).strip("-")
    return slug


# ---------------------------------------------------------------------------
# Performer ops
# ---------------------------------------------------------------------------

def get_or_create_performer(conn: sqlite3.Connection, canonical_name: str) -> int:
    """Return performer id matching slug(canonical_name), creating row if missing."""
    canonical_name = (canonical_name or "").strip()
    if not canonical_name:
        raise ValueError("canonical_name must be non-empty")

    slug = to_slug(canonical_name)
    if not slug:
        raise ValueError(f"canonical_name {canonical_name!r} produces empty slug")

    row = conn.execute(
        "SELECT id FROM performer WHERE slug = ?", (slug,)
    ).fetchone()
    if row is not None:
        return int(row[0])

    # Try alias lookup
    row = conn.execute(
        "SELECT performer_id FROM performer_alias WHERE alias_slug = ?", (slug,)
    ).fetchone()
    if row is not None:
        return int(row[0])

    cur = conn.execute(
        """
        INSERT INTO performer (canonical_name, slug)
        VALUES (?, ?)
        """,
        (canonical_name, slug),
    )
    conn.commit()
    return int(cur.lastrowid)


# ---------------------------------------------------------------------------
# File curation ops
# ---------------------------------------------------------------------------

_FILE_CURATION_UPDATABLE = {
    "mount",
    "studio",
    "title",
    "release_date",
    "resolution",
    "extraction_method",
    "extraction_confidence",
    "status",
    "proposed_filename",
    "user_notes",
    "reviewed_at",
    "renamed_at",
    "tpdb_scene_id",
    "tpdb_lookup_at",
}


def upsert_file_curation(
    conn: sqlite3.Connection, path: str, mount: str, **kwargs: Any
) -> int:
    """Insert a file_curation row for `path` or update existing one.

    Only keys in _FILE_CURATION_UPDATABLE are honoured from kwargs.
    `mount` is always written. Returns the row id.
    """
    if not path:
        raise ValueError("path must be non-empty")

    row = conn.execute(
        "SELECT id, status FROM file_curation WHERE path = ?", (path,)
    ).fetchone()

    # Filter kwargs to known columns
    filtered = {k: v for k, v in kwargs.items() if k in _FILE_CURATION_UPDATABLE}

    if row is None:
        cols = ["path", "mount"] + list(filtered.keys())
        placeholders = ", ".join("?" for _ in cols)
        values = [path, mount] + [filtered[k] for k in filtered.keys()]
        cur = conn.execute(
            f"INSERT INTO file_curation ({', '.join(cols)}) VALUES ({placeholders})",
            values,
        )
        conn.commit()
        return int(cur.lastrowid)

    file_id = int(row[0])
    existing_status = row[1]

    # Don't clobber terminal states with 'pending'
    if (
        filtered.get("status") == "pending"
        and existing_status in {"renamed", "skipped", "approved", "reviewed"}
    ):
        filtered.pop("status", None)

    # Mount can change if file moved; always set it explicitly
    filtered["mount"] = mount

    if not filtered:
        return file_id

    set_clause = ", ".join(f"{k} = ?" for k in filtered.keys())
    set_clause += ", updated_at = datetime('now')"
    values = list(filtered.values()) + [file_id]
    conn.execute(
        f"UPDATE file_curation SET {set_clause} WHERE id = ?",
        values,
    )
    conn.commit()
    return file_id


# ---------------------------------------------------------------------------
# Stats / listings
# ---------------------------------------------------------------------------

def _row_to_dict(cursor: sqlite3.Cursor, row: tuple) -> dict:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


def get_curation_stats(conn: sqlite3.Connection) -> dict:
    """Return counts keyed by status plus total."""
    cur = conn.execute(
        "SELECT status, COUNT(*) FROM file_curation GROUP BY status"
    )
    by_status: dict[str, int] = {}
    total = 0
    for status, count in cur.fetchall():
        by_status[status or "unknown"] = int(count)
        total += int(count)
    return {"total": total, "by_status": by_status}


def get_library_stats(conn: sqlite3.Connection) -> list[dict]:
    """Per-mount counts: total, pending, approved, renamed, skipped."""
    cur = conn.execute(
        """
        SELECT
            mount,
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'pending'  THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN status = 'reviewed' THEN 1 ELSE 0 END) AS reviewed,
            SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
            SUM(CASE WHEN status = 'renamed'  THEN 1 ELSE 0 END) AS renamed,
            SUM(CASE WHEN status = 'skipped'  THEN 1 ELSE 0 END) AS skipped,
            SUM(CASE WHEN status = 'unknown'  THEN 1 ELSE 0 END) AS unknown
        FROM file_curation
        WHERE mount IS NOT NULL
        GROUP BY mount
        ORDER BY mount
        """
    )
    out: list[dict] = []
    for row in cur.fetchall():
        out.append(
            {
                "mount": row[0],
                "total": int(row[1] or 0),
                "pending": int(row[2] or 0),
                "reviewed": int(row[3] or 0),
                "approved": int(row[4] or 0),
                "renamed": int(row[5] or 0),
                "skipped": int(row[6] or 0),
                "unknown": int(row[7] or 0),
            }
        )
    return out


def get_performers_list(
    conn: sqlite3.Connection, limit: int = 200, offset: int = 0
) -> list[dict]:
    """Return all performers ordered by name with their video count."""
    cur = conn.execute(
        """
        SELECT
            p.id,
            p.canonical_name,
            p.slug,
            COALESCE(p.profile_thumb,
                (SELECT fe.thumbnail_path
                   FROM face_embedding fe
                  WHERE fe.performer_id = p.id
                    AND fe.thumbnail_path IS NOT NULL
                  ORDER BY COALESCE(fe.quality_score, 0) DESC
                  LIMIT 1)
            ) AS profile_thumb,
            p.embedding_count,
            p.is_reference_ready,
            (SELECT COUNT(*) FROM file_performer fp WHERE fp.performer_id = p.id)
                AS video_count
        FROM performer p
        ORDER BY p.canonical_name COLLATE NOCASE
        LIMIT ? OFFSET ?
        """,
        (int(limit), int(offset)),
    )
    return [_row_to_dict(cur, row) for row in cur.fetchall()]


def get_performer_videos(
    conn: sqlite3.Connection, performer_id: int
) -> list[dict]:
    """All file_curation rows linked to performer_id, plus match confidence."""
    cur = conn.execute(
        """
        SELECT
            fc.id,
            fc.path,
            fc.mount,
            fc.studio,
            fc.title,
            fc.release_date,
            fc.resolution,
            fc.status,
            fc.proposed_filename,
            fp.position,
            fp.source,
            fmr.similarity AS match_similarity,
            fmr.status     AS match_status
        FROM file_performer fp
        JOIN file_curation fc ON fc.id = fp.file_curation_id
        LEFT JOIN face_match_result fmr
            ON fmr.file_curation_id = fc.id AND fmr.performer_id = fp.performer_id
        WHERE fp.performer_id = ?
        ORDER BY fc.release_date DESC, fc.title COLLATE NOCASE
        """,
        (int(performer_id),),
    )
    return [_row_to_dict(cur, row) for row in cur.fetchall()]


def get_file_face_matches(
    conn: sqlite3.Connection, file_curation_id: int
) -> list[dict]:
    """Face match candidates for a file ordered by rank."""
    cur = conn.execute(
        """
        SELECT
            fmr.id,
            fmr.performer_id,
            p.canonical_name,
            p.slug,
            p.profile_thumb,
            fmr.similarity,
            fmr.match_count,
            fmr.total_faces,
            fmr.rank,
            fmr.status,
            fmr.sample_thumb_id,
            fe.thumbnail_path AS sample_thumb_path,
            fmr.created_at,
            fmr.resolved_at
        FROM face_match_result fmr
        JOIN performer p ON p.id = fmr.performer_id
        LEFT JOIN face_embedding fe ON fe.id = fmr.sample_thumb_id
        WHERE fmr.file_curation_id = ?
        ORDER BY fmr.rank ASC, fmr.similarity DESC
        """,
        (int(file_curation_id),),
    )
    return [_row_to_dict(cur, row) for row in cur.fetchall()]


def list_files_for_mount(
    conn: sqlite3.Connection,
    mount: str,
    status_filter: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> list[dict]:
    """List files in a mount with joined performer names."""
    params: list[Any] = [mount]
    where = "WHERE fc.mount = ?"
    if status_filter:
        where += " AND fc.status = ?"
        params.append(status_filter)

    sql = f"""
        SELECT
            fc.id,
            fc.path,
            fc.mount,
            fc.studio,
            fc.title,
            fc.release_date,
            fc.resolution,
            fc.extraction_method,
            fc.extraction_confidence,
            fc.status,
            fc.proposed_filename,
            fc.user_notes,
            fc.reviewed_at,
            fc.renamed_at,
            fc.created_at,
            fc.updated_at,
            (
                SELECT GROUP_CONCAT(p.canonical_name, ', ')
                FROM file_performer fp
                JOIN performer p ON p.id = fp.performer_id
                WHERE fp.file_curation_id = fc.id
                ORDER BY fp.position
            ) AS performers
        FROM file_curation fc
        {where}
        ORDER BY fc.path
        LIMIT ? OFFSET ?
    """
    params.extend([int(limit), int(offset)])
    cur = conn.execute(sql, params)
    return [_row_to_dict(cur, row) for row in cur.fetchall()]


def get_pending_renames(
    conn: sqlite3.Connection, mount: str | None = None
) -> list[dict]:
    """Approved files with a proposed_filename, ready to be renamed."""
    params: list[Any] = []
    where = "WHERE fc.status = 'approved' AND fc.proposed_filename IS NOT NULL"
    if mount is not None:
        where += " AND fc.mount = ?"
        params.append(mount)

    sql = f"""
        SELECT
            fc.id,
            fc.path,
            fc.mount,
            fc.proposed_filename,
            fc.studio,
            fc.title,
            fc.release_date,
            fc.resolution
        FROM file_curation fc
        {where}
        ORDER BY fc.path
    """
    cur = conn.execute(sql, params)
    return [_row_to_dict(cur, row) for row in cur.fetchall()]
