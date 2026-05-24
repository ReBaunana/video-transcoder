"""Background face-recognition worker thread (no Celery, no Redis)."""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Callable

from app.face import extractor as extractor_mod
from app.face import matcher as matcher_mod
from app.face.model import reset_face_app

log = logging.getLogger(__name__)

_stop_event = threading.Event()
_worker_thread: threading.Thread | None = None
_stats_lock = threading.Lock()
_stats = {"done": 0, "failed": 0, "started_at": 0}

POLL_INTERVAL_SEC = 3.0
MAX_ATTEMPTS = 3
RESET_AFTER_N_JOBS = 50  # mitigate ONNX VRAM fragmentation


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #

def start_worker(conn_factory: Callable[[], sqlite3.Connection]) -> None:
    """Start the background worker thread.

    conn_factory: callable that returns a fresh sqlite3.Connection
    (SQLite connections are not safe to share across threads).
    """
    global _worker_thread
    if _worker_thread is not None and _worker_thread.is_alive():
        log.debug("start_worker: already running")
        return
    _stop_event.clear()
    with _stats_lock:
        _stats["started_at"] = int(time.time())
    _worker_thread = threading.Thread(
        target=_worker_loop,
        args=(conn_factory,),
        name="face-worker",
        daemon=True,
    )
    _worker_thread.start()
    log.info("Face worker started")


def stop_worker() -> None:
    """Signal the worker to exit at the next iteration."""
    _stop_event.set()
    log.info("Face worker stop requested")


def get_worker_status() -> dict:
    """Return current worker stats. Queue counts are best-effort (no conn here)."""
    running = bool(_worker_thread and _worker_thread.is_alive() and not _stop_event.is_set())
    with _stats_lock:
        return {
            "running": running,
            "queued": _stats.get("queued", 0),
            "done": _stats.get("done", 0),
            "failed": _stats.get("failed", 0),
            "started_at": _stats.get("started_at", 0),
        }


def refresh_queue_count(conn: sqlite3.Connection) -> int:
    """Update the cached 'queued' stat using a caller-provided connection."""
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM face_recognition_job WHERE status IN ('pending','running')")
        n = int(cur.fetchone()[0])
    except Exception:
        log.exception("refresh_queue_count failed")
        n = 0
    with _stats_lock:
        _stats["queued"] = n
    return n


def enqueue_job(
    conn: sqlite3.Connection,
    file_curation_id: int,
    job_type: str,
    priority: int = 100,
) -> bool:
    """Add a job; ignore if already queued/running for this file+type."""
    if job_type not in ("seed_known", "match_unknown"):
        raise ValueError(f"enqueue_job: invalid job_type={job_type!r}")

    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT 1 FROM face_recognition_job
             WHERE file_curation_id = ?
               AND job_type = ?
               AND status IN ('pending', 'running')
             LIMIT 1
            """,
            (file_curation_id, job_type),
        )
        if cur.fetchone() is not None:
            return False

        cur.execute(
            """
            INSERT INTO face_recognition_job
                (file_curation_id, job_type, status, priority, attempts,
                 last_error, enqueued_at, started_at, finished_at)
            VALUES (?, ?, 'pending', ?, 0, NULL, ?, NULL, NULL)
            """,
            (file_curation_id, job_type, priority, int(time.time())),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        log.exception(
            "enqueue_job failed file_id=%s type=%s",
            file_curation_id, job_type,
        )
        return False


def enqueue_all_unknown(conn: sqlite3.Connection) -> int:
    """Enqueue match_unknown jobs for all eligible files."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT fc.id
              FROM file_curation fc
             WHERE fc.status NOT IN ('skipped', 'unknown')
               AND NOT EXISTS (
                   SELECT 1 FROM face_match_result mr
                    WHERE mr.file_curation_id = fc.id AND mr.status = 'accepted'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM face_recognition_job j
                    WHERE j.file_curation_id = fc.id
                      AND j.job_type = 'match_unknown'
                      AND j.status IN ('pending', 'running')
               )
            """
        )
        ids = [int(r[0]) for r in cur.fetchall()]
    except Exception:
        log.exception("enqueue_all_unknown: select failed")
        return 0

    now = int(time.time())
    enqueued = 0
    try:
        for fid in ids:
            cur.execute(
                """
                INSERT INTO face_recognition_job
                    (file_curation_id, job_type, status, priority, attempts,
                     last_error, enqueued_at, started_at, finished_at)
                VALUES (?, 'match_unknown', 'pending', 100, 0, NULL, ?, NULL, NULL)
                """,
                (fid, now),
            )
            enqueued += 1
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("enqueue_all_unknown: insert failed")
        return enqueued

    log.info("enqueue_all_unknown: enqueued=%d", enqueued)
    return enqueued


def enqueue_seed_for_performer(conn: sqlite3.Connection, performer_id: int) -> int:
    """Enqueue seed_known jobs for all single-performer files of this performer."""
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT fp.file_curation_id
              FROM file_performer fp
              JOIN file_curation fc ON fc.id = fp.file_curation_id
             WHERE fp.performer_id = ?
               AND fp.source IN ('auto', 'manual')
               AND (
                   SELECT COUNT(*) FROM file_performer fp2
                    WHERE fp2.file_curation_id = fp.file_curation_id
               ) = 1
               AND NOT EXISTS (
                   SELECT 1 FROM face_recognition_job j
                    WHERE j.file_curation_id = fp.file_curation_id
                      AND j.job_type = 'seed_known'
                      AND j.status IN ('pending', 'running')
               )
            """,
            (performer_id,),
        )
        ids = [int(r[0]) for r in cur.fetchall()]
    except Exception:
        log.exception("enqueue_seed_for_performer: select failed performer_id=%s", performer_id)
        return 0

    now = int(time.time())
    enqueued = 0
    try:
        for fid in ids:
            cur.execute(
                """
                INSERT INTO face_recognition_job
                    (file_curation_id, job_type, status, priority, attempts,
                     last_error, enqueued_at, started_at, finished_at)
                VALUES (?, 'seed_known', 'pending', 10, 0, NULL, ?, NULL, NULL)
                """,
                (fid, now),
            )
            enqueued += 1
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("enqueue_seed_for_performer: insert failed performer_id=%s", performer_id)
        return enqueued

    log.info(
        "enqueue_seed_for_performer: performer_id=%s enqueued=%d",
        performer_id, enqueued,
    )
    return enqueued


# --------------------------------------------------------------------------- #
# Internal                                                                    #
# --------------------------------------------------------------------------- #

def _row_to_job_dict(row: sqlite3.Row | tuple) -> dict:
    keys = (
        "id", "file_curation_id", "job_type", "status", "priority",
        "attempts", "last_error", "enqueued_at", "started_at", "finished_at",
    )
    if isinstance(row, sqlite3.Row):
        return {k: row[k] for k in row.keys()}
    return dict(zip(keys, row))


def _claim_next_job(conn: sqlite3.Connection) -> dict | None:
    """Atomically claim next pending job (priority asc, enqueued_at asc).

    Uses UPDATE ... RETURNING (SQLite >= 3.35, bundled with Python 3.11+).
    Falls back to a SELECT+UPDATE under IMMEDIATE transaction if RETURNING is
    unavailable.
    """
    now = int(time.time())
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE face_recognition_job
               SET status = 'running',
                   started_at = ?,
                   attempts = attempts + 1
             WHERE id = (
                 SELECT id FROM face_recognition_job
                  WHERE status = 'pending'
                  ORDER BY priority ASC, enqueued_at ASC
                  LIMIT 1
             )
            RETURNING id, file_curation_id, job_type, status, priority,
                      attempts, last_error, enqueued_at, started_at, finished_at
            """,
            (now,),
        )
        row = cur.fetchone()
        conn.commit()
        if row is None:
            return None
        return _row_to_job_dict(row)
    except sqlite3.OperationalError:
        # Fallback for older SQLite.
        conn.rollback()
        try:
            conn.execute("BEGIN IMMEDIATE")
            cur.execute(
                """
                SELECT id, file_curation_id, job_type, priority, attempts
                  FROM face_recognition_job
                 WHERE status = 'pending'
                 ORDER BY priority ASC, enqueued_at ASC
                 LIMIT 1
                """
            )
            row = cur.fetchone()
            if row is None:
                conn.commit()
                return None
            job_id = int(row[0])
            cur.execute(
                """
                UPDATE face_recognition_job
                   SET status = 'running', started_at = ?, attempts = attempts + 1
                 WHERE id = ? AND status = 'pending'
                """,
                (now, job_id),
            )
            if cur.rowcount == 0:
                conn.commit()
                return None
            cur.execute(
                """
                SELECT id, file_curation_id, job_type, status, priority,
                       attempts, last_error, enqueued_at, started_at, finished_at
                  FROM face_recognition_job
                 WHERE id = ?
                """,
                (job_id,),
            )
            full = cur.fetchone()
            conn.commit()
            return _row_to_job_dict(full)
        except Exception:
            conn.rollback()
            log.exception("_claim_next_job fallback failed")
            return None


def _mark_done(conn: sqlite3.Connection, job_id: int) -> None:
    try:
        conn.execute(
            "UPDATE face_recognition_job SET status='done', finished_at=?, last_error=NULL WHERE id=?",
            (int(time.time()), job_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("_mark_done failed job_id=%s", job_id)


def _mark_failed(conn: sqlite3.Connection, job_id: int, attempts: int, err: str) -> None:
    status = "failed" if attempts >= MAX_ATTEMPTS else "pending"
    finished = int(time.time()) if status == "failed" else None
    try:
        conn.execute(
            """
            UPDATE face_recognition_job
               SET status = ?,
                   finished_at = ?,
                   last_error = ?
             WHERE id = ?
            """,
            (status, finished, err[:1000], job_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("_mark_failed update failed job_id=%s", job_id)


def _load_file_context(conn: sqlite3.Connection, file_curation_id: int) -> dict | None:
    """Resolve absolute video path + duration for a file_curation row.

    Tries common column names; if `duration_sec` is missing, returns 0.0 and
    lets the extractor handle the empty-frame case.
    """
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA table_info(file_curation)")
        cols = {row[1] for row in cur.fetchall()}
    except Exception:
        log.exception("_load_file_context: PRAGMA failed")
        return None

    duration_col = next(
        (c for c in ("duration_sec", "duration", "duration_s") if c in cols),
        None,
    )
    select_cols = ["path", "mount"]
    if duration_col:
        select_cols.append(duration_col)

    try:
        cur.execute(
            f"SELECT {', '.join(select_cols)} FROM file_curation WHERE id = ?",
            (file_curation_id,),
        )
        row = cur.fetchone()
    except Exception:
        log.exception("_load_file_context: select failed file_id=%s", file_curation_id)
        return None

    if row is None:
        return None

    path, mount = row[0], row[1]
    duration = float(row[2]) if duration_col and row[2] is not None else 0.0

    # Resolve to absolute path under /media/{mount}/ if relative.
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = Path("/media") / str(mount) / path
    return {"path": str(candidate), "duration": duration}


def _process_job(conn: sqlite3.Connection, job: dict) -> None:
    """Dispatch a single job; raises on failure."""
    job_type = job["job_type"]
    file_id = int(job["file_curation_id"])

    ctx = _load_file_context(conn, file_id)
    if ctx is None:
        raise RuntimeError(f"file_curation row missing for id={file_id}")

    path = ctx["path"]
    duration = ctx["duration"]
    if not Path(path).exists():
        raise FileNotFoundError(f"video not found: {path}")
    if duration <= 0.0:
        # Best-effort probe via ffprobe; the extractor still handles empty results.
        duration = _probe_duration(path) or 0.0
        if duration <= 0.0:
            raise RuntimeError(f"unable to determine duration for {path}")

    if job_type == "seed_known":
        # Pick the single performer attached to this file.
        cur = conn.cursor()
        cur.execute(
            """
            SELECT performer_id FROM file_performer
             WHERE file_curation_id = ?
            """,
            (file_id,),
        )
        rows = cur.fetchall()
        if len(rows) != 1:
            raise RuntimeError(
                f"seed_known requires exactly one performer, got {len(rows)} for file_id={file_id}"
            )
        performer_id = int(rows[0][0])
        stored = extractor_mod.process_video_for_seeding(
            conn, file_id, performer_id, path, duration,
        )
        if stored > 0:
            try:
                matcher_mod.get_index().reload(conn)
            except Exception:
                log.exception("worker: index reload failed after seeding")

    elif job_type == "match_unknown":
        faces = extractor_mod.process_video_for_matching(conn, file_id, path, duration)
        if faces:
            matcher_mod.match_video(conn, file_id, faces)

    else:
        raise ValueError(f"unknown job_type: {job_type!r}")


def _probe_duration(path: str) -> float | None:
    """Best-effort ffprobe of duration in seconds."""
    import subprocess
    try:
        proc = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                path,
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
        if proc.returncode != 0:
            return None
        return float(proc.stdout.strip() or 0.0)
    except Exception:
        log.debug("_probe_duration failed for %s", path, exc_info=True)
        return None


def _worker_loop(conn_factory: Callable[[], sqlite3.Connection]) -> None:
    """Main worker loop."""
    log.info("face-worker loop entered")
    jobs_since_reset = 0

    # Warm the index once.
    try:
        conn = conn_factory()
        try:
            matcher_mod.get_index().load(conn)
            refresh_queue_count(conn)
        finally:
            conn.close()
    except Exception:
        log.exception("face-worker: initial index load failed")

    while not _stop_event.is_set():
        conn = None
        try:
            conn = conn_factory()
            try:
                conn.row_factory = sqlite3.Row
            except Exception:
                pass

            job = _claim_next_job(conn)
            if job is None:
                refresh_queue_count(conn)
                conn.close()
                conn = None
                # Sleep in small slices so stop_event is responsive.
                end = time.monotonic() + POLL_INTERVAL_SEC
                while time.monotonic() < end and not _stop_event.is_set():
                    time.sleep(0.25)
                continue

            job_id = int(job["id"])
            try:
                _process_job(conn, job)
                _mark_done(conn, job_id)
                with _stats_lock:
                    _stats["done"] = _stats.get("done", 0) + 1
                log.info(
                    "face-worker: job done id=%s type=%s file_id=%s",
                    job_id, job["job_type"], job["file_curation_id"],
                )
            except Exception as exc:
                log.exception(
                    "face-worker: job FAILED id=%s type=%s file_id=%s",
                    job_id, job.get("job_type"), job.get("file_curation_id"),
                )
                _mark_failed(conn, job_id, int(job.get("attempts", 1)), repr(exc))
                with _stats_lock:
                    _stats["failed"] = _stats.get("failed", 0) + 1

            refresh_queue_count(conn)
            jobs_since_reset += 1

            if jobs_since_reset >= RESET_AFTER_N_JOBS:
                log.info(
                    "face-worker: resetting InsightFace after %d jobs (VRAM defrag)",
                    jobs_since_reset,
                )
                try:
                    reset_face_app()
                except Exception:
                    log.exception("face-worker: reset_face_app failed")
                jobs_since_reset = 0

        except Exception:
            log.exception("face-worker: unexpected loop error; sleeping briefly")
            time.sleep(1.0)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    log.info("face-worker loop exiting")
