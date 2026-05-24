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
_worker_threads: list[threading.Thread] = []
_stats_lock = threading.Lock()
_stats = {"done": 0, "failed": 0, "started_at": 0}

# Total jobs processed across all worker threads since the last VRAM reset.
# Protected by _stats_lock together with the reset trigger.
_jobs_since_reset = 0

POLL_INTERVAL_SEC = 3.0
MAX_ATTEMPTS = 3
RESET_AFTER_N_JOBS = 50  # mitigate ONNX VRAM fragmentation
DEFAULT_WORKERS = 2  # CPU extract + GPU detect overlap nicely with 2 threads


# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #

def start_worker(
    conn_factory: Callable[[], sqlite3.Connection],
    n_workers: int = DEFAULT_WORKERS,
) -> None:
    """Start N background worker threads sharing the same job queue.

    Each thread polls _claim_next_job, which is race-safe at the DB level
    (UPDATE ... WHERE id = (SELECT ... LIMIT 1) serializes claims).

    conn_factory: callable that returns a fresh sqlite3.Connection.
    Each thread gets its own connection — SQLite connections are not safe
    to share across threads.
    """
    global _worker_threads
    # Already running? Don't double-start.
    if any(t.is_alive() for t in _worker_threads):
        log.debug("start_worker: already running (%d threads)", len(_worker_threads))
        return

    n = max(1, int(n_workers))
    _stop_event.clear()
    with _stats_lock:
        _stats["started_at"] = int(time.time())

    _worker_threads = []
    for i in range(n):
        t = threading.Thread(
            target=_worker_loop,
            args=(conn_factory, i),
            name=f"face-worker-{i}",
            daemon=True,
        )
        t.start()
        _worker_threads.append(t)
    log.info("Face worker started (%d threads)", n)


def stop_worker() -> None:
    """Signal all worker threads to exit at the next poll iteration."""
    _stop_event.set()
    log.info("Face worker stop requested (%d threads)", len(_worker_threads))


def get_worker_status() -> dict:
    """Return current worker stats. Queue counts are best-effort (no conn here)."""
    running = (
        any(t.is_alive() for t in _worker_threads)
        and not _stop_event.is_set()
    )
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
            SELECT status FROM face_recognition_job
             WHERE file_curation_id = ?
             LIMIT 1
            """,
            (file_curation_id,),
        )
        row = cur.fetchone()
        if row is not None:
            existing_status = row[0]
            if existing_status in ("pending", "running"):
                return False
            # done/failed row exists — update it in place (UNIQUE constraint prevents INSERT).
            cur.execute(
                """
                UPDATE face_recognition_job
                   SET job_type = ?, status = 'pending', priority = ?,
                       attempts = 0, last_error = NULL,
                       enqueued_at = ?, started_at = NULL, finished_at = NULL
                 WHERE file_curation_id = ?
                """,
                (job_type, priority, int(time.time()), file_curation_id),
            )
            conn.commit()
            return True

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
    """Enqueue match_unknown jobs for all eligible files.

    Includes 'unknown' status files (TPDB couldn't identify them, but face
    recognition may still match performers). Excludes 'skipped' and 'renamed'.
    Skips files that already have an assigned performer or accepted face match.
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT fc.id
              FROM file_curation fc
             WHERE fc.status NOT IN ('skipped', 'renamed')
               AND NOT EXISTS (
                   SELECT 1 FROM face_match_result mr
                    WHERE mr.file_curation_id = fc.id AND mr.status = 'accepted'
               )
               AND NOT EXISTS (
                   SELECT 1 FROM file_performer fp
                    WHERE fp.file_curation_id = fc.id
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

    enqueued = 0
    for fid in ids:
        try:
            # Flip done/failed seed_known rows to match_unknown/pending in place;
            # the UNIQUE(file_curation_id) constraint blocks a plain INSERT.
            cur.execute(
                """
                UPDATE face_recognition_job
                   SET job_type = 'match_unknown', status = 'pending', priority = 100,
                       attempts = 0, last_error = NULL,
                       started_at = NULL, finished_at = NULL
                 WHERE file_curation_id = ?
                   AND status IN ('done', 'failed')
                """,
                (fid,),
            )
            if cur.rowcount:
                enqueued += 1
                continue
            cur.execute(
                """
                INSERT OR IGNORE INTO face_recognition_job
                    (file_curation_id, job_type, status, priority, attempts,
                     last_error, started_at, finished_at)
                VALUES (?, 'match_unknown', 'pending', 100, 0, NULL, NULL, NULL)
                """,
                (fid,),
            )
            if cur.rowcount:
                enqueued += 1
        except Exception:
            log.exception("enqueue_all_unknown: insert failed file_id=%s", fid)
    conn.commit()

    log.info("enqueue_all_unknown: enqueued=%d skipped=%d", enqueued, len(ids) - enqueued)
    return enqueued


def enqueue_all_seed_known(conn: sqlite3.Connection) -> int:
    """Enqueue seed_known jobs for single-performer files not yet seeded from video frames.

    Skips files that already have a completed or in-progress seed_known job.
    Priority 10 (runs before match_unknown at priority 100).
    """
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT fp.file_curation_id
              FROM file_performer fp
              JOIN file_curation fc ON fc.id = fp.file_curation_id
             WHERE fc.status NOT IN ('skipped', 'renamed')
               AND (
                   SELECT COUNT(*) FROM file_performer fp2
                    WHERE fp2.file_curation_id = fp.file_curation_id
               ) = 1
               AND NOT EXISTS (
                   SELECT 1 FROM face_recognition_job j
                    WHERE j.file_curation_id = fp.file_curation_id
                      AND j.job_type = 'seed_known'
                      AND j.status IN ('pending', 'running', 'done')
               )
            """
        )
        ids = [int(r[0]) for r in cur.fetchall()]
    except Exception:
        log.exception("enqueue_all_seed_known: select failed")
        return 0

    enqueued = 0
    for fid in ids:
        try:
            # The table has UNIQUE(file_curation_id) so a done/failed match_unknown
            # row blocks INSERT OR IGNORE. UPDATE it in place to seed_known/pending.
            cur.execute(
                """
                UPDATE face_recognition_job
                   SET job_type = 'seed_known', status = 'pending', priority = 10,
                       attempts = 0, last_error = NULL,
                       started_at = NULL, finished_at = NULL
                 WHERE file_curation_id = ?
                   AND status IN ('done', 'failed')
                """,
                (fid,),
            )
            if cur.rowcount:
                enqueued += 1
                continue
            # No done/failed row — try fresh insert (OR IGNORE skips if pending/running).
            cur.execute(
                """
                INSERT OR IGNORE INTO face_recognition_job
                    (file_curation_id, job_type, status, priority, attempts,
                     last_error, started_at, finished_at)
                VALUES (?, 'seed_known', 'pending', 10, 0, NULL, NULL, NULL)
                """,
                (fid,),
            )
            if cur.rowcount:
                enqueued += 1
        except Exception:
            log.exception("enqueue_all_seed_known: insert failed file_id=%s", fid)
    conn.commit()

    log.info("enqueue_all_seed_known: enqueued=%d skipped=%d", enqueued, len(ids) - enqueued)
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

    enqueued = 0
    for fid in ids:
        try:
            cur.execute(
                """
                UPDATE face_recognition_job
                   SET job_type = 'seed_known', status = 'pending', priority = 10,
                       attempts = 0, last_error = NULL,
                       started_at = NULL, finished_at = NULL
                 WHERE file_curation_id = ?
                   AND status IN ('done', 'failed')
                """,
                (fid,),
            )
            if cur.rowcount:
                enqueued += 1
                continue
            cur.execute(
                """
                INSERT OR IGNORE INTO face_recognition_job
                    (file_curation_id, job_type, status, priority, attempts,
                     last_error, started_at, finished_at)
                VALUES (?, 'seed_known', 'pending', 10, 0, NULL, NULL, NULL)
                """,
                (fid,),
            )
            if cur.rowcount:
                enqueued += 1
        except Exception:
            log.exception("enqueue_seed_for_performer: insert failed file_id=%s", fid)
    conn.commit()

    log.info(
        "enqueue_seed_for_performer: performer_id=%s enqueued=%d skipped=%d",
        performer_id, enqueued, len(ids) - enqueued,
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


def _worker_loop(
    conn_factory: Callable[[], sqlite3.Connection],
    worker_idx: int = 0,
) -> None:
    """Main worker loop. One instance per worker thread.

    Each thread keeps its own SQLite connection per job (re-opened on every
    poll iteration, matching the original single-threaded behavior).
    The VRAM-defrag reset counter is shared across all threads via
    _stats_lock to honor the global RESET_AFTER_N_JOBS budget.
    """
    global _jobs_since_reset
    log.info("face-worker loop entered (idx=%d)", worker_idx)

    # Only one thread should warm the index; the rest skip it.
    if worker_idx == 0:
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
                    "face-worker[%d]: job done id=%s type=%s file_id=%s",
                    worker_idx, job_id, job["job_type"], job["file_curation_id"],
                )
            except Exception as exc:
                log.exception(
                    "face-worker[%d]: job FAILED id=%s type=%s file_id=%s",
                    worker_idx, job_id, job.get("job_type"), job.get("file_curation_id"),
                )
                _mark_failed(conn, job_id, int(job.get("attempts", 1)), repr(exc))
                with _stats_lock:
                    _stats["failed"] = _stats.get("failed", 0) + 1

            refresh_queue_count(conn)

            # Shared reset counter across all threads. One thread (whichever
            # crosses the threshold first) performs the reset; the rest see
            # the counter back at 0 on the next iteration.
            should_reset = False
            with _stats_lock:
                _jobs_since_reset += 1
                if _jobs_since_reset >= RESET_AFTER_N_JOBS:
                    should_reset = True
                    _jobs_since_reset = 0

            if should_reset:
                log.info(
                    "face-worker[%d]: resetting InsightFace after %d total jobs (VRAM defrag)",
                    worker_idx, RESET_AFTER_N_JOBS,
                )
                # Hold the GPU lock so no other thread enters app.get() while
                # the singleton is being torn down and reloaded.
                try:
                    with extractor_mod._gpu_lock:
                        reset_face_app()
                except Exception:
                    log.exception("face-worker[%d]: reset_face_app failed", worker_idx)

        except Exception:
            log.exception(
                "face-worker[%d]: unexpected loop error; sleeping briefly",
                worker_idx,
            )
            time.sleep(1.0)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass

    log.info("face-worker loop exiting (idx=%d)", worker_idx)
