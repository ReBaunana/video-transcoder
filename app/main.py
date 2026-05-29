import asyncio
import logging
import os
import sqlite3
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from app.database import (
    init as db_init, backup as db_backup, reset_db, clean_jobs, clean_failed_jobs, BACKUP_DIR,
    DB_PATH,
    get_stats, get_codec_stats, get_recent_jobs, get_mount_stats,
    get_corrupt_files, delete_corrupt_cache_entries, get_cache_mount_stats,
    prune_stale_cache, delete_cache_entry, reset_corrupt_cache,
)
from app import transcoder

# Curation / performer routers — imported eagerly so route registration
# happens at import time and shows up in the OpenAPI schema.
from app.curation import routes as curation_routes
from app.performers import routes as performer_routes

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-8s  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

app       = FastAPI(title='Video Transcoder')
templates = Jinja2Templates(directory='app/templates')
db        = None

_ver_file   = Path(__file__).parent.parent / 'VERSION'
APP_VERSION = os.getenv('APP_VERSION') or (_ver_file.read_text().strip() if _ver_file.exists() else 'dev')

last_backup_at: str | None = None
last_prune_at:  str | None = None

# Face-recognition thumbnails are served from /data/face_thumbs at /static/faces.
# The directory must exist before StaticFiles is mounted, otherwise the mount
# raises RuntimeError at startup.
FACE_THUMB_DIR = Path('/data/face_thumbs')
FACE_THUMB_DIR.mkdir(parents=True, exist_ok=True)
app.mount('/static/faces', StaticFiles(directory=str(FACE_THUMB_DIR)), name='faces')

# Register the new routers. The curation/performer routes look up the shared
# DB connection via request.app.state.db (set during startup below).
app.include_router(curation_routes.router)
app.include_router(performer_routes.router)


@app.on_event('startup')
async def startup():
    global db, last_backup_at
    db = db_init()
    # Mark jobs left as 'running' by a previous crash
    db.execute(
        "UPDATE jobs SET status='failed', error='process killed', finished_at=? WHERE status='running'",
        (datetime.now().isoformat(),),
    )
    db.commit()
    transcoder.load_settings()
    transcoder.cleanup_leftover_temps()
    if BACKUP_DIR.exists():
        backups = sorted(BACKUP_DIR.glob('transcoder_*.db'))
        if backups:
            mtime = backups[-1].stat().st_mtime
            last_backup_at = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')

    # ── Curation schema + face worker wiring ─────────────────────────────
    # database_curation.init_curation is idempotent — it ALTERs / CREATEs only
    # what's missing, so safe to call on every boot.
    log = logging.getLogger('startup')
    try:
        from app.database_curation import init_curation
        init_curation(db)
    except Exception as exc:
        log.error(f'init_curation failed: {exc}')

    # Apply TPDB schema migration after the base schema is in place. The
    # migration is idempotent (it catches "duplicate column" errors).
    try:
        from app.curation.tpdb import migrate_tpdb
        migrate_tpdb(db)
    except Exception as exc:
        log.error(f'migrate_tpdb failed: {exc}')

    # Reset face jobs left in 'running' state by a previous crash so they are
    # retried instead of silently hanging forever.
    try:
        cur = db.execute(
            "UPDATE face_recognition_job SET status='pending', started_at=NULL WHERE status='running'"
        )
        if cur.rowcount:
            log.info('Reset %d stuck face recognition jobs to pending', cur.rowcount)
        db.commit()
    except Exception as exc:
        log.warning(f'Failed to reset stuck face jobs: {exc}')

    # Make the shared DB connection available to APIRouters via app.state.
    app.state.db = db

    # Ensure the face-thumb directory exists (also covered above, but harmless
    # to retry in case the volume was mounted late).
    FACE_THUMB_DIR.mkdir(parents=True, exist_ok=True)

    try:
        from app.face.worker import start_worker
        from app.face.model import is_face_rec_available
        if is_face_rec_available():
            db_path = DB_PATH

            def _conn_factory():
                conn = sqlite3.connect(str(db_path), check_same_thread=False)
                conn.row_factory = sqlite3.Row
                return conn

            start_worker(_conn_factory, n_workers=transcoder.FACE_WORKERS)
            log.info('Face worker started')

            # Seed performers + enqueue face jobs without blocking startup.
            threading.Thread(
                target=_startup_face_pipeline,
                args=(str(DB_PATH),),
                name='startup-face-pipeline',
                daemon=True,
            ).start()
            log.info('Startup face pipeline spawned')
        else:
            log.info('InsightFace not available — face recognition disabled')
    except ImportError as exc:
        log.info(f'Face stack unavailable ({exc}) — face recognition disabled')
    except Exception as exc:
        log.error(f'Face worker startup failed: {exc}')

    _start_scheduler()


def _startup_face_pipeline(db_path: str) -> None:
    """One-shot background task at startup: seed + enqueue all face jobs.

    Opens its own SQLite connection — never touches the shared `db` object.
    Runs seed_known before match_unknown so the index has references first.
    """
    _log = logging.getLogger('startup.face_pipeline')
    conn = None
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        try:
            from app.curation.tpdb import seed_performers_without_embeddings
            result = seed_performers_without_embeddings(conn, max_performers=100)
            _log.info('performer seed result: %s', result)
            # Reload index so workers can immediately use the newly seeded photo embeddings.
            from app.face.matcher import get_index
            get_index().reload(conn)
            _log.info('performer index reloaded after TPDB seeding: size=%d', get_index().size())
        except Exception:
            _log.exception('seed_performers_without_embeddings failed')

        try:
            from app.face.worker import enqueue_all_seed_known
            n = enqueue_all_seed_known(conn)
            _log.info('enqueued %d seed_known jobs', n)
        except Exception:
            _log.exception('enqueue_all_seed_known failed')

        try:
            from app.face.worker import enqueue_all_unknown
            n = enqueue_all_unknown(conn)
            _log.info('enqueued %d match_unknown jobs', n)
        except Exception:
            _log.exception('enqueue_all_unknown failed')

    except Exception:
        _log.exception('startup face pipeline crashed')
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _run_tpdb_batch(db_path: str) -> None:
    """Scheduled TPDB enrichment: process up to 100 un-enriched files per run."""
    _log = logging.getLogger('scheduler.tpdb')
    conn = None
    try:
        from app.curation.tpdb import enrich_file_from_tpdb, is_configured, seed_performers_without_embeddings
        if not is_configured():
            return

        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        rows = conn.execute(
            """
            SELECT id FROM file_curation
             WHERE tpdb_lookup_at IS NULL
               AND status NOT IN ('skipped', 'renamed')
             ORDER BY id LIMIT 100
            """
        ).fetchall()
        if not rows:
            _log.info('tpdb batch: no candidates')
            return

        applied = skipped = errors = 0
        for r in rows:
            try:
                res = enrich_file_from_tpdb(conn, int(r['id']))
                if res.get('applied'):
                    applied += 1
                else:
                    skipped += 1
            except Exception:
                _log.exception('tpdb batch: file_id=%s', r['id'])
                errors += 1

        _log.info('tpdb batch: total=%d applied=%d skipped=%d errors=%d',
                  len(rows), applied, skipped, errors)

        if applied:
            try:
                seed_performers_without_embeddings(conn, max_performers=100)
            except Exception:
                _log.exception('post-tpdb performer seed failed')
            try:
                from app.face.worker import enqueue_all_seed_known
                enqueue_all_seed_known(conn)
            except Exception:
                _log.exception('post-tpdb enqueue_all_seed_known failed')

    except Exception:
        _log.exception('_run_tpdb_batch crashed')
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _run_face_enqueue(db_path: str) -> None:
    """Scheduled sweep: enqueue seed_known + match_unknown for all eligible files."""
    _log = logging.getLogger('scheduler.face_enqueue')
    conn = None
    try:
        from app.face.model import is_face_rec_available
        if not is_face_rec_available():
            return
        from app.face.worker import enqueue_all_seed_known, enqueue_all_unknown

        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        n_seed = enqueue_all_seed_known(conn)
        _log.info('sweep: %d seed_known enqueued', n_seed)
        n_match = enqueue_all_unknown(conn)
        _log.info('sweep: %d match_unknown enqueued', n_match)

    except Exception:
        _log.exception('_run_face_enqueue crashed')
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _run_auto_rename(db_path: str) -> None:
    """Scheduled auto-approve and batch rename for ready files.

    1. Files enriched by TPDB already have proposed_filename + approved status set
       by the enrichment code. This step covers any stragglers (reviewed face
       matches whose proposed_filename was built by accept_match).
    2. Run execute_batch_rename for all approved files (up to 200 per run).
    """
    _log = logging.getLogger('scheduler.auto_rename')
    conn = None
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row

        # Reset any 'approved' files that have no proposed_filename (orphaned by
        # failed enrichment) — they can never be renamed and block the queue.
        orphan_cur = conn.execute(
            """UPDATE file_curation SET status = 'unknown', updated_at = datetime('now')
                WHERE status = 'approved' AND proposed_filename IS NULL"""
        )
        if orphan_cur.rowcount:
            conn.commit()
            _log.warning('auto_rename: reset %d orphaned approved files to unknown', orphan_cur.rowcount)

        # Batch-accept pending face matches that meet the auto-accept threshold.
        # Handles existing rows that were stored before the threshold was reached
        # and any matches processed while the app was offline.
        from app.face.matcher import accept_match as _accept_match, AUTO_ACCEPT_THRESHOLD as _AAT
        pending_fc_ids = [
            row[0] for row in conn.execute(
                "SELECT DISTINCT file_curation_id FROM face_match_result WHERE status = 'pending'"
            ).fetchall()
        ]
        batch_accepted = 0
        for fc_id in pending_fc_ids:
            candidates = conn.execute(
                """SELECT mr.id, mr.similarity, mr.match_count,
                          COALESCE(p.gender, 'unknown') AS gender
                     FROM face_match_result mr
                     JOIN performer p ON p.id = mr.performer_id
                    WHERE mr.file_curation_id = ? AND mr.status = 'pending'
                    ORDER BY CASE COALESCE(p.gender,'unknown')
                                 WHEN 'female'  THEN 0
                                 WHEN 'unknown' THEN 1
                                 ELSE 2
                             END ASC,
                             mr.match_count DESC, mr.similarity DESC""",
                (fc_id,),
            ).fetchall()
            if not candidates:
                continue
            primary = candidates[0]    # (id, similarity, match_count, gender)
            secondaries = candidates[1:]
            def _g_rank(g: str) -> int:
                return 0 if g == "female" else (2 if g == "male" else 1)
            same_tier = [s for s in secondaries if _g_rank(s[3]) <= _g_rank(primary[3])]
            top_is_dominant = (
                not same_tier
                or same_tier[0][2] < 0.7 * primary[2]
            )
            if primary[1] >= _AAT and top_is_dominant:
                try:
                    secondary_match_ids = [int(s[0]) for s in secondaries]
                    _accept_match(conn, primary[0],
                                  secondary_match_ids=secondary_match_ids or None)
                    batch_accepted += 1
                except Exception:
                    _log.exception('auto_rename: batch-accept failed fc_id=%s', fc_id)
        if batch_accepted:
            _log.info('auto_rename: batch-accepted %d pending face matches', batch_accepted)

        # Promote any file with proposed_filename + a performer that isn't yet
        # approved — covers pending files enriched before auto-approve was added
        # and reviewed files whose proposed_filename was rebuilt by accept_match.
        cur = conn.execute(
            """UPDATE file_curation SET status = 'approved'
                WHERE proposed_filename IS NOT NULL
                  AND status NOT IN ('approved', 'renamed', 'skipped')
                  AND EXISTS (SELECT 1 FROM file_performer fp WHERE fp.file_curation_id = id)"""
        )
        if cur.rowcount:
            conn.commit()
            _log.info('auto_rename: promoted %d files to approved', cur.rowcount)

        # Catch files that are 'reviewed' (face match accepted) but still lack
        # proposed_filename — rebuild it and approve them.
        stale = conn.execute(
            """SELECT fc.id, fc.studio, fc.release_date, fc.title, fc.resolution, fc.path
                 FROM file_curation fc
                WHERE fc.status = 'reviewed'
                  AND fc.proposed_filename IS NULL
                  AND EXISTS (SELECT 1 FROM file_performer fp WHERE fp.file_curation_id = fc.id)"""
        ).fetchall()
        if stale:
            from app.curation.tpdb import _rebuild_proposed_filename
            import os as _os
            for r in stale:
                performer_names = [
                    rr[0] for rr in conn.execute(
                        """SELECT p.canonical_name FROM file_performer fp
                             JOIN performer p ON p.id = fp.performer_id
                            WHERE fp.file_curation_id = ? ORDER BY fp.position""",
                        (r['id'],),
                    ).fetchall()
                ]
                if not performer_names:
                    continue
                ext = _os.path.splitext(str(r['path']))[1]
                proposed = _rebuild_proposed_filename(
                    studio=r['studio'], release_date=r['release_date'],
                    title=r['title'], performers=performer_names,
                    resolution=r['resolution'], ext=ext,
                )
                if proposed:
                    conn.execute(
                        "UPDATE file_curation SET proposed_filename=?, status='approved' WHERE id=?",
                        (proposed, r['id']),
                    )
            conn.commit()
            _log.info('auto_rename: rebuilt proposed_filename for %d reviewed files', len(stale))

        conn.commit()  # close any implicit transaction before execute_rename opens BEGIN IMMEDIATE
        from app.curation.rename import execute_batch_rename
        result = execute_batch_rename(conn, mount=None, limit=200, default_mount=transcoder.PERFORMER_DEFAULT_MOUNT)
        renamed = result.get('ok', 0)
        errors = result.get('errors') or []
        _log.info('auto_rename: renamed=%d failed=%d', renamed, result.get('failed', 0))
        for e in errors[:5]:
            _log.warning('auto_rename error: %s', e)

        # Re-queue files that are still uncertain after their first scan
        # (priority=100 done jobs with pending results → re-scan at priority=50).
        # Files still uncertain after the deep scan (priority=50 done) are left
        # for manual review — they will not be re-queued automatically again.
        try:
            from app.face.worker import enqueue_pending_rematch as _rematch
            n_requeued = _rematch(conn)
            if n_requeued:
                _log.info('auto_rename: re-queued %d files for deep rescan', n_requeued)
        except Exception:
            _log.exception('auto_rename: enqueue_pending_rematch failed')

    except Exception:
        _log.exception('_run_auto_rename crashed')
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _run_curation_scan(db_path: str, mount: str) -> None:
    """Scheduled discovery scan for an inbox mount (e.g. jdownloader).

    Walks /media/<mount>/ and upserts any new video files into file_curation.
    New files land as 'unknown' (opaque filename) or 'pending' (parseable).
    """
    _log = logging.getLogger('scheduler.curation_scan')
    conn = None
    try:
        from app.curation.extractor import scan_mount
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        mount_path = f'/media/{mount}'
        result = scan_mount(conn, mount_path, mount)
        conn.commit()
        _log.info(
            'curation scan %s: total=%d parsed=%d opaque=%d errors=%d',
            mount, result.get('total', 0), result.get('parsed', 0),
            result.get('opaque', 0), result.get('errors', 0),
        )
    except Exception:
        _log.exception('_run_curation_scan crashed (mount=%s)', mount)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


def _run_auto_match(db_path: str) -> None:
    """Scheduled filename-based performer matching for unknown files.

    Runs phase1 (existing performers) and phase2 (new performers from filename).
    Phase3 (face rec) is handled by _run_auto_rename; phase4 (re-enqueue) by
    the face worker sweep.
    """
    _log = logging.getLogger('scheduler.auto_match')
    conn = None
    try:
        from app.curation.auto_match import run_auto_match
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        result = run_auto_match(conn)
        _log.info(
            'auto_match: phase1=%d phase2=%d new_performers=%d',
            result['phase1'], result['phase2'], result['new_performers'],
        )
    except Exception:
        _log.exception('_run_auto_match crashed')
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


@app.on_event('shutdown')
async def shutdown():
    """Stop background workers cleanly so the container shuts down quickly."""
    log = logging.getLogger('shutdown')
    try:
        from app.face.worker import stop_worker
        stop_worker()
    except Exception as exc:
        log.warning(f'stop_worker failed: {exc}')


def _start_scheduler():
    def _loop():
        global last_backup_at, last_prune_at
        import time as _time
        log = logging.getLogger('scheduler')

        # Seed from most recent backup file so we don't back up immediately
        # if one is recent enough.
        last_backup_ts = 0.0
        if BACKUP_DIR.exists():
            backups = sorted(BACKUP_DIR.glob('transcoder_*.db'))
            if backups:
                last_backup_ts = backups[-1].stat().st_mtime

        last_prune_ts = 0.0
        last_tpdb_batch_ts = 0.0    # fire on first tick
        last_face_enqueue_ts = 0.0  # fire on first tick
        last_auto_rename_ts = 0.0   # fire on first tick
        last_curation_scan_ts = 0.0   # fire on first tick
        last_auto_match_ts = 0.0      # fire on first tick
        db_path_str = str(DB_PATH)

        while True:
            now    = datetime.now()
            now_ts = now.timestamp()

            interval_h = transcoder.BACKUP_INTERVAL_H
            if interval_h > 0 and (now_ts - last_backup_ts) >= interval_h * 3600:
                try:
                    path = db_backup(db, keep=transcoder.BACKUP_KEEP)
                    last_backup_ts = now_ts
                    last_backup_at = now.strftime('%Y-%m-%d %H:%M')
                    log.info(f'Scheduled backup → {path.name}')
                except Exception as e:
                    log.error(f'Backup failed: {e}')

            prune_interval_h = transcoder.PRUNE_INTERVAL_H
            if prune_interval_h > 0 and (now_ts - last_prune_ts) >= prune_interval_h * 3600:
                if not transcoder.state['running']:
                    try:
                        pruned = prune_stale_cache(db)
                        last_prune_ts = now_ts
                        last_prune_at = now.strftime('%Y-%m-%d %H:%M')
                        transcoder.state['last_prune_at'] = last_prune_at
                        log.info(f'Scheduled prune: {pruned} stale cache entries removed')
                    except Exception as e:
                        log.error(f'Prune failed: {e}')

            # TPDB auto-batch: enrich un-enriched files every 30 min.
            if (now_ts - last_tpdb_batch_ts) >= 30 * 60:
                last_tpdb_batch_ts = now_ts
                threading.Thread(
                    target=_run_tpdb_batch,
                    args=(db_path_str,),
                    name='sched-tpdb-batch',
                    daemon=True,
                ).start()

            # Face sweep: seed + match all eligible files every 60 min.
            if (now_ts - last_face_enqueue_ts) >= 60 * 60:
                last_face_enqueue_ts = now_ts
                threading.Thread(
                    target=_run_face_enqueue,
                    args=(db_path_str,),
                    name='sched-face-enqueue',
                    daemon=True,
                ).start()

            # Auto-approve and rename files with known performers every 30 min.
            if (now_ts - last_auto_rename_ts) >= 30 * 60:
                last_auto_rename_ts = now_ts
                threading.Thread(
                    target=_run_auto_rename,
                    args=(db_path_str,),
                    name='sched-auto-rename',
                    daemon=True,
                ).start()

            # Curation scan: discover new files in jdownloader every 60 min.
            if (now_ts - last_curation_scan_ts) >= 60 * 60:
                last_curation_scan_ts = now_ts
                threading.Thread(
                    target=_run_curation_scan,
                    args=(db_path_str, 'jdownloader'),
                    name='sched-curation-scan',
                    daemon=True,
                ).start()

            # Auto-match: assign performers to unknown files via filename every 60 min.
            if (now_ts - last_auto_match_ts) >= 60 * 60:
                last_auto_match_ts = now_ts
                threading.Thread(
                    target=_run_auto_match,
                    args=(db_path_str,),
                    name='sched-auto-match',
                    daemon=True,
                ).start()

            if transcoder.SCHEDULE_HOUR >= 0 and now.hour == transcoder.SCHEDULE_HOUR and now.minute == 0:
                if not transcoder.state['running']:
                    log.info('Scheduled scan starting')
                    transcoder.start_scan(db)
                _time.sleep(61)
            else:
                _time.sleep(30)

    threading.Thread(target=_loop, daemon=True, name='scheduler').start()


# ── Sysinfo helpers ───────────────────────────────────────────────────────────

_cpu_prev: tuple | None = None
_net_prev: dict | None = None   # {iface: (rx_bytes, tx_bytes)}
_net_prev_ts: float = 0.0

# Override with NET_IFACE=eno1 (comma-separated) to pin specific interfaces.
# Leave unset to auto-detect all physical-looking interfaces.
_NET_IFACE_OVERRIDE: list[str] = [
    i.strip() for i in os.getenv('NET_IFACE', '').split(',') if i.strip()
]

# Path to proc/net/dev — can be overridden via PROC_NET_DEV env var so that a
# bind-mount of the host's /proc/net into /host/proc/net works without
# network_mode: host.  Falls back to /proc/net/dev (correct when host network
# is used, or for single-host installs without Docker).
_PROC_NET_DEV = Path(os.getenv('PROC_NET_DEV', '/proc/net/dev'))


def _is_physical_iface(name: str) -> bool:
    """Return True for interfaces that look like real NICs.

    Keeps: eno*, enp*, ens*, eth*, wlan*, wlp*, wls*, bond*, team*
    Drops: lo, docker*, veth*, br-*, virbr*, tun*, tap*, macvlan*, dummy*,
           tailscale*, flannel*, cali*, cilium*, ovs-*, vxlan*, sit*
    """
    if name == 'lo':
        return False
    _drop = ('docker', 'veth', 'br-', 'virbr', 'tun', 'tap',
             'dummy', 'flannel', 'cali', 'cilium', 'ovs',
             'vxlan', 'sit', 'gre', 'ipip', 'macvlan-',)
    if any(name.startswith(p) for p in _drop):
        return False
    # tailscale / wireguard / zerotier look like "tailscale0", "wg0", "zt*"
    if name.startswith(('tailscale', 'wg', 'zt')):
        return False
    return True


def _read_cpu_pct() -> float:
    global _cpu_prev
    try:
        parts = Path('/proc/stat').read_text().split('\n', 1)[0].split()
        vals = [int(x) for x in parts[1:9]]   # user nice system idle iowait irq softirq steal
        idle = vals[3] + vals[4]
        total = sum(vals)
        if _cpu_prev is None:
            _cpu_prev = (idle, total)
            return 0.0
        d_idle = idle - _cpu_prev[0]
        d_total = total - _cpu_prev[1]
        _cpu_prev = (idle, total)
        if d_total == 0:
            return 0.0
        return round(max(0.0, min(100.0, (1 - d_idle / d_total) * 100)), 1)
    except Exception:
        return 0.0


def _read_mem_pct() -> float:
    try:
        info = {}
        for line in Path('/proc/meminfo').read_text().splitlines():
            k, v = line.split(':', 1)
            info[k.strip()] = int(v.split()[0])
        total = info.get('MemTotal', 0)
        avail = info.get('MemAvailable', 0)
        if total == 0:
            return 0.0
        return round((total - avail) / total * 100, 1)
    except Exception:
        return 0.0


def _read_gpu_pct() -> float:
    try:
        r = subprocess.run(
            ['nvidia-smi', '--query-gpu=utilization.gpu', '--format=csv,noheader,nounits'],
            capture_output=True, text=True, timeout=3,
        )
        if r.returncode == 0:
            return float(r.stdout.strip().split('\n')[0])
    except Exception:
        pass
    return -1.0


# Intel iGPU stats — computed from /proc/<pid>/fdinfo DRM engine counters.
# i915 fdinfo exports cumulative nanoseconds per engine for each DRM client.
# We aggregate across all ffmpeg processes (same uid = readable without privileges)
# and compute a delta-over-time utilisation percentage every 2 seconds.
# 0.0 = idle (no ffmpeg active), -1.0 = never sampled yet (bar hidden in UI)
_intel_stats: dict[str, float] = {'video': -1.0, 'render': -1.0}
_intel_stats_lock = threading.Lock()

import glob as _glob
import re as _re

_DRM_ENGINE_RE = _re.compile(r'^drm-engine-(\w[\w-]*):\s+(\d+)\s+ns', _re.MULTILINE)


def _read_drm_fdinfo() -> dict[str, int]:
    """Aggregate cumulative DRM engine ns across all ffmpeg /proc/<pid>/fd/* entries."""
    totals: dict[str, int] = {}
    for comm_path in _glob.glob('/proc/*/comm'):
        try:
            pid = comm_path.split('/')[2]
            with open(comm_path) as f:
                if 'ffmpeg' not in f.read():
                    continue
            for fd_link in _glob.glob(f'/proc/{pid}/fd/*'):
                try:
                    target = os.readlink(fd_link)
                    if not target.startswith('/dev/dri/'):
                        continue
                    fd_num = os.path.basename(fd_link)
                    with open(f'/proc/{pid}/fdinfo/{fd_num}') as f:
                        for m in _DRM_ENGINE_RE.finditer(f.read()):
                            name = m.group(1)
                            totals[name] = totals.get(name, 0) + int(m.group(2))
                except OSError:
                    pass
        except (OSError, ValueError, IndexError):
            pass
    return totals


def _intel_gpu_monitor():
    time.sleep(3)
    prev = _read_drm_fdinfo()
    prev_t = time.monotonic()
    while True:
        time.sleep(2)
        now = time.monotonic()
        elapsed_ns = (now - prev_t) * 1e9
        curr = _read_drm_fdinfo()
        if elapsed_ns > 0:
            with _intel_stats_lock:
                for key, engine in (('video', 'video'), ('render', 'render')):
                    delta = max(0, curr.get(engine, 0) - prev.get(engine, 0))
                    _intel_stats[key] = min(100.0, round(delta / elapsed_ns * 100, 1))
        prev = curr
        prev_t = now


threading.Thread(target=_intel_gpu_monitor, daemon=True, name='intel-gpu-mon').start()


def _read_intel_engines() -> dict[str, float]:
    with _intel_stats_lock:
        return dict(_intel_stats)


_net_log = logging.getLogger('net_mbps')
# Suppress the "no physical interfaces" warning after the first emission so it
# doesn't flood the log on every poll interval.
_net_no_iface_warned: bool = False


def _read_net_mbps() -> tuple[float, float]:
    """Return (rx_mbps, tx_mbps) summed over all selected network interfaces.

    Interface selection (in priority order):
    1. NET_IFACE env var — comma-separated list of interface names to use.
    2. Auto-detect physical interfaces from /proc/net/dev (or PROC_NET_DEV path).

    First call always returns (0, 0) — that seeds the delta baseline.
    """
    global _net_prev, _net_prev_ts, _net_no_iface_warned
    try:
        now = time.monotonic()
        samples: dict[str, tuple[int, int]] = {}
        all_ifaces: list[str] = []

        for line in _PROC_NET_DEV.read_text().splitlines()[2:]:
            p = line.split()
            if len(p) < 10:
                continue
            iface = p[0].rstrip(':')
            all_ifaces.append(iface)
            if _NET_IFACE_OVERRIDE:
                if iface not in _NET_IFACE_OVERRIDE:
                    continue
            else:
                if not _is_physical_iface(iface):
                    continue
            samples[iface] = (int(p[1]), int(p[9]))

        if not samples and not _net_no_iface_warned:
            _net_log.warning(
                'No network interfaces selected for RX/TX metrics. '
                'Reading from %s — found interfaces: [%s]. '
                'Set NET_IFACE=<iface> (comma-separated) to pin a specific interface.',
                _PROC_NET_DEV,
                ', '.join(all_ifaces) if all_ifaces else '<none>',
            )
            _net_no_iface_warned = True

        if _net_prev is None:
            _net_prev = samples
            _net_prev_ts = now
            return 0.0, 0.0

        dt = now - _net_prev_ts
        if dt < 0.1:
            # Called too quickly — skip update to avoid div-by-zero.
            return 0.0, 0.0

        rx_delta = tx_delta = 0
        for iface, (rx, tx) in samples.items():
            prev_rx, prev_tx = _net_prev.get(iface, (rx, tx))
            rx_delta += max(0, rx - prev_rx)
            tx_delta += max(0, tx - prev_tx)

        _net_prev = samples
        _net_prev_ts = now

        return round(rx_delta / dt / 1_000_000, 2), round(tx_delta / dt / 1_000_000, 2)
    except Exception as exc:
        _net_log.warning('Failed to read network stats from %s: %s', _PROC_NET_DEV, exc)
        return 0.0, 0.0


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get('/', response_class=HTMLResponse)
async def dashboard(request: Request):
    mount_stats = {r['mount']: r for r in get_mount_stats(db)}
    mounts = [
        {**mount_stats.get(name, {'done': 0, 'failed': 0, 'src_bytes': 0, 'dest_bytes': 0}), 'name': name}
        for name in transcoder.get_mounts()
    ]
    return templates.TemplateResponse(request, 'index.html',
        headers={"Cache-Control": "no-store"},
        context={
            'state':              transcoder.state,
            'stats':              get_stats(db),
            'codec_stats':        get_codec_stats(db),
            'recent':             get_recent_jobs(db, 50),
            'mounts':             mounts,
            'schedule_hour':      transcoder.SCHEDULE_HOUR,
            'cq':                 transcoder.CQ,
            'preset':             transcoder.PRESET,
            'dry_run':            transcoder.DRY_RUN,
            'last_backup':        last_backup_at,
            'version':            APP_VERSION,
            'workers_count':      transcoder.WORKERS + transcoder.VAAPI_WORKERS,
            'nvenc_workers':      transcoder.WORKERS,
            'vaapi_workers':      transcoder.VAAPI_WORKERS,
            'backup_interval_h':  transcoder.BACKUP_INTERVAL_H,
            'backup_keep':        transcoder.BACKUP_KEEP,
            'corrupt_count':           len(get_corrupt_files(db)),
            'disabled_mounts':         list(transcoder.DISABLED_MOUNTS),
            'cache_mount_stats':       get_cache_mount_stats(db),
            'retranscode_originals':   transcoder.RETRANSCODE_ORIGINALS,
            'performer_default_mount': transcoder.PERFORMER_DEFAULT_MOUNT,
        })


@app.get('/api/status')
async def api_status():
    return JSONResponse({
        'state': transcoder.state,
        'stats': get_stats(db),
        'mount_totals': transcoder.state.get('mount_totals', {}),
    })


@app.get('/api/jobs')
async def api_jobs():
    return JSONResponse({'jobs': get_recent_jobs(db, 50)})


@app.get('/api/sysinfo')
async def api_sysinfo():
    rx, tx = _read_net_mbps()
    return JSONResponse({
        'cpu': _read_cpu_pct(),
        'mem': _read_mem_pct(),
        'gpu': _read_gpu_pct(),
        'intel': _read_intel_engines(),
        'rx_mbps': rx,
        'tx_mbps': tx,
    })


@app.post('/api/run')
async def api_run():
    if transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Already running'})
    transcoder.start_scan(db)
    return JSONResponse({'ok': True, 'msg': 'Scan started'})


@app.post('/api/stop')
async def api_stop():
    transcoder.stop_scan()
    return JSONResponse({'ok': True, 'msg': 'Stop signal sent — finishing current file'})


@app.post('/api/soft-stop')
async def api_soft_stop():
    if not transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Not running'})
    if transcoder.state['stopping']:
        return JSONResponse({'ok': False, 'msg': 'Already finishing up'})
    transcoder.stop_scan_soft()
    return JSONResponse({'ok': True, 'msg': 'Finishing current files, then stopping'})


@app.post('/api/worker/{slot_id}/kill')
async def api_kill_worker(slot_id: int, request: Request):
    body = await request.json()
    delete = bool(body.get('delete', False))

    with transcoder._lock:
        w = transcoder.state['workers'].get(slot_id)
    if not w:
        return JSONResponse({'ok': False, 'msg': 'Worker not active'}, status_code=404)

    src_path = w.get('src_path', '')

    # Signal the process immediately — does not block
    killed = transcoder.kill_worker(slot_id)

    # Run NFS unlink in a thread pool so parallel kill requests don't
    # serialize on each other while waiting for the filesystem
    deleted = False
    if delete and src_path:
        def _delete():
            Path(src_path).unlink(missing_ok=True)
            delete_cache_entry(db, src_path)
        try:
            await asyncio.to_thread(_delete)
            deleted = True
        except OSError as e:
            return JSONResponse({'ok': False, 'msg': f'Kill sent but delete failed: {e}'})

    return JSONResponse({'ok': True, 'killed': killed, 'deleted': deleted, 'file': src_path})


@app.post('/api/workers')
async def api_set_workers(request: Request):
    body = await request.json()
    n = max(1, min(int(body.get('count', transcoder.WORKERS)), 8))
    transcoder.WORKERS = n
    transcoder.save_settings()
    return JSONResponse({'ok': True, 'workers': n})


_VALID_PRESETS = {
    'ultrafast', 'superfast', 'veryfast', 'faster', 'fast',
    'medium', 'slow', 'slower', 'veryslow',
    'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7',
}


@app.get('/api/config')
async def api_get_config():
    return JSONResponse({
        'cq':                     transcoder.CQ,
        'preset':                 transcoder.PRESET,
        'dry_run':                transcoder.DRY_RUN,
        'retranscode_originals':  transcoder.RETRANSCODE_ORIGINALS,
        'workers':                transcoder.WORKERS,
        'vaapi_workers':          transcoder.VAAPI_WORKERS,
        'backup_interval_h':      transcoder.BACKUP_INTERVAL_H,
        'backup_keep':            transcoder.BACKUP_KEEP,
        'schedule_hour':          transcoder.SCHEDULE_HOUR,
        'performer_default_mount': transcoder.PERFORMER_DEFAULT_MOUNT,
    })


@app.post('/api/config')
async def api_set_config(request: Request):
    body = await request.json()
    if 'cq' in body:
        v = int(body['cq'])
        if 1 <= v <= 51:
            transcoder.CQ = str(v)
    if 'preset' in body and body['preset'] in _VALID_PRESETS:
        transcoder.PRESET = body['preset']
    if 'dry_run' in body:
        transcoder.DRY_RUN = bool(body['dry_run'])
    if 'retranscode_originals' in body:
        transcoder.RETRANSCODE_ORIGINALS = bool(body['retranscode_originals'])
    if 'workers' in body:
        transcoder.WORKERS = max(1, min(int(body['workers']), 8))
    if 'vaapi_workers' in body:
        transcoder.VAAPI_WORKERS = max(0, min(int(body['vaapi_workers']), 3))
    if 'backup_interval_h' in body:
        transcoder.BACKUP_INTERVAL_H = max(0, int(body['backup_interval_h']))
    if 'backup_keep' in body:
        transcoder.BACKUP_KEEP = max(1, min(int(body['backup_keep']), 30))
    if 'schedule_hour' in body:
        transcoder.SCHEDULE_HOUR = max(-1, min(int(body['schedule_hour']), 23))
    if 'performer_default_mount' in body:
        from app.curation.rename import HOME_MOUNTS
        if body['performer_default_mount'] in HOME_MOUNTS:
            transcoder.PERFORMER_DEFAULT_MOUNT = body['performer_default_mount']
    transcoder.save_settings()
    return JSONResponse({
        'ok':                    True,
        'cq':                    transcoder.CQ,
        'preset':                transcoder.PRESET,
        'dry_run':               transcoder.DRY_RUN,
        'retranscode_originals': transcoder.RETRANSCODE_ORIGINALS,
        'workers':               transcoder.WORKERS,
        'vaapi_workers':         transcoder.VAAPI_WORKERS,
        'backup_interval_h':     transcoder.BACKUP_INTERVAL_H,
        'backup_keep':           transcoder.BACKUP_KEEP,
        'schedule_hour':          transcoder.SCHEDULE_HOUR,
        'performer_default_mount': transcoder.PERFORMER_DEFAULT_MOUNT,
    })


@app.post('/api/reset')
async def api_reset():
    if transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Stop the scan before resetting'}, status_code=409)
    reset_db(db)
    transcoder.state['session'] = {'done': 0, 'failed': 0, 'skipped': 0}
    return JSONResponse({'ok': True})


@app.post('/api/clean-jobs')
async def api_clean_jobs():
    if transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Stop the scan before cleaning jobs'}, status_code=409)
    clean_jobs(db)
    transcoder.state['session'] = {'done': 0, 'failed': 0, 'skipped': 0}
    return JSONResponse({'ok': True})


@app.post('/api/cache/prune')
async def api_cache_prune():
    if transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Stop the scan before pruning cache'}, status_code=409)
    pruned = prune_stale_cache(db)
    logging.info(f'Pruned {pruned} stale cache entries')
    return JSONResponse({'ok': True, 'pruned': pruned})


@app.post('/api/clean-jobs/failed')
async def api_clean_failed_jobs():
    if transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Stop the scan before cleaning jobs'}, status_code=409)
    clean_failed_jobs(db)
    return JSONResponse({'ok': True})


@app.post('/api/mounts/toggle')
async def api_mount_toggle(request: Request):
    body = await request.json()
    name = body.get('name', '').strip()
    if not name:
        return JSONResponse({'ok': False, 'error': 'no name'}, status_code=400)
    if name in transcoder.DISABLED_MOUNTS:
        transcoder.DISABLED_MOUNTS.discard(name)
        disabled = False
    else:
        transcoder.DISABLED_MOUNTS.add(name)
        disabled = True
    transcoder.save_settings()
    return JSONResponse({'ok': True, 'name': name, 'disabled': disabled})


@app.get('/api/corrupt-files')
async def api_corrupt_files():
    files = get_corrupt_files(db)
    # attach mount name (second path component under /media/)
    for f in files:
        parts = Path(f['path']).parts
        f['mount'] = parts[2] if len(parts) > 2 else '?'
        f['filename'] = Path(f['path']).name
    return JSONResponse({'ok': True, 'files': files})


@app.post('/api/corrupt-files/delete')
async def api_delete_corrupt_files(request: Request):
    body = await request.json()
    paths = body.get('paths', [])
    if not paths:
        return JSONResponse({'ok': False, 'error': 'no paths'}, status_code=400)
    deleted, errors = [], []
    for p in paths:
        try:
            Path(p).unlink(missing_ok=True)
            deleted.append(p)
        except Exception as e:
            errors.append(str(e))
    if deleted:
        delete_corrupt_cache_entries(db, deleted)
    return JSONResponse({'ok': True, 'deleted': len(deleted), 'errors': errors})


@app.post('/api/corrupt-files/reset-all')
async def api_reset_corrupt_files():
    deleted = reset_corrupt_cache(db)
    return JSONResponse({'ok': True, 'deleted': deleted})


@app.post('/api/backup')
async def api_backup():
    global last_backup_at
    try:
        path = db_backup(db, keep=transcoder.BACKUP_KEEP)
        last_backup_at = datetime.now().strftime('%Y-%m-%d %H:%M')
        return JSONResponse({'ok': True, 'path': str(path), 'at': last_backup_at})
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)


@app.post('/api/face/rematch-pending')
async def api_face_rematch_pending():
    """Re-queue files with sub-threshold pending face matches for a deeper rescan.

    Bypasses the 24 h cooldown — use after adding new performer embeddings or
    to force an immediate pass with more sampling windows on the manual-review backlog.
    """
    try:
        from app.face.worker import enqueue_pending_rematch
        conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        try:
            n = enqueue_pending_rematch(conn)
        finally:
            conn.close()
        return JSONResponse({'ok': True, 'requeued': n})
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)
