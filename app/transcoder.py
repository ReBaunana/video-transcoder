import json
import logging
import os
import queue as _queue
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

MEDIA_ROOT         = Path('/media')
SETTINGS_PATH      = Path('/data/settings.json')
IDEAL_CODECS       = frozenset({'hevc', 'av1'})
SKIP_CODECS        = frozenset({'gif', 'png', 'unknown', 'mjpeg'})
CQ                 = os.getenv('FFMPEG_CQ', '28')
PRESET             = os.getenv('FFMPEG_PRESET', 'fast')
DRY_RUN            = os.getenv('DRY_RUN', 'false').lower() == 'true'
WORKERS            = max(1, int(os.getenv('FFMPEG_WORKERS', '2')))
BACKUP_INTERVAL_H  = int(os.getenv('BACKUP_INTERVAL_H', '24'))  # 0 = disabled
BACKUP_KEEP        = int(os.getenv('BACKUP_KEEP', '7'))
SCHEDULE_HOUR      = int(os.getenv('SCHEDULE_HOUR', '3'))       # -1 = disabled

VIDEO_EXTENSIONS = frozenset({
    '.mkv', '.mp4', '.avi', '.wmv', '.mov', '.flv',
    '.m4v', '.ts', '.mpg', '.mpeg', '.divx', '.webm',
})

OUTPUT_EXT = {
    '.mkv': '.mkv', '.mp4': '.mp4', '.m4v': '.mp4', '.mov': '.mp4',
    '.avi': '.mkv', '.wmv': '.mkv', '.flv': '.mkv', '.ts': '.mkv',
    '.mpg': '.mkv', '.mpeg': '.mkv', '.divx': '.mkv', '.webm': '.mkv',
}

# ── Shared state (read by web UI) ─────────────────────────────────────────────

state: dict = {
    'running':       False,
    'stopping':      False,
    'workers':       {},   # {slot_id: None | {'file', 'codec', 'progress', 'started_at'}}
    'current_mount': None,
    'session':       {'done': 0, 'failed': 0, 'skipped': 0},
}
_stop      = threading.Event()
_soft_stop = threading.Event()
_lock      = threading.Lock()


# ── Settings persistence ──────────────────────────────────────────────────────

def load_settings():
    global CQ, PRESET, DRY_RUN, WORKERS, BACKUP_INTERVAL_H, BACKUP_KEEP, SCHEDULE_HOUR
    try:
        data = json.loads(SETTINGS_PATH.read_text())
        cq = int(data.get('cq', CQ))
        if 1 <= cq <= 51:
            CQ = str(cq)
        if data.get('preset') in {
            'ultrafast', 'superfast', 'veryfast', 'faster', 'fast',
            'medium', 'slow', 'slower', 'veryslow',
            'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7',
        }:
            PRESET = data['preset']
        DRY_RUN           = bool(data.get('dry_run', DRY_RUN))
        WORKERS           = max(1, min(int(data.get('workers', WORKERS)), 8))
        BACKUP_INTERVAL_H = max(0, int(data.get('backup_interval_h', BACKUP_INTERVAL_H)))
        BACKUP_KEEP       = max(1, min(int(data.get('backup_keep', BACKUP_KEEP)), 30))
        if 'schedule_hour' in data:
            SCHEDULE_HOUR = max(-1, min(int(data['schedule_hour']), 23))
        log.info(
            f'Settings loaded: CQ={CQ} preset={PRESET} dry_run={DRY_RUN} '
            f'workers={WORKERS} backup_interval_h={BACKUP_INTERVAL_H} backup_keep={BACKUP_KEEP} '
            f'schedule_hour={SCHEDULE_HOUR}'
        )
    except FileNotFoundError:
        pass
    except Exception:
        log.warning('Could not load settings.json — using defaults')


def save_settings():
    try:
        SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        tmp = SETTINGS_PATH.with_suffix('.json.tmp')
        tmp.write_text(json.dumps({
            'cq': CQ, 'preset': PRESET, 'dry_run': DRY_RUN,
            'workers': WORKERS, 'backup_interval_h': BACKUP_INTERVAL_H,
            'backup_keep': BACKUP_KEEP, 'schedule_hour': SCHEDULE_HOUR,
        }))
        tmp.replace(SETTINGS_PATH)
    except Exception:
        log.warning('Could not save settings.json')


# ── Helpers ───────────────────────────────────────────────────────────────────

def probe(path: Path) -> dict | None:
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'quiet', '-print_format', 'json',
             '-show_streams', '-show_format', str(path)],
            capture_output=True, text=True, timeout=30,
        )
        if r.returncode != 0:
            return None
        data = json.loads(r.stdout)
        video = next(
            (s for s in data.get('streams', []) if s.get('codec_type') == 'video'),
            None,
        )
        if not video:
            return None
        return {
            'codec':    video.get('codec_name', '').lower(),
            'duration': float(data.get('format', {}).get('duration', 0)),
        }
    except Exception:
        return None


def _parse_time(s: str) -> float:
    try:
        h, m, sec = s.split(':')
        return int(h) * 3600 + int(m) * 60 + float(sec)
    except Exception:
        return 0.0


_NVENC_SESSION_LIMIT = 'OpenEncodeSessionEx failed'

def _run_ffmpeg(cmd: list, duration: float, slot_id: int) -> tuple[int, str]:
    """Return (returncode, stderr_tail).  rc=-1 means stopped by request."""
    proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1)
    time_re    = re.compile(r'time=(\d+:\d+:\d+\.\d+)')
    noise_re   = re.compile(r'frame=|fps=|speed=|time=|bitrate=|size=')
    stderr_tail: list[str] = []
    try:
        for line in proc.stderr:
            if _stop.is_set():
                proc.terminate()
                proc.wait()
                return -1, ''
            m = time_re.search(line)
            if m and duration > 0:
                progress = min(_parse_time(m.group(1)) / duration * 100, 99.9)
                with _lock:
                    if state['workers'].get(slot_id):
                        state['workers'][slot_id]['progress'] = progress
            stripped = line.strip()
            if stripped and not noise_re.search(stripped):
                stderr_tail.append(stripped)
                if len(stderr_tail) > 8:
                    stderr_tail.pop(0)
    finally:
        if proc.poll() is None:
            proc.terminate()
        proc.wait()
    return proc.returncode, '\n'.join(stderr_tail)


def get_mounts() -> list[str]:
    if not MEDIA_ROOT.exists():
        return []
    return sorted(p.name for p in MEDIA_ROOT.iterdir() if p.is_dir())


def cleanup_leftover_temps():
    if not MEDIA_ROOT.exists():
        return
    for root, _, files in os.walk(MEDIA_ROOT):
        for f in files:
            if '.transcoding.' in f:
                p = Path(root) / f
                log.warning(f'Removing leftover temp: {p}')
                p.unlink(missing_ok=True)


# ── Core transcoding ──────────────────────────────────────────────────────────

def transcode_file(path: Path, db, slot_id: int) -> str:
    from app.database import record_start, record_finish, cache_get, cache_set
    _cq, _preset = CQ, PRESET  # snapshot globals; they may change while we transcode

    try:
        st = path.stat()
    except OSError:
        return 'failed'

    cached = cache_get(db, str(path), st.st_size, st.st_mtime)
    if cached:
        codec = cached['codec']
        if codec in SKIP_CODECS:
            return 'skipped'
        if codec in IDEAL_CODECS:
            cached_cq = cached.get('cq', '')
            # 'original' = was already HEVC when first scanned, never our encode → always skip
            # CQ matches current setting → skip
            # '' (legacy) or different CQ → re-transcode
            if cached_cq == 'original' or cached_cq == _cq:
                return 'skipped'
            log.info(f'Re-transcode {path.name}: cached CQ={repr(cached_cq)} current={_cq}')
        info = {'codec': codec, 'duration': cached['duration']}
        if info['duration'] == 0:
            full = probe(path)
            info['duration'] = full['duration'] if full else 0
    else:
        info = probe(path)
        if not info:
            log.warning(f'probe failed: {path}')
            return 'failed'
        codec = info['codec']
        if codec in SKIP_CODECS:
            cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'])
            return 'skipped'
        if codec in IDEAL_CODECS:
            # Mark as original HEVC — not our encode, always skip in future
            cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'], cq='original')
            return 'skipped'

    out_ext  = OUTPUT_EXT.get(path.suffix.lower(), '.mkv')
    tmp      = path.with_name(path.stem + '.transcoding' + out_ext)
    dest     = path.with_suffix(out_ext)
    mount    = path.parts[2] if len(path.parts) > 2 else ''
    src_size = st.st_size

    log.info(f'[W{slot_id}] TRANSCODE  {path.name}  [{codec}→hevc_nvenc CQ{_cq}]  {src_size / 1e6:.0f}MB')

    if DRY_RUN:
        log.info(f'[W{slot_id}]   DRY_RUN – skip')
        return 'skipped'

    job_id  = record_start(db, str(path), path.name, codec, src_size, mount, cq=_cq)
    started = datetime.now(timezone.utc)

    with _lock:
        state['workers'][slot_id] = {
            'file':       path.name,
            'codec':      codec,
            'progress':   0.0,
            'started_at': started.isoformat(),
        }

    cmd = [
        'ffmpeg', '-y',
        '-hwaccel', 'cuda',
        '-i', str(path),
        '-c:v', 'hevc_nvenc', '-cq', _cq, '-preset', _preset,
        '-c:a', 'copy', '-c:s', 'copy',
    ]
    if out_ext == '.mp4':
        cmd += ['-movflags', '+faststart']
    cmd.append(str(tmp))

    rc, err = _run_ffmpeg(cmd, info['duration'], slot_id)
    elapsed  = (datetime.now(timezone.utc) - started).total_seconds()

    # NVENC session limit hit — one retry after a short wait
    if rc not in (0, -1) and _NVENC_SESSION_LIMIT in err:
        log.warning(f'[W{slot_id}]   NVENC session limit — waiting 15 s then retrying')
        time.sleep(15)
        with _lock:
            if state['workers'].get(slot_id):
                state['workers'][slot_id]['progress'] = 0.0
        rc, err = _run_ffmpeg(cmd, info['duration'], slot_id)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()

    if rc != 0 or not tmp.exists():
        tmp.unlink(missing_ok=True)
        if rc == -1:
            record_finish(db, job_id, 'skipped', None, elapsed, 'stopped')
            with _lock:
                state['workers'][slot_id] = None
            return 'skipped'
        err_short = err.splitlines()[-1] if err else f'ffmpeg exit {rc}'
        log.error(f'[W{slot_id}]   ffmpeg failed (rc={rc}): {err_short}')
        record_finish(db, job_id, 'failed', None, elapsed, err_short[:200])
        with _lock:
            state['workers'][slot_id] = None
        return 'failed'

    out_info = probe(tmp)
    if not out_info or out_info['codec'] != 'hevc':
        log.error(f'[W{slot_id}]   verify failed: codec={out_info and out_info["codec"]}')
        tmp.unlink(missing_ok=True)
        record_finish(db, job_id, 'failed', None, elapsed, 'codec verification failed')
        with _lock:
            state['workers'][slot_id] = None
        return 'failed'

    if info['duration'] > 0:
        drift = abs(out_info['duration'] - info['duration']) / info['duration']
        if drift > 0.02:
            log.error(f'[W{slot_id}]   duration drift {drift:.1%}')
            tmp.unlink(missing_ok=True)
            record_finish(db, job_id, 'failed', None, elapsed, f'duration drift {drift:.1%}')
            with _lock:
                state['workers'][slot_id] = None
            return 'failed'

    dest_size = tmp.stat().st_size
    log.info(
        f'[W{slot_id}]   {src_size / 1e6:.0f}MB → {dest_size / 1e6:.0f}MB'
        f'  ({dest_size / src_size * 100:.0f}%)  {elapsed:.0f}s'
    )

    tmp.replace(dest)           # atomic rename first — dest is safe
    if dest != path:
        path.unlink(missing_ok=True)  # only then remove the original

    record_finish(db, job_id, 'done', dest_size, elapsed)

    try:
        dest_st = dest.stat()
        cache_set(db, str(dest), dest_st.st_size, dest_st.st_mtime, 'hevc', out_info['duration'], cq=_cq)
    except OSError:
        pass

    with _lock:
        state['workers'][slot_id] = None
    log.info(f'[W{slot_id}]   OK  {dest.name}')
    return 'done'


# ── Scan loop ─────────────────────────────────────────────────────────────────

def run_scan(db):
    with _lock:
        if state['running']:
            log.warning('Scan already running')
            return
        state['running']  = True
        state['stopping'] = False
        state['session']  = {'done': 0, 'failed': 0, 'skipped': 0}
        state['workers']  = {i: None for i in range(WORKERS)}
    _stop.clear()
    _soft_stop.clear()

    log.info(f'=== Scan started ({WORKERS} workers) ===')

    file_q: _queue.Queue = _queue.Queue()

    def _worker(slot_id: int):
        while True:
            path = file_q.get()
            if path is None:
                file_q.task_done()
                break
            if _stop.is_set() or _soft_stop.is_set():
                file_q.task_done()
                continue
            try:
                result = transcode_file(path, db, slot_id)
            except Exception:
                log.exception(f'[W{slot_id}] unhandled error on {path}')
                result = 'failed'
            with _lock:
                state['session'][result] = state['session'].get(result, 0) + 1
            file_q.task_done()

    threads = [
        threading.Thread(target=_worker, args=(i,), daemon=True, name=f'worker-{i}')
        for i in range(WORKERS)
    ]
    for t in threads:
        t.start()

    try:
        if not MEDIA_ROOT.exists():
            log.error(f'{MEDIA_ROOT} not found — check volume mounts')
            return

        mounts = sorted(p for p in MEDIA_ROOT.iterdir() if p.is_dir())
        log.info(f'Mounts: {[m.name for m in mounts]}')

        for mount in mounts:
            log.info(f'--- {mount.name} ---')
            with _lock:
                state['current_mount'] = mount.name

            for root, dirs, files in os.walk(mount, topdown=True):
                dirs[:] = sorted(d for d in dirs if not d.startswith('.'))
                if _stop.is_set() or _soft_stop.is_set():
                    log.info('Stopped by request')
                    return
                for name in sorted(files):
                    if _stop.is_set():
                        return
                    if _soft_stop.is_set():
                        log.info('Soft stop — finishing current files')
                        return
                    if '.transcoding.' in name:
                        continue
                    p = Path(root) / name
                    if p.suffix.lower() not in VIDEO_EXTENSIONS:
                        continue
                    file_q.put(p)

    finally:
        for _ in range(WORKERS):
            file_q.put(None)
        for t in threads:
            t.join()

        with _lock:
            state.update({
                'running':       False,
                'stopping':      False,
                'workers':       {},
                'current_mount': None,
            })
        log.info(f'=== Scan done: {state["session"]} ===')


def start_scan(db):
    t = threading.Thread(target=run_scan, args=(db,), daemon=True, name='transcoder')
    t.start()


def stop_scan():
    _stop.set()


def stop_scan_soft():
    _soft_stop.set()
    with _lock:
        state['stopping'] = True
