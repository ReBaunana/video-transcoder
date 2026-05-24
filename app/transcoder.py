import json
import logging
import os
import queue as _queue
import re
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

MEDIA_ROOT         = Path('/media')
SETTINGS_PATH      = Path('/data/settings.json')
IDEAL_CODECS       = frozenset({'hevc', 'av1'})
SKIP_CODECS        = frozenset({'gif', 'png', 'unknown', 'mjpeg', 'corrupt'})
CQ                 = os.getenv('FFMPEG_CQ', '28')
PRESET             = os.getenv('FFMPEG_PRESET', 'fast')
DRY_RUN                = os.getenv('DRY_RUN', 'false').lower() == 'true'
RETRANSCODE_ORIGINALS  = False
DISABLED_MOUNTS: set   = set()
WORKERS            = max(1, int(os.getenv('FFMPEG_WORKERS', '2')))    # NVENC workers
VAAPI_WORKERS      = int(os.getenv('VAAPI_WORKERS', '0'))             # Intel VAAPI workers
BACKUP_INTERVAL_H  = int(os.getenv('BACKUP_INTERVAL_H', '24'))  # 0 = disabled
BACKUP_KEEP        = int(os.getenv('BACKUP_KEEP', '7'))
PRUNE_INTERVAL_H   = max(1, int(os.getenv('PRUNE_INTERVAL_H', '24')))
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
    'workers':       {},   # {slot_id: None | {'file', 'codec', 'progress', 'fps', 'started_at', 'backend'}}
    'current_mount': None,
    'mount_totals':  {},   # {mount_name: total_video_file_count}
    'session':       {'done': 0, 'failed': 0, 'skipped': 0},
    'last_prune_at': None,
}
_stop      = threading.Event()
_soft_stop = threading.Event()
_lock      = threading.Lock()

_worker_procs: dict[int, subprocess.Popen] = {}
_worker_procs_lock = threading.Lock()
_killed_slots: set[int] = set()


# ── Settings persistence ──────────────────────────────────────────────────────

def load_settings():
    global CQ, PRESET, DRY_RUN, WORKERS, VAAPI_WORKERS, BACKUP_INTERVAL_H, BACKUP_KEEP, SCHEDULE_HOUR, RETRANSCODE_ORIGINALS, DISABLED_MOUNTS
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
        DRY_RUN                = bool(data.get('dry_run', DRY_RUN))
        RETRANSCODE_ORIGINALS  = bool(data.get('retranscode_originals', RETRANSCODE_ORIGINALS))
        DISABLED_MOUNTS        = set(data.get('disabled_mounts', list(DISABLED_MOUNTS)))
        WORKERS           = max(1, min(int(data.get('workers', WORKERS)), 8))
        VAAPI_WORKERS     = max(0, min(int(data.get('vaapi_workers', VAAPI_WORKERS)), 3))
        BACKUP_INTERVAL_H = max(0, int(data.get('backup_interval_h', BACKUP_INTERVAL_H)))
        BACKUP_KEEP       = max(1, min(int(data.get('backup_keep', BACKUP_KEEP)), 30))
        if 'schedule_hour' in data:
            SCHEDULE_HOUR = max(-1, min(int(data['schedule_hour']), 23))
        log.info(
            f'Settings loaded: CQ={CQ} preset={PRESET} dry_run={DRY_RUN} '
            f'workers={WORKERS} vaapi_workers={VAAPI_WORKERS} backup_interval_h={BACKUP_INTERVAL_H} '
            f'backup_keep={BACKUP_KEEP} schedule_hour={SCHEDULE_HOUR}'
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
            'workers': WORKERS, 'vaapi_workers': VAAPI_WORKERS,
            'backup_interval_h': BACKUP_INTERVAL_H,
            'backup_keep': BACKUP_KEEP, 'schedule_hour': SCHEDULE_HOUR,
            'retranscode_originals': RETRANSCODE_ORIGINALS,
            'disabled_mounts':       sorted(DISABLED_MOUNTS),
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
            'codec':    (video.get('codec_name') or 'unknown').lower(),
            'duration': float(data.get('format', {}).get('duration', 0)),
            'cq':       data.get('format', {}).get('tags', {}).get('nvtranscode_cq', ''),
            'bitrate':  int(data.get('format', {}).get('bit_rate', 0)) // 1000,  # kbps
            'height':   video.get('height', 0),
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

_fps_re   = re.compile(r'\bfps=\s*(\d+(?:\.\d+)?)')
_time_re  = re.compile(r'time=(\d+:\d+:\d+\.\d+)')
_noise_re = re.compile(r'frame=|fps=|speed=|time=|bitrate=|size=')

# RTX 30xx with driver 510+: typically 5 concurrent sessions; older/laptop: 3.
# Override with NVENC_MAX_SESSIONS env var if you hit session errors at high worker counts.
_NVENC_SEM = threading.BoundedSemaphore(int(os.getenv('NVENC_MAX_SESSIONS', '3')))

# Intel VAAPI semaphore — controls max concurrent hevc_vaapi encodes.
_VAAPI_SEM    = threading.BoundedSemaphore(int(os.getenv('VAAPI_MAX_SESSIONS', '3')))
_VAAPI_DEVICE = os.getenv('VAAPI_DEVICE', '/dev/dri/renderD128')

# Codecs the iHD VAAPI driver (AlderLake GT1) can hardware-decode.
# These get the full GPU pipeline: -hwaccel vaapi -hwaccel_output_format vaapi.
# Codecs NOT in this set (mpeg4/xvid/divx, vc1, wmv3) fall back to CPU decode.
_VAAPI_DECODE_OK = frozenset({'h264', 'hevc', 'mpeg2video', 'vp8', 'vp9', 'av1'})

# Hardware decoders available on Ampere (RTX 30xx) and newer.
# av1_cuvid requires Ampere+; mpeg4_cuvid can fail on malformed DivX/Xvid headers
# but the Intel VAAPI or CPU-decode fallback in transcode_file handles that transparently.
# vc1/wmv3 omitted — vc1_cuvid hangs silently on some files; CPU decode is reliable.
_CUVID_MAP = {
    'h264':       'h264_cuvid',
    'hevc':       'hevc_cuvid',
    'mpeg2video': 'mpeg2_cuvid',
    'mpeg4':      'mpeg4_cuvid',
    'vp8':        'vp8_cuvid',
    'vp9':        'vp9_cuvid',
    'av1':        'av1_cuvid',
}

# Admission thresholds: (min_height_px, min_bitrate_kbps).
# Files below the kbps threshold for their resolution are skipped before encoding —
# they are already compact and hevc_nvenc/vaapi is unlikely to shrink them further.
_ADMISSION_THRESHOLDS = [(2160, 8000), (1080, 3000), (720, 1500), (0, 800)]


def _admission_ok(info: dict) -> bool:
    """Return True if the file is likely to shrink; False to skip before encoding."""
    bitrate = info.get('bitrate', 0)
    if bitrate <= 0:
        return True  # can't determine — let the size guard decide
    h = info.get('height', 0)
    for min_h, threshold in _ADMISSION_THRESHOLDS:
        if h >= min_h:
            return bitrate >= threshold
    return True


def _vaapi_preexec():
    """Lower scheduling priority for VAAPI workers to protect HA/Jellyfin on same host."""
    os.nice(10)
    try:
        os.sched_setscheduler(0, os.SCHED_BATCH, os.sched_param(0))
    except (AttributeError, OSError):
        pass  # SCHED_BATCH not available on all kernels


def _run_ffmpeg(cmd: list, duration: float, slot_id: int,
                sem=None, preexec_fn=None) -> tuple[int, str]:
    """Return (returncode, stderr_tail).  rc=-1 means stopped by request."""
    if sem is None:
        sem = _NVENC_SEM
    stderr_tail: list[str] = []
    last_raw_line: str = ''
    # Wait for semaphore with _stop awareness
    acquired = False
    while not acquired:
        acquired = sem.acquire(blocking=False)
        if not acquired:
            if _stop.wait(timeout=0.5):
                return -1, 'stopped'
    try:
        proc = subprocess.Popen(cmd, stderr=subprocess.PIPE, text=True, bufsize=1,
                                preexec_fn=preexec_fn)
        with _worker_procs_lock:
            _worker_procs[slot_id] = proc
        try:
            for line in proc.stderr:
                if _stop.is_set() or slot_id in _killed_slots:
                    proc.terminate()
                    proc.wait()
                    _killed_slots.discard(slot_id)
                    return -1, ''
                with _lock:
                    w = state['workers'].get(slot_id)
                    if w:
                        m = _time_re.search(line)
                        if m and duration > 0:
                            w['progress'] = min(_parse_time(m.group(1)) / duration * 100, 99.9)
                        fm = _fps_re.search(line)
                        if fm:
                            w['fps'] = float(fm.group(1))
                stripped = line.strip()
                if stripped:
                    last_raw_line = stripped
                if stripped and not _noise_re.search(stripped):
                    stderr_tail.append(stripped)
                    if len(stderr_tail) > 8:
                        stderr_tail.pop(0)
        finally:
            if proc.poll() is None:
                proc.terminate()
            proc.wait()
        if slot_id in _killed_slots:
            _killed_slots.discard(slot_id)
            return -1, ''
        tail = '\n'.join(stderr_tail)
        if last_raw_line and last_raw_line not in tail:
            tail = (tail + '\n' + last_raw_line).strip()
        return proc.returncode, tail
    finally:
        with _worker_procs_lock:
            _worker_procs.pop(slot_id, None)
        sem.release()


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

def _make_cmd(path: Path, out_ext: str, cq: str, preset: str, tmp: Path,
              decoder: str | None = None) -> list[str]:
    # -hwaccel_output_format cuda omitted — causes filter reinit errors under concurrent load
    cmd = ['ffmpeg', '-y', '-hwaccel', 'cuda']
    if decoder:
        cmd += ['-c:v', decoder]
    cmd += [
        '-i', str(path),
        '-c:v', 'hevc_nvenc', '-cq', cq, '-preset', preset,
        '-rc-lookahead', '0', '-multipass', 'disabled',
        '-b_ref_mode', '0', '-bf', '0',
        '-c:a', 'copy', '-c:s', 'copy',
        '-metadata', f'nvtranscode_cq={cq}',
    ]
    if out_ext == '.mp4':
        cmd += ['-movflags', '+faststart']
    cmd.append(str(tmp))
    return cmd


def _make_vaapi_cmd(path: Path, out_ext: str, cq: str, tmp: Path,
                    codec: str | None = None) -> list[str]:
    use_hw_decode = codec in _VAAPI_DECODE_OK
    cmd = ['ffmpeg', '-y']
    if use_hw_decode:
        # Full GPU pipeline: iHD VAAPI decode → GPU surface → hevc_vaapi encode
        cmd += [
            '-hwaccel', 'vaapi',
            '-hwaccel_device', _VAAPI_DEVICE,
            '-hwaccel_output_format', 'vaapi',
            '-threads', '1', '-filter_threads', '1',
            '-i', str(path),
            '-vf', 'scale_vaapi=format=nv12',
        ]
    else:
        # CPU decode (mpeg4/xvid/divx/vc1 — no VAAPI decoder) → upload to GPU
        cmd += [
            '-threads', '2', '-filter_threads', '1',
            '-vaapi_device', _VAAPI_DEVICE,
            '-i', str(path),
            '-vf', 'format=nv12,hwupload',
        ]
    cmd += [
        '-c:v', 'hevc_vaapi', '-global_quality', cq,
        '-c:a', 'copy', '-c:s', 'copy',
        '-metadata', f'nvtranscode_cq={cq}',
    ]
    if out_ext == '.mp4':
        cmd += ['-movflags', '+faststart']
    cmd.append(str(tmp))
    return cmd


def transcode_file(path: Path, db, slot_id: int, backend: str = 'nvenc') -> str:
    from app.database import record_start, record_finish, cache_get, cache_set
    _cq, _preset = CQ, PRESET  # snapshot globals; they may change while we transcode

    try:
        st = path.stat()
    except OSError:
        return 'failed'

    if st.st_size == 0:
        return 'failed'

    cached = cache_get(db, str(path), st.st_size, st.st_mtime)
    if cached:
        codec = cached['codec']
        cached_cq = cached.get('cq', '')
        if codec in SKIP_CODECS:
            return 'skipped'
        if cached_cq == f'guard:{_cq}':
            return 'skipped'
        # Intel tried and failed at this CQ — VAAPI skips again, NVENC gets a shot next scan
        if backend == 'vaapi' and cached_cq == f'guard_intel:{_cq}':
            return 'skipped'
        if codec in IDEAL_CODECS:
            if cached_cq == _cq:
                return 'skipped'
            if cached_cq == 'original' and not RETRANSCODE_ORIGINALS:
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
            embed_cq = info.get('cq', '')
            if embed_cq:
                # Our encode — restore provenance from embedded tag
                cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'], cq=embed_cq)
                if embed_cq == _cq:
                    return 'skipped'
                log.info(f'Re-transcode {path.name}: embedded CQ={repr(embed_cq)} current={_cq}')
                # fall through to transcode
            else:
                # No tag — truly original HEVC, never re-transcode
                cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'], cq='original')
                return 'skipped'

    # VAAPI: for codecs without hardware decode (mpeg4/xvid/divx), cap at 720p to avoid
    # CPU software-decode overload. h264/hevc/vp9/av1 use full GPU pipeline — no cap.
    if backend == 'vaapi' and codec not in _VAAPI_DECODE_OK and info.get('height', 0) > 720:
        log.info(f'[W{slot_id}] VAAPI skip {path.name} [SW-decode {codec} {info.get("height", 0)}p > 720p, defer to NVENC]')
        return 'skipped'

    # Layer 1: skip files unlikely to shrink based on bitrate vs resolution
    if not _admission_ok(info):
        log.info(
            f'[W{slot_id}] ADMISSION skip {path.name} '
            f'[{info.get("height", 0)}p @ {info.get("bitrate", 0)} kbps]'
        )
        prior_cq = cached.get('cq', '') if cached else ''
        cq_to_store = 'original' if (codec == 'hevc' and prior_cq == 'original') else f'guard:{_cq}'
        cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'], cq=cq_to_store)
        return 'skipped'

    out_ext  = OUTPUT_EXT.get(path.suffix.lower(), '.mkv')
    tmp      = path.with_name(path.stem + '.transcoding' + out_ext)
    if len(tmp.name) > 255:
        log.warning(f'[W{slot_id}] filename too long ({len(tmp.name)} chars) — skipping permanently')
        cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'], cq=f'guard:{_cq}')
        return 'skipped'
    dest     = path.with_suffix(out_ext)
    mount    = path.parts[2] if len(path.parts) > 2 else ''
    src_size = st.st_size

    log.info(f'[W{slot_id}/{backend}] TRANSCODE  {path.name}  [{codec}->hevc_{backend} CQ{_cq}]  {src_size / 1e6:.0f}MB')

    if DRY_RUN:
        log.info(f'[W{slot_id}]   DRY_RUN - skip')
        return 'skipped'

    job_id  = record_start(db, str(path), path.name, codec, src_size, mount, cq=_cq)
    started = datetime.now(timezone.utc)

    with _lock:
        state['workers'][slot_id] = {
            'file':       path.name,
            'src_path':   str(path),
            'codec':      codec,
            'progress':   0.0,
            'fps':        0.0,
            'started_at': started.isoformat(),
            'backend':    backend,
        }

    if backend == 'vaapi':
        cmd = _make_vaapi_cmd(path, out_ext, _cq, tmp, codec=codec)
        rc, err = _run_ffmpeg(cmd, info['duration'], slot_id, sem=_VAAPI_SEM,
                              preexec_fn=_vaapi_preexec)
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    else:
        _decoder = _CUVID_MAP.get(codec)
        cmd      = _make_cmd(path, out_ext, _cq, _preset, tmp, decoder=_decoder)
        rc, err  = _run_ffmpeg(cmd, info['duration'], slot_id, sem=_NVENC_SEM)
        elapsed  = (datetime.now(timezone.utc) - started).total_seconds()

        # HW decode failure -> retry with Intel VAAPI (if enabled); no CPU fallback
        # Also retry on early hwaccel-level crash (< 3s, no frames) even without CUVID decoder
        _early_crash = elapsed < 3.0
        if rc not in (0, -1) and (_decoder or _early_crash):
            tmp.unlink(missing_ok=True)
            with _lock:
                if state['workers'].get(slot_id):
                    state['workers'][slot_id]['progress'] = 0.0
            if VAAPI_WORKERS > 0:
                reason = f'HW decode failed ({_decoder})' if _decoder else f'early crash rc={rc} ({elapsed:.1f}s)'
                log.warning(f'[W{slot_id}]   {reason} -- retrying with Intel VAAPI')
                with _lock:
                    if state['workers'].get(slot_id):
                        state['workers'][slot_id]['backend'] = 'vaapi'
                cmd = _make_vaapi_cmd(path, out_ext, _cq, tmp, codec=codec)
                rc, err = _run_ffmpeg(cmd, info['duration'], slot_id, sem=_VAAPI_SEM,
                                      preexec_fn=_vaapi_preexec)
            else:
                log.warning(f'[W{slot_id}]   HW decode failed ({_decoder}) -- no GPU fallback available, marking failed')
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
        # Cache a guard so this file is not retried endlessly by the same backend.
        # VAAPI failure → guard_intel so NVENC gets a shot next scan.
        # NVENC failure (with or without prior VAAPI fallback) → permanent guard.
        if backend == 'vaapi':
            cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'],
                      cq=f'guard_intel:{_cq}')
            log.info(f'[W{slot_id}]   guard_intel:{_cq} set — NVENC will retry next scan')
        else:
            cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'],
                      cq=f'guard:{_cq}')
        with _lock:
            state['workers'][slot_id] = None
        return 'failed'

    out_info = probe(tmp)
    if not out_info or out_info['codec'] != 'hevc':
        log.error(f'[W{slot_id}]   verify failed: codec={out_info and out_info["codec"]}')
        tmp.unlink(missing_ok=True)
        record_finish(db, job_id, 'failed', None, elapsed, 'codec verification failed')
        if backend == 'vaapi':
            cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'],
                      cq=f'guard_intel:{_cq}')
        else:
            cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'],
                      cq=f'guard:{_cq}')
        with _lock:
            state['workers'][slot_id] = None
        return 'failed'

    if info['duration'] > 0:
        drift = abs(out_info['duration'] - info['duration']) / info['duration']
        if drift > 0.02:
            log.error(f'[W{slot_id}]   duration drift {drift:.1%}')
            tmp.unlink(missing_ok=True)
            record_finish(db, job_id, 'failed', None, elapsed, f'duration drift {drift:.1%}')
            cache_set(db, str(path), st.st_size, st.st_mtime, 'corrupt', info['duration'])
            with _lock:
                state['workers'][slot_id] = None
            return 'failed'

    dest_size = tmp.stat().st_size
    log.info(
        f'[W{slot_id}]   {src_size / 1e6:.0f}MB -> {dest_size / 1e6:.0f}MB'
        f'  ({dest_size / src_size * 100:.0f}%)  {elapsed:.0f}s'
    )

    if dest_size >= src_size * 0.95:
        tmp.unlink(missing_ok=True)
        log.warning(
            f'[W{slot_id}]   size guard: output {dest_size / 1e6:.0f}MB >= source {src_size / 1e6:.0f}MB'
            f' ({dest_size / src_size * 100:.0f}%) -- keeping original'
        )
        record_finish(db, job_id, 'skipped', None, elapsed, 'size guard: output not smaller')
        prior_cq = cached.get('cq', '') if cached else ''
        if backend == 'vaapi':
            # Intel failed -- NVENC will retry next scan
            cq_to_store = f'guard_intel:{_cq}'
        else:
            cq_to_store = 'original' if (codec == 'hevc' and prior_cq == 'original') else f'guard:{_cq}'
        cache_set(db, str(path), st.st_size, st.st_mtime, codec, info['duration'], cq=cq_to_store)
        with _lock:
            state['workers'][slot_id] = None
        return 'skipped'

    tmp.replace(dest)           # atomic rename first -- dest is safe
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
        _total = WORKERS + VAAPI_WORKERS
        state['running']  = True
        state['stopping'] = False
        state['session']  = {'done': 0, 'failed': 0, 'skipped': 0}
        state['workers']  = {i: None for i in range(_total)}
    _stop.clear()
    _soft_stop.clear()

    log.info(f'=== Scan started ({WORKERS} NVENC + {VAAPI_WORKERS} VAAPI workers) ===')

    file_q: _queue.Queue = _queue.Queue()
    _total = WORKERS + VAAPI_WORKERS

    def _worker(slot_id: int, backend: str):
        while True:
            path = file_q.get()
            if path is None:
                file_q.task_done()
                break
            if _stop.is_set() or _soft_stop.is_set():
                file_q.task_done()
                continue
            try:
                result = transcode_file(path, db, slot_id, backend=backend)
            except Exception:
                log.exception(f'[W{slot_id}] unhandled error on {path}')
                result = 'failed'
            with _lock:
                state['session'][result] = state['session'].get(result, 0) + 1
            file_q.task_done()

    threads = []
    for i in range(WORKERS):
        threads.append(
            threading.Thread(target=_worker, args=(i, 'nvenc'), daemon=True, name=f'worker-nvenc-{i}')
        )
    for i in range(VAAPI_WORKERS):
        slot = WORKERS + i
        threads.append(
            threading.Thread(target=_worker, args=(slot, 'vaapi'), daemon=True, name=f'worker-vaapi-{i}')
        )
    for t in threads:
        t.start()

    try:
        if not MEDIA_ROOT.exists():
            log.error(f'{MEDIA_ROOT} not found -- check volume mounts')
            return

        from app.database import prune_stale_cache
        try:
            pruned = prune_stale_cache(db)
            state['last_prune_at'] = datetime.now().strftime('%Y-%m-%d %H:%M')
            log.info(f'Pre-scan prune: {pruned} stale cache entries removed')
        except Exception as e:
            log.warning(f'Pre-scan prune failed: {e}')

        mounts = sorted(p for p in MEDIA_ROOT.iterdir() if p.is_dir())
        log.info(f'Mounts: {[m.name for m in mounts]}')

        for mount in mounts:
            if mount.name in DISABLED_MOUNTS:
                log.info(f'--- {mount.name} --- (disabled, skipping)')
                continue
            log.info(f'--- {mount.name} ---')
            with _lock:
                state['current_mount'] = mount.name
                state['mount_totals'][mount.name] = 0

            for root, dirs, files in os.walk(mount, topdown=True):
                dirs[:] = sorted(d for d in dirs if not d.startswith('.'))
                if _stop.is_set() or _soft_stop.is_set():
                    log.info('Stopped by request')
                    return
                for name in sorted(files):
                    if _stop.is_set():
                        return
                    if _soft_stop.is_set():
                        log.info('Soft stop -- finishing current files')
                        return
                    if '.transcoding.' in name:
                        continue
                    p = Path(root) / name
                    if p.suffix.lower() not in VIDEO_EXTENSIONS:
                        continue
                    # Key was inserted under _lock above; += 1 on an existing int is GIL-safe.
                    state['mount_totals'][mount.name] += 1
                    file_q.put(p)

    finally:
        for _ in range(_total):
            file_q.put(None)
        for t in threads:
            t.join()

        with _lock:
            state.update({
                'running':       False,
                'stopping':      False,
                'workers':       {},
                'current_mount': None,
                'mount_totals':  {},
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


def kill_worker(slot_id: int) -> bool:
    """Terminate a specific worker's ffmpeg process. Returns False if not active.

    Sends SIGTERM first, then escalates to SIGKILL after 2s if the process
    hasn't exited — necessary for vc1_cuvid/WMV encodes that ignore SIGTERM.
    SIGKILL closes the stderr pipe which unblocks _run_ffmpeg's read loop.
    """
    _killed_slots.add(slot_id)
    with _worker_procs_lock:
        proc = _worker_procs.get(slot_id)
    if proc is None:
        _killed_slots.discard(slot_id)
        return False
    try:
        proc.terminate()
    except OSError:
        pass

    def _escalate():
        import time as _t
        _t.sleep(2)
        try:
            if proc.poll() is None:
                proc.kill()
        except OSError:
            pass

    threading.Thread(target=_escalate, daemon=True, name=f'kill-esc-{slot_id}').start()
    return True
