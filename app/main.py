import logging
import os
import subprocess
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.database import (
    init as db_init, backup as db_backup, reset_db, clean_jobs, BACKUP_DIR,
    get_stats, get_codec_stats, get_recent_jobs, get_mount_stats,
    get_corrupt_files, delete_corrupt_cache_entries, get_cache_mount_stats,
)
from app import transcoder

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
    _start_scheduler()


def _start_scheduler():
    def _loop():
        global last_backup_at
        import time as _time
        log = logging.getLogger('scheduler')

        # Seed from most recent backup file so we don't back up immediately
        # if one is recent enough.
        last_backup_ts = 0.0
        if BACKUP_DIR.exists():
            backups = sorted(BACKUP_DIR.glob('transcoder_*.db'))
            if backups:
                last_backup_ts = backups[-1].stat().st_mtime

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
            'workers_count':      transcoder.WORKERS,
            'backup_interval_h':  transcoder.BACKUP_INTERVAL_H,
            'backup_keep':        transcoder.BACKUP_KEEP,
            'corrupt_count':           len(get_corrupt_files(db)),
            'cache_mount_stats':       get_cache_mount_stats(db),
            'retranscode_originals':   transcoder.RETRANSCODE_ORIGINALS,
        })


@app.get('/api/status')
async def api_status():
    return JSONResponse({
        'state': transcoder.state,
        'stats': get_stats(db),
        'mount_totals': transcoder.state.get('mount_totals', {}),
    })


@app.get('/api/sysinfo')
async def api_sysinfo():
    rx, tx = _read_net_mbps()
    return JSONResponse({
        'cpu': _read_cpu_pct(),
        'mem': _read_mem_pct(),
        'gpu': _read_gpu_pct(),
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
        'backup_interval_h':      transcoder.BACKUP_INTERVAL_H,
        'backup_keep':            transcoder.BACKUP_KEEP,
        'schedule_hour':          transcoder.SCHEDULE_HOUR,
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
    if 'backup_interval_h' in body:
        transcoder.BACKUP_INTERVAL_H = max(0, int(body['backup_interval_h']))
    if 'backup_keep' in body:
        transcoder.BACKUP_KEEP = max(1, min(int(body['backup_keep']), 30))
    if 'schedule_hour' in body:
        transcoder.SCHEDULE_HOUR = max(-1, min(int(body['schedule_hour']), 23))
    transcoder.save_settings()
    return JSONResponse({
        'ok':                    True,
        'cq':                    transcoder.CQ,
        'preset':                transcoder.PRESET,
        'dry_run':               transcoder.DRY_RUN,
        'retranscode_originals': transcoder.RETRANSCODE_ORIGINALS,
        'workers':               transcoder.WORKERS,
        'backup_interval_h':     transcoder.BACKUP_INTERVAL_H,
        'backup_keep':           transcoder.BACKUP_KEEP,
        'schedule_hour':         transcoder.SCHEDULE_HOUR,
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


@app.post('/api/backup')
async def api_backup():
    global last_backup_at
    try:
        path = db_backup(db, keep=transcoder.BACKUP_KEEP)
        last_backup_at = datetime.now().strftime('%Y-%m-%d %H:%M')
        return JSONResponse({'ok': True, 'path': str(path), 'at': last_backup_at})
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)
