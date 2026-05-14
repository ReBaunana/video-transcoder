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

APP_VERSION   = os.getenv('APP_VERSION', 'dev')

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
_net_prev: tuple | None = None
_net_prev_ts: float = 0.0


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


def _read_net_mbps() -> tuple[float, float]:
    global _net_prev, _net_prev_ts
    try:
        now = time.monotonic()
        rx = tx = 0
        for line in Path('/proc/net/dev').read_text().splitlines()[2:]:
            p = line.split()
            if len(p) < 10 or p[0].rstrip(':') == 'lo':
                continue
            rx += int(p[1])
            tx += int(p[9])
        if _net_prev is None:
            _net_prev = (rx, tx)
            _net_prev_ts = now
            return 0.0, 0.0
        dt = now - _net_prev_ts
        if dt < 0.1:
            return 0.0, 0.0
        rx_mbps = max(0.0, (rx - _net_prev[0]) / dt / 1_000_000)
        tx_mbps = max(0.0, (tx - _net_prev[1]) / dt / 1_000_000)
        _net_prev = (rx, tx)
        _net_prev_ts = now
        return round(rx_mbps, 2), round(tx_mbps, 2)
    except Exception:
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
        })


@app.get('/api/status')
async def api_status():
    return JSONResponse({
        'state': transcoder.state,
        'stats': get_stats(db),
    })


@app.get('/api/sysinfo')
async def api_sysinfo():
    rx, tx = _read_net_mbps()
    return JSONResponse({
        'cpu': _read_cpu_pct(),
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
        'cq':                transcoder.CQ,
        'preset':            transcoder.PRESET,
        'dry_run':           transcoder.DRY_RUN,
        'workers':           transcoder.WORKERS,
        'backup_interval_h': transcoder.BACKUP_INTERVAL_H,
        'backup_keep':       transcoder.BACKUP_KEEP,
        'schedule_hour':     transcoder.SCHEDULE_HOUR,
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
        'ok':                True,
        'cq':                transcoder.CQ,
        'preset':            transcoder.PRESET,
        'dry_run':           transcoder.DRY_RUN,
        'workers':           transcoder.WORKERS,
        'backup_interval_h': transcoder.BACKUP_INTERVAL_H,
        'backup_keep':       transcoder.BACKUP_KEEP,
        'schedule_hour':     transcoder.SCHEDULE_HOUR,
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


@app.post('/api/backup')
async def api_backup():
    global last_backup_at
    try:
        path = db_backup(db, keep=transcoder.BACKUP_KEEP)
        last_backup_at = datetime.now().strftime('%Y-%m-%d %H:%M')
        return JSONResponse({'ok': True, 'path': str(path), 'at': last_backup_at})
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)
