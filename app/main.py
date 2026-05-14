import logging
import os
import threading
import time
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.database import (
    init as db_init, backup as db_backup, reset_db, BACKUP_DIR,
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

SCHEDULE_HOUR = int(os.getenv('SCHEDULE_HOUR', '3'))
APP_VERSION   = os.getenv('APP_VERSION', 'dev')

last_backup_at: str | None = None


@app.on_event('startup')
async def startup():
    global db, last_backup_at
    db = db_init()
    transcoder.cleanup_leftover_temps()
    if BACKUP_DIR.exists():
        backups = sorted(BACKUP_DIR.glob('transcoder_*.db'))
        if backups:
            mtime = backups[-1].stat().st_mtime
            last_backup_at = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M')
    _start_scheduler()


def _start_scheduler():
    backup_hour = (SCHEDULE_HOUR - 1) % 24

    def _loop():
        global last_backup_at
        log = logging.getLogger('scheduler')
        last_backup_date = None
        while True:
            now = datetime.now()
            if now.hour == backup_hour and last_backup_date != now.date():
                try:
                    path = db_backup(db)
                    log.info(f'Scheduled backup → {path.name}')
                    last_backup_date = now.date()
                    last_backup_at = now.strftime('%Y-%m-%d %H:%M')
                except Exception as e:
                    log.error(f'Backup failed: {e}')
            if now.hour == SCHEDULE_HOUR and now.minute == 0:
                if not transcoder.state['running']:
                    log.info('Scheduled scan starting')
                    transcoder.start_scan(db)
                time.sleep(61)
            else:
                time.sleep(30)

    threading.Thread(target=_loop, daemon=True, name='scheduler').start()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get('/', response_class=HTMLResponse)
async def dashboard(request: Request):
    mount_stats = {r['mount']: r for r in get_mount_stats(db)}
    mounts = [
        {**mount_stats.get(name, {'done': 0, 'failed': 0, 'src_bytes': 0, 'dest_bytes': 0}), 'name': name}
        for name in transcoder.get_mounts()
    ]
    return templates.TemplateResponse(request, 'index.html', context={
        'state':         transcoder.state,
        'stats':         get_stats(db),
        'codec_stats':   get_codec_stats(db),
        'recent':        get_recent_jobs(db, 50),
        'mounts':        mounts,
        'schedule_hour': SCHEDULE_HOUR,
        'cq':            transcoder.CQ,
        'preset':        transcoder.PRESET,
        'dry_run':       transcoder.DRY_RUN,
        'last_backup':   last_backup_at,
        'version':       APP_VERSION,
        'workers_count': transcoder.WORKERS,
    })


@app.get('/api/status')
async def api_status():
    return JSONResponse({
        'state': transcoder.state,
        'stats': get_stats(db),
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
    return JSONResponse({'ok': True, 'workers': n})


_VALID_PRESETS = {
    'ultrafast', 'superfast', 'veryfast', 'faster', 'fast',
    'medium', 'slow', 'slower', 'veryslow',
    'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7',
}


@app.get('/api/config')
async def api_get_config():
    return JSONResponse({
        'cq':      transcoder.CQ,
        'preset':  transcoder.PRESET,
        'dry_run': transcoder.DRY_RUN,
        'workers': transcoder.WORKERS,
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
    return JSONResponse({
        'ok':      True,
        'cq':      transcoder.CQ,
        'preset':  transcoder.PRESET,
        'dry_run': transcoder.DRY_RUN,
        'workers': transcoder.WORKERS,
    })


@app.post('/api/reset')
async def api_reset():
    if transcoder.state['running']:
        return JSONResponse({'ok': False, 'msg': 'Stop the scan before resetting'}, status_code=409)
    reset_db(db)
    return JSONResponse({'ok': True})


@app.post('/api/backup')
async def api_backup():
    global last_backup_at
    try:
        path = db_backup(db)
        last_backup_at = datetime.now().strftime('%Y-%m-%d %H:%M')
        return JSONResponse({'ok': True, 'path': str(path), 'at': last_backup_at})
    except Exception as e:
        return JSONResponse({'ok': False, 'error': str(e)}, status_code=500)
