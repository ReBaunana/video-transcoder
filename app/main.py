import logging
import os
import threading
import time
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app.database import init as db_init, get_stats, get_codec_stats, get_recent_jobs
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


@app.on_event('startup')
async def startup():
    global db
    db = db_init()
    transcoder.cleanup_leftover_temps()
    _start_scheduler()


def _start_scheduler():
    def _loop():
        log = logging.getLogger('scheduler')
        while True:
            now = datetime.now()
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
    return templates.TemplateResponse('index.html', {
        'request':      request,
        'state':        transcoder.state,
        'stats':        get_stats(db),
        'codec_stats':  get_codec_stats(db),
        'recent':       get_recent_jobs(db, 50),
        'schedule_hour': SCHEDULE_HOUR,
        'cq':           transcoder.CQ,
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
