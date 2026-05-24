"""HTTP routes for the library / curation surface.

Routes are mounted by :mod:`app.main` via ``app.include_router``. The shared
SQLite connection lives on ``request.app.state.db`` — main.py assigns it there
during startup so the rest of the application can use the FastAPI-canonical
``app.state`` channel instead of a cross-module global.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app import database_curation as dbc
from app.curation import extractor as curation_extractor
from app.curation import rename as curation_rename
from app.face import worker as face_worker
from app.face.model import is_face_rec_available

router = APIRouter()
templates = Jinja2Templates(directory='app/templates')
log = logging.getLogger('curation.routes')

MEDIA_ROOT = Path('/media')
PAGE_SIZE = 50
MEDIA_MOUNTS = (
    'ddMovie', 'intensoP1', 'intensoP2', 'jdownloader',
    'movies', 'serien', 'training',
)


def _db(request: Request) -> sqlite3.Connection:
    """Return the shared SQLite connection placed on ``app.state`` at startup."""
    conn = getattr(request.app.state, 'db', None)
    if conn is None:
        raise HTTPException(status_code=503, detail='database not initialised')
    return conn


def _safe_mount(name: str) -> str:
    """Validate a mount name against the static allow-list.

    The mount string is interpolated into a filesystem path that we feed to the
    extractor, so anything outside the allow-list is rejected upfront.
    """
    if name not in MEDIA_MOUNTS:
        raise HTTPException(status_code=400, detail=f'unknown mount: {name!r}')
    return name


async def _read_json(request: Request) -> dict[str, Any]:
    """Tolerant JSON body reader — returns ``{}`` for empty/missing bodies."""
    raw = await request.body()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail=f'invalid json: {exc}') from exc
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail='json body must be an object')
    return data


def _enrich_files(conn: sqlite3.Connection, files: list[dict]) -> list[dict]:
    """Attach performer names and face-suggestion counts to each file row.

    Single-pass enrichment using two batched queries instead of N+1.
    """
    if not files:
        return files

    ids = [int(f['id']) for f in files]
    placeholders = ','.join('?' * len(ids))

    perf_rows = conn.execute(
        f"""
        SELECT fp.file_curation_id   AS file_id,
               p.id                  AS performer_id,
               p.canonical_name      AS name,
               p.slug                AS slug,
               fp.confidence         AS confidence,
               fp.source             AS source
          FROM file_performer fp
          JOIN performer p ON p.id = fp.performer_id
         WHERE fp.file_curation_id IN ({placeholders})
         ORDER BY fp.confidence DESC NULLS LAST, p.canonical_name
        """,
        ids,
    ).fetchall()
    by_file_perf: dict[int, list[dict]] = {}
    for r in perf_rows:
        by_file_perf.setdefault(int(r['file_id']), []).append({
            'id': r['performer_id'],
            'name': r['name'],
            'slug': r['slug'],
            'confidence': r['confidence'],
            'source': r['source'],
        })

    sugg_rows = conn.execute(
        f"""
        SELECT file_curation_id AS file_id, COUNT(*) AS cnt
          FROM face_match_result
         WHERE file_curation_id IN ({placeholders})
           AND status = 'pending'
         GROUP BY file_curation_id
        """,
        ids,
    ).fetchall()
    by_file_sugg: dict[int, int] = {int(r['file_id']): int(r['cnt']) for r in sugg_rows}

    for f in files:
        fid = int(f['id'])
        f['performers'] = by_file_perf.get(fid, [])
        f['face_suggestions'] = by_file_sugg.get(fid, 0)
    return files


# ── Pages ────────────────────────────────────────────────────────────────────

@router.get('/library', response_class=HTMLResponse)
async def library_page(
    request: Request,
    mount: str | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    status: str | None = Query(default=None),
):
    """Render the library browser.

    Without ``mount`` we only show the per-mount overview. With a mount we
    additionally list files and any pending renames for that mount.
    """
    conn = _db(request)

    selected_mount = _safe_mount(mount) if mount else None
    page_size = PAGE_SIZE
    offset = (page - 1) * page_size

    files: list[dict] = []
    pending_renames: list[dict] = []
    if selected_mount:
        rows = dbc.list_files_for_mount(
            conn,
            selected_mount,
            status_filter=status,
            limit=page_size,
            offset=offset,
        )
        files = [dict(r) for r in rows]
        files = _enrich_files(conn, files)
        pending_renames = [dict(r) for r in dbc.get_pending_renames(conn, selected_mount)]

    stats = dbc.get_library_stats(conn)

    # Build mount dicts (name + stats) for the sidebar template.
    _stats_by_mount = {s['mount']: s for s in stats}
    mounts_ctx = [
        {
            'name':     m,
            'total':    _stats_by_mount.get(m, {}).get('total', 0),
            'renamed':  _stats_by_mount.get(m, {}).get('renamed', 0),
            'approved': _stats_by_mount.get(m, {}).get('approved', 0),
            'pending':  _stats_by_mount.get(m, {}).get('pending', 0),
            'skipped':  _stats_by_mount.get(m, {}).get('skipped', 0),
            'unknown':  _stats_by_mount.get(m, {}).get('unknown', 0),
        }
        for m in MEDIA_MOUNTS
    ]

    try:
        worker_status = face_worker.get_worker_status()
    except Exception as exc:  # pragma: no cover — defensive
        log.warning('face_worker.get_worker_status failed: %s', exc)
        worker_status = {'running': False, 'queue_size': 0, 'error': str(exc)}

    context = {
        'mounts': mounts_ctx,
        'selected_mount': selected_mount,
        'status_filter': status,
        'page': page,
        'page_size': page_size,
        'files': files,
        'pending_renames': pending_renames,
        'stats': stats,
        'face_worker': worker_status,
        'face_rec_available': is_face_rec_available(),
    }
    return templates.TemplateResponse(
        request,
        'library.html',
        headers={'Cache-Control': 'no-store'},
        context=context,
    )


# ── Scan ─────────────────────────────────────────────────────────────────────

@router.post('/library/scan')
async def library_scan(request: Request):
    """Trigger a filesystem scan for one mount and persist parsed metadata."""
    body = await _read_json(request)
    mount = (body.get('mount') or '').strip()
    if not mount:
        raise HTTPException(status_code=400, detail='mount required')
    safe = _safe_mount(mount)

    mount_path = f'/media/{safe}'
    try:
        stats = curation_extractor.scan_mount(_db(request), mount_path, safe)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        log.exception('scan failed for mount=%s', safe)
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)

    # Normalise the stats payload so the frontend can rely on all four keys.
    payload = {
        'total':  int(stats.get('total', 0)),
        'parsed': int(stats.get('parsed', 0)),
        'opaque': int(stats.get('opaque', 0)),
        'errors': int(stats.get('errors', 0)),
    }
    return JSONResponse({'ok': True, 'stats': payload})


# ── Per-file actions ─────────────────────────────────────────────────────────

@router.post('/library/files/{file_id}/approve')
async def library_file_approve(file_id: int, request: Request):
    """Approve a single parsed file.

    If ``proposed_filename`` isn't set yet we recompute it from the stored
    parse result so the rename worker has something to act on.
    """
    conn = _db(request)
    row = conn.execute(
        "SELECT id, proposed_filename, parse_result FROM file_curation WHERE id = ?",
        (file_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail='file not found')

    proposed = row['proposed_filename']
    if not proposed and row['parse_result']:
        try:
            parsed = json.loads(row['parse_result'])
            proposed = curation_extractor.build_target_filename(parsed)
        except (json.JSONDecodeError, Exception) as exc:  # noqa: BLE001
            log.warning('approve: could not rebuild proposed filename id=%s: %s', file_id, exc)
            proposed = None

    with conn:
        if proposed:
            conn.execute(
                "UPDATE file_curation SET status='approved', proposed_filename=? WHERE id=?",
                (proposed, file_id),
            )
        else:
            conn.execute(
                "UPDATE file_curation SET status='approved' WHERE id=?",
                (file_id,),
            )
    return JSONResponse({'ok': True})


@router.post('/library/files/{file_id}/skip')
async def library_file_skip(file_id: int, request: Request):
    """Mark a file as skipped so it stops appearing in pending queues."""
    conn = _db(request)
    cur = conn.execute("SELECT 1 FROM file_curation WHERE id = ?", (file_id,))
    if cur.fetchone() is None:
        raise HTTPException(status_code=404, detail='file not found')
    with conn:
        conn.execute("UPDATE file_curation SET status='skipped' WHERE id=?", (file_id,))
    return JSONResponse({'ok': True})


# ── Batch approve ────────────────────────────────────────────────────────────

@router.post('/library/batch/approve')
async def library_batch_approve(request: Request):
    """Bulk-approve a list of file IDs in a single transaction."""
    body = await _read_json(request)
    raw_ids = body.get('ids')
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail='ids must be a non-empty list')

    ids: list[int] = []
    for v in raw_ids:
        try:
            ids.append(int(v))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail=f'invalid id: {v!r}')

    conn = _db(request)
    with conn:
        cur = conn.executemany(
            "UPDATE file_curation SET status='approved' WHERE id=?",
            [(i,) for i in ids],
        )
        # executemany.rowcount is unreliable across drivers — re-verify.
        count = conn.execute(
            f"SELECT COUNT(*) FROM file_curation WHERE status='approved' AND id IN ({','.join('?' * len(ids))})",
            ids,
        ).fetchone()[0]
    _ = cur  # silence linter
    return JSONResponse({'ok': True, 'count': int(count)})


# ── Rename batch / rollback ──────────────────────────────────────────────────

@router.post('/library/rename/run')
async def library_rename_run(request: Request):
    """Run the batch rename worker for approved files."""
    body = await _read_json(request)
    mount = body.get('mount')
    if mount is not None:
        mount = _safe_mount(str(mount))

    try:
        limit = int(body.get('limit', 50))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail='limit must be an integer')
    limit = max(1, min(limit, 1000))

    try:
        result = curation_rename.execute_batch_rename(_db(request), mount, limit)
    except Exception as exc:
        log.exception('batch rename failed mount=%s limit=%s', mount, limit)
        return JSONResponse(
            {'ok': 0, 'failed': 0, 'errors': [str(exc)]},
            status_code=500,
        )

    return JSONResponse({
        'ok':     int(result.get('ok', 0)),
        'failed': int(result.get('failed', 0)),
        'errors': list(result.get('errors', [])),
    })


@router.post('/library/rename/{log_id}/rollback')
async def library_rename_rollback(log_id: int, request: Request):
    """Undo a previously executed rename by log id."""
    try:
        ok = curation_rename.rollback_rename(_db(request), log_id)
    except Exception as exc:
        log.exception('rollback failed log_id=%s', log_id)
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)
    return JSONResponse({'ok': bool(ok), 'error': None})


# ── Face queue ───────────────────────────────────────────────────────────────

@router.post('/library/face/enqueue-all')
async def library_face_enqueue_all(request: Request):
    """Enqueue face-detection jobs for every file with unknown performers."""
    if not is_face_rec_available():
        return JSONResponse(
            {'ok': False, 'error': 'face_rec_unavailable', 'enqueued': 0},
            status_code=503,
        )
    try:
        enqueued = face_worker.enqueue_all_unknown(_db(request))
    except Exception as exc:
        log.exception('enqueue_all_unknown failed')
        return JSONResponse({'ok': False, 'error': str(exc), 'enqueued': 0}, status_code=500)
    return JSONResponse({'ok': True, 'enqueued': int(enqueued)})
