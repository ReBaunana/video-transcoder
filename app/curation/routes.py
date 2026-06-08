"""HTTP routes for the library / curation surface.

Routes are mounted by :mod:`app.main` via ``app.include_router``. The shared
SQLite connection lives on ``request.app.state.db`` — main.py assigns it there
during startup so the rest of the application can use the FastAPI-canonical
``app.state`` channel instead of a cross-module global.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app import database_curation as dbc
from app.curation import extractor as curation_extractor
from app.curation import rename as curation_rename
from app.curation import tpdb as curation_tpdb
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


def _get_recent_rename_log(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Return the most recent rename_log rows, shaped for the library template.

    The template expects ``from``, ``to``, ``ok`` keys per row — the table
    stores ``from_path``, ``to_path`` and ``success`` so we map them here.
    """
    try:
        rows = conn.execute(
            """
            SELECT id, file_curation_id, from_path, to_path,
                   executed_at, success, error_message, rolled_back_at
              FROM rename_log
             ORDER BY id DESC
             LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        # Older schemas without rename_log — degrade gracefully.
        return []
    out: list[dict] = []
    for r in rows:
        from_path = r['from_path']
        to_path = r['to_path']
        out.append({
            'id':           int(r['id']),
            'file_id':      int(r['file_curation_id']),
            'from':         Path(from_path).name if from_path else '',
            'to':           Path(to_path).name if to_path else '',
            'from_path':    from_path,
            'to_path':      to_path,
            'executed_at':  r['executed_at'],
            'ok':           bool(r['success']),
            'error':        r['error_message'],
            'rolled_back':  r['rolled_back_at'] is not None,
        })
    return out


def _face_queue_status(conn: sqlite3.Connection) -> dict:
    """Build the dict the library template renders for the Face queue card.

    ``face_worker.get_worker_status`` returns ``queued`` as the combined
    pending + running count, plus a boolean ``running`` flag. The template
    wants ``running`` to be a *count* of jobs currently running so it can sum
    ``queued + running`` for the badge. Query the table directly to split.
    """
    worker = face_worker.get_worker_status()
    pending = 0
    running = 0
    try:
        cur = conn.execute(
            """
            SELECT status, COUNT(*)
              FROM face_recognition_job
             WHERE status IN ('pending', 'running')
             GROUP BY status
            """
        )
        for status, cnt in cur.fetchall():
            if status == 'pending':
                pending = int(cnt or 0)
            elif status == 'running':
                running = int(cnt or 0)
    except sqlite3.OperationalError:
        # Table missing on minimal fixtures — fall back to whatever the worker
        # gave us so the page still renders.
        pending = int(worker.get('queued', 0) or 0)
        running = 0
    return {
        'worker_running': bool(worker.get('running', False)),
        'queued':         pending,
        'running':        running,
        'done':           int(worker.get('done', 0) or 0),
        'failed':         int(worker.get('failed', 0) or 0),
        'error':          worker.get('error'),
    }


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
               fp.position           AS position,
               fp.source             AS source
          FROM file_performer fp
          JOIN performer p ON p.id = fp.performer_id
         WHERE fp.file_curation_id IN ({placeholders})
         ORDER BY fp.position, p.canonical_name
        """,
        ids,
    ).fetchall()
    by_file_perf: dict[int, list[dict]] = {}
    for r in perf_rows:
        by_file_perf.setdefault(int(r['file_id']), []).append({
            'id': r['performer_id'],
            'name': r['name'],
            'slug': r['slug'],
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
        perf_list = by_file_perf.get(fid, [])
        f['performers'] = [p['name'] for p in perf_list if p.get('name')]
        f['performer_details'] = perf_list
        f['face_suggestions'] = by_file_sugg.get(fid, 0)
        # Template uses f.filename (basename); the DB row has f.path (full path).
        f['filename'] = Path(f['path']).name if f.get('path') else ''
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
    total_files = 0
    approved_count = 0
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

    # Pull the totals the template renders in the stats row and pagination.
    # Falls back to 0 for both when no mount is selected.
    if selected_mount:
        sel_stats = _stats_by_mount.get(selected_mount, {})
        total_files = int(sel_stats.get('total', 0) or 0)
        approved_count = int(sel_stats.get('approved', 0) or 0)
    total_pages = max(1, math.ceil(total_files / page_size)) if total_files else 1

    # Face queue dict in the shape the library template expects (queued/running
    # counts plus done/failed totals).
    try:
        face_queue_status = _face_queue_status(conn)
    except Exception as exc:  # pragma: no cover — defensive
        log.warning('face_queue_status failed: %s', exc)
        face_queue_status = {
            'worker_running': False, 'queued': 0, 'running': 0,
            'done': 0, 'failed': 0, 'error': str(exc),
        }

    rename_log = _get_recent_rename_log(conn, limit=10) if selected_mount else []

    context = {
        'mounts':             mounts_ctx,
        'selected_mount':     selected_mount,
        'sel_mount':          selected_mount,
        'status_filter':      status,
        'qs_status':          ('&status=' + status) if status else '',
        'page':               page,
        'page_size':          page_size,
        'files':              files,
        'pending_renames':    pending_renames,
        'stats':              stats,
        'face_queue_status':  face_queue_status,
        'face_rec_available': is_face_rec_available(),
        'total_files':        total_files,
        'total_pages':        total_pages,
        'approved_count':     approved_count,
        'rename_log':         rename_log,
        'version':            os.getenv('APP_VERSION', 'dev'),
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


# ── Purge missing files ──────────────────────────────────────────────────────

@router.post('/library/purge-missing')
async def library_purge_missing(request: Request):
    """Delete file_curation rows whose files no longer exist on disk.

    Checks every row for the requested mount and removes those where the
    resolved path is absent. Cascades to file_performer and face-related rows
    via foreign-key DELETEs (or explicit cleanup if FK enforcement is off).
    """
    body = await _read_json(request)
    mount = (body.get('mount') or '').strip()
    if not mount:
        raise HTTPException(status_code=400, detail='mount required')
    safe = _safe_mount(mount)

    conn = _db(request)
    rows = conn.execute(
        "SELECT id, path, mount FROM file_curation WHERE mount = ?",
        (safe,),
    ).fetchall()

    removed = 0
    for row in rows:
        path = row['path']
        p = Path(path) if Path(path).is_absolute() else Path('/media') / safe / path
        if not p.exists():
            try:
                with conn:
                    conn.execute("DELETE FROM file_performer WHERE file_curation_id = ?", (row['id'],))
                    conn.execute("DELETE FROM face_recognition_job WHERE file_curation_id = ?", (row['id'],))
                    conn.execute("DELETE FROM face_match_result WHERE file_curation_id = ?", (row['id'],))
                    conn.execute("DELETE FROM face_embedding WHERE file_curation_id = ?", (row['id'],))
                    conn.execute("DELETE FROM file_curation WHERE id = ?", (row['id'],))
                removed += 1
            except Exception:
                log.exception('purge-missing: delete failed for id=%s path=%s', row['id'], path)

    log.info('purge-missing: mount=%s checked=%d removed=%d', safe, len(rows), removed)
    return JSONResponse({'ok': True, 'removed': removed, 'checked': len(rows)})


@router.post('/library/purge-orphan-performers')
async def library_purge_orphan_performers(request: Request):
    """Delete performer rows that have no linked files.

    A performer is an orphan when every file_curation row it was linked to has
    been removed (e.g. by purge-missing). The cascade also removes their
    face_embedding and performer_alias rows.
    """
    conn = _db(request)
    orphans = conn.execute(
        """
        SELECT id, canonical_name FROM performer
         WHERE id NOT IN (SELECT DISTINCT performer_id FROM file_performer)
        """
    ).fetchall()

    removed = 0
    for row in orphans:
        try:
            with conn:
                conn.execute("DELETE FROM face_embedding WHERE performer_id = ?", (row['id'],))
                conn.execute("DELETE FROM performer_alias WHERE performer_id = ?", (row['id'],))
                conn.execute("DELETE FROM performer WHERE id = ?", (row['id'],))
            removed += 1
        except Exception:
            log.exception('purge-orphan-performers: delete failed for id=%s name=%s', row['id'], row['canonical_name'])

    log.info('purge-orphan-performers: checked=%d removed=%d', len(orphans), removed)
    return JSONResponse({'ok': True, 'removed': removed, 'checked': len(orphans)})


# ── Per-file actions ─────────────────────────────────────────────────────────

@router.post('/library/files/{file_id}/approve')
async def library_file_approve(file_id: int, request: Request):
    """Approve a single parsed file.

    If ``proposed_filename`` isn't set yet we recompute it from the stored
    parse result so the rename worker has something to act on.
    """
    conn = _db(request)
    row = conn.execute(
        "SELECT id, proposed_filename FROM file_curation WHERE id = ?",
        (file_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail='file not found')

    proposed = row['proposed_filename']
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

# ── ThePornDB enrichment ─────────────────────────────────────────────────────

@router.get('/library/tpdb/status')
async def library_tpdb_status() -> JSONResponse:
    """Quick health-check the frontend uses to decide whether to show TPDB UI."""
    return JSONResponse({'available': bool(curation_tpdb.is_configured())})


@router.get('/api/library-stats')
async def api_library_stats(request: Request) -> JSONResponse:
    """Mount stats for JS polling — total/renamed/approved/pending per mount."""
    conn = _db(request)
    stats = dbc.get_library_stats(conn)
    by_mount = {s['mount']: s for s in stats}
    return JSONResponse([
        {
            'name':     m,
            'total':    by_mount.get(m, {}).get('total', 0),
            'renamed':  by_mount.get(m, {}).get('renamed', 0),
            'approved': by_mount.get(m, {}).get('approved', 0),
            'pending':  by_mount.get(m, {}).get('pending', 0),
            'skipped':  by_mount.get(m, {}).get('skipped', 0),
            'unknown':  by_mount.get(m, {}).get('unknown', 0),
        }
        for m in MEDIA_MOUNTS
    ])


def _thumb_url(path: str | None) -> str | None:
    """Convert /data/face_thumbs/123.jpg → /static/faces/123.jpg."""
    if not path:
        return None
    return '/static/faces/' + Path(path).name


@router.get('/review')
async def review_page(request: Request):
    conn = _db(request)
    total = conn.execute(
        "SELECT COUNT(DISTINCT file_curation_id) FROM face_match_result WHERE status='pending'"
    ).fetchone()[0]
    return templates.TemplateResponse('review.html', {
        'request': request,
        'page_id': 'review',
        'queue_total': total,
    })


@router.get('/api/review/queue')
async def review_queue(
    request: Request,
    offset: int = Query(0, ge=0),
    limit: int = Query(1, ge=1, le=20),
):
    conn = _db(request)

    def _g_rank(g: str) -> int:
        return 0 if g == 'female' else (2 if g == 'male' else 1)

    total = conn.execute(
        "SELECT COUNT(DISTINCT file_curation_id) FROM face_match_result WHERE status='pending'"
    ).fetchone()[0]

    fc_ids = [
        r[0] for r in conn.execute("""
            SELECT fmr.file_curation_id
            FROM face_match_result fmr
            WHERE fmr.status = 'pending'
            GROUP BY fmr.file_curation_id
            ORDER BY MAX(fmr.similarity) DESC
            LIMIT ? OFFSET ?
        """, (limit, offset)).fetchall()
    ]

    items = []
    for fc_id in fc_ids:
        fc = conn.execute(
            "SELECT path, mount, studio, title, release_date, resolution FROM file_curation WHERE id=?",
            (fc_id,),
        ).fetchone()
        if not fc:
            continue

        candidates_raw = conn.execute("""
            SELECT fmr.id, fmr.performer_id, p.canonical_name,
                   COALESCE(p.gender,'unknown') AS gender,
                   fmr.similarity, fmr.match_count,
                   p.profile_thumb
            FROM face_match_result fmr
            JOIN performer p ON p.id = fmr.performer_id
            WHERE fmr.file_curation_id = ? AND fmr.status = 'pending'
            ORDER BY CASE COALESCE(p.gender,'unknown')
                         WHEN 'female'  THEN 0
                         WHEN 'unknown' THEN 1
                         ELSE 2
                     END ASC,
                     fmr.match_count DESC, fmr.similarity DESC
        """, (fc_id,)).fetchall()

        candidates = []
        for i, c in enumerate(candidates_raw):
            # Up to 4 reference thumbnails for this performer
            ref_thumbs = [
                _thumb_url(r[0])
                for r in conn.execute("""
                    SELECT thumbnail_path FROM face_embedding
                    WHERE performer_id=? AND thumbnail_path IS NOT NULL
                    ORDER BY COALESCE(quality_score,0) DESC LIMIT 4
                """, (c[1],)).fetchall()
                if r[0]
            ]
            candidates.append({
                'match_id': c[0],
                'performer_id': c[1],
                'name': c[2],
                'gender': c[3],
                'similarity': round(float(c[4]), 3),
                'match_count': int(c[5]),
                'rank': i + 1,
                'profile_thumb': _thumb_url(c[6]),
                'ref_thumbs': ref_thumbs,
            })

        is_ambiguous = False
        if len(candidates) >= 2:
            p0, p1 = candidates[0], candidates[1]
            if _g_rank(p1['gender']) <= _g_rank(p0['gender']):
                if p1['match_count'] >= 0.7 * p0['match_count']:
                    is_ambiguous = True

        items.append({
            'file_id': fc_id,
            'filename': Path(fc[0]).name,
            'path': fc[0],
            'mount': fc[1] or '',
            'studio': fc[2] or '',
            'title': fc[3] or '',
            'release_date': fc[4] or '',
            'resolution': fc[5] or '',
            'is_ambiguous': is_ambiguous,
            'candidates': candidates,
        })

    return JSONResponse({'total': total, 'offset': offset, 'items': items})


@router.post('/api/face/accept/{match_id}')
async def face_accept(match_id: int, request: Request):
    conn = _db(request)
    row = conn.execute(
        "SELECT file_curation_id FROM face_match_result WHERE id=?", (match_id,)
    ).fetchone()
    if not row:
        return JSONResponse({'ok': False, 'error': 'not_found'}, status_code=404)

    fc_id = row[0]
    secondaries = [
        r[0] for r in conn.execute(
            "SELECT id FROM face_match_result WHERE file_curation_id=? AND id!=? AND status='pending'",
            (fc_id, match_id),
        ).fetchall()
    ]
    try:
        from app.face.matcher import accept_match
        accept_match(conn, match_id, secondary_match_ids=secondaries or None)
        remaining = conn.execute(
            "SELECT COUNT(DISTINCT file_curation_id) FROM face_match_result WHERE status='pending'"
        ).fetchone()[0]
        return JSONResponse({'ok': True, 'remaining': remaining})
    except Exception as exc:
        log.exception('face_accept failed match_id=%s', match_id)
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)


@router.post('/api/face/reject/{match_id}')
async def face_reject(match_id: int, request: Request):
    conn = _db(request)
    cur = conn.execute(
        "UPDATE face_match_result SET status='rejected', resolved_at=datetime('now') "
        "WHERE id=? AND status='pending'",
        (match_id,),
    )
    if cur.rowcount == 0:
        return JSONResponse({'ok': False, 'error': 'not_found_or_not_pending'}, status_code=404)
    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(DISTINCT file_curation_id) FROM face_match_result WHERE status='pending'"
    ).fetchone()[0]
    return JSONResponse({'ok': True, 'remaining': remaining})


@router.post('/api/review/delete-all')
async def review_delete_all(request: Request):
    """Delete all files currently in the review queue from filesystem and DB."""
    conn = _db(request)
    rows = conn.execute(
        """
        SELECT fc.id, fc.path
          FROM file_curation fc
         WHERE fc.id IN (
             SELECT DISTINCT file_curation_id FROM face_match_result WHERE status='pending'
         )
        """
    ).fetchall()

    deleted_files = 0
    deleted_db = 0
    errors: list[str] = []

    for fc_id, path in rows:
        if path:
            try:
                if os.path.lexists(path):
                    os.unlink(path)
                    deleted_files += 1
            except OSError as exc:
                errors.append(f"{path}: {exc}")
        try:
            conn.execute("DELETE FROM face_match_result  WHERE file_curation_id=?", (fc_id,))
            conn.execute("DELETE FROM face_recognition_job WHERE file_curation_id=?", (fc_id,))
            conn.execute("DELETE FROM face_embedding      WHERE file_curation_id=?", (fc_id,))
            conn.execute("DELETE FROM file_performer      WHERE file_curation_id=?", (fc_id,))
            conn.execute("DELETE FROM file_curation       WHERE id=?", (fc_id,))
            deleted_db += 1
        except Exception as exc:
            errors.append(f"db id={fc_id}: {exc}")

    conn.commit()
    return JSONResponse({
        'ok': True,
        'deleted_files': deleted_files,
        'deleted_db': deleted_db,
        'errors': errors[:10],
    })


@router.post('/api/face/bulk-reject-males')
async def face_bulk_reject_males(request: Request):
    conn = _db(request)
    cur = conn.execute(
        """
        UPDATE face_match_result SET status='rejected', resolved_at=datetime('now')
        WHERE status='pending'
          AND performer_id IN (SELECT id FROM performer WHERE gender='male')
        """
    )
    count = cur.rowcount
    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(DISTINCT file_curation_id) FROM face_match_result WHERE status='pending'"
    ).fetchone()[0]
    return JSONResponse({'ok': True, 'rejected': count, 'remaining': remaining})


@router.post('/api/face/accept-all-clear')
async def face_accept_all_clear(request: Request):
    from app.face.matcher import accept_match, AUTO_ACCEPT_THRESHOLD

    conn = _db(request)

    def _g_rank(g: str) -> int:
        return 0 if g == 'female' else (2 if g == 'male' else 1)

    pending_fc_ids = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT file_curation_id FROM face_match_result WHERE status='pending'"
        ).fetchall()
    ]

    accepted = 0
    for fc_id in pending_fc_ids:
        candidates = conn.execute(
            """
            SELECT mr.id, mr.similarity, mr.match_count, COALESCE(p.gender,'unknown')
              FROM face_match_result mr
              JOIN performer p ON p.id = mr.performer_id
             WHERE mr.file_curation_id = ? AND mr.status = 'pending'
             ORDER BY CASE COALESCE(p.gender,'unknown')
                          WHEN 'female' THEN 0 WHEN 'unknown' THEN 1 ELSE 2
                      END,
                      mr.match_count DESC, mr.similarity DESC
            """,
            (fc_id,),
        ).fetchall()
        if not candidates:
            continue
        primary = candidates[0]
        secondaries = candidates[1:]
        same_tier = [s for s in secondaries if _g_rank(s[3]) <= _g_rank(primary[3])]
        dominant = not same_tier or same_tier[0][2] < 0.7 * primary[2]
        if primary[1] < AUTO_ACCEPT_THRESHOLD or not dominant:
            continue
        secondary_ids = [int(s[0]) for s in secondaries]
        try:
            accept_match(conn, primary[0], secondary_match_ids=secondary_ids or None)
            accepted += 1
        except Exception:
            log.exception('accept_all_clear failed fc_id=%s', fc_id)

    remaining = conn.execute(
        "SELECT COUNT(DISTINCT file_curation_id) FROM face_match_result WHERE status='pending'"
    ).fetchone()[0]
    return JSONResponse({'ok': True, 'accepted': accepted, 'remaining': remaining})


@router.get('/api/face-status')
async def api_face_status(request: Request) -> JSONResponse:
    """Live face-recognition queue snapshot — running jobs, throughput, ETA."""
    conn = _db(request)

    running_rows = conn.execute(
        """
        SELECT id, job_type, started_at
          FROM face_recognition_job
         WHERE status = 'running'
         ORDER BY started_at
        """
    ).fetchall()
    running_jobs = [
        {
            'id':         int(r['id']),
            'job_type':   r['job_type'],
            'started_at': r['started_at'],
        }
        for r in running_rows
    ]

    current_file: str | None = None
    current_files: list[str] = []
    if running_jobs:
        running_file_rows = conn.execute(
            """
            SELECT fc.path
              FROM face_recognition_job j
              JOIN file_curation fc ON fc.id = j.file_curation_id
             WHERE j.status = 'running'
             ORDER BY j.started_at DESC
            """
        ).fetchall()
        current_files = [os.path.basename(r['path']) for r in running_file_rows if r['path']]
        current_file = current_files[0] if current_files else None

    queued_row = conn.execute(
        "SELECT COUNT(*) FROM face_recognition_job WHERE status IN ('queued', 'pending')"
    ).fetchone()
    queued = int(queued_row[0] or 0) if queued_row else 0

    totals_row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status='done' THEN 1 ELSE 0 END),
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END)
          FROM face_recognition_job
        """
    ).fetchone()
    done_total = int((totals_row[0] if totals_row else 0) or 0)
    failed_total = int((totals_row[1] if totals_row else 0) or 0)

    failure_rate_row = conn.execute(
        """
        SELECT
          SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) * 100 / COUNT(*)
          FROM (
            SELECT status FROM face_recognition_job
             WHERE status IN ('done','failed')
             ORDER BY id DESC LIMIT 50
          )
        """
    ).fetchone()
    failure_rate_pct = int(failure_rate_row[0] or 0) if failure_rate_row and failure_rate_row[0] is not None else 0

    recent_failure_rows = conn.execute(
        """
        SELECT id, last_error, finished_at
          FROM face_recognition_job
         WHERE status='failed'
         ORDER BY id DESC LIMIT 3
        """
    ).fetchall()
    recent_failures = [
        {
            'id':          int(r['id']),
            'last_error':  r['last_error'],
            'finished_at': r['finished_at'],
        }
        for r in recent_failure_rows
    ]

    throughput_row = conn.execute(
        """
        SELECT COUNT(*) * 3600.0 / (strftime('%s','now') - MIN(finished_at))
          FROM face_recognition_job
         WHERE status = 'done'
           AND typeof(finished_at) = 'integer'
           AND finished_at > strftime('%s','now') - 3600
        HAVING COUNT(*) > 0
        """
    ).fetchone()
    throughput_per_hour = float(throughput_row[0]) if throughput_row and throughput_row[0] is not None else 0.0

    if throughput_per_hour > 0 and queued > 0:
        eta_minutes: int | None = int(queued / (throughput_per_hour / 60))
    else:
        eta_minutes = None

    last_finished_row = conn.execute(
        """
        SELECT finished_at FROM face_recognition_job
         WHERE status IN ('done','failed') AND typeof(finished_at) = 'integer'
         ORDER BY finished_at DESC LIMIT 1
        """
    ).fetchone()
    last_finished_at = last_finished_row[0] if last_finished_row else None

    return JSONResponse({
        'running_jobs':        running_jobs,
        'current_file':        current_file,
        'current_files':       current_files,
        'queued':              queued,
        'done_total':          done_total,
        'failed_total':        failed_total,
        'recent_failures':     recent_failures,
        'throughput_per_hour': throughput_per_hour,
        'eta_minutes':         eta_minutes,
        'failure_rate_pct':    failure_rate_pct,
        'last_finished_at':    last_finished_at,
    })


@router.get('/api/face-workers')
async def api_get_face_workers() -> JSONResponse:
    from app.face.worker import get_n_workers
    return JSONResponse({'count': get_n_workers()})


@router.post('/api/face-workers')
async def api_set_face_workers(request: Request) -> JSONResponse:
    body = await request.json()
    n = max(1, min(int(body.get('count', 3)), 8))
    from app.face.worker import resize_pool
    resize_pool(n)
    import app.transcoder as _t
    _t.FACE_WORKERS = n
    _t.save_settings()
    return JSONResponse({'count': n})


@router.post('/library/files/{file_id}/tpdb')
async def library_file_tpdb(file_id: int, request: Request) -> JSONResponse:
    """Lookup a single file against ThePornDB and auto-apply if confident."""
    if not curation_tpdb.is_configured():
        return JSONResponse(
            {'ok': False, 'error': 'tpdb_not_configured'},
            status_code=503,
        )

    conn = _db(request)
    row = conn.execute(
        "SELECT id FROM file_curation WHERE id = ?",
        (file_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail='file not found')

    try:
        # Network IO + DB writes — push off the event loop.
        result = await asyncio.to_thread(
            curation_tpdb.enrich_file_from_tpdb, conn, int(file_id),
        )
    except Exception as exc:
        log.exception('tpdb enrichment failed file_id=%s', file_id)
        return JSONResponse(
            {'ok': False, 'error': str(exc)},
            status_code=500,
        )

    return JSONResponse({
        'ok':              bool(result.get('ok', True)),
        'applied':         bool(result.get('applied', False)),
        'score':           float(result.get('score', 0.0) or 0.0),
        'tpdb_scene_id':   result.get('tpdb_scene_id'),
        'scene':           result.get('scene'),
        'title':           (result.get('scene') or {}).get('title'),
        'studio':          (result.get('scene') or {}).get('studio'),
        'release_date':    (result.get('scene') or {}).get('date'),
        'performers':      result.get('performers', []),
        'candidates':      result.get('candidates', []),
        'proposed_filename': result.get('proposed_filename'),
        'error':           result.get('error'),
    })


@router.post('/library/tpdb/batch')
async def library_tpdb_batch(request: Request) -> JSONResponse:
    """Run TPDB lookups for a batch of pending files on a mount.

    Body: ``{mount?: str, max_files?: int}``
    """
    if not curation_tpdb.is_configured():
        return JSONResponse(
            {'ok': False, 'error': 'tpdb_not_configured'},
            status_code=503,
        )

    body = await _read_json(request)
    mount_raw = body.get('mount')
    mount = _safe_mount(str(mount_raw)) if mount_raw else None

    try:
        max_files = int(body.get('max_files', 50))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail='max_files must be an integer')
    max_files = max(1, min(max_files, 200))

    conn = _db(request)
    sql = """
        SELECT id
          FROM file_curation
         WHERE tpdb_lookup_at IS NULL
           AND status IN ('pending', 'reviewed')
    """
    params: list[Any] = []
    if mount:
        sql += " AND mount = ?"
        params.append(mount)
    sql += " ORDER BY id LIMIT ?"
    params.append(int(max_files))

    try:
        rows = conn.execute(sql, params).fetchall()
    except sqlite3.OperationalError as exc:
        log.warning('tpdb batch lookup query failed: %s', exc)
        return JSONResponse(
            {'ok': False, 'error': f'db_error: {exc}'},
            status_code=500,
        )

    ids = [int(r['id']) for r in rows]

    def _run_batch() -> dict[str, int | list[str]]:
        applied = 0
        skipped = 0
        errors_out: list[str] = []
        for fid in ids:
            try:
                res = curation_tpdb.enrich_file_from_tpdb(conn, fid)
                if res.get('applied'):
                    applied += 1
                else:
                    skipped += 1
            except Exception as exc:  # pragma: no cover — defensive
                log.warning('tpdb batch: file_id=%s failed: %s', fid, exc)
                errors_out.append(f'{fid}: {exc}')
        return {
            'applied':  applied,
            'skipped':  skipped,
            'errors':   errors_out,
        }

    summary = await asyncio.to_thread(_run_batch)

    return JSONResponse({
        'ok':      True,
        'total':   len(ids),
        'applied': summary['applied'],
        'skipped': summary['skipped'],
        'errors':  summary['errors'],
    })


@router.patch('/api/performers/{performer_id}/gender')
async def set_performer_gender(performer_id: int, request: Request):
    """Set the gender for a performer ('female', 'male', 'unknown')."""
    body = await request.json()
    gender = str(body.get('gender', '')).lower()
    if gender not in ('female', 'male', 'unknown'):
        return JSONResponse({'ok': False, 'error': 'gender must be female, male, or unknown'}, status_code=400)
    conn = _db(request)
    cur = conn.execute(
        "UPDATE performer SET gender = ?, updated_at = datetime('now') WHERE id = ?",
        (gender, performer_id),
    )
    if cur.rowcount == 0:
        return JSONResponse({'ok': False, 'error': 'performer not found'}, status_code=404)
    conn.commit()
    # Reload the face index so the new gender is picked up immediately.
    try:
        from app.face.matcher import get_index
        get_index().reload(conn)
    except Exception:
        log.exception('set_performer_gender: index reload failed')
    return JSONResponse({'ok': True, 'performer_id': performer_id, 'gender': gender})


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


# ---------------------------------------------------------------------------
# Watermark-URL OCR identification
# ---------------------------------------------------------------------------

@router.get('/watermark/pending')
async def watermark_pending(request: Request):
    """Distinct unmapped watermark keys with file counts — map each once."""
    from app.curation import watermark
    try:
        return JSONResponse({'ok': True, 'pending': watermark.pending_mappings(_db(request))})
    except Exception as exc:
        log.exception('watermark_pending failed')
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)


@router.post('/watermark/map')
async def watermark_map(request: Request):
    """Map a watermark key (URL/handle) to a performer; auto-assigns waiting files."""
    from app.curation import watermark
    body = await _read_json(request)
    key = (body.get('url_key') or '').strip()
    performer_id = body.get('performer_id')
    if not key or performer_id is None:
        raise HTTPException(status_code=400, detail='url_key and performer_id required')
    try:
        assigned = watermark.map_url(_db(request), key, int(performer_id))
    except Exception as exc:
        log.exception('watermark_map failed')
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)
    return JSONResponse({'ok': True, 'assigned': assigned})


@router.post('/watermark/run')
async def watermark_run(request: Request):
    """Trigger a watermark-OCR batch now (default 40 files)."""
    from app.curation import watermark
    body = await _read_json(request)
    limit = int(body.get('limit', 40))
    if not watermark.ocr_available():
        return JSONResponse({'ok': False, 'error': 'tesseract not available'}, status_code=503)
    try:
        result = watermark.run_watermark_ocr(_db(request), limit=limit)
    except Exception as exc:
        log.exception('watermark_run failed')
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)
    return JSONResponse({'ok': True, 'result': result})
