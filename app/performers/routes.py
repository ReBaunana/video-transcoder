"""HTTP routes for performer management.

All endpoints use the shared SQLite connection via ``request.app.state.db``.
Photo upload integrates with InsightFace when available and degrades gracefully
to ``{'ok': False, 'error': 'face_rec_unavailable'}`` when it isn't installed.
"""

from __future__ import annotations

import asyncio
import io
import json
import logging
import math
import os
import sqlite3
from pathlib import Path
from typing import Any

from fastapi import (
    APIRouter,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from app import database_curation as dbc
from app.curation import tpdb as curation_tpdb
from app.face import worker as face_worker
from app.face import matcher as face_matcher
from app.face.model import embed_to_blob, is_face_rec_available

router = APIRouter()
templates = Jinja2Templates(directory='app/templates')
log = logging.getLogger('performers.routes')

PAGE_SIZE = 50
MAX_PHOTO_BYTES = 8 * 1024 * 1024  # 8 MiB
ALLOWED_PHOTO_MIME = {'image/jpeg', 'image/jpg', 'image/png'}
ALLOWED_PHOTO_EXT = {'.jpg', '.jpeg', '.png'}

FACE_THUMB_DIR = Path('/data/face_thumbs')


def _db(request: Request) -> sqlite3.Connection:
    """Return the shared SQLite connection placed on ``app.state`` at startup."""
    conn = getattr(request.app.state, 'db', None)
    if conn is None:
        raise HTTPException(status_code=503, detail='database not initialised')
    return conn


async def _read_json(request: Request) -> dict[str, Any]:
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


def _get_performer_or_404(conn: sqlite3.Connection, performer_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT id, canonical_name, slug, profile_thumb,
               embedding_count, is_reference_ready,
               (SELECT COUNT(*) FROM file_performer fp WHERE fp.performer_id = performer.id)
                   AS video_count
          FROM performer WHERE id = ?
        """,
        (performer_id,),
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail='performer not found')
    return row


def _shape_video_rows(rows: list[dict]) -> list[dict]:
    """Adapt rows from ``dbc.get_performer_videos`` for the performers template.

    The template reads ``v.filename``, ``v.confidence``, ``v.thumbnail`` and
    ``v.mount``; the query exposes ``path``, ``match_similarity`` and no
    thumbnail. Map them here so the template doesn't need to know about the
    raw schema.
    """
    out: list[dict] = []
    for r in rows:
        path = r.get('path') or ''
        filename = Path(path).name if path else (r.get('title') or '')
        out.append({
            **r,
            'filename':   filename,
            'mount':      r.get('mount'),
            'confidence': r.get('match_similarity') or 0.0,
            'thumbnail':  r.get('thumbnail') or None,
            'status':     r.get('status') or 'pending',
        })
    return out


def _shape_face_match_rows(rows: list[dict]) -> list[dict]:
    """Adapt face_match_result rows for the performers template.

    Template uses ``v.id``, ``v.filename``, ``v.confidence``, ``v.mount`` and
    ``v.thumbnail`` — the underlying query returns match-centric keys.
    """
    out: list[dict] = []
    for r in rows:
        original = r.get('original_path') or ''
        mount = None
        if original.startswith('/media/'):
            parts = original.split('/', 3)
            if len(parts) >= 3:
                mount = parts[2]
        filename = r.get('original_name') or (Path(original).name if original else '')
        out.append({
            'id':         r.get('file_id'),
            'match_id':   r.get('match_id'),
            'filename':   filename,
            'confidence': r.get('score') or 0.0,
            'mount':      mount,
            'thumbnail':  r.get('thumb_path'),
            'status':     r.get('status') or 'pending',
        })
    return out


def _get_unknown_faces(conn: sqlite3.Connection, limit: int = 24) -> list[dict]:
    """Recent face embeddings with no performer assignment for the index page."""
    try:
        rows = conn.execute(
            """
            SELECT fe.id              AS id,
                   fe.thumbnail_path  AS face_thumb,
                   fc.path            AS source_path
              FROM face_embedding fe
              LEFT JOIN file_curation fc ON fc.id = fe.file_curation_id
             WHERE fe.performer_id IS NULL
               AND fe.thumbnail_path IS NOT NULL
             ORDER BY fe.id DESC
             LIMIT ?
            """,
            (int(limit),),
        ).fetchall()
    except sqlite3.OperationalError:
        return []
    out: list[dict] = []
    for r in rows:
        src = r['source_path'] or ''
        out.append({
            'id':              int(r['id']),
            'face_thumb':      r['face_thumb'],
            'source_filename': Path(src).name if src else '',
        })
    return out


# ── Pages ────────────────────────────────────────────────────────────────────

_SORT_CLAUSES = {
    'name':   'p.canonical_name COLLATE NOCASE',
    'videos': 'video_count DESC, p.canonical_name COLLATE NOCASE',
}

_PERF_SELECT = """
    SELECT p.id, p.canonical_name, p.slug,
           COALESCE(p.profile_thumb,
               (SELECT fe.thumbnail_path FROM face_embedding fe
                 WHERE fe.performer_id = p.id AND fe.thumbnail_path IS NOT NULL
                 ORDER BY COALESCE(fe.quality_score, 0) DESC LIMIT 1)
           ) AS profile_thumb,
           p.embedding_count, p.is_reference_ready,
           (SELECT COUNT(*) FROM file_performer fp WHERE fp.performer_id = p.id) AS video_count
      FROM performer p
"""


@router.get('/performers', response_class=HTMLResponse)
async def performers_index(
    request: Request,
    search: str = Query(default=''),
    sort: str = Query(default='name'),
    page: int = Query(default=1, ge=1),
):
    """Render the performer index — paginated, sortable, with optional name filter."""
    conn = _db(request)
    page_size = PAGE_SIZE
    offset = (page - 1) * page_size
    order = _SORT_CLAUSES.get(sort, _SORT_CLAUSES['name'])

    if search:
        like = f'%{search}%'
        rows = conn.execute(
            _PERF_SELECT + f" WHERE canonical_name LIKE ? ORDER BY {order} LIMIT ? OFFSET ?",
            (like, page_size, offset),
        ).fetchall()
        total = conn.execute(
            "SELECT COUNT(*) FROM performer WHERE canonical_name LIKE ?", (like,),
        ).fetchone()[0]
    else:
        rows = conn.execute(
            _PERF_SELECT + f" ORDER BY {order} LIMIT ? OFFSET ?",
            (page_size, offset),
        ).fetchall()
        total = conn.execute("SELECT COUNT(*) FROM performer").fetchone()[0]

    performers = [dict(r) for r in rows]
    total_pages = max(1, math.ceil(int(total) / page_size)) if total else 1

    context = {
        'performers':         performers,
        'selected_performer': None,
        'performer_videos':   [],
        'face_matches':       [],
        'unknown_faces':      _get_unknown_faces(conn),
        'search':             search,
        'sort':               sort,
        'page':               page,
        'page_size':          page_size,
        'total':              int(total),
        'total_pages':        total_pages,
        'face_rec_available': is_face_rec_available(),
        'version':            os.getenv('APP_VERSION', 'dev'),
    }
    return templates.TemplateResponse(
        request,
        'performers.html',
        headers={'Cache-Control': 'no-store'},
        context=context,
    )


@router.get('/performers/{performer_id}', response_class=HTMLResponse)
async def performers_detail(performer_id: int, request: Request):
    """Render the detail view for one performer: videos + pending face matches."""
    conn = _db(request)
    selected = _get_performer_or_404(conn, performer_id)

    videos = _shape_video_rows(
        [dict(r) for r in dbc.get_performer_videos(conn, performer_id)]
    )

    match_rows = conn.execute(
        """
        SELECT fm.id               AS match_id,
               fm.file_curation_id AS file_id,
               fm.similarity       AS score,
               fm.status           AS status,
               fe.thumbnail_path   AS thumb_path,
               fc.path             AS original_path
          FROM face_match_result fm
          JOIN file_curation fc ON fc.id = fm.file_curation_id
          LEFT JOIN face_embedding fe ON fe.id = fm.sample_thumb_id
         WHERE fm.performer_id = ?
           AND fm.status = 'pending'
         ORDER BY fm.similarity DESC
         LIMIT 200
        """,
        (performer_id,),
    ).fetchall()
    face_matches = _shape_face_match_rows([dict(r) for r in match_rows])

    # First page of all performers for the sidebar/index — keeps the template
    # simple (one template handles both index and detail views).
    performers = [dict(r) for r in dbc.get_performers_list(conn, PAGE_SIZE, 0)]
    total = conn.execute("SELECT COUNT(*) FROM performer").fetchone()[0]
    total_pages = max(1, math.ceil(int(total) / PAGE_SIZE)) if total else 1

    context = {
        'performers':         performers,
        'selected_performer': dict(selected),
        'performer_videos':   videos,
        'face_matches':       face_matches,
        'unknown_faces':      _get_unknown_faces(conn),
        'search':             '',
        'page':               1,
        'page_size':          PAGE_SIZE,
        'total':              int(total),
        'total_pages':        total_pages,
        'face_rec_available': is_face_rec_available(),
        'version':            os.getenv('APP_VERSION', 'dev'),
    }
    return templates.TemplateResponse(
        request,
        'performers.html',
        headers={'Cache-Control': 'no-store'},
        context=context,
    )


# ── Edit / merge ─────────────────────────────────────────────────────────────

@router.patch('/performers/{performer_id}')
async def performers_update(performer_id: int, request: Request):
    """Rename a performer.

    The old name is preserved as an alias so previously linked filenames remain
    discoverable. The slug is recomputed from the new canonical name.
    """
    body = await _read_json(request)
    new_name = (body.get('canonical_name') or '').strip()
    if not new_name:
        raise HTTPException(status_code=400, detail='canonical_name required')

    conn = _db(request)
    existing = _get_performer_or_404(conn, performer_id)
    old_name = existing['canonical_name']
    new_slug = dbc.to_slug(new_name)

    # Guard against slug collisions with a different performer.
    clash = conn.execute(
        "SELECT id FROM performer WHERE slug = ? AND id != ?",
        (new_slug, performer_id),
    ).fetchone()
    if clash is not None:
        raise HTTPException(
            status_code=409,
            detail=f'slug {new_slug!r} already in use by performer {clash["id"]}',
        )

    with conn:
        conn.execute(
            "UPDATE performer SET canonical_name = ?, slug = ? WHERE id = ?",
            (new_name, new_slug, performer_id),
        )
        if old_name and old_name != new_name:
            conn.execute(
                """
                INSERT OR IGNORE INTO performer_alias (performer_id, alias)
                VALUES (?, ?)
                """,
                (performer_id, old_name),
            )

    return JSONResponse({'ok': True, 'slug': new_slug})


@router.post('/performers/merge')
async def performers_merge(request: Request):
    """Merge ``source_id`` into ``target_id``.

    Moves file links and embeddings, records source name as alias, and deletes
    the source performer. All in one transaction to keep the graph consistent.
    """
    body = await _read_json(request)
    try:
        source_id = int(body['source_id'])
        target_id = int(body['target_id'])
    except (KeyError, TypeError, ValueError):
        raise HTTPException(status_code=400, detail='source_id and target_id required (int)')
    if source_id == target_id:
        raise HTTPException(status_code=400, detail='source_id and target_id must differ')

    conn = _db(request)
    source = _get_performer_or_404(conn, source_id)
    _get_performer_or_404(conn, target_id)

    with conn:
        # Move file_performer links — drop dups via INSERT OR IGNORE then DELETE source.
        conn.execute(
            """
            INSERT OR IGNORE INTO file_performer (file_curation_id, performer_id, position, source)
            SELECT file_curation_id, ?, position, source
              FROM file_performer WHERE performer_id = ?
            """,
            (target_id, source_id),
        )
        conn.execute("DELETE FROM file_performer WHERE performer_id = ?", (source_id,))

        # Reassign embeddings.
        conn.execute(
            "UPDATE face_embedding SET performer_id = ? WHERE performer_id = ?",
            (target_id, source_id),
        )

        # Reassign any pending face_match_result rows so they don't get orphaned.
        try:
            conn.execute(
                "UPDATE face_match_result SET performer_id = ? WHERE performer_id = ?",
                (target_id, source_id),
            )
        except sqlite3.OperationalError:
            # Table may not exist in some test fixtures; ignore.
            pass

        # Reassign aliases.
        conn.execute(
            "UPDATE performer_alias SET performer_id = ? WHERE performer_id = ?",
            (target_id, source_id),
        )
        # Add source canonical name as alias on target.
        conn.execute(
            "INSERT OR IGNORE INTO performer_alias (performer_id, alias) VALUES (?, ?)",
            (target_id, source['canonical_name']),
        )

        conn.execute("DELETE FROM performer WHERE id = ?", (source_id,))

    return JSONResponse({'ok': True})


# ── Photo upload (seed embedding) ────────────────────────────────────────────

@router.post('/performers/{performer_id}/photo')
async def performers_upload_photo(
    performer_id: int,
    request: Request,
    file: UploadFile = File(...),
):
    """Accept a reference photo and store the detected face embedding.

    Returns ``{'ok': False, 'error': 'face_rec_unavailable'}`` when InsightFace
    isn't installed so the caller can render a graceful UI state.
    """
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)

    # Validate type + extension up-front. We trust neither value alone — both
    # have to agree to reject obviously-spoofed payloads.
    ext = Path(file.filename or '').suffix.lower()
    if ext not in ALLOWED_PHOTO_EXT:
        raise HTTPException(status_code=400, detail=f'unsupported extension: {ext!r}')
    if file.content_type and file.content_type.lower() not in ALLOWED_PHOTO_MIME:
        raise HTTPException(status_code=400, detail=f'unsupported content-type: {file.content_type}')

    # Stream up to MAX_PHOTO_BYTES + 1 so we can detect oversize uploads.
    raw = await file.read(MAX_PHOTO_BYTES + 1)
    if len(raw) > MAX_PHOTO_BYTES:
        raise HTTPException(status_code=413, detail=f'file too large (max {MAX_PHOTO_BYTES} bytes)')
    if not raw:
        raise HTTPException(status_code=400, detail='empty upload')

    if not is_face_rec_available():
        return JSONResponse({'ok': False, 'error': 'face_rec_unavailable'})

    # All heavy imports are deferred so we never pay the InsightFace cost when
    # the optional dependency is missing in a dev environment.
    try:
        import numpy as np  # noqa: WPS433 — deferred on purpose
        from PIL import Image  # noqa: WPS433
    except ImportError as exc:
        log.warning('Pillow/numpy not available: %s', exc)
        return JSONResponse({'ok': False, 'error': 'face_rec_unavailable'})

    try:
        img = Image.open(io.BytesIO(raw)).convert('RGB')
        arr = np.array(img)
    except Exception as exc:
        log.warning('photo decode failed performer_id=%s: %s', performer_id, exc)
        return JSONResponse({'ok': False, 'error': f'decode_failed: {exc}'})

    try:
        index = face_matcher.get_index()
        app_obj = getattr(index, 'app', None) or getattr(index, 'face_app', None)
        if app_obj is None:
            # Fall back to a module-level helper if matcher doesn't expose .app
            from app.face import model as face_model
            get_app = getattr(face_model, 'get_face_app', None)
            if get_app is None:
                raise RuntimeError('no face_app accessor available')
            app_obj = get_app()

        from app.face.extractor import _gpu_lock
        with _gpu_lock:
            faces = app_obj.get(arr)
    except ImportError as exc:
        log.warning('face stack import failed: %s', exc)
        return JSONResponse({'ok': False, 'error': 'face_rec_unavailable'})
    except Exception as exc:
        log.exception('face detection failed performer_id=%s', performer_id)
        return JSONResponse({'ok': False, 'error': f'detection_failed: {exc}'})

    if not faces:
        return JSONResponse({'ok': False, 'error': 'no_face_detected'})

    # Largest detected bounding box wins — that's our subject.
    def _area(f):
        x1, y1, x2, y2 = f.bbox
        return max(0.0, (x2 - x1) * (y2 - y1))

    face = max(faces, key=_area)
    embedding = getattr(face, 'normed_embedding', None)
    if embedding is None:
        embedding = getattr(face, 'embedding', None)
    if embedding is None:
        return JSONResponse({'ok': False, 'error': 'embedding_missing'})

    embedding_bytes = np.asarray(embedding, dtype='float32').tobytes()

    bbox_json = '[]'
    try:
        x1, y1, x2, y2 = (int(v) for v in face.bbox)
        bbox_json = json.dumps([x1, y1, x2, y2])
    except Exception:
        pass
    det = float(getattr(face, 'det_score', 0.0) or 0.0)

    # Persist a thumb so the UI has something to show.
    try:
        FACE_THUMB_DIR.mkdir(parents=True, exist_ok=True)
        thumb_name = f'performer_{performer_id}{ext}'
        thumb_path = FACE_THUMB_DIR / thumb_name
        x1, y1, x2, y2 = (int(v) for v in face.bbox)
        x1 = max(0, x1); y1 = max(0, y1)
        cropped = img.crop((x1, y1, x2, y2))
        cropped.thumbnail((256, 256))
        cropped.save(thumb_path)
        thumb_url = f'/static/faces/{thumb_name}'
    except Exception as exc:
        log.warning('thumb save failed performer_id=%s: %s', performer_id, exc)
        thumb_url = None

    with conn:
        cur = conn.execute(
            """
            INSERT INTO face_embedding
                (performer_id, embedding, source, thumbnail_path, det_score, bbox)
            VALUES (?, ?, 'seed', ?, ?, ?)
            """,
            (performer_id, embedding_bytes, thumb_url, det, bbox_json),
        )
        embedding_id = cur.lastrowid
        if thumb_url:
            conn.execute(
                "UPDATE performer SET profile_thumb = ? WHERE id = ?",
                (thumb_url, performer_id),
            )

    # Best-effort: rebuild the in-memory face index so the new embedding takes
    # effect without an app restart.
    try:
        reload_fn = getattr(face_matcher, 'reload_index', None)
        if callable(reload_fn):
            reload_fn(conn)
    except Exception as exc:  # pragma: no cover — defensive
        log.warning('reload_index after upload failed: %s', exc)

    return JSONResponse({'ok': True, 'embedding_id': int(embedding_id)})


# ── Queue / scan ─────────────────────────────────────────────────────────────

@router.post('/performers/{performer_id}/enqueue-scan')
async def performers_enqueue_scan(performer_id: int, request: Request):
    """Enqueue face-matching jobs against the seed embedding for this performer."""
    if not is_face_rec_available():
        return JSONResponse(
            {'ok': False, 'error': 'face_rec_unavailable', 'enqueued': 0},
            status_code=503,
        )
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)
    try:
        enqueued = face_worker.enqueue_seed_for_performer(conn, performer_id)
    except Exception as exc:
        log.exception('enqueue_seed_for_performer failed performer_id=%s', performer_id)
        return JSONResponse({'ok': False, 'error': str(exc), 'enqueued': 0}, status_code=500)
    return JSONResponse({'ok': True, 'enqueued': int(enqueued)})


# ── Confirm / reject a video assignment ──────────────────────────────────────

@router.post('/performers/{performer_id}/videos/{file_id}/confirm')
async def performers_confirm_video(
    performer_id: int,
    file_id: int,
    request: Request,
):
    """Confirm a performer↔video link.

    If a pending face_match_result row exists for this pair we route through
    ``accept_match`` so the matcher's bookkeeping stays consistent. Otherwise
    we ensure a manual file_performer link is present.
    """
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)

    match_row = conn.execute(
        """
        SELECT id FROM face_match_result
         WHERE performer_id = ? AND file_curation_id = ? AND status = 'pending'
         ORDER BY similarity DESC LIMIT 1
        """,
        (performer_id, file_id),
    ).fetchone()

    if match_row is not None:
        try:
            face_matcher.accept_match(conn, int(match_row['id']))
        except Exception as exc:
            log.exception(
                'accept_match failed performer_id=%s file_id=%s', performer_id, file_id,
            )
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)
    else:
        with conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO file_performer
                    (file_curation_id, performer_id, position, source)
                VALUES (?, ?, 0, 'manual')
                """,
                (file_id, performer_id),
            )

    # Auto-seed thumbnail so the performer photo fills in without manual action.
    try:
        face_worker.enqueue_seed_for_performer(conn, performer_id)
    except Exception as exc:
        log.warning('auto enqueue_seed_for_performer failed performer_id=%s: %s', performer_id, exc)

    return JSONResponse({'ok': True})


@router.post('/performers/{performer_id}/videos/{file_id}/reject')
async def performers_reject_video(
    performer_id: int,
    file_id: int,
    request: Request,
):
    """Remove a performer↔video link and reject any pending face matches."""
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)

    with conn:
        conn.execute(
            "DELETE FROM file_performer WHERE file_curation_id = ? AND performer_id = ?",
            (file_id, performer_id),
        )
        try:
            conn.execute(
                """
                UPDATE face_match_result
                   SET status = 'rejected'
                 WHERE file_curation_id = ? AND performer_id = ? AND status = 'pending'
                """,
                (file_id, performer_id),
            )
        except sqlite3.OperationalError:
            # face_match_result may not exist in older schemas; ignore.
            pass

    return JSONResponse({'ok': True})


# ── Bulk approve all videos for a performer ──────────────────────────────────

@router.post('/performers/{performer_id}/rename-all')
async def performers_rename_all(performer_id: int, request: Request):
    """Approve every linked, not-yet-approved file for this performer at once."""
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)

    with conn:
        cur = conn.execute(
            """
            UPDATE file_curation
               SET status = 'approved'
             WHERE id IN (
                 SELECT fp.file_curation_id
                   FROM file_performer fp
                  WHERE fp.performer_id = ?
             )
               AND status != 'approved'
            """,
            (performer_id,),
        )
        count = cur.rowcount if cur.rowcount is not None else 0

    return JSONResponse({'ok': True, 'count': int(count)})


# ── Face match accept / reject ───────────────────────────────────────────────

@router.post('/performers/{performer_id}/face-matches/{match_id}/accept')
async def performers_face_match_accept(
    performer_id: int,
    match_id: int,
    request: Request,
):
    """Accept a pending face match suggestion."""
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)
    try:
        face_matcher.accept_match(conn, match_id)
    except Exception as exc:
        log.exception('accept_match failed match_id=%s', match_id)
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)
    # Auto-seed: extract a face thumbnail from the accepted video so the
    # performer photo appears without needing a manual "Enqueue scan" click.
    try:
        face_worker.enqueue_seed_for_performer(conn, performer_id)
    except Exception as exc:
        log.warning('auto enqueue_seed_for_performer failed performer_id=%s: %s', performer_id, exc)
    return JSONResponse({'ok': True})


@router.post('/performers/{performer_id}/face-matches/{match_id}/reject')
async def performers_face_match_reject(
    performer_id: int,
    match_id: int,
    request: Request,
):
    """Reject a pending face match suggestion."""
    conn = _db(request)
    _get_performer_or_404(conn, performer_id)
    try:
        face_matcher.reject_match(conn, match_id)
    except Exception as exc:
        log.exception('reject_match failed match_id=%s', match_id)
        return JSONResponse({'ok': False, 'error': str(exc)}, status_code=500)
    return JSONResponse({'ok': True})


# ── TPDB face seeding ────────────────────────────────────────────────────────

def _resolve_tpdb_performer(
    tpdb_performer_id: str | None, tpdb_name: str | None,
) -> dict | None:
    """Locate a TPDB performer by ID (preferred) or by best-match name."""
    if tpdb_performer_id:
        record = curation_tpdb.get_performer(tpdb_performer_id)
        if record:
            return record
    if tpdb_name:
        candidates = curation_tpdb.search_performers(tpdb_name, limit=5)
        if candidates:
            return candidates[0]
    return None


def _collect_image_urls(performer: dict) -> list[str]:
    """Return ordered list of image URLs to try (face > image > thumbnail)."""
    urls: list[str] = []
    parent = performer.get('parent') if isinstance(performer.get('parent'), dict) else {}
    for src in (performer, parent):
        if not isinstance(src, dict):
            continue
        for key in ('face', 'image', 'thumbnail', 'poster'):
            val = src.get(key)
            if isinstance(val, str) and val and val not in urls:
                urls.append(val)
        # Some payloads nest under "posters"/"images" arrays
        for key in ('posters', 'images'):
            val = src.get(key)
            if isinstance(val, list):
                for item in val:
                    if isinstance(item, str) and item not in urls:
                        urls.append(item)
                    elif isinstance(item, dict):
                        for k in ('url', 'src', 'href'):
                            v = item.get(k)
                            if isinstance(v, str) and v and v not in urls:
                                urls.append(v)
    return urls


def _seed_performer_from_tpdb_sync(
    conn: sqlite3.Connection,
    performer_id: int,
    tpdb_performer_id: str | None,
    tpdb_name: str | None,
) -> dict[str, Any]:
    """Synchronous seed: download image, run InsightFace, store embeddings.

    Runs inside a worker thread because every step blocks: network, ONNX model
    inference, and DB writes. The caller (an async route) hands us off via
    ``asyncio.to_thread``.
    """
    record = _resolve_tpdb_performer(tpdb_performer_id, tpdb_name)
    if not record:
        return {'ok': False, 'error': 'performer_not_found', 'embeddings_added': 0}

    urls = _collect_image_urls(record)
    if not urls:
        return {'ok': False, 'error': 'no_face_image', 'embeddings_added': 0}

    # Deferred heavy imports — only when we have something to process.
    try:
        import numpy as np  # type: ignore
        from PIL import Image  # type: ignore
    except ImportError as exc:
        log.warning('seed: numpy/Pillow missing: %s', exc)
        return {'ok': False, 'error': 'face_rec_unavailable', 'embeddings_added': 0}

    try:
        from app.face.model import get_face_app
        face_app = get_face_app()
    except Exception as exc:
        log.exception('seed: face app unavailable')
        return {
            'ok': False,
            'error': f'face_app_unavailable: {exc}',
            'embeddings_added': 0,
        }

    embeddings_added = 0
    last_error: str | None = None
    for url in urls:
        raw = curation_tpdb.download_face_image(url)
        if not raw:
            last_error = 'download_failed'
            continue
        try:
            img = Image.open(io.BytesIO(raw)).convert('RGB')
            arr = np.array(img)
        except Exception as exc:
            last_error = f'decode_failed: {exc}'
            continue
        try:
            from app.face.extractor import _gpu_lock
            with _gpu_lock:
                faces = face_app.get(arr)
        except Exception as exc:
            last_error = f'detection_failed: {exc}'
            continue
        if not faces:
            last_error = 'no_face_detected'
            continue

        # Pick the largest detected face (most likely the subject).
        def _area(f):
            x1, y1, x2, y2 = f.bbox[:4]
            return max(0.0, float((x2 - x1) * (y2 - y1)))

        face = max(faces, key=_area)
        embedding = getattr(face, 'normed_embedding', None)
        if embedding is None:
            embedding = getattr(face, 'embedding', None)
        if embedding is None:
            last_error = 'embedding_missing'
            continue

        try:
            blob = embed_to_blob(np.asarray(embedding, dtype=np.float32))
            bbox = [float(v) for v in face.bbox[:4]]
            bbox_json = json.dumps([round(v, 2) for v in bbox])
            det_score = float(getattr(face, 'det_score', 0.0) or 0.0)
            with conn:
                conn.execute(
                    """
                    INSERT INTO face_embedding
                        (performer_id, file_curation_id, source, embedding,
                         det_score, bbox, frame_time_sec, thumbnail_path, quality_score)
                    VALUES (?, NULL, 'tpdb_seed', ?, ?, ?, NULL, NULL, NULL)
                    """,
                    (int(performer_id), blob, det_score, bbox_json),
                )
                # Recount + flip is_reference_ready when crossing the threshold.
                total = int(conn.execute(
                    "SELECT COUNT(*) FROM face_embedding WHERE performer_id = ?",
                    (int(performer_id),),
                ).fetchone()[0])
                conn.execute(
                    """
                    UPDATE performer
                       SET embedding_count = ?,
                           is_reference_ready = CASE WHEN ? >= 1 THEN 1 ELSE is_reference_ready END,
                           profile_thumb = CASE WHEN profile_thumb IS NULL THEN ? ELSE profile_thumb END
                     WHERE id = ?
                    """,
                    (total, total, url, int(performer_id)),
                )
            embeddings_added += 1
        except Exception as exc:
            log.exception('seed: DB write failed performer_id=%s', performer_id)
            last_error = f'db_error: {exc}'
            continue

    if embeddings_added == 0:
        return {
            'ok': False,
            'error': last_error or 'no_embeddings_added',
            'embeddings_added': 0,
        }

    # Best-effort: refresh the face matcher index so newly seeded embeddings
    # participate in matching without a restart.
    try:
        reload_fn = getattr(face_matcher, 'reload_index', None)
        if callable(reload_fn):
            reload_fn(conn)
    except Exception as exc:  # pragma: no cover — defensive
        log.warning('seed: reload_index failed: %s', exc)

    return {
        'ok': True,
        'embeddings_added': embeddings_added,
        'tpdb_performer': {
            'id': record.get('id') or record.get('_id'),
            'name': record.get('name') or record.get('full_name'),
            'slug': record.get('slug'),
        },
    }


@router.post('/performers/{performer_id}/tpdb-seed')
async def performers_tpdb_seed(performer_id: int, request: Request) -> JSONResponse:
    """Seed face embeddings for a performer using TPDB profile imagery.

    Body: ``{tpdb_performer_id?: str, tpdb_name?: str}`` — at least one is
    required. If neither is supplied we fall back to the performer's canonical
    name as a search query.
    """
    if not curation_tpdb.is_configured():
        return JSONResponse(
            {'ok': False, 'error': 'tpdb_not_configured', 'embeddings_added': 0},
            status_code=503,
        )
    if not is_face_rec_available():
        return JSONResponse(
            {'ok': False, 'error': 'face_rec_unavailable', 'embeddings_added': 0},
            status_code=503,
        )

    conn = _db(request)
    performer = _get_performer_or_404(conn, performer_id)

    body = await _read_json(request)
    tpdb_performer_id = (body.get('tpdb_performer_id') or '').strip() or None
    tpdb_name = (body.get('tpdb_name') or '').strip() or None
    if not tpdb_performer_id and not tpdb_name:
        tpdb_name = performer['canonical_name']

    try:
        result = await asyncio.to_thread(
            _seed_performer_from_tpdb_sync,
            conn,
            int(performer_id),
            tpdb_performer_id,
            tpdb_name,
        )
    except Exception as exc:
        log.exception('tpdb-seed failed performer_id=%s', performer_id)
        return JSONResponse(
            {'ok': False, 'error': str(exc), 'embeddings_added': 0},
            status_code=500,
        )

    status_code = 200 if result.get('ok') else 422
    return JSONResponse(result, status_code=status_code)
