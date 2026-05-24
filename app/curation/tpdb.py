"""ThePornDB API client and scene enrichment logic.

Provides:
- Search helpers for scenes and performers
- A simple thread-safe token-bucket rate limiter (~60 req/min)
- A fuzzy scoring function to rank scene matches against a local file row
- ``enrich_file_from_tpdb`` which auto-applies high-confidence matches
- ``download_face_image`` for performer face seeding
- ``migrate_tpdb`` schema migration adding tpdb_scene_id / tpdb_lookup_at
"""

from __future__ import annotations

import json
import logging
import os
import re
import sqlite3
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

try:
    from app.database_curation import (  # type: ignore
        _FILE_CURATION_UPDATABLE,
        get_or_create_performer,
    )
    from app.curation.extractor import (  # type: ignore
        ParseResult,
        build_target_filename,
        parse_filename,
    )
except Exception:  # pragma: no cover — import-path fallback for tests
    import sys as _sys
    _here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _here not in _sys.path:
        _sys.path.insert(0, _here)
    from database_curation import (  # type: ignore
        _FILE_CURATION_UPDATABLE,
        get_or_create_performer,
    )
    from curation.extractor import (  # type: ignore
        ParseResult,
        build_target_filename,
        parse_filename,
    )


log = logging.getLogger("curation.tpdb")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TPDB_API_KEY: str = os.getenv("TPDB_API_KEY", "")
TPDB_BASE: str = "https://api.theporndb.net"
TPDB_TIMEOUT_SEC: float = 12.0
TPDB_MAX_IMAGE_BYTES: int = 2 * 1024 * 1024  # 2 MiB cap on face image downloads
TPDB_USER_AGENT: str = "video-transcoder/tpdb-client (+https://github.com/ReBaunana/video-transcoder)"

# Match thresholds
AUTO_APPLY_SCORE: float = 0.70
MIN_CANDIDATE_SCORE: float = 0.45


def is_configured() -> bool:
    """Return True when the API key is present in the environment."""
    return bool(TPDB_API_KEY)


# ---------------------------------------------------------------------------
# Rate limiter — token bucket, ~60 req/min, thread-safe
# ---------------------------------------------------------------------------


class _RateLimiter:
    """Sliding token-bucket limiter (default 60 events / 60 s).

    Calls to :meth:`acquire` block until a token is available. Thread-safe
    via a single ``threading.Lock`` — the API only sees one outbound request
    at a time per process.
    """

    def __init__(self, max_per_minute: int = 60, min_interval_sec: float = 0.9):
        self.max_per_minute = max(1, int(max_per_minute))
        self.min_interval = max(0.0, float(min_interval_sec))
        self._lock = threading.Lock()
        self._timestamps: list[float] = []
        self._last_call: float = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            # Enforce minimum spacing between requests so bursts don't blow
            # past the limit even if the window allows it.
            wait = (self._last_call + self.min_interval) - now
            if wait > 0:
                time.sleep(wait)
                now = time.monotonic()

            # Trim timestamps older than 60 s
            cutoff = now - 60.0
            self._timestamps = [t for t in self._timestamps if t >= cutoff]

            if len(self._timestamps) >= self.max_per_minute:
                sleep_for = max(0.0, self._timestamps[0] + 60.0 - now)
                if sleep_for > 0:
                    time.sleep(sleep_for)
                    now = time.monotonic()
                    cutoff = now - 60.0
                    self._timestamps = [t for t in self._timestamps if t >= cutoff]

            self._timestamps.append(now)
            self._last_call = now


_rate_limiter = _RateLimiter(max_per_minute=60, min_interval_sec=0.9)


# ---------------------------------------------------------------------------
# HTTP helpers — stdlib urllib, sync. Async routes call via run_in_executor.
# ---------------------------------------------------------------------------


class TPDBError(Exception):
    """Wraps any failure (auth, network, decode) talking to ThePornDB."""


def _build_url(path: str, params: dict[str, Any] | None = None) -> str:
    if not path.startswith("/"):
        path = "/" + path
    url = TPDB_BASE.rstrip("/") + path
    if params:
        cleaned = {k: v for k, v in params.items() if v is not None and v != ""}
        if cleaned:
            url = url + "?" + urllib.parse.urlencode(cleaned, doseq=True)
    return url


def _get_json(path: str, params: dict[str, Any] | None = None) -> dict:
    """GET <TPDB_BASE><path>, return parsed JSON. Honours the rate limiter."""
    if not is_configured():
        raise TPDBError("tpdb_not_configured")

    url = _build_url(path, params)
    _rate_limiter.acquire()

    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {TPDB_API_KEY}",
            "Accept": "application/json",
            "User-Agent": TPDB_USER_AGENT,
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=TPDB_TIMEOUT_SEC) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as exc:
        log.warning("tpdb http %s for %s", exc.code, url)
        raise TPDBError(f"http_{exc.code}") from exc
    except urllib.error.URLError as exc:
        log.warning("tpdb network error for %s: %s", url, exc)
        raise TPDBError(f"network_error: {exc}") from exc
    except Exception as exc:  # pragma: no cover — defensive
        log.exception("tpdb unexpected error for %s", url)
        raise TPDBError(f"unexpected: {exc}") from exc

    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        raise TPDBError(f"decode_error: {exc}") from exc


# ---------------------------------------------------------------------------
# Public search helpers
# ---------------------------------------------------------------------------


def search_scenes(
    title: str, site: str | None = None, limit: int = 5
) -> list[dict]:
    """Hit ``GET /scenes?query=…`` and return the ``data`` list (possibly empty)."""
    query = (title or "").strip()
    if not query:
        return []
    params: dict[str, Any] = {"q": query, "limit": int(max(1, min(limit, 20)))}
    # ``query`` is the canonical parameter the API uses; include both for safety
    # since older deployments accepted ``q``.
    params["query"] = query
    try:
        payload = _get_json("/scenes", params)
    except TPDBError as exc:
        log.info("search_scenes failed (%s) query=%r", exc, query)
        return []
    data = payload.get("data") if isinstance(payload, dict) else None
    return list(data) if isinstance(data, list) else []


def search_performers(name: str, limit: int = 5) -> list[dict]:
    """Hit ``GET /performers?query=…`` and return the ``data`` list."""
    query = (name or "").strip()
    if not query:
        return []
    params = {
        "q": query,
        "query": query,
        "limit": int(max(1, min(limit, 20))),
    }
    try:
        payload = _get_json("/performers", params)
    except TPDBError as exc:
        log.info("search_performers failed (%s) name=%r", exc, name)
        return []
    data = payload.get("data") if isinstance(payload, dict) else None
    return list(data) if isinstance(data, list) else []


def get_performer(tpdb_id: str) -> dict | None:
    """Hit ``GET /performers/{id}`` and return the unwrapped record or None."""
    pid = (tpdb_id or "").strip()
    if not pid:
        return None
    try:
        payload = _get_json(f"/performers/{urllib.parse.quote(pid, safe='')}")
    except TPDBError as exc:
        log.info("get_performer failed (%s) id=%r", exc, pid)
        return None
    if not isinstance(payload, dict):
        return None
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload if "id" in payload or "_id" in payload else None


def download_face_image(url: str) -> bytes | None:
    """Download a performer face image. Caps at TPDB_MAX_IMAGE_BYTES, 8s timeout."""
    if not url:
        return None
    if not (url.startswith("http://") or url.startswith("https://")):
        return None

    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": TPDB_USER_AGENT,
            "Accept": "image/*",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=8.0) as resp:
            # Respect Content-Length when present
            try:
                clen = int(resp.headers.get("Content-Length") or 0)
            except (TypeError, ValueError):
                clen = 0
            if clen and clen > TPDB_MAX_IMAGE_BYTES:
                log.info("download_face_image: skip oversize %d bytes %s", clen, url)
                return None
            data = resp.read(TPDB_MAX_IMAGE_BYTES + 1)
            if len(data) > TPDB_MAX_IMAGE_BYTES:
                log.info("download_face_image: truncated stream exceeded cap %s", url)
                return None
            return data if data else None
    except urllib.error.HTTPError as exc:
        log.info("download_face_image http %s for %s", exc.code, url)
        return None
    except urllib.error.URLError as exc:
        log.info("download_face_image network error for %s: %s", url, exc)
        return None
    except Exception as exc:  # pragma: no cover — defensive
        log.warning("download_face_image unexpected for %s: %s", url, exc)
        return None


# ---------------------------------------------------------------------------
# Scoring / matching helpers
# ---------------------------------------------------------------------------

_NORM_STRIP = re.compile(r"[^a-z0-9]+")


def _norm(s: str | None) -> str:
    """Normalise a string for fuzzy comparison (strip accents/punctuation, lowercase)."""
    if not s:
        return ""
    decomposed = unicodedata.normalize("NFKD", str(s))
    ascii_only = "".join(c for c in decomposed if not unicodedata.combining(c))
    lowered = ascii_only.lower()
    return _NORM_STRIP.sub(" ", lowered).strip()


def _ratio(a: str | None, b: str | None) -> float:
    """SequenceMatcher ratio over normalised inputs. 0 if either side is empty."""
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def _scene_field(scene: dict, *keys: str) -> Any:
    """Pull the first non-empty value from a chain of dotted-ish keys."""
    for key in keys:
        parts = key.split(".")
        cur: Any = scene
        ok = True
        for p in parts:
            if isinstance(cur, dict) and p in cur:
                cur = cur[p]
            else:
                ok = False
                break
        if ok and cur not in (None, ""):
            return cur
    return None


def score_scene_match(scene: dict, file_row: dict) -> float:
    """Combined weighted match score in [0, 1] for ranking candidates."""
    if not isinstance(scene, dict) or not isinstance(file_row, dict):
        return 0.0

    # Title comparison — fall back to the stem when title is empty
    file_title = file_row.get("title") or ""
    if not file_title and file_row.get("path"):
        file_title = Path(str(file_row["path"])).stem
    scene_title = _scene_field(scene, "title") or ""
    title_ratio = _ratio(scene_title, file_title)

    # Site / studio comparison — TPDB nests site.name / site.slug
    file_studio = file_row.get("studio") or ""
    site_name = _scene_field(scene, "site.name", "site.short_name")
    site_slug = _scene_field(scene, "site.slug")
    site_ratio = max(
        _ratio(site_name, file_studio),
        _ratio(site_slug, file_studio),
    )

    # Date comparison — exact match is a strong signal; near-misses still count
    file_date = (file_row.get("release_date") or "")[:10]
    scene_date_raw = _scene_field(scene, "date", "release_date") or ""
    scene_date = str(scene_date_raw)[:10]
    date_bonus = 0.0
    if file_date and scene_date:
        if file_date == scene_date:
            date_bonus = 0.30
        elif file_date[:7] == scene_date[:7]:
            date_bonus = 0.18
        elif file_date[:4] == scene_date[:4]:
            date_bonus = 0.08

    # Performer overlap — gives a small boost when at least one parsed
    # performer slug shows up in scene.performers.
    parsed_performers = file_row.get("performers") or []
    if isinstance(parsed_performers, str):
        parsed_performers = [p.strip() for p in parsed_performers.split(",") if p.strip()]
    performer_bonus = 0.0
    scene_perf_names: list[str] = []
    for p in scene.get("performers") or []:
        if not isinstance(p, dict):
            continue
        name = (
            p.get("name")
            or (p.get("parent") or {}).get("name")
            or (p.get("parent") or {}).get("full_name")
        )
        if name:
            scene_perf_names.append(_norm(name))
    if parsed_performers and scene_perf_names:
        hits = sum(
            1
            for parsed in parsed_performers
            if any(_ratio(parsed, sp) >= 0.85 for sp in scene_perf_names)
        )
        if hits:
            performer_bonus = min(0.15, 0.07 * hits)

    score = (0.55 * title_ratio) + (0.30 * site_ratio) + date_bonus + performer_bonus
    return float(max(0.0, min(1.0, score)))


# ---------------------------------------------------------------------------
# DB plumbing for enrichment
# ---------------------------------------------------------------------------


def _file_row_to_dict(conn: sqlite3.Connection, file_curation_id: int) -> dict | None:
    cur = conn.execute(
        """
        SELECT id, path, mount, studio, title, release_date, resolution,
               extraction_method, extraction_confidence, status,
               proposed_filename, tpdb_scene_id, tpdb_lookup_at
          FROM file_curation
         WHERE id = ?
        """,
        (int(file_curation_id),),
    )
    row = cur.fetchone()
    if row is None:
        return None
    cols = [c[0] for c in cur.description]
    out = {cols[i]: row[i] for i in range(len(cols))}
    # Attach performers for scoring
    try:
        perf_rows = conn.execute(
            """
            SELECT p.canonical_name
              FROM file_performer fp
              JOIN performer p ON p.id = fp.performer_id
             WHERE fp.file_curation_id = ?
             ORDER BY fp.position
            """,
            (int(file_curation_id),),
        ).fetchall()
        out["performers"] = [r[0] for r in perf_rows if r and r[0]]
    except sqlite3.OperationalError:
        out["performers"] = []
    return out


def _shape_performer_summary(performer: dict) -> dict:
    """Trim a TPDB performer object down to what we want to render in the UI."""
    if not isinstance(performer, dict):
        return {}
    parent = performer.get("parent") if isinstance(performer.get("parent"), dict) else {}
    name = (
        performer.get("name")
        or parent.get("name")
        or parent.get("full_name")
        or ""
    )
    return {
        "name": name,
        "tpdb_id": (parent.get("id") if parent else None) or performer.get("id"),
        "slug": (parent.get("slug") if parent else None) or performer.get("slug"),
        "image": (parent.get("image") if parent else None) or performer.get("image"),
        "face": (parent.get("face") if parent else None) or performer.get("face"),
        "thumbnail": (parent.get("thumbnail") if parent else None) or performer.get("thumbnail"),
    }


def _shape_scene_summary(scene: dict, score: float) -> dict:
    """Trim a TPDB scene for client-side rendering."""
    if not isinstance(scene, dict):
        return {}
    site = scene.get("site") if isinstance(scene.get("site"), dict) else {}
    performers = [
        _shape_performer_summary(p) for p in (scene.get("performers") or []) if p
    ]
    return {
        "id": scene.get("id") or scene.get("_id"),
        "title": scene.get("title"),
        "date": scene.get("date") or scene.get("release_date"),
        "url": scene.get("url"),
        "studio": site.get("name") or site.get("short_name"),
        "studio_slug": site.get("slug"),
        "performers": performers,
        "score": round(float(score), 3),
    }


def _update_file_row(
    conn: sqlite3.Connection,
    file_curation_id: int,
    updates: dict[str, Any],
) -> None:
    """Direct UPDATE on file_curation honouring the updatable column allow-list."""
    filtered = {k: v for k, v in updates.items() if k in _FILE_CURATION_UPDATABLE}
    if not filtered:
        return
    set_clause = ", ".join(f"{k} = ?" for k in filtered.keys())
    set_clause += ", updated_at = datetime('now')"
    values = list(filtered.values()) + [int(file_curation_id)]
    conn.execute(
        f"UPDATE file_curation SET {set_clause} WHERE id = ?",
        values,
    )


def _replace_file_performers(
    conn: sqlite3.Connection,
    file_curation_id: int,
    performer_names: Iterable[str],
) -> list[tuple[int, str]]:
    """Wipe auto-source links and re-create them in order.

    Returns a list of ``(performer_id, canonical_name)`` pairs for the
    performers that were successfully linked.
    """
    conn.execute(
        "DELETE FROM file_performer WHERE file_curation_id = ? AND source IN ('auto', 'tpdb')",
        (int(file_curation_id),),
    )
    ids: list[tuple[int, str]] = []
    for position, name in enumerate(performer_names):
        name = (name or "").strip()
        if not name:
            continue
        try:
            pid = get_or_create_performer(conn, name)
        except Exception:
            continue
        ids.append((pid, name))
        conn.execute(
            """
            INSERT OR IGNORE INTO file_performer
                (file_curation_id, performer_id, position, source)
            VALUES (?, ?, ?, 'tpdb')
            """,
            (int(file_curation_id), pid, position),
        )
    return ids


def _extract_performer_image_urls(p: dict) -> list[str]:
    """Pull face/image/thumbnail/poster URLs from a TPDB performer dict.

    Also checks the nested ``parent`` block (TPDB scene payloads nest the
    canonical performer there). De-duplicates while preserving priority order.
    """
    urls: list[str] = []
    seen: set[str] = set()
    parent = p.get("parent") if isinstance(p.get("parent"), dict) else {}
    for src in (p, parent):
        if not isinstance(src, dict):
            continue
        for key in ("face", "image", "thumbnail", "poster"):
            val = src.get(key)
            if isinstance(val, str) and val and val not in seen:
                seen.add(val)
                urls.append(val)
    return urls


def _auto_seed_performers_from_tpdb(
    conn: sqlite3.Connection,
    id_name_pairs: list[tuple[int, str]],
    scene_performers: list[dict],
) -> None:
    """Download TPDB profile images and store face embeddings for new performers.

    Silently skips performers that already have embeddings. Degrades gracefully
    when InsightFace / Pillow / numpy are not installed.
    """
    if not id_name_pairs:
        return
    try:
        from app.face.model import get_face_app, is_face_rec_available, embed_to_blob  # type: ignore
        from app.face.extractor import _gpu_lock  # type: ignore
        import numpy as np  # type: ignore
        import io
        from PIL import Image  # type: ignore
    except ImportError:
        log.debug("_auto_seed_performers_from_tpdb: face stack not available, skipping")
        return

    if not is_face_rec_available():
        return

    try:
        face_app = get_face_app()
    except Exception as exc:
        log.warning("_auto_seed_performers_from_tpdb: get_face_app failed: %s", exc)
        return

    # Build name→TPDB-dict map from the scene payload (already fetched, no extra requests)
    tpdb_by_name: dict[str, dict] = {}
    for p in scene_performers:
        if not isinstance(p, dict):
            continue
        parent = p.get("parent") if isinstance(p.get("parent"), dict) else {}
        name = (parent.get("name") or parent.get("full_name") or p.get("name") or "").strip()
        if name:
            tpdb_by_name[_norm(name)] = p

    for performer_id, canonical_name in id_name_pairs:
        try:
            row = conn.execute(
                "SELECT embedding_count FROM performer WHERE id = ?",
                (performer_id,),
            ).fetchone()
            if row and int(row[0] or 0) > 0:
                continue  # already has embeddings

            # Find TPDB data: prefer scene payload, fall back to API search
            tpdb_data = tpdb_by_name.get(_norm(canonical_name))
            if not tpdb_data:
                results = search_performers(canonical_name, limit=3)
                tpdb_data = results[0] if results else None

            if not tpdb_data:
                log.debug("_auto_seed: no TPDB data for %r", canonical_name)
                continue

            urls = _extract_performer_image_urls(tpdb_data)
            seeded = False
            for url in urls[:3]:
                raw = download_face_image(url)
                if not raw:
                    continue
                try:
                    img = Image.open(io.BytesIO(raw)).convert("RGB")
                    arr = np.array(img)
                    with _gpu_lock:
                        faces = face_app.get(arr)
                except Exception as exc:
                    log.debug("_auto_seed: image decode/detect failed url=%s: %s", url, exc)
                    continue
                if not faces:
                    continue

                def _area(f):
                    x1, y1, x2, y2 = f.bbox[:4]
                    return max(0.0, float((x2 - x1) * (y2 - y1)))

                face = max(faces, key=_area)
                embedding = getattr(face, "normed_embedding", None)
                if embedding is None:
                    embedding = getattr(face, "embedding", None)
                if embedding is None:
                    continue

                blob = embed_to_blob(np.asarray(embedding, dtype=np.float32))
                det_score = float(getattr(face, "det_score", 0.0) or 0.0)
                bbox_json = json.dumps([round(float(v), 2) for v in face.bbox[:4]])
                with conn:
                    conn.execute(
                        """
                        INSERT INTO face_embedding
                            (performer_id, file_curation_id, source, embedding,
                             det_score, bbox, frame_time_sec, thumbnail_path, quality_score)
                        VALUES (?, NULL, 'tpdb_seed', ?, ?, ?, NULL, NULL, NULL)
                        """,
                        (performer_id, blob, det_score, bbox_json),
                    )
                    conn.execute(
                        """
                        UPDATE performer
                           SET embedding_count = embedding_count + 1,
                               is_reference_ready = 1,
                               profile_thumb = CASE WHEN profile_thumb IS NULL THEN ? ELSE profile_thumb END
                         WHERE id = ?
                        """,
                        (url, performer_id),
                    )
                log.info(
                    "_auto_seed: seeded performer_id=%s name=%r from %s",
                    performer_id, canonical_name, url,
                )
                seeded = True
                break

            if not seeded:
                log.debug("_auto_seed: no face found for %r", canonical_name)

        except Exception:
            log.exception("_auto_seed_performers_from_tpdb: failed performer_id=%s", performer_id)


def seed_performers_without_embeddings(
    conn: sqlite3.Connection,
    max_performers: int = 50,
) -> dict:
    """Seed face embeddings from TPDB profile images for performers with no embeddings.

    Iterates performers with embedding_count=0, searches TPDB for a profile photo,
    runs InsightFace detection, and stores the embedding + profile_thumb.
    Safe to call repeatedly — skips performers that already have embeddings.
    Returns {'ok': bool, 'checked': int, 'seeded': int}.
    """
    if not is_configured():
        log.debug("seed_performers_without_embeddings: TPDB not configured")
        return {"ok": False, "error": "tpdb_not_configured", "checked": 0, "seeded": 0}

    try:
        from app.face.model import is_face_rec_available
        if not is_face_rec_available():
            log.debug("seed_performers_without_embeddings: face rec unavailable")
            return {"ok": False, "error": "face_rec_unavailable", "checked": 0, "seeded": 0}
    except ImportError:
        return {"ok": False, "error": "face_stack_missing", "checked": 0, "seeded": 0}

    rows = conn.execute(
        "SELECT id, canonical_name FROM performer WHERE embedding_count = 0 ORDER BY id LIMIT ?",
        (max(1, min(int(max_performers), 500)),),
    ).fetchall()

    if not rows:
        log.info("seed_performers_without_embeddings: all performers already have embeddings")
        return {"ok": True, "checked": 0, "seeded": 0}

    id_name_pairs = [(int(r[0]), str(r[1])) for r in rows]
    seeded_before = int(
        conn.execute("SELECT COUNT(*) FROM face_embedding WHERE performer_id IS NOT NULL").fetchone()[0]
    )

    for pid, name in id_name_pairs:
        try:
            _auto_seed_performers_from_tpdb(conn, [(pid, name)], scene_performers=[])
        except Exception:
            log.exception(
                "seed_performers_without_embeddings: failed performer_id=%s name=%r", pid, name,
            )

    seeded_after = int(
        conn.execute("SELECT COUNT(*) FROM face_embedding WHERE performer_id IS NOT NULL").fetchone()[0]
    )
    seeded = seeded_after - seeded_before
    log.info(
        "seed_performers_without_embeddings: checked=%d new_embeddings=%d",
        len(id_name_pairs), seeded,
    )
    return {"ok": True, "checked": len(id_name_pairs), "seeded": seeded}


def _ext_from_path(path: str) -> str:
    ext = Path(path).suffix.lower()
    return ext or ".mp4"


def _rebuild_proposed_filename(
    *,
    studio: str | None,
    release_date: str | None,
    title: str | None,
    performers: list[str],
    resolution: str | None,
    ext: str,
) -> str | None:
    try:
        pr = ParseResult(
            pattern_id="tpdb",
            confidence=1.0,
            performers=list(performers or []),
            studio=studio or None,
            release_date=release_date or None,
            title=title or None,
            resolution=resolution or None,
            ext=ext or ".mp4",
        )
        return build_target_filename(pr)
    except Exception:
        log.debug("rebuild_proposed_filename failed", exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Query-strategy builder
# ---------------------------------------------------------------------------

_QUALITY_NOISE_RE = re.compile(
    r"\b(?:2160p|1080p|1080i|720p|720i|480p|360p"
    r"|4k|uhd|sd|hd|fhd|xxx"
    r"|webrip|web-?dl|webdl|bluray|bdrip|dvdrip|hdrip"
    r"|x264|x265|h\.?264|h\.?265|hevc|avc|xvid|divx"
    r"|aac|mp3|flac|ac3|dts)\b",
    re.IGNORECASE,
)
_BRACKETED_RE = re.compile(r"[\[\(][^\]\)]*[\]\)]")
_WS_COLLAPSE_RE = re.compile(r"\s+")


def _clean_query(raw: str | None) -> str:
    """Strip bracketed junk, quality/codec noise, and separators from a query candidate."""
    if not raw:
        return ""
    s = _BRACKETED_RE.sub(" ", str(raw))
    s = re.sub(r"[._\-]+", " ", s)
    s = _QUALITY_NOISE_RE.sub(" ", s)
    s = _WS_COLLAPSE_RE.sub(" ", s).strip()
    return s


def _build_query_strategies(row: dict) -> list[str]:
    """Return ordered TPDB search queries to try, best-signal first.

    1. Cleaned DB title
    2. First performer name from the file_performer join
    3. First performer name extracted directly from the filename
    4. Cleaned filename stem
    """
    queries: list[str] = []
    seen: set[str] = set()

    def _push(candidate: str | None) -> None:
        cleaned = _clean_query(candidate)
        if not cleaned:
            return
        key = cleaned.lower()
        if key in seen:
            return
        seen.add(key)
        queries.append(cleaned)

    _push(row.get("title"))

    performers = row.get("performers") or []
    if isinstance(performers, str):
        performers = [p.strip() for p in performers.split(",") if p.strip()]
    if performers:
        _push(performers[0])

    path = row.get("path")
    if path:
        try:
            parsed = parse_filename(Path(str(path)).name)
        except Exception:
            parsed = None
        if parsed and parsed.performers:
            _push(parsed.performers[0])

    if path:
        _push(Path(str(path)).stem)

    return queries


# ---------------------------------------------------------------------------
# Main enrichment entry point
# ---------------------------------------------------------------------------


def enrich_file_from_tpdb(
    conn: sqlite3.Connection, file_curation_id: int
) -> dict:
    """Search TPDB for the best scene match and (optionally) auto-apply it.

    Behaviour:
    - score > AUTO_APPLY_SCORE → update file_curation + performers, return applied=True
    - MIN_CANDIDATE_SCORE < score ≤ AUTO_APPLY_SCORE → return candidates for review
    - otherwise → applied=False, candidates=[], scene=None
    """
    if not is_configured():
        return {
            "applied": False,
            "ok": False,
            "error": "tpdb_not_configured",
            "scene": None,
            "performers": [],
            "candidates": [],
            "score": 0.0,
        }

    row = _file_row_to_dict(conn, file_curation_id)
    if row is None:
        return {
            "applied": False,
            "ok": False,
            "error": "file_not_found",
            "scene": None,
            "performers": [],
            "candidates": [],
            "score": 0.0,
        }

    # Build ordered search strategies and try each until we get candidates.
    queries = _build_query_strategies(row)
    if not queries:
        return {
            "applied": False,
            "ok": True,
            "scene": None,
            "performers": [],
            "candidates": [],
            "score": 0.0,
            "error": "no_query",
        }

    scored: list[tuple[float, dict]] = []
    seen_scene_ids: set[str] = set()
    for query in queries:
        scenes = search_scenes(query, limit=5)
        if not scenes:
            log.debug("enrich: no results for query=%r id=%s", query, file_curation_id)
            continue
        for sc in scenes:
            sid = sc.get("id") or sc.get("_id")
            sid_key = str(sid) if sid is not None else json.dumps(sc, sort_keys=True)[:64]
            if sid_key in seen_scene_ids:
                continue
            seen_scene_ids.add(sid_key)
            s = score_scene_match(sc, row)
            if s >= MIN_CANDIDATE_SCORE:
                scored.append((s, sc))
        if any(s >= AUTO_APPLY_SCORE for s, _ in scored):
            break
    scored.sort(key=lambda t: t[0], reverse=True)

    # Always stamp tpdb_lookup_at so we don't keep re-searching for no-result files.
    now_iso = datetime.utcnow().isoformat(timespec="seconds")

    if not scored:
        try:
            _update_file_row(conn, file_curation_id, {"tpdb_lookup_at": now_iso})
            conn.commit()
        except Exception:
            conn.rollback()
            log.exception("enrich: failed to stamp tpdb_lookup_at id=%s", file_curation_id)
        return {
            "applied": False,
            "ok": True,
            "scene": None,
            "performers": [],
            "candidates": [],
            "score": 0.0,
        }

    best_score, best_scene = scored[0]

    if best_score < AUTO_APPLY_SCORE:
        try:
            _update_file_row(conn, file_curation_id, {"tpdb_lookup_at": now_iso})
            conn.commit()
        except Exception:
            conn.rollback()
        candidates = [_shape_scene_summary(sc, sc_score) for sc_score, sc in scored]
        return {
            "applied": False,
            "ok": True,
            "scene": None,
            "performers": [],
            "candidates": candidates,
            "score": float(best_score),
        }

    # ── Auto-apply ───────────────────────────────────────────────────────
    site = best_scene.get("site") if isinstance(best_scene.get("site"), dict) else {}
    new_studio = site.get("name") or site.get("short_name") or row.get("studio")
    new_title = best_scene.get("title") or row.get("title")
    new_date_raw = best_scene.get("date") or best_scene.get("release_date") or row.get("release_date")
    new_date = str(new_date_raw)[:10] if new_date_raw else row.get("release_date")
    scene_tpdb_id = best_scene.get("id") or best_scene.get("_id")
    scene_tpdb_id_str = str(scene_tpdb_id) if scene_tpdb_id is not None else None

    performer_names: list[str] = []
    for p in best_scene.get("performers") or []:
        if not isinstance(p, dict):
            continue
        parent = p.get("parent") if isinstance(p.get("parent"), dict) else {}
        name = (
            parent.get("name")
            or parent.get("full_name")
            or p.get("name")
            or ""
        )
        name = name.strip()
        if name and name not in performer_names:
            performer_names.append(name)

    id_name_pairs: list[tuple[int, str]] = []
    try:
        _update_file_row(
            conn,
            file_curation_id,
            {
                "studio": new_studio,
                "title": new_title,
                "release_date": new_date,
                "tpdb_scene_id": scene_tpdb_id_str,
                "tpdb_lookup_at": now_iso,
            },
        )
        if performer_names:
            id_name_pairs = _replace_file_performers(conn, file_curation_id, performer_names)

        proposed = _rebuild_proposed_filename(
            studio=new_studio,
            release_date=new_date,
            title=new_title,
            performers=performer_names,
            resolution=row.get("resolution"),
            ext=_ext_from_path(str(row.get("path") or "")),
        )
        if proposed:
            _update_file_row(conn, file_curation_id, {"proposed_filename": proposed})
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log.exception("enrich: auto-apply failed id=%s", file_curation_id)
        return {
            "applied": False,
            "ok": False,
            "error": f"db_error: {exc}",
            "scene": _shape_scene_summary(best_scene, best_score),
            "performers": performer_names,
            "candidates": [_shape_scene_summary(sc, sc_score) for sc_score, sc in scored[1:4]],
            "score": float(best_score),
        }

    # Auto-seed face embeddings for newly created performers so face
    # recognition can find them in other videos without manual steps.
    # Must run AFTER conn.commit() — opens its own `with conn:` transactions.
    if id_name_pairs:
        _auto_seed_performers_from_tpdb(
            conn, id_name_pairs, best_scene.get("performers") or []
        )

    return {
        "applied": True,
        "ok": True,
        "scene": _shape_scene_summary(best_scene, best_score),
        "performers": performer_names,
        "candidates": [_shape_scene_summary(sc, sc_score) for sc_score, sc in scored[1:4]],
        "score": float(best_score),
        "tpdb_scene_id": scene_tpdb_id_str,
        "proposed_filename": _rebuild_proposed_filename(
            studio=new_studio,
            release_date=new_date,
            title=new_title,
            performers=performer_names,
            resolution=row.get("resolution"),
            ext=_ext_from_path(str(row.get("path") or "")),
        ),
    }


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

_TPDB_MIGRATION = (
    "ALTER TABLE file_curation ADD COLUMN tpdb_scene_id TEXT",
    "ALTER TABLE file_curation ADD COLUMN tpdb_lookup_at TEXT",
)


def migrate_tpdb(conn: sqlite3.Connection) -> None:
    """Add tpdb_scene_id / tpdb_lookup_at columns. Idempotent."""
    for stmt in _TPDB_MIGRATION:
        try:
            conn.execute(stmt)
            conn.commit()
        except sqlite3.OperationalError as exc:
            # "duplicate column name" — already migrated. Anything else is logged.
            msg = str(exc).lower()
            if "duplicate column" in msg:
                continue
            log.warning("migrate_tpdb: %s (statement=%s)", exc, stmt)
    # Add a helpful index on tpdb_scene_id for reverse lookups.
    try:
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_fc_tpdb_scene ON file_curation(tpdb_scene_id)"
        )
        conn.commit()
    except sqlite3.OperationalError as exc:
        log.warning("migrate_tpdb: index creation failed: %s", exc)
