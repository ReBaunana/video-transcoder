"""Watermark-URL OCR identification.

Many camsite/OF rips burn the performer's profile URL or handle into the video
frames as a static overlay (e.g. ``clynks.me/lingusana``, ``onlyfans.com/x``,
``@ana_lingus11``). When present this is a *deterministic* identifier — cheaper
and more precise than face recognition — so we read it via OCR and use it to
route the file to a performer.

Design: a deterministic pre-step before face-rec, gated on generic/coded
filenames. Hit → map URL/handle to a performer (via ``performer_url``) and assign.
Miss → fall through to face-rec.

Caveat: a watermark identifies the *channel*, not necessarily the *person* on
screen (collab channels feature several performers). So a watermark hit should
be treated as a strong source/channel signal, cross-checkable by face-rec — it
is never auto-merged into the embedding reference.

The pure helpers (regex extraction, normalization, voting) have no IO and are
unit-tested. ``identify_watermark`` shells out to ffmpeg + tesseract.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
import tempfile
from collections import Counter
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# Known platform + link-aggregator domains whose path segment is the handle.
_PLATFORMS = (
    "onlyfans", "fansly", "manyvids", "stripchat", "chaturbate",
    "fancentro", "loyalfans", "fansone",
)
_AGGREGATORS = ("clynks.me", "linktr.ee", "allmylinks.com", "beacons.ai", "linkr.bio", "snipfeed.co")

_PLATFORM_RE = re.compile(
    r"(?:https?://)?(?:www\.)?((?:" + "|".join(_PLATFORMS) + r")\.com/[A-Za-z0-9_.\-]{2,40})",
    re.IGNORECASE,
)
_AGG_RE = re.compile(
    r"(?:https?://)?(?:www\.)?((?:" + "|".join(re.escape(a) for a in _AGGREGATORS) + r")/[A-Za-z0-9_.\-]{2,40})",
    re.IGNORECASE,
)
_HANDLE_RE = re.compile(r"@([A-Za-z][A-Za-z0-9_.]{2,30})")


def normalize_key(value: str) -> str:
    """Normalize a URL or handle to a stable lookup key.

    'https://www.OnlyFans.com/Ana_Lingus/' -> 'onlyfans.com/ana_lingus'
    'Clynks.me/lingusana'                  -> 'clynks.me/lingusana'
    '@Ana_Lingus11'                        -> '@ana_lingus11'
    Trailing OCR junk (a clipped char) is left intact; matching is exact, so
    the voting step is what guards against single-frame misreads.
    """
    v = (value or "").strip().strip("/").lower()
    v = re.sub(r"^https?://", "", v)
    v = re.sub(r"^www\.", "", v)
    return v


def extract_identifiers(text: str) -> dict:
    """Pull platform URLs, aggregator URLs and @handles out of raw OCR text."""
    if not text:
        return {"urls": [], "handles": []}
    urls = []
    for m in _PLATFORM_RE.findall(text):
        urls.append(normalize_key(m))
    for m in _AGG_RE.findall(text):
        urls.append(normalize_key(m))
    handles = ["@" + normalize_key(h) for h in _HANDLE_RE.findall(text)]
    # de-dup, preserve order
    return {"urls": list(dict.fromkeys(urls)), "handles": list(dict.fromkeys(handles))}


@dataclass
class WatermarkResult:
    url: str | None = None          # best platform/aggregator URL key
    handle: str | None = None       # best @handle key
    confidence: float = 0.0         # fraction of frames the winner appeared in
    frames_total: int = 0
    raw: list[str] = field(default_factory=list)

    @property
    def key(self) -> str | None:
        """Preferred lookup key: a real URL beats a bare handle."""
        return self.url or self.handle

    @property
    def found(self) -> bool:
        return self.key is not None


def vote_identifiers(per_frame_texts: list[str], min_agree: int = 2) -> WatermarkResult:
    """Majority-vote identifiers across frames.

    A winner must appear in >= min_agree frames (guards against a one-frame
    OCR hallucination). URLs and handles are voted separately.
    """
    url_counts: Counter[str] = Counter()
    handle_counts: Counter[str] = Counter()
    for t in per_frame_texts:
        ids = extract_identifiers(t)
        for u in ids["urls"]:
            url_counts[u] += 1
        for h in ids["handles"]:
            handle_counts[h] += 1

    n = max(len(per_frame_texts), 1)
    res = WatermarkResult(frames_total=len(per_frame_texts), raw=list(per_frame_texts))
    if url_counts:
        url, c = url_counts.most_common(1)[0]
        if c >= min_agree:
            res.url, res.confidence = url, c / n
    if handle_counts:
        handle, c = handle_counts.most_common(1)[0]
        if c >= min_agree and res.url is None:
            res.handle, res.confidence = handle, c / n
        elif c >= min_agree:
            res.handle = handle
    return res


# ---------------------------------------------------------------------------
# IO: frame extraction + OCR (shells out; not unit-tested)
# ---------------------------------------------------------------------------

# Watermark overlays cluster in the corners — OCR those bands, not the whole
# busy frame. (w/h are the source dims; crops are fractions.)
_CROP_ZONES = (
    "iw:ih*0.18:0:0",            # top band
    "iw:ih*0.18:0:ih*0.82",     # bottom band
)


def _probe_duration(path: str) -> float:
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "csv=p=0", path],
            capture_output=True, text=True, timeout=30,
        ).stdout.strip()
        return float(out) if out else 0.0
    except Exception:
        return 0.0


def _ocr_image(img_path: str) -> str:
    try:
        out = subprocess.run(
            ["tesseract", img_path, "stdout", "--psm", "6"],
            capture_output=True, text=True, timeout=30,
        )
        return out.stdout or ""
    except FileNotFoundError:
        log.warning("tesseract not installed — watermark OCR disabled")
        return ""
    except Exception:
        return ""


def ocr_available() -> bool:
    try:
        subprocess.run(["tesseract", "--version"], capture_output=True, timeout=10)
        return True
    except Exception:
        return False


def identify_watermark(path: str, n_frames: int = 5, min_agree: int = 2) -> WatermarkResult:
    """Grab frames spread over the video, OCR corner bands, vote on identifiers."""
    dur = _probe_duration(path)
    if dur <= 0:
        return WatermarkResult()
    pcts = [0.05, 0.3, 0.5, 0.7, 0.9][:max(1, n_frames)]
    texts: list[str] = []
    with tempfile.TemporaryDirectory(prefix="wm_ocr_") as tmp:
        for i, pct in enumerate(pcts):
            ts = dur * pct
            for z, zone in enumerate(_CROP_ZONES):
                out = os.path.join(tmp, f"f{i}_{z}.png")
                # extract, crop a band, upscale 2x + greyscale to help tesseract
                vf = f"crop={zone},scale=iw*2:ih*2,format=gray"
                try:
                    subprocess.run(
                        ["ffmpeg", "-y", "-ss", str(ts), "-i", path,
                         "-frames:v", "1", "-vf", vf, out],
                        capture_output=True, timeout=60,
                    )
                except Exception:
                    continue
                if os.path.exists(out):
                    texts.append(_ocr_image(out))
    return vote_identifiers(texts, min_agree=min_agree)


# ---------------------------------------------------------------------------
# DB integration
# ---------------------------------------------------------------------------

import sqlite3  # noqa: E402

# Filenames with no parseable performer name — the population watermark OCR
# targets. Generic sequential dumps, hashes, scene codes.
_GENERIC_FILENAME_RE = re.compile(
    r"(^[0-9a-f]{16,}|_source\.|^[A-Za-z][A-Za-z0-9\-]*[._]\d+(_\d+)?\.|^[0-9a-f-]{8,}-|^eb\d|^hd\d|^video-\d)",
    re.IGNORECASE,
)


def looks_generic(filename: str) -> bool:
    """Heuristic: filename gives no usable performer name (coded/sequential)."""
    return bool(_GENERIC_FILENAME_RE.search(filename or ""))


def resolve_performer(conn: sqlite3.Connection, result: WatermarkResult) -> int | None:
    """Map a watermark result to a performer via the performer_url table.

    Tries the URL key first, then the handle key. Exact match on url_key.
    """
    for key in (result.url, result.handle):
        if not key:
            continue
        row = conn.execute("SELECT performer_id FROM performer_url WHERE url_key = ?", (key,)).fetchone()
        if row:
            return int(row[0])
    return None


def store_ocr_result(conn: sqlite3.Connection, file_curation_id: int,
                     result: WatermarkResult, performer_id: int | None, status: str) -> None:
    raw = "\n".join(result.raw)[:4000] if result.raw else None
    conn.execute(
        """
        INSERT INTO file_ocr_result
            (file_curation_id, url_key, handle_key, confidence, performer_id, status, raw_text)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_curation_id) DO UPDATE SET
            url_key=excluded.url_key, handle_key=excluded.handle_key,
            confidence=excluded.confidence, performer_id=excluded.performer_id,
            status=excluded.status, raw_text=excluded.raw_text
        """,
        (file_curation_id, result.url, result.handle, result.confidence,
         performer_id, status, raw),
    )


def map_url(conn: sqlite3.Connection, url_key: str, performer_id: int) -> int:
    """Map a watermark key to a performer, then resolve every file waiting on it.

    Returns how many previously-unmapped files got assigned. Lets the user map a
    new channel once; all its files auto-resolve.
    """
    key = normalize_key(url_key)
    conn.execute(
        "INSERT INTO performer_url (performer_id, url_key) VALUES (?, ?) "
        "ON CONFLICT(url_key) DO UPDATE SET performer_id=excluded.performer_id",
        (performer_id, key),
    )
    waiting = conn.execute(
        "SELECT file_curation_id FROM file_ocr_result "
        "WHERE status='needs_mapping' AND (url_key=? OR handle_key=?)",
        (key, key),
    ).fetchall()
    n = 0
    for (fid,) in waiting:
        if assign_file(conn, int(fid), performer_id):
            conn.execute(
                "UPDATE file_ocr_result SET performer_id=?, status='assigned' WHERE file_curation_id=?",
                (performer_id, fid),
            )
            n += 1
    conn.commit()
    return n


def assign_file(conn: sqlite3.Connection, file_curation_id: int, performer_id: int) -> bool:
    """Attach performer (source='watermark_ocr') and stage the file for rename.

    Mirrors the manual-folder-assign flow: keep the original stem, set approved.
    Does NOT touch face embeddings — a watermark identifies the channel, not the
    person, so it must never seed the face reference.
    """
    from app.curation import auto_match
    row = conn.execute("SELECT path FROM file_curation WHERE id=?", (file_curation_id,)).fetchone()
    if not row:
        return False
    path = row[0]
    name = conn.execute("SELECT canonical_name FROM performer WHERE id=?", (performer_id,)).fetchone()
    if not name:
        return False
    conn.execute(
        "INSERT OR IGNORE INTO file_performer (file_curation_id, performer_id, position, source) "
        "VALUES (?, ?, 0, 'watermark_ocr')",
        (file_curation_id, performer_id),
    )
    try:
        proposed = auto_match.build_proposed_filename(conn, file_curation_id, [name[0]], path)
        conn.execute(
            "UPDATE file_curation SET status='approved', proposed_filename=?, updated_at=datetime('now') WHERE id=?",
            (proposed, file_curation_id),
        )
    except Exception:
        log.exception("assign_file: proposed filename failed file=%s", file_curation_id)
        conn.execute(
            "UPDATE file_curation SET status='approved', updated_at=datetime('now') WHERE id=?",
            (file_curation_id,),
        )
    return True


def select_candidates(conn: sqlite3.Connection, limit: int = 50) -> list[tuple[int, str]]:
    """Files that should get a watermark-OCR pass: unknown status, generic
    filename, no OCR result yet."""
    rows = conn.execute(
        """
        SELECT fc.id, fc.path FROM file_curation fc
         WHERE fc.status = 'unknown'
           AND NOT EXISTS (SELECT 1 FROM file_ocr_result o WHERE o.file_curation_id = fc.id)
         ORDER BY fc.id
         LIMIT ?
        """,
        (limit * 4,),
    ).fetchall()
    out = []
    for fid, path in rows:
        if looks_generic(os.path.basename(path)):
            out.append((int(fid), path))
        if len(out) >= limit:
            break
    return out


def process_candidate(conn: sqlite3.Connection, file_curation_id: int, path: str,
                      min_conf: float = 0.25) -> str:
    """OCR one file, store the result, assign if the channel is already mapped.

    Returns the outcome status: 'assigned' | 'needs_mapping' | 'no_watermark'.
    """
    if not os.path.exists(path):
        store_ocr_result(conn, file_curation_id, WatermarkResult(), None, 'source_missing')
        conn.commit()
        return 'source_missing'
    result = identify_watermark(path)
    if not result.found or result.confidence < min_conf:
        store_ocr_result(conn, file_curation_id, result, None, 'no_watermark')
        conn.commit()
        return 'no_watermark'
    performer_id = resolve_performer(conn, result)
    if performer_id is not None:
        assign_file(conn, file_curation_id, performer_id)
        store_ocr_result(conn, file_curation_id, result, performer_id, 'assigned')
        conn.commit()
        return 'assigned'
    store_ocr_result(conn, file_curation_id, result, None, 'needs_mapping')
    conn.commit()
    return 'needs_mapping'


def run_watermark_ocr(conn: sqlite3.Connection, limit: int = 50) -> dict:
    """Process a batch of candidates. Returns outcome counts."""
    if not ocr_available():
        log.warning("run_watermark_ocr: tesseract unavailable, skipping")
        return {"skipped": "no_tesseract"}
    counts: Counter[str] = Counter()
    for fid, path in select_candidates(conn, limit):
        try:
            counts[process_candidate(conn, fid, path)] += 1
        except Exception:
            log.exception("run_watermark_ocr: candidate failed file=%s", fid)
            counts["error"] += 1
    log.info("run_watermark_ocr: %s", dict(counts))
    return dict(counts)


def pending_mappings(conn: sqlite3.Connection) -> list[dict]:
    """Distinct unmapped watermark keys with file counts — the user maps each once."""
    rows = conn.execute(
        """
        SELECT COALESCE(url_key, handle_key) AS key, COUNT(*) AS n,
               MAX(confidence) AS conf
          FROM file_ocr_result
         WHERE status='needs_mapping' AND COALESCE(url_key, handle_key) IS NOT NULL
         GROUP BY key ORDER BY n DESC
        """
    ).fetchall()
    return [{"key": r[0], "files": r[1], "confidence": r[2]} for r in rows]
