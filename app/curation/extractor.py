# app/curation/extractor.py
"""Filename parsing and target-name construction for adult video curation.

Pure-stdlib regex parser. Tries a sequence of patterns ordered from most
specific to most permissive. Each pattern returns a ParseResult with a
confidence score; the first match wins.

Target naming convention:
    Studio.YYYY-MM-DD.Firstname-Lastname[_Performer2[_Performer3]].Title.Resolution.ext

Performer name uses hyphens for spaces inside one name, underscore between
multiple performers. Studio is split on capital letters and hyphen-joined.
"""

from __future__ import annotations

import os
import re
import sqlite3
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

# Allow `from app.database_curation import ...` when running inside container
try:
    from app.database_curation import (  # type: ignore
        get_or_create_performer,
        upsert_file_curation,
    )
except Exception:  # pragma: no cover - import-path fallback
    # Allow running as standalone (tests etc.); resolve sibling module
    _here = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _here not in sys.path:
        sys.path.insert(0, _here)
    from database_curation import (  # type: ignore
        get_or_create_performer,
        upsert_file_curation,
    )


VIDEO_EXTENSIONS = frozenset(
    {
        ".mkv",
        ".mp4",
        ".avi",
        ".wmv",
        ".mov",
        ".flv",
        ".m4v",
        ".ts",
        ".mpg",
        ".mpeg",
        ".divx",
        ".webm",
    }
)


# ---------------------------------------------------------------------------
# Data class
# ---------------------------------------------------------------------------


@dataclass
class ParseResult:
    pattern_id: str
    confidence: float
    performers: list[str] = field(default_factory=list)
    studio: str | None = None
    release_date: str | None = None
    title: str | None = None
    resolution: str | None = None
    ext: str = ""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_RES_PATTERNS = [
    (re.compile(r"\b2160p\b", re.IGNORECASE), "2160p"),
    (re.compile(r"\b4k\b", re.IGNORECASE), "2160p"),
    (re.compile(r"\b1440p\b", re.IGNORECASE), "1440p"),
    (re.compile(r"\b1080p\b", re.IGNORECASE), "1080p"),
    (re.compile(r"\b720p\b", re.IGNORECASE), "720p"),
    (re.compile(r"\b540p\b", re.IGNORECASE), "540p"),
    (re.compile(r"\b480p\b", re.IGNORECASE), "480p"),
    (re.compile(r"\b360p\b", re.IGNORECASE), "360p"),
    (re.compile(r"\bfull[\s_\-\.]?hd\b", re.IGNORECASE), "1080p"),
    (re.compile(r"\bfhd\b", re.IGNORECASE), "1080p"),
    (re.compile(r"\bhd\b", re.IGNORECASE), "720p"),
]

_UNSAFE_CHARS_RE = re.compile(r'[:/?*"<>|\\]')
_WS_RE = re.compile(r"\s+")
_SAFE_FIELD_RE = re.compile(r"[^A-Za-z0-9._\-]")
_CAMEL_SPLIT_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
_TITLE_WORD_RE = re.compile(r"^[A-Z][a-zA-Z]+$")
_DATE_DOT_RE = re.compile(r"(?P<y>\d{2})\.(?P<m>\d{2})\.(?P<d>\d{2})")

# Quality tags often glued to titles in P4-P8
_QUALITY_TAGS_RE = re.compile(
    r"\b(?:full[\s_\-\.]?hd|fhd|hd|2160p|1440p|1080p|720p|540p|480p|360p|4k)\b",
    re.IGNORECASE,
)


def extract_resolution_from_name(name: str) -> str | None:
    """Return canonical resolution token found in name, or None."""
    for rx, label in _RES_PATTERNS:
        if rx.search(name):
            return label
    return None


def sanitize_field(s: str) -> str:
    """Strip filesystem-unsafe chars and collapse whitespace."""
    if not s:
        return ""
    cleaned = _UNSAFE_CHARS_RE.sub("", s)
    cleaned = _WS_RE.sub(" ", cleaned).strip()
    return cleaned


def _safe_component(s: str) -> str:
    """Stricter cleanup for a single filename component (no dots in name parts)."""
    s = sanitize_field(s)
    # Replace spaces with hyphens inside one component
    s = s.replace(" ", "-")
    # Drop anything outside the allowed set
    s = _SAFE_FIELD_RE.sub("", s)
    # Collapse runs of dashes
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def split_studio_name(studio: str) -> str:
    """CamelCase -> hyphen joined. 'SheLovesBlack' -> 'She-Loves-Black'."""
    if not studio:
        return ""
    studio = sanitize_field(studio)
    # If the studio already contains separators, normalize on them
    if any(sep in studio for sep in (" ", "_", "-", ".")):
        parts = re.split(r"[\s_\-\.]+", studio)
        parts = [p for p in parts if p]
        return "-".join(p[:1].upper() + p[1:] for p in parts)
    parts = _CAMEL_SPLIT_RE.split(studio)
    parts = [p for p in parts if p]
    if not parts:
        return studio
    return "-".join(parts)


def _normalize_year(yy: str) -> str:
    """Two-digit year -> four-digit. 70-99 => 19xx, 00-69 => 20xx."""
    n = int(yy)
    return f"19{n:02d}" if n >= 70 else f"20{n:02d}"


def _format_date(yy: str, mm: str, dd: str) -> str | None:
    try:
        y = int(_normalize_year(yy))
        m = int(mm)
        d = int(dd)
        if not (1 <= m <= 12 and 1 <= d <= 31):
            return None
        return f"{y:04d}-{m:02d}-{d:02d}"
    except (TypeError, ValueError):
        return None


def _is_title_word(token: str) -> bool:
    return bool(_TITLE_WORD_RE.match(token))


# ---------------------------------------------------------------------------
# Pattern matchers
# ---------------------------------------------------------------------------

# P1/P2/P3 share a dot-separated tokenization.
# Layout: Studio . YY . MM . DD . <name tokens...> [XXX] [<title tokens>] [res] .ext
_DOT_NAME_RE = re.compile(
    r"""^
    (?P<studio>[A-Za-z][A-Za-z0-9]*)
    \.
    (?P<y>\d{2})\.(?P<m>\d{2})\.(?P<d>\d{2})
    \.
    (?P<rest>.+)
    $""",
    re.VERBOSE,
)


def _try_dot_pattern(stem: str, ext: str) -> ParseResult | None:
    """Handles P1, P2, P3."""
    m = _DOT_NAME_RE.match(stem)
    if not m:
        return None

    date = _format_date(m["y"], m["m"], m["d"])
    if not date:
        return None

    studio = m["studio"]
    rest = m["rest"]
    tokens = [t for t in rest.split(".") if t]
    if not tokens:
        return None

    # Find resolution token (rightmost matching)
    resolution = None
    res_idx = None
    for i in range(len(tokens) - 1, -1, -1):
        canonical = extract_resolution_from_name(tokens[i])
        if canonical:
            resolution = canonical
            res_idx = i
            break

    upper = res_idx if res_idx is not None else len(tokens)
    body = tokens[:upper]

    # Strip trailing 'XXX' marker(s)
    xxx_idx = None
    for i in range(len(body) - 1, -1, -1):
        if body[i].upper() == "XXX":
            xxx_idx = i
            break

    if xxx_idx is not None:
        name_and_title = body[:xxx_idx]
        title_tokens: list[str] = []  # P1/P3
        # Take greedy First+Last from start, anything else is title (P2)
        if len(name_and_title) >= 2 and _is_title_word(name_and_title[0]) and _is_title_word(name_and_title[1]):
            performers = [f"{name_and_title[0]} {name_and_title[1]}"]
            title_tokens = name_and_title[2:]
            if title_tokens:
                pattern_id, confidence = "P2", 0.92
                title = " ".join(title_tokens)
            else:
                pattern_id, confidence = "P1", 0.95
                title = None
        elif len(name_and_title) == 1 and _is_title_word(name_and_title[0]):
            performers = [name_and_title[0]]
            title = None
            pattern_id, confidence = "P3", 0.90
        else:
            # Couldn't pull a clean performer; treat as low-confidence
            return None
    else:
        # No XXX marker; try First+Last only
        if len(body) >= 2 and _is_title_word(body[0]) and _is_title_word(body[1]):
            performers = [f"{body[0]} {body[1]}"]
            extra = body[2:]
            if extra:
                pattern_id, confidence = "P2", 0.85
                title = " ".join(extra)
            else:
                pattern_id, confidence = "P1", 0.88
                title = None
        elif len(body) == 1 and _is_title_word(body[0]):
            performers = [body[0]]
            title = None
            pattern_id, confidence = "P3", 0.82
        else:
            return None

    return ParseResult(
        pattern_id=pattern_id,
        confidence=confidence,
        performers=performers,
        studio=studio,
        release_date=date,
        title=title,
        resolution=resolution,
        ext=ext,
    )


# P4/P5/P6 — "Performer - Title (Quality)" or "P1 & P2 - Title (Quality)"
_AMPERSAND_RE = re.compile(r"\s*&\s*|\s+and\s+", re.IGNORECASE)
_QUALITY_BRACKET_RE = re.compile(
    r"[\(\[\{][^)\]\}]*"
    r"(?:full[\s_\-\.]?hd|fhd|hd(?:\s*\d{3,4}p)?|2160p|1440p|1080p|720p|540p|480p|360p|4k)"
    r"[^)\]\}]*[\)\]\}]",
    re.IGNORECASE,
)


def _strip_trailing_quality(stem: str) -> tuple[str, str | None]:
    """Pull a bracketed quality tag off the end. Returns (clean_stem, resolution)."""
    m = _QUALITY_BRACKET_RE.search(stem)
    resolution = None
    if m:
        resolution = extract_resolution_from_name(m.group(0))
        stem = (stem[: m.start()] + stem[m.end():]).strip()
    if resolution is None:
        resolution = extract_resolution_from_name(stem)
    return stem.strip(" -._"), resolution


def _looks_like_performer(name: str) -> bool:
    parts = name.strip().split()
    if not (2 <= len(parts) <= 3):
        return False
    return all(_is_title_word(p) for p in parts)


def _try_space_dash_pattern(stem: str, ext: str) -> ParseResult | None:
    """Handles P4, P5, P6."""
    clean, resolution = _strip_trailing_quality(stem)

    if " - " in clean:
        left, _, right = clean.partition(" - ")
        left = left.strip()
        right = right.strip()
        # Split on & / and
        names = [n.strip() for n in _AMPERSAND_RE.split(left) if n.strip()]
        if names and all(_looks_like_performer(n) for n in names):
            performers = names
            title = right if right else None
            if len(performers) == 1:
                pattern_id, confidence = "P4", 0.88
            else:
                pattern_id, confidence = "P5", 0.85
            return ParseResult(
                pattern_id=pattern_id,
                confidence=confidence,
                performers=performers,
                studio=None,
                release_date=None,
                title=title,
                resolution=resolution,
                ext=ext,
            )

    # P6: no " - ", maybe just "P1 & P2"
    names = [n.strip() for n in _AMPERSAND_RE.split(clean) if n.strip()]
    if len(names) >= 2 and all(_looks_like_performer(n) for n in names):
        return ParseResult(
            pattern_id="P6",
            confidence=0.83,
            performers=names,
            studio=None,
            release_date=None,
            title=None,
            resolution=resolution,
            ext=ext,
        )
    return None


# P7/P8 — underscore-separated
_UNDERSCORE_DASH_SEP_RE = re.compile(r"_-_|_\-_")


def _try_underscore_pattern(stem: str, ext: str) -> ParseResult | None:
    """Handles P7 (1 performer) and P8 (2 performers), underscore tokens."""
    if "_" not in stem:
        return None

    # Pull off trailing resolution token
    resolution = extract_resolution_from_name(stem)
    work = stem
    if resolution:
        # Strip the rightmost resolution token + surrounding separators
        work = re.sub(
            r"[_\.\-]*(?:2160p|1440p|1080p|720p|540p|480p|360p|4k|full[\s_\-\.]?hd|fhd|hd)"
            r"[_\.\-0-9]*$",
            "",
            work,
            flags=re.IGNORECASE,
        )

    parts = _UNDERSCORE_DASH_SEP_RE.split(work)
    if len(parts) < 2:
        return None

    left = parts[0].strip("_-. ")
    right_raw = "_-_".join(parts[1:]).strip("_-. ")
    title = right_raw.replace("_", " ").strip() if right_raw else None
    if title:
        # Remove trailing date-in-parens artifacts like __04.11.2020__
        title = re.sub(r"\s*_+\d{2}\.\d{2}\.\d{4}_*\s*", " ", title).strip()
        title = _WS_RE.sub(" ", title)
        title = title.strip(" _-.")

    left_tokens = [t for t in left.split("_") if t]
    if not left_tokens or not all(_is_title_word(t) for t in left_tokens):
        return None

    if len(left_tokens) == 2:
        performers = [f"{left_tokens[0]} {left_tokens[1]}"]
        return ParseResult(
            pattern_id="P7",
            confidence=0.80,
            performers=performers,
            studio=None,
            release_date=None,
            title=title or None,
            resolution=resolution,
            ext=ext,
        )
    if len(left_tokens) == 4:
        performers = [
            f"{left_tokens[0]} {left_tokens[1]}",
            f"{left_tokens[2]} {left_tokens[3]}",
        ]
        return ParseResult(
            pattern_id="P8",
            confidence=0.78,
            performers=performers,
            studio=None,
            release_date=None,
            title=title or None,
            resolution=resolution,
            ext=ext,
        )
    return None


# P9 — comma-separated names with trailing resolution
_COMMA_NAME_RE = re.compile(r"^[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+){0,2}$")


def _try_comma_pattern(stem: str, ext: str) -> ParseResult | None:
    """Handles P9."""
    if "," not in stem:
        return None
    resolution = extract_resolution_from_name(stem)
    work = stem
    if resolution:
        work = re.sub(
            r"\s*\b(?:2160p|1440p|1080p|720p|540p|480p|360p|4k|full[\s_\-\.]?hd|fhd|hd)\b.*$",
            "",
            work,
            flags=re.IGNORECASE,
        )
    work = work.strip(" ,._-")

    raw_names = [n.strip() for n in work.split(",")]
    names = [n for n in raw_names if n]
    if len(names) < 2:
        return None
    if not all(_COMMA_NAME_RE.match(n) for n in names):
        return None

    return ParseResult(
        pattern_id="P9",
        confidence=0.75,
        performers=names,
        studio=None,
        release_date=None,
        title=None,
        resolution=resolution,
        ext=ext,
    )


# P10 — two Title-case words + free text (very low confidence)
_P10_RE = re.compile(
    r"^(?P<a>[A-Z][a-zA-Z]+)\s+(?P<b>[A-Z][a-zA-Z]+)\s+(?P<rest>[A-Za-z].*)$"
)


def _try_low_conf_pattern(stem: str, ext: str) -> ParseResult | None:
    """Handles P10. Permissive last resort."""
    clean, resolution = _strip_trailing_quality(stem)
    m = _P10_RE.match(clean)
    if not m:
        return None
    # Reject if first two words look like a brand/site rather than a person:
    # Skip if more than 3 capitalized words run together — too noisy.
    rest = m["rest"].strip()
    if not rest:
        return None
    performer = f"{m['a']} {m['b']}"
    return ParseResult(
        pattern_id="P10",
        confidence=0.40,
        performers=[performer],
        studio=None,
        release_date=None,
        title=rest if rest else None,
        resolution=resolution,
        ext=ext,
    )


# ---------------------------------------------------------------------------
# Opacity gate
# ---------------------------------------------------------------------------

_OPAQUE_NUMERIC_RE = re.compile(r"^\d+p?\d+$", re.IGNORECASE)
_OPAQUE_SHORTCODE_RE = re.compile(r"^[a-z]{2,3}\d+", re.IGNORECASE)


def _looks_opaque(stem: str) -> bool:
    """Quick gate for stems that should never be parsed."""
    s = stem.strip()
    if not s:
        return True
    # Pure numeric-ish like '1080p3002066'
    if re.fullmatch(r"[0-9p]+", s, re.IGNORECASE):
        return True
    # 'pr181080p2422179' — letter prefix + long digit run, no word chars
    if re.fullmatch(r"[a-z]{1,4}\d{4,}p?\d*", s, re.IGNORECASE):
        return True
    # Starts with date-of-broken-shape like '02.17.05.Trouble...' (no studio prefix)
    if re.match(r"^\d{2}\.\d{2}\.\d{2}\.", s):
        return True
    # SKU-style 'LP23.034.LP-UF_1080p' — uppercase letters + digits + dots
    if re.fullmatch(r"[A-Z0-9]{2,}\.[A-Z0-9\.\-_]+", s) and not re.search(r"[a-z]", s):
        return True
    return False


# ---------------------------------------------------------------------------
# Public API: parse / build
# ---------------------------------------------------------------------------


def parse_filename(name: str) -> ParseResult | None:
    """Try ordered patterns. Return first match or None for opaque names."""
    if not name:
        return None
    p = Path(name)
    stem = p.stem
    ext = p.suffix.lower()
    if not ext:
        return None
    if _looks_opaque(stem):
        return None

    for fn in (
        _try_dot_pattern,
        _try_space_dash_pattern,
        _try_underscore_pattern,
        _try_comma_pattern,
        _try_low_conf_pattern,
    ):
        try:
            result = fn(stem, ext)
        except Exception:
            result = None
        if result is not None:
            return result
    return None


# ---------------------------------------------------------------------------
# Build target filename
# ---------------------------------------------------------------------------

_MAX_NAME_LEN = 200


def _format_performer(name: str) -> str:
    name = sanitize_field(name)
    parts = [p for p in re.split(r"[\s_]+", name) if p]
    parts = [_safe_component(p) for p in parts]
    parts = [p for p in parts if p]
    return "-".join(parts)


def build_target_filename(result: ParseResult) -> str:
    """Render a ParseResult into the canonical target filename."""
    if result is None:
        raise ValueError("result must not be None")
    if not result.ext:
        raise ValueError("ParseResult.ext is required")

    fields: list[str] = []

    if result.studio:
        studio = split_studio_name(result.studio)
        studio = _safe_component(studio)
        if studio:
            fields.append(studio)

    if result.release_date:
        date = sanitize_field(result.release_date)
        # Already in YYYY-MM-DD; just strip unsafe chars
        date = _SAFE_FIELD_RE.sub("", date)
        if date:
            fields.append(date)

    if result.performers:
        performer_tokens: list[str] = []
        for p in result.performers:
            fp = _format_performer(p)
            if fp:
                performer_tokens.append(fp)
        if performer_tokens:
            fields.append("_".join(performer_tokens))

    title_field_index = len(fields)  # remember where title goes
    title_value = None
    if result.title:
        title_clean = sanitize_field(result.title)
        # Strip noisy quality bracket residues
        title_clean = _QUALITY_TAGS_RE.sub("", title_clean)
        title_clean = _WS_RE.sub(" ", title_clean).strip()
        # Convert spaces in title to dashes within the component
        title_value = _safe_component(title_clean)
        if title_value:
            fields.append(title_value)

    if result.resolution:
        res = _safe_component(result.resolution)
        if res:
            fields.append(res)

    if not fields:
        raise ValueError("Cannot build target filename: no fields populated")

    ext = result.ext if result.ext.startswith(".") else "." + result.ext
    ext = ext.lower()

    candidate = ".".join(fields) + ext

    # Enforce max length by truncating Title at word (dash) boundary
    if len(candidate) > _MAX_NAME_LEN and title_value is not None:
        budget = _MAX_NAME_LEN - (len(candidate) - len(title_value))
        if budget < 1:
            # Title can't fit at all; drop it
            fields.pop(title_field_index)
        else:
            truncated = title_value[:budget]
            # Trim back to last dash to preserve word boundary
            if "-" in truncated and len(truncated) < len(title_value):
                trim_at = truncated.rfind("-")
                if trim_at >= 8:  # keep at least a couple of words
                    truncated = truncated[:trim_at]
            truncated = truncated.rstrip("-._")
            if truncated:
                fields[title_field_index] = truncated
            else:
                fields.pop(title_field_index)
        candidate = ".".join(fields) + ext

    # Final hard cap (e.g. very long studio + performer chain): truncate stem
    if len(candidate) > _MAX_NAME_LEN:
        stem_budget = _MAX_NAME_LEN - len(ext)
        stem_only = ".".join(fields)[:stem_budget].rstrip("._-")
        candidate = stem_only + ext

    return candidate


# ---------------------------------------------------------------------------
# Mount scan
# ---------------------------------------------------------------------------


def _iter_video_files(root: str) -> Iterable[str]:
    for dirpath, _dirs, files in os.walk(root):
        for fn in files:
            stem, ext = os.path.splitext(fn)
            if ext.lower() not in VIDEO_EXTENSIONS:
                continue
            # Skip in-progress transcode temp files ("<stem>.transcoding<ext>")
            # so a scan running mid-encode never persists them as real videos.
            if stem.lower().endswith(".transcoding"):
                continue
            yield os.path.join(dirpath, fn)


def scan_mount(
    conn: sqlite3.Connection, mount_path: str, mount_name: str
) -> dict:
    """Walk a mount and persist parse results into file_curation."""
    if not os.path.isdir(mount_path):
        return {"total": 0, "parsed": 0, "opaque": 0, "errors": 0}

    total = 0
    parsed = 0
    opaque = 0
    errors = 0

    for full_path in _iter_video_files(mount_path):
        total += 1
        try:
            base = os.path.basename(full_path)
            result = parse_filename(base)

            if result is None:
                opaque += 1
                upsert_file_curation(
                    conn,
                    path=full_path,
                    mount=mount_name,
                    extraction_method="none",
                    extraction_confidence=0.0,
                    status="unknown",
                )
                continue

            target_name = None
            try:
                target_name = build_target_filename(result)
            except Exception:
                target_name = None

            method_map = {
                "P1": "filename_studio_date",
                "P2": "filename_studio_date_title",
                "P3": "filename_studio_date_single",
                "P4": "filename_dash",
                "P5": "filename_dash_multi",
                "P6": "filename_amp_only",
                "P7": "filename_underscore",
                "P8": "filename_underscore_multi",
                "P9": "filename_comma",
                "P10": "filename_low_conf",
            }
            extraction_method = method_map.get(result.pattern_id, "unknown")

            file_id = upsert_file_curation(
                conn,
                path=full_path,
                mount=mount_name,
                studio=result.studio,
                title=result.title,
                release_date=result.release_date,
                resolution=result.resolution,
                extraction_method=extraction_method,
                extraction_confidence=float(result.confidence),
                proposed_filename=target_name,
                status="pending",
            )

            # Link performers (position by order, source 'auto')
            for position, performer_name in enumerate(result.performers or []):
                if not performer_name:
                    continue
                try:
                    pid = get_or_create_performer(conn, performer_name)
                except Exception:
                    continue
                conn.execute(
                    """
                    INSERT OR IGNORE INTO file_performer
                        (file_curation_id, performer_id, position, source)
                    VALUES (?, ?, ?, 'auto')
                    """,
                    (file_id, pid, position),
                )
            conn.commit()
            parsed += 1
        except Exception:
            errors += 1
            try:
                conn.rollback()
            except Exception:
                pass

    return {"total": total, "parsed": parsed, "opaque": opaque, "errors": errors}
