# app/curation/auto_match.py
"""Filename-based performer matching for unknown file_curation rows.

Called by the scheduler every 60 min.  Two phases:
  Phase 1 — match against existing performers (canonical names + aliases).
  Phase 2 — extract a new performer name from the filename and create them.

Phase 3 (face rec fallback) is handled by _run_auto_rename in main.py.
Phase 4 (re-enqueue) is handled by enqueue_all_unknown in face/worker.py.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3

log = logging.getLogger(__name__)

MAX_FILENAME = 255
MAX_COLLISIONS = 1000

QUALITY_RE = re.compile(
    r'\b(1080p?|2160p?|720p?|480p?|4k|uhd|full[\s_]?hd|hd|hdrip|'
    r'webrip|web[-_]?dl|blu[-_]?ray|x264|x265|hevc|avc|xvid|divx|'
    r'xxx|(?:bd|cam|dvd|tv|web|hd|vhs|blu)rip)\b',
    re.IGNORECASE,
)
DATE_RE = re.compile(
    r'\b\d{4}[-./]\d{2}[-./]\d{2}\b'
    r'|\b\d{2}[-./]\d{2}[-./]\d{2}\b'
    r'|\b\d{4}\b'
)
STOPWORDS: frozenset[str] = frozenset({
    'hot', 'slutty', 'slut', 'teen', 'big', 'young', 'new', 'sexy', 'hard',
    'full', 'scene', 'episode', 'ep', 'part', 'pt', 'vol', 'video', 'movie',
    'clip', 'hd', 'sd', 'xxx', 'cum', 'fuck', 'fucking', 'sex', 'anal',
    'oral', 'dp', 'bbc', 'milf', 'mature', 'amateur', 'casting', 'outdoor',
    'czech', 'german', 'french', 'italian', 'spanish', 'russian', 'backstage',
    'behind', 'bonus', 'extra', 'compilation', 'best', 'trailer', 'preview',
    'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or',
    'with', 'her', 'his', 'is', 'are', 'was', 'wants', 'works', 'loves',
    'gets', 'takes', 'gives', 'lets', 'makes', 'from', 'she', 'he', 'they',
    'me', 'i', 'we', 'you', 'my', 'your', 'our', 'this', 'that', 'its',
    'know', 'like', 'am', 'do', 'did', 'will', 'can', 'let', 'go', 'see',
    'creampie', 'blowjob', 'handjob', 'threesome', 'gangbang', 'hardcore',
    'softcore', 'lesbian', 'solo', 'squirt', 'squirting', 'facial', 'swallow',
    'interracial', 'pov', 'fetish', 'busty', 'naughty', 'dirty', 'nubile',
    'first', 'time', 'beach', 'pool', 'office', 'kitchen', 'bathroom',
    'public', 'private',
})
_NAME_PARTICLES: frozenset[str] = frozenset({
    'de', 'di', 'da', 'le', 'la', 'du', 'el', 'al', 'st', 'mc', 'van', 'von',
})


def normalize_stem(path: str) -> str:
    """Return a normalised, lowercased stem suitable for substring matching."""
    stem = os.path.splitext(os.path.basename(path))[0]
    stem = stem.replace('_', ' ').replace('.', ' ')
    stem = QUALITY_RE.sub(' ', stem)
    stem = DATE_RE.sub(' ', stem)
    stem = re.sub(r'[^\w\s]', ' ', stem)
    return re.sub(r'\s+', ' ', stem).strip().lower()


def _contiguous(haystack: list[str], needle: list[str]) -> bool:
    """Return True if needle appears as a contiguous sub-sequence of haystack."""
    n = len(needle)
    if n == 0:
        return False
    for i in range(len(haystack) - n + 1):
        if haystack[i:i + n] == needle:
            return True
    return False


def match_existing_performers(
    norm_stem: str,
    performers: list[tuple[int, str, list[str]]],
) -> list[tuple[int, str]]:
    """Return (performer_id, canonical_name) pairs whose name appears in norm_stem.

    Skips single-token performers to avoid false positives.
    Each performer appears at most once; order follows first match position in stem.
    """
    stem_tokens = norm_stem.split()
    matches: list[tuple[int, str]] = []
    seen: set[int] = set()
    for perf_id, canon, aliases in performers:
        if len(normalize_stem(canon + '.mp4').split()) < 2:
            continue
        for name in [canon] + list(aliases):
            needle = normalize_stem(name + '.mp4').split()
            if needle and _contiguous(stem_tokens, needle):
                if perf_id not in seen:
                    matches.append((perf_id, canon))
                    seen.add(perf_id)
                break
    return matches


def extract_new_performer_name(path: str) -> str | None:
    """Extract a likely performer name from a filename.

    Returns a title-cased name (2-3 tokens, 4-40 chars) or None if extraction
    fails any of the rejection criteria.
    """
    stem = os.path.splitext(os.path.basename(path))[0]
    stem = stem.replace('_', ' ').replace('.', ' ')
    first_part = re.split(r'\s+-\s+|\s+–\s+', stem)[0].strip()

    if re.match(r'^\d', first_part):
        return None

    first_part = QUALITY_RE.sub(' ', first_part)
    first_part = DATE_RE.sub(' ', first_part)
    first_part = re.sub(r'[^\w\s]', ' ', first_part)
    first_part = re.sub(r'\s+', ' ', first_part).strip()

    first_part = re.sub(r'^s\d+', '', first_part, flags=re.IGNORECASE).strip()
    first_part = re.sub(r'^\d+', '', first_part).strip()

    if not first_part:
        return None

    tokens = first_part.split()
    name_tokens: list[str] = []
    for tok in tokens:
        t = tok.lower()
        if t in STOPWORDS or not re.match(r'^[a-zA-Z]', t):
            if name_tokens:
                break
            continue
        name_tokens.append(tok)
        if len(name_tokens) >= 3:
            break

    if any(len(t) < 3 and t.lower() not in _NAME_PARTICLES for t in name_tokens):
        return None

    for tok in name_tokens:
        if re.search(r'\d', tok):
            return None
        if tok == tok.upper() and len(tok) > 3:
            return None

    if len(name_tokens) < 2:
        return None

    candidate = ' '.join(name_tokens)
    if not (4 <= len(candidate) <= 40):
        return None
    return candidate.title()


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r'[^\w\s-]', '', s)
    s = re.sub(r'[\s_]+', '-', s)
    return s.strip('-')


def build_proposed_filename_str(performer_names: list[str], path: str) -> str:
    """Build the proposed filename string without collision checking."""
    ext = os.path.splitext(path)[1].lower() or '.mp4'
    stem = os.path.splitext(os.path.basename(path))[0]
    safe_stem = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', stem).strip('._')
    safe_stem = re.sub(r'_{2,}', '_', safe_stem)
    performer_part = '.'.join(n.replace(' ', '-') for n in performer_names)
    full = f"{performer_part}.{safe_stem}{ext}"
    return full if len(full) <= MAX_FILENAME else f"{performer_part}{ext}"


def build_proposed_filename(
    conn: sqlite3.Connection,
    fc_id: int,
    performer_names: list[str],
    path: str,
) -> str:
    """Return a collision-free proposed_filename for this file."""
    base = build_proposed_filename_str(performer_names, path)
    base_stem, base_ext = os.path.splitext(base)
    proposed, counter = base, 2
    while counter <= MAX_COLLISIONS:
        row = conn.execute(
            "SELECT id FROM file_curation WHERE proposed_filename = ? AND id != ?",
            (proposed, fc_id),
        ).fetchone()
        if not row:
            return proposed
        proposed = f"{base_stem}_{counter}{base_ext}"
        counter += 1
    log.error('build_proposed_filename: exhausted %d collision attempts for fc_id=%s', MAX_COLLISIONS, fc_id)
    raise RuntimeError(
        f"Could not find unique filename after {MAX_COLLISIONS} attempts for fc_id={fc_id}"
    )


def phase1(conn: sqlite3.Connection) -> int:
    """Match unknown files against existing performers by filename.

    Returns count of files matched.
    Only processes files with no performer already assigned.
    """
    performers: list[tuple[int, str, list[str]]] = []
    for row in conn.execute("SELECT id, canonical_name FROM performer ORDER BY id"):
        aliases = [r[0] for r in conn.execute(
            "SELECT alias FROM performer_alias WHERE performer_id = ?", (row[0],)
        )]
        performers.append((row[0], row[1], aliases))

    matched = 0
    rows = conn.execute(
        """SELECT fc.id, fc.path FROM file_curation fc
            WHERE fc.status = 'unknown'
              AND NOT EXISTS (
                  SELECT 1 FROM file_performer fp WHERE fp.file_curation_id = fc.id
              )"""
    ).fetchall()

    for fc_id, path in rows:
        try:
            norm = normalize_stem(path)
            hits = match_existing_performers(norm, performers)
            if not hits:
                continue
            for pos, (p_id, _) in enumerate(hits):
                conn.execute(
                    "INSERT OR IGNORE INTO file_performer "
                    "(file_curation_id, performer_id, position, source) VALUES (?,?,?,'filename')",
                    (fc_id, p_id, pos),
                )
            p_names = [h[1] for h in hits]
            proposed = build_proposed_filename(conn, fc_id, p_names, path)
            conn.execute(
                "UPDATE file_curation SET proposed_filename=?, status='approved', "
                "updated_at=datetime('now') "
                "WHERE id=? AND status NOT IN ('renamed','skipped')",
                (proposed, fc_id),
            )
            matched += 1
            if matched % 100 == 0:
                conn.commit()
        except Exception:
            conn.rollback()
            log.exception('phase1: skipping fc_id=%s path=%s', fc_id, path)

    conn.commit()
    return matched


def phase2(conn: sqlite3.Connection) -> tuple[int, list[str]]:
    """Extract new performer names from filenames of still-unknown files.

    Returns (count_matched, list_of_new_performer_canonical_names).
    Only processes files with no performer already assigned.
    """
    created_names: list[str] = []
    matched = 0

    rows = conn.execute(
        """SELECT fc.id, fc.path FROM file_curation fc
            WHERE fc.status = 'unknown'
              AND NOT EXISTS (
                  SELECT 1 FROM file_performer fp WHERE fp.file_curation_id = fc.id
              )"""
    ).fetchall()

    for fc_id, path in rows:
        try:
            name = extract_new_performer_name(path)
            if not name:
                continue
            slug = slugify(name)
            # All reads first
            existing = conn.execute(
                "SELECT id FROM performer WHERE slug = ?", (slug,)
            ).fetchone()
            # Now writes — everything after this can be rolled back if it fails
            if existing:
                p_id = existing[0]
                newly_created = False
            else:
                cur = conn.execute(
                    "INSERT INTO performer (canonical_name, slug, gender) VALUES (?,?,'unknown')",
                    (name, slug),
                )
                p_id = cur.lastrowid
                newly_created = True
            conn.execute(
                "INSERT OR IGNORE INTO file_performer "
                "(file_curation_id, performer_id, position, source) VALUES (?,?,0,'filename')",
                (fc_id, p_id),
            )
            proposed = build_proposed_filename(conn, fc_id, [name], path)
            conn.execute(
                "UPDATE file_curation SET proposed_filename=?, status='approved', "
                "updated_at=datetime('now') "
                "WHERE id=? AND status NOT IN ('renamed','skipped')",
                (proposed, fc_id),
            )
            if newly_created:
                created_names.append(name)
            matched += 1
            if matched % 100 == 0:
                conn.commit()
        except Exception:
            conn.rollback()
            log.exception('phase2: skipping fc_id=%s path=%s', fc_id, path)

    conn.commit()
    return matched, created_names


def run_auto_match(conn: sqlite3.Connection) -> dict:
    """Run phases 1 and 2 and return a summary dict.

    Returned keys: phase1 (int), phase2 (int), new_performers (int),
                   new_performer_names (list[str]).
    """
    n1 = phase1(conn)
    n2, names = phase2(conn)
    if names:
        log.info('auto_match: new performers created: %s', ', '.join(sorted(names)))
    return {
        'phase1': n1,
        'phase2': n2,
        'new_performers': len(names),
        'new_performer_names': names,
    }
