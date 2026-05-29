# jdownloader Automation Pipeline — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make jdownloader fully automated — new downloads are discovered, filename-matched, face-rec matched, and moved to the correct performer folder without manual intervention.

**Architecture:** A new `app/curation/auto_match.py` module houses the filename-based performer matching logic (converted from today's one-time `/tmp/auto_match.py` batch script). Two new scheduler tasks wire it in: a jdownloader scan every 60 min and an auto-match pass every 60 min after face rec completes. The face rec auto-accept threshold is raised from 0.55 → 0.65 to reduce false positives at scale. A one-time cleanup script removes the ~25 garbage performers created by today's batch run.

**Tech Stack:** Python 3, SQLite (sqlite3 stdlib), pytest, FastAPI scheduler loop (threading.Thread), existing app structure in `app/curation/`, `app/face/`, `app/main.py`.

**Context for the implementer:**
- All live code lives in `/Users/reba/Developer/video-transcoder/`
- `app/main.py` runs a `while True` scheduler loop in a daemon thread (`_start_scheduler`). New tasks follow the exact same pattern as existing ones (`_run_tpdb_batch`, `_run_face_enqueue`, `_run_auto_rename`): standalone function that opens its own DB connection, wrapped in try/except, spawned via `threading.Thread`.
- `app/curation/auto_match.py` does NOT exist yet — we're creating it.
- `tests/` directory does NOT exist yet — create it with `__init__.py`.
- The `/tmp/auto_match.py` script contains battle-tested logic from today's production run (1140 files processed). We are porting it — not rewriting it.
- Deploy: edit locally → `git push` → GHA bumps version → Watchtower auto-deploys to hpc02. Never edit files directly on hpc02 (except the `/tmp/` cleanup script).
- `file_curation.status` flow: `unknown` → `approved` → `renamed`. Our module only touches `unknown` files.

---

## File Structure

| File | Action | Purpose |
|---|---|---|
| `app/curation/auto_match.py` | **Create** | All helpers + phase1 + phase2 + `run_auto_match()` |
| `tests/__init__.py` | **Create** | Makes tests/ a package |
| `tests/test_auto_match.py` | **Create** | Pytest unit tests for all helpers |
| `app/main.py` | **Modify** | Add `_run_curation_scan`, `_run_auto_match`, wire into scheduler |
| `app/face/matcher.py` | **Modify** | Raise `AUTO_ACCEPT_THRESHOLD` 0.55 → 0.65 |
| `/tmp/cleanup_garbage_performers.py` | **Create** | One-time script — NOT committed to repo |

---

## Task 1: Create `app/curation/auto_match.py` — helpers

**Files:**
- Create: `app/curation/auto_match.py`

- [ ] **Step 1: Create the file with helpers only**

```python
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

    Returns a title-cased name (2–3 tokens, 4–40 chars) or None if extraction
    fails any of the rejection criteria.
    """
    stem = os.path.splitext(os.path.basename(path))[0]
    stem = stem.replace('_', ' ').replace('.', ' ')
    first_part = re.split(r'\s+-\s+|\s+–\s+', stem)[0].strip()

    if re.match(r'^\d', first_part):
        return None

    first_part = QUALITY_RE.sub('', first_part)
    first_part = DATE_RE.sub('', first_part)
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
    raise RuntimeError(
        f"Could not find unique filename after {MAX_COLLISIONS} attempts for fc_id={fc_id}"
    )
```

- [ ] **Step 2: Verify file was created**

```bash
python3 -c "
import sys; sys.path.insert(0, '.')
from app.curation.auto_match import normalize_stem, extract_new_performer_name
print(normalize_stem('Freya Dee 1080p.mp4'))
print(extract_new_performer_name('jane doe scene.mp4'))
"
```

Expected:
```
freya dee
Jane Doe
```

- [ ] **Step 3: Commit**

```bash
git add app/curation/auto_match.py
git commit -m "Add auto_match helpers: normalize_stem, match_existing_performers, extract_new_performer_name"
```

---

## Task 2: Add `phase1`, `phase2`, `run_auto_match` to `app/curation/auto_match.py`

**Files:**
- Modify: `app/curation/auto_match.py`

- [ ] **Step 1: Append phase functions to the module**

Add these functions at the bottom of `app/curation/auto_match.py`:

```python
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
            row = conn.execute(
                "SELECT id FROM performer WHERE slug = ?", (slug,)
            ).fetchone()
            if row:
                p_id = row[0]
            else:
                conn.execute(
                    "INSERT INTO performer (canonical_name, slug, gender) VALUES (?,?,'unknown')",
                    (name, slug),
                )
                p_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
                created_names.append(name)
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
            matched += 1
            if matched % 100 == 0:
                conn.commit()
        except Exception:
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
```

- [ ] **Step 2: Smoke test**

```bash
python3 -c "
import sys, sqlite3; sys.path.insert(0, '.')
from app.curation.auto_match import run_auto_match
# Just verify it imports and returns the right shape
print('run_auto_match importable: OK')
print('phase1/phase2 importable: OK')
"
```

Expected:
```
run_auto_match importable: OK
phase1/phase2 importable: OK
```

- [ ] **Step 3: Commit**

```bash
git add app/curation/auto_match.py
git commit -m "Add phase1, phase2, run_auto_match to auto_match module"
```

---

## Task 3: Write tests for `app/curation/auto_match.py`

**Files:**
- Create: `tests/__init__.py`
- Create: `tests/test_auto_match.py`

- [ ] **Step 1: Create tests directory and empty __init__.py**

Create `tests/__init__.py` as an empty file.

- [ ] **Step 2: Write the test file**

Create `tests/test_auto_match.py`:

```python
"""Unit tests for app.curation.auto_match helpers.

All tests operate on pure functions — no DB required for most.
The phase1/phase2 tests use an in-memory SQLite DB.
"""

import sqlite3
import pytest

from app.curation.auto_match import (
    normalize_stem,
    match_existing_performers,
    extract_new_performer_name,
    build_proposed_filename_str,
    build_proposed_filename,
    phase1,
    phase2,
    run_auto_match,
    slugify,
)


# ---------------------------------------------------------------------------
# normalize_stem
# ---------------------------------------------------------------------------

def test_normalize_stem_underscores():
    assert normalize_stem("anabell_evans_beach_scene.mp4") == "anabell evans beach scene"

def test_normalize_stem_dots():
    assert normalize_stem("lola.dee.scene.mp4") == "lola dee scene"

def test_normalize_stem_1080p():
    assert normalize_stem("Freya Dee 1080p.mp4") == "freya dee"

def test_normalize_stem_720p():
    assert normalize_stem("Jane Doe 720p scene.mp4") == "jane doe scene"

def test_normalize_stem_x264():
    assert normalize_stem("Some.Girl.x264.mp4") == "some girl"

def test_normalize_stem_date_iso():
    assert normalize_stem("Eva 2023-04-01 scene.mp4") == "eva scene"

def test_normalize_stem_date_short():
    assert normalize_stem("Eva 23-04-01 scene.mp4") == "eva scene"

def test_normalize_stem_year_only():
    assert normalize_stem("Scene 2021.mp4") == "scene"

def test_normalize_stem_lowercased():
    assert normalize_stem("FREYA_DEE_1080P.mp4") == "freya dee"

def test_normalize_stem_non_mp4():
    assert normalize_stem("hello world.mkv") == "hello world"

def test_normalize_stem_path_stripped():
    assert normalize_stem("/some/deep/path/hello world.mp4") == "hello world"


# ---------------------------------------------------------------------------
# match_existing_performers
# ---------------------------------------------------------------------------

PERFORMERS = [
    (1, "Freya Dee", []),
    (2, "Lola De Mons", ["Lola de Mons"]),
    (3, "Mia Malkova", ["Mia M"]),
    (4, "Anna Bell Peaks", []),
]

def _stem(path):
    return normalize_stem(path)

def test_match_exact_canon():
    assert match_existing_performers(_stem("Freya Dee 1080p.mp4"), PERFORMERS) == [(1, "Freya Dee")]

def test_match_alias():
    result = match_existing_performers(_stem("lola de mons beach.mp4"), PERFORMERS)
    assert result == [(2, "Lola De Mons")]

def test_match_short_alias():
    result = match_existing_performers(_stem("mia m hardcore.mp4"), PERFORMERS)
    assert result == [(3, "Mia Malkova")]

def test_match_three_token():
    result = match_existing_performers(_stem("anna bell peaks scene.mp4"), PERFORMERS)
    assert result == [(4, "Anna Bell Peaks")]

def test_no_match():
    assert match_existing_performers(_stem("Unknown Studio Scene.mp4"), PERFORMERS) == []

def test_partial_token_not_matched():
    # "Freya" alone should NOT match "Freya Dee" (needs both tokens)
    assert match_existing_performers(_stem("freya solo.mp4"), PERFORMERS) == []

def test_multiple_performers():
    result = match_existing_performers(
        normalize_stem("Freya Dee and Mia Malkova scene.mp4"), PERFORMERS
    )
    assert result == [(1, "Freya Dee"), (3, "Mia Malkova")]

def test_no_duplicates_for_same_performer():
    result = match_existing_performers(
        normalize_stem("Lola De Mons and Lola de Mons scene.mp4"), PERFORMERS
    )
    assert len(result) == 1


# ---------------------------------------------------------------------------
# extract_new_performer_name
# ---------------------------------------------------------------------------

def test_extract_basic():
    assert extract_new_performer_name("Freya Dee 1080p.mp4") == "Freya Dee"

def test_extract_underscore():
    assert extract_new_performer_name("anabell_evans_beach_scene.mp4") == "Anabell Evans"

def test_extract_with_separator():
    assert extract_new_performer_name("Lola de Mons - Lola 28 Years.mp4") == "Lola De Mons"

def test_extract_starts_with_digit_returns_none():
    assert extract_new_performer_name("3_Cocks_Dream.mp4") is None

def test_extract_studio_code_returns_none():
    # MonstersOfCock → 1 token after normalisation → None
    assert extract_new_performer_name("MonstersOfCock.25.05.11.Miami.mp4") is None

def test_extract_date_prefix_returns_none():
    assert extract_new_performer_name("22.06-02.10.GP2339_HD.mp4") is None

def test_extract_title_cased():
    assert extract_new_performer_name("jane doe scene.mp4") == "Jane Doe"

def test_extract_three_words():
    assert extract_new_performer_name("Anna Bell Peaks hardcore.mp4") == "Anna Bell Peaks"

def test_extract_stopword_at_start_skipped():
    assert extract_new_performer_name("hot jane doe scene.mp4") == "Jane Doe"

def test_extract_single_word_returns_none():
    assert extract_new_performer_name("Madonna_scene.mp4") is None

def test_extract_em_dash():
    assert extract_new_performer_name("Eva Long – Beach Fun.mp4") == "Eva Long"

def test_extract_too_short_returns_none():
    assert extract_new_performer_name("A B scene.mp4") is None

def test_extract_digit_mid_name_stops():
    # "Eva" + digit year → only 1 name token → None
    assert extract_new_performer_name("Eva 2023 scene.mp4") is None

def test_extract_site_code_prefix_stripped():
    # s100 prefix should be stripped
    assert extract_new_performer_name("s100Adriana Chechik scene.mp4") == "Adriana Chechik"

def test_extract_short_code_tokens_rejected():
    # "Hh Cn" → tokens < 3 chars → None
    assert extract_new_performer_name("Hh Cn scene.mp4") is None


# ---------------------------------------------------------------------------
# build_proposed_filename_str
# ---------------------------------------------------------------------------

def test_build_filename_basic():
    assert build_proposed_filename_str(["Freya Dee"], "/data/Freya Dee 1080p.mp4") == \
        "Freya-Dee.Freya Dee 1080p.mp4"

def test_build_filename_mp4_extension():
    assert build_proposed_filename_str(["Jane Doe"], "/data/scene.mp4").endswith(".mp4")

def test_build_filename_uppercase_ext_lowercased():
    assert build_proposed_filename_str(["Jane Doe"], "/data/scene.MP4").endswith(".mp4")

def test_build_filename_missing_ext_defaults_mp4():
    assert build_proposed_filename_str(["Jane Doe"], "/data/scene").endswith(".mp4")

def test_build_filename_multiple_performers():
    assert build_proposed_filename_str(["Freya Dee", "Lola Dee"], "/data/scene.mp4") == \
        "Freya-Dee.Lola-Dee.scene.mp4"


# ---------------------------------------------------------------------------
# phase1 / phase2 / run_auto_match — in-memory DB
# ---------------------------------------------------------------------------

def _make_db() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE performer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            canonical_name TEXT NOT NULL,
            slug TEXT UNIQUE NOT NULL,
            gender TEXT DEFAULT 'unknown'
        );
        CREATE TABLE performer_alias (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            performer_id INTEGER NOT NULL,
            alias TEXT NOT NULL
        );
        CREATE TABLE file_curation (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            mount TEXT,
            status TEXT DEFAULT 'unknown',
            proposed_filename TEXT,
            updated_at TEXT
        );
        CREATE TABLE file_performer (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_curation_id INTEGER NOT NULL,
            performer_id INTEGER NOT NULL,
            position INTEGER DEFAULT 0,
            source TEXT,
            UNIQUE(file_curation_id, performer_id)
        );
    """)
    return conn


def test_phase1_matches_existing_performer():
    conn = _make_db()
    conn.execute("INSERT INTO performer (canonical_name, slug) VALUES ('Freya Dee','freya-dee')")
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/Freya Dee 1080p.mp4','jdownloader','unknown')")
    conn.commit()

    n = phase1(conn)

    assert n == 1
    row = conn.execute("SELECT status, proposed_filename FROM file_curation WHERE id=1").fetchone()
    assert row['status'] == 'approved'
    assert row['proposed_filename'].startswith("Freya-Dee.")

    fp = conn.execute("SELECT performer_id, source FROM file_performer WHERE file_curation_id=1").fetchone()
    assert fp['source'] == 'filename'


def test_phase1_skips_file_with_existing_performer():
    conn = _make_db()
    conn.execute("INSERT INTO performer (canonical_name, slug) VALUES ('Freya Dee','freya-dee')")
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/Freya Dee 1080p.mp4','jdownloader','unknown')")
    # Pre-assign a performer
    conn.execute("INSERT INTO file_performer (file_curation_id, performer_id, position, source) VALUES (1,1,0,'test')")
    conn.commit()

    n = phase1(conn)
    assert n == 0


def test_phase2_creates_new_performer():
    conn = _make_db()
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/Jane Doe hardcore.mp4','jdownloader','unknown')")
    conn.commit()

    n, names = phase2(conn)

    assert n == 1
    assert names == ["Jane Doe"]
    p = conn.execute("SELECT canonical_name, slug FROM performer WHERE slug='jane-doe'").fetchone()
    assert p['canonical_name'] == 'Jane Doe'


def test_phase2_reuses_existing_performer_by_slug():
    conn = _make_db()
    conn.execute("INSERT INTO performer (canonical_name, slug) VALUES ('Jane Doe','jane-doe')")
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/Jane Doe hardcore.mp4','jdownloader','unknown')")
    conn.commit()

    n, names = phase2(conn)

    assert n == 1
    assert names == []  # no NEW performer created
    count = conn.execute("SELECT COUNT(*) FROM performer").fetchone()[0]
    assert count == 1  # still only one performer


def test_phase2_skips_unextractable_filename():
    conn = _make_db()
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/3_Cocks_Dream.mp4','jdownloader','unknown')")
    conn.commit()

    n, names = phase2(conn)
    assert n == 0
    assert names == []


def test_run_auto_match_returns_summary():
    conn = _make_db()
    conn.execute("INSERT INTO performer (canonical_name, slug) VALUES ('Freya Dee','freya-dee')")
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/Freya Dee scene.mp4','jdownloader','unknown')")
    conn.execute("INSERT INTO file_curation (path, mount, status) VALUES ('/media/jdownloader/Jane Doe beach.mp4','jdownloader','unknown')")
    conn.commit()

    result = run_auto_match(conn)

    assert result['phase1'] == 1   # Freya Dee matched existing
    assert result['phase2'] == 1   # Jane Doe extracted as new
    assert result['new_performers'] == 1
    assert result['new_performer_names'] == ['Jane Doe']
```

- [ ] **Step 3: Run tests and verify they all pass**

```bash
cd /Users/reba/Developer/video-transcoder
python3 -m pytest tests/test_auto_match.py -v
```

Expected: all tests PASS. Fix any failures before continuing.

- [ ] **Step 4: Commit**

```bash
git add tests/__init__.py tests/test_auto_match.py
git commit -m "Add pytest suite for auto_match helpers and phase1/phase2"
```

---

## Task 4: Add scheduled jdownloader scan to `app/main.py`

**Files:**
- Modify: `app/main.py`

- [ ] **Step 1: Add `_run_curation_scan` function**

Find the block after `_run_auto_rename` (around line 430) and insert the new function before `@app.on_event('shutdown')`:

```python
def _run_curation_scan(db_path: str, mount: str) -> None:
    """Scheduled discovery scan for an inbox mount (e.g. jdownloader).

    Walks /media/<mount>/ and upserts any new video files into file_curation.
    New files land as 'unknown' (opaque filename) or 'pending' (parseable).
    """
    _log = logging.getLogger('scheduler.curation_scan')
    conn = None
    try:
        from app.curation.extractor import scan_mount
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        mount_path = f'/media/{mount}'
        result = scan_mount(conn, mount_path, mount)
        conn.commit()
        _log.info(
            'curation scan %s: total=%d parsed=%d opaque=%d errors=%d',
            mount, result.get('total', 0), result.get('parsed', 0),
            result.get('opaque', 0), result.get('errors', 0),
        )
    except Exception:
        _log.exception('_run_curation_scan crashed (mount=%s)', mount)
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
```

- [ ] **Step 2: Add `_run_auto_match` function** (right after `_run_curation_scan`)

```python
def _run_auto_match(db_path: str) -> None:
    """Scheduled filename-based performer matching for unknown files.

    Runs phase1 (existing performers) and phase2 (new performers from filename).
    Phase3 (face rec) is handled by _run_auto_rename; phase4 (re-enqueue) by
    the face worker sweep.
    """
    _log = logging.getLogger('scheduler.auto_match')
    conn = None
    try:
        from app.curation.auto_match import run_auto_match
        conn = sqlite3.connect(db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        result = run_auto_match(conn)
        _log.info(
            'auto_match: phase1=%d phase2=%d new_performers=%d',
            result['phase1'], result['phase2'], result['new_performers'],
        )
    except Exception:
        _log.exception('_run_auto_match crashed')
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
```

- [ ] **Step 3: Wire both into `_start_scheduler`**

In `_start_scheduler`, find the timestamp vars block (around lines 458–462):

```python
last_tpdb_batch_ts = 0.0    # fire on first tick
last_face_enqueue_ts = 0.0  # fire on first tick
last_auto_rename_ts = 0.0   # fire on first tick
```

Add two new variables after these three:

```python
last_curation_scan_ts = 0.0   # fire on first tick
last_auto_match_ts = 0.0      # fire on first tick
```

Then inside the `while True` loop, after the `_run_auto_rename` block (after line ~518), add:

```python
            # Curation scan: discover new files in jdownloader every 60 min.
            if (now_ts - last_curation_scan_ts) >= 60 * 60:
                last_curation_scan_ts = now_ts
                threading.Thread(
                    target=_run_curation_scan,
                    args=(db_path_str, 'jdownloader'),
                    name='sched-curation-scan',
                    daemon=True,
                ).start()

            # Auto-match: assign performers to unknown files via filename every 60 min.
            if (now_ts - last_auto_match_ts) >= 60 * 60:
                last_auto_match_ts = now_ts
                threading.Thread(
                    target=_run_auto_match,
                    args=(db_path_str,),
                    name='sched-auto-match',
                    daemon=True,
                ).start()
```

- [ ] **Step 4: Verify the file still imports cleanly**

```bash
cd /Users/reba/Developer/video-transcoder
python3 -c "import app.main; print('imports OK')"
```

Expected: `imports OK` (will also trigger FastAPI startup checks but no errors)

- [ ] **Step 5: Commit**

```bash
git add app/main.py
git commit -m "Schedule jdownloader scan and auto-match every 60 min"
```

---

## Task 5: Raise face rec auto-accept threshold in `app/face/matcher.py`

**Files:**
- Modify: `app/face/matcher.py`

**Context:** `AUTO_ACCEPT_THRESHOLD = 0.55` means a face similarity of 55% triggers automatic accept without human review. At jdownloader's volume this creates false positives. Raising to 0.65 means only high-confidence matches are auto-accepted; 0.55–0.64 scores still show as "green" suggestions in the UI but require a click to accept.

`SIMILARITY_HIGH` stays at 0.55 — that's the UI colour threshold (green/yellow), not the auto-accept gate. Only `AUTO_ACCEPT_THRESHOLD` changes.

- [ ] **Step 1: Change the constant**

In `app/face/matcher.py`, find:

```python
AUTO_ACCEPT_THRESHOLD = 0.55     # rank-1 sim >= this with clear gap triggers automatic accept
```

Change to:

```python
AUTO_ACCEPT_THRESHOLD = 0.65     # rank-1 sim >= this with clear gap triggers automatic accept
```

- [ ] **Step 2: Verify nothing else in the codebase hard-codes 0.55 as the accept threshold**

```bash
grep -rn "0\.55\|AUTO_ACCEPT" /Users/reba/Developer/video-transcoder/app/
```

Expected: only `SIMILARITY_HIGH = 0.55` and `AUTO_ACCEPT_THRESHOLD = 0.65`. If any other file references `AUTO_ACCEPT_THRESHOLD`, confirm it imports from `app.face.matcher` (not its own copy).

- [ ] **Step 3: Commit**

```bash
git add app/face/matcher.py
git commit -m "Raise AUTO_ACCEPT_THRESHOLD from 0.55 to 0.65 to reduce false positives"
```

---

## Task 6: Write garbage performer cleanup script (one-time, NOT committed)

**Files:**
- Create: `/tmp/cleanup_garbage_performers.py`

This script runs ONCE on hpc02 inside the container. It removes the ~25 performers created today whose names are clearly not real performers. Files that are still `approved` (not yet moved) are reset to `unknown` so the new auto_match scheduler will re-process them. Files already `renamed` (physically moved) are reported but left in place — their files still exist on disk.

- [ ] **Step 1: Create the script locally**

Create `/tmp/cleanup_garbage_performers.py`:

```python
#!/usr/bin/env python3
"""
One-time cleanup: remove garbage performers created by today's auto_match batch run.

Run inside the video-transcoder container:
  docker cp /tmp/cleanup_garbage_performers.py video-transcoder:/tmp/cleanup_garbage_performers.py
  docker exec video-transcoder python3 /tmp/cleanup_garbage_performers.py

Files still 'approved': performer link removed, reset to 'unknown', proposed_filename cleared.
  → auto_match scheduler will re-process them on next run.
Files already 'renamed': reported only — they've been physically moved, manual check needed.
"""

import sqlite3

DB_PATH = '/data/transcoder.db'

# Slugs of performers confirmed as garbage from today's batch run.
GARBAGE_SLUGS = [
    'breeding-party',
    'spy-cam',
    'sweet-pleasure',
    'stuffing-skylar',
    'xangels-eva-fire',
    'balls-deep',
    'blindfolded-swingers',
    'hairy-babe-taking',
    'gameday-pussy-massacre',
    'slumber-party-orgy',
    'real-life',
    'yoga-pants-copy',
    'student-flamingo',
    'shi-official',
    'xcite-real',
    'art-cove',
    'simone-cheating',
    'bts-sweet',
    'drnk-girl',
    'mistress-oliviadrnk-bitch',
    'arschgefickte-schnaps-drosseln',
]


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    total_approved_reset = 0
    total_renamed_skipped = 0
    total_deleted = 0

    for slug in GARBAGE_SLUGS:
        row = conn.execute(
            "SELECT id, canonical_name FROM performer WHERE slug = ?", (slug,)
        ).fetchone()
        if not row:
            print(f"  SKIP (not found): {slug}")
            continue

        p_id = row['id']
        name = row['canonical_name']

        # Find all files assigned to this performer
        files = conn.execute(
            """SELECT fc.id, fc.path, fc.status
                 FROM file_curation fc
                 JOIN file_performer fp ON fp.file_curation_id = fc.id
                WHERE fp.performer_id = ?""",
            (p_id,)
        ).fetchall()

        approved = [f for f in files if f['status'] == 'approved']
        renamed  = [f for f in files if f['status'] == 'renamed']

        # Reset approved files: remove performer link, clear proposed_filename, reset to unknown
        for f in approved:
            conn.execute(
                "DELETE FROM file_performer WHERE file_curation_id = ? AND performer_id = ?",
                (f['id'], p_id)
            )
            conn.execute(
                "UPDATE file_curation SET status='unknown', proposed_filename=NULL, "
                "updated_at=datetime('now') WHERE id=?",
                (f['id'],)
            )
            total_approved_reset += 1

        if renamed:
            print(f"  WARNING '{name}': {len(renamed)} file(s) already renamed — manual check:")
            for f in renamed:
                print(f"    {f['path']}")
            total_renamed_skipped += len(renamed)

        # Delete performer (only if no remaining file_performer rows)
        remaining = conn.execute(
            "SELECT COUNT(*) FROM file_performer WHERE performer_id = ?", (p_id,)
        ).fetchone()[0]
        if remaining == 0:
            conn.execute("DELETE FROM performer WHERE id = ?", (p_id,))
            total_deleted += 1
            print(f"  DELETED '{name}' (slug={slug}), reset {len(approved)} approved file(s)")
        else:
            print(f"  KEPT '{name}' — still has {remaining} file_performer rows (renamed files)")

        conn.commit()

    print(f"\nSummary:")
    print(f"  Performers deleted:      {total_deleted}")
    print(f"  Approved files reset:    {total_approved_reset}")
    print(f"  Renamed files (manual):  {total_renamed_skipped}")


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Copy to hpc02 and run**

```bash
scp /tmp/cleanup_garbage_performers.py hpc02:/tmp/cleanup_garbage_performers.py
ssh hpc02 "docker cp /tmp/cleanup_garbage_performers.py video-transcoder:/tmp/cleanup_garbage_performers.py && docker exec video-transcoder python3 /tmp/cleanup_garbage_performers.py"
```

Expected output: lines showing each garbage performer deleted and how many approved files were reset. Any WARNING lines mean files already physically moved — review those paths manually.

- [ ] **Step 3: Verify no garbage performers remain**

```bash
ssh hpc02 "docker exec video-transcoder python3 -c \"
import sqlite3; conn = sqlite3.connect('/data/transcoder.db')
slugs = ['breeding-party','spy-cam','sweet-pleasure','balls-deep','slumber-party-orgy']
for s in slugs:
    r = conn.execute('SELECT id FROM performer WHERE slug=?', (s,)).fetchone()
    print(s, '->', r)
\""
```

Expected: all print `-> None`.

---

## Task 7: Deploy and verify end-to-end

- [ ] **Step 1: Push to trigger deployment**

```bash
git push
```

Wait for GHA build + Watchtower pull (~3–5 min).

- [ ] **Step 2: Verify app starts cleanly**

```bash
ssh hpc02 "docker logs video-transcoder --tail=50"
```

Look for: no import errors, scheduler started, `sched-curation-scan` and `sched-auto-match` log lines appearing after the first 60-second tick.

- [ ] **Step 3: Trigger a manual scan to confirm jdownloader scan works**

```bash
ssh hpc02 "docker exec video-transcoder python3 -c \"
import sqlite3, sys
sys.path.insert(0, '/app')
from app.curation.extractor import scan_mount
conn = sqlite3.connect('/data/transcoder.db')
conn.row_factory = sqlite3.Row
result = scan_mount(conn, '/media/jdownloader', 'jdownloader')
conn.commit()
print(result)
\""
```

Expected: `{'total': N, 'parsed': M, 'opaque': K, 'errors': 0}` — no errors, counts match what's on disk.

- [ ] **Step 4: Trigger a manual auto_match run to confirm phase1/phase2 work**

```bash
ssh hpc02 "docker exec video-transcoder python3 -c \"
import sqlite3, sys
sys.path.insert(0, '/app')
from app.curation.auto_match import run_auto_match
conn = sqlite3.connect('/data/transcoder.db')
conn.row_factory = sqlite3.Row
result = run_auto_match(conn)
conn.commit()
print(result)
\""
```

Expected: dict with phase1/phase2/new_performers counts. No exceptions.

---

## Self-Review

### Spec coverage

| Requirement | Task |
|---|---|
| Garbage performer cleanup | Task 6 |
| Scheduled jdownloader scan | Task 4 |
| Auto-match as continuous scheduled task | Tasks 1–2 + Task 4 |
| Tests for auto_match helpers | Task 3 |
| Raise AUTO_ACCEPT_THRESHOLD | Task 5 |
| Deploy + verify | Task 7 |

### Notes

- Phase 3 (face rec fallback) is already handled by `_run_auto_rename` in main.py — not duplicated here.
- Phase 4 (re-enqueue) is already handled by `enqueue_all_unknown` in `_run_face_enqueue` — not duplicated.
- The `/tmp/cleanup_garbage_performers.py` script is deliberately NOT committed — it's a one-time surgical fix, not a permanent feature.
- `SIMILARITY_HIGH = 0.55` is intentionally left unchanged — it controls UI display colour, not auto-accept behaviour.
