# Auto-Match Unknown Files Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Write a one-time batch script that assigns performers to `unknown` files using filename parsing (existing + new performers) and face rec fallback, then re-enqueues the remainder for a deeper scan.

**Architecture:** Single `/tmp/auto_match.py` script (not committed). Phase 1: filename × DB performers. Phase 2: extract new performer name from filename. Phase 3: call `accept_match()` for face rec matches ≥0.70. Phase 4: re-enqueue remainder. Run inside the `video-transcoder` container on hpc02.

**Tech Stack:** Python 3.11, SQLite (`/data/transcoder.db`), existing `app.face.matcher.accept_match`.

---

## File Map

| File | Action |
|---|---|
| `/tmp/auto_match.py` (local) | Create — full script |
| `/tmp/test_auto_match.py` (local) | Create — unit tests for pure functions |

Neither file is committed to git. Both are run locally or copied into the container.

---

## Task 1: Helper functions + unit tests

**Files:**
- Create: `/tmp/auto_match.py` (helpers section only)
- Create: `/tmp/test_auto_match.py`

These are pure functions — no DB required. Test them locally with `python3 /tmp/test_auto_match.py`.

- [ ] **Step 1: Write failing tests**

Write `/tmp/test_auto_match.py`:

```python
import sys, os, re
sys.path.insert(0, '/Users/reba/Developer/video-transcoder')

# --- Import helpers (will fail until Step 2) ---
from auto_match import (
    normalize_stem, match_existing_performers,
    extract_new_performer_name, build_proposed_filename_str,
)

failures = []

def check(label, got, expected):
    if got != expected:
        failures.append(f"FAIL {label}: got {got!r}, expected {expected!r}")
    else:
        print(f"  OK  {label}")

# normalize_stem
check("normalize: underscores", normalize_stem("riley_reid_beach_fuck_4k.mp4"), "riley reid beach fuck")
check("normalize: 1080p", normalize_stem("Freya Dee 1080p.mp4"), "freya dee")
check("normalize: dots", normalize_stem("Lola.de.Mons.Full.HD.mp4"), "lola de mons")
check("normalize: date-coded", normalize_stem("22.06-02.10.GP2339_HD.mp4"), "gp")  # mostly stripped
check("normalize: spaces preserved", normalize_stem("anabell evans wants cum.mp4"), "anabell evans wants cum")

# match_existing_performers
performers = [
    (1, "Riley Reid", ["Riley R"]),
    (2, "Lia Lin", []),
    (3, "Stella Cox", ["Stella"]),
]
check("match: exact", match_existing_performers("riley reid beach fuck", performers), [(1, "Riley Reid")])
check("match: alias", match_existing_performers("stella scene", performers), [(3, "Stella Cox")])
check("match: no match", match_existing_performers("random unknown title", performers), [])
check("match: multi", match_existing_performers("riley reid and lia lin scene", performers), [(1, "Riley Reid"), (2, "Lia Lin")])

# extract_new_performer_name
check("extract: first two words", extract_new_performer_name("Freya Dee 1080p.mp4"), "Freya Dee")
check("extract: with dash separator", extract_new_performer_name("Lola de Mons - Lola 28 Years.mp4"), "Lola De Mons")
check("extract: underscore file", extract_new_performer_name("anabell_evans_beach_scene.mp4"), "Anabell Evans")
check("extract: date-coded → None", extract_new_performer_name("22.06-02.10.GP2339_HD.mp4"), None)
check("extract: stopword start → skip to name", extract_new_performer_name("Hot Teen Magda Scene.mp4"), None)  # 'hot', 'teen' are stopwords → only 'Magda' left = 1 word → None
check("extract: studio code → None", extract_new_performer_name("MonstersOfCock.25.05.11.Miami.mp4"), None)
check("extract: three words", extract_new_performer_name("Lola del Rio scene HD.mp4"), "Lola Del Rio")

# build_proposed_filename_str (pure string version, no DB)
check("filename: basic", build_proposed_filename_str(["Riley Reid"], "riley_reid_scene.mp4"), "Riley-Reid.riley_reid_scene.mp4")
check("filename: multi", build_proposed_filename_str(["Riley Reid", "Lia Lin"], "scene.mp4"), "Riley-Reid.Lia-Lin.scene.mp4")
check("filename: unsafe chars", build_proposed_filename_str(["Test"], "file:name<bad>.mp4"), "Test.file_name_bad_.mp4")
check("filename: truncate long", build_proposed_filename_str(["Riley Reid"], "a" * 300 + ".mp4"), "Riley-Reid.mp4")

if failures:
    for f in failures:
        print(f)
    sys.exit(1)
else:
    print("\nAll tests passed.")
```

- [ ] **Step 2: Run tests — confirm they fail**

```bash
cd /tmp && python3 test_auto_match.py
```

Expected: `ModuleNotFoundError: No module named 'auto_match'`

- [ ] **Step 3: Write the helper functions**

Write `/tmp/auto_match.py` (helpers section only — no phases yet):

```python
#!/usr/bin/env python3
"""
Auto-match unknown file_curation rows to performers.
Run inside the video-transcoder container: python3 /tmp/auto_match.py
"""
import os, re, sqlite3, sys, time

DB_PATH = '/data/transcoder.db'
MAX_FILENAME = 255

# ---------------------------------------------------------------------------
# Regex constants
# ---------------------------------------------------------------------------

QUALITY_RE = re.compile(
    r'\b(1080p?|2160p?|720p?|480p?|4k|uhd|full[\s_]?hd|hd|hdrip|'
    r'webrip|web[-_]?dl|blu[-_]?ray|x264|x265|hevc|avc|xvid|divx|'
    r'xxx|[a-z]+rip)\b',
    re.IGNORECASE,
)
DATE_RE = re.compile(
    r'\b\d{4}[-./]\d{2}[-./]\d{2}\b'
    r'|\b\d{2}[-./]\d{2}[-./]\d{2}\b'
    r'|\b\d{4}\b'
)
STOPWORDS = frozenset({
    'hot', 'slutty', 'slut', 'teen', 'big', 'young', 'new', 'sexy', 'hard',
    'full', 'scene', 'episode', 'ep', 'part', 'pt', 'vol', 'video', 'movie',
    'clip', 'hd', 'sd', 'xxx', 'cum', 'fuck', 'fucking', 'sex', 'anal',
    'oral', 'dp', 'bbc', 'milf', 'mature', 'amateur', 'casting', 'outdoor',
    'czech', 'german', 'french', 'italian', 'spanish', 'russian', 'backstage',
    'behind', 'bonus', 'extra', 'compilation', 'best', 'trailer', 'preview',
    'the', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'of', 'and', 'or',
    'with', 'her', 'his', 'is', 'are', 'was', 'wants', 'works', 'loves',
    'gets', 'takes', 'gives', 'lets', 'makes', 'from', 'she', 'he', 'they',
    'my', 'your', 'our', 'this', 'that', 'its', 'creampie', 'blowjob',
    'handjob', 'threesome', 'gangbang', 'hardcore', 'softcore', 'lesbian',
    'solo', 'squirt', 'squirting', 'facial', 'swallow', 'interracial', 'pov',
    'fetish', 'busty', 'gets', 'naughty', 'dirty', 'nubile', 'first', 'time',
})

# ---------------------------------------------------------------------------
# Pure helper functions
# ---------------------------------------------------------------------------

def normalize_stem(path: str) -> str:
    """Lowercase stem with underscores, dots, quality markers and dates removed."""
    stem = os.path.splitext(os.path.basename(path))[0]
    stem = stem.replace('_', ' ').replace('.', ' ')
    stem = QUALITY_RE.sub(' ', stem)
    stem = DATE_RE.sub(' ', stem)
    stem = re.sub(r'[^\w\s]', ' ', stem)
    return re.sub(r'\s+', ' ', stem).strip().lower()


def _contiguous(haystack: list, needle: list) -> bool:
    n = len(needle)
    if n == 0:
        return False
    for i in range(len(haystack) - n + 1):
        if haystack[i:i + n] == needle:
            return True
    return False


def match_existing_performers(norm_stem: str, performers: list) -> list:
    """Return list of (performer_id, canonical_name) found in norm_stem.

    performers: list of (id, canonical_name, [alias, ...])
    Matches are contiguous token sequences; order preserves first appearance.
    """
    stem_tokens = norm_stem.split()
    matches, seen = [], set()
    for perf_id, canon, aliases in performers:
        for name in [canon] + list(aliases):
            needle = normalize_stem(name + '.mp4').split()
            if needle and _contiguous(stem_tokens, needle):
                if perf_id not in seen:
                    matches.append((perf_id, canon))
                    seen.add(perf_id)
                break
    return matches


def extract_new_performer_name(path: str) -> str | None:
    """Heuristically extract a performer name from filename. Returns None if unsure."""
    stem = os.path.splitext(os.path.basename(path))[0]
    stem = stem.replace('_', ' ').replace('.', ' ')
    # Take everything before the first ' - ' separator
    first_part = re.split(r'\s+-\s+|\s+–\s+', stem)[0].strip()
    first_part = QUALITY_RE.sub('', first_part)
    first_part = DATE_RE.sub('', first_part)
    first_part = re.sub(r'[^\w\s]', ' ', first_part)
    first_part = re.sub(r'\s+', ' ', first_part).strip()

    # Reject date-coded or studio-code filenames
    if re.match(r'^\d', first_part):
        return None

    tokens = first_part.split()
    name_tokens = []
    for tok in tokens:
        t = tok.lower()
        if t in STOPWORDS or not re.match(r'^[a-zA-Z]', t):
            if name_tokens:
                break
            continue
        name_tokens.append(tok)
        if len(name_tokens) >= 3:
            break

    if len(name_tokens) < 2:
        return None

    candidate = ' '.join(name_tokens)
    if not (4 <= len(candidate) <= 40):
        return None
    return candidate.title()


def slugify(name: str) -> str:
    s = name.lower()
    s = re.sub(r"[^\w\s-]", '', s)
    s = re.sub(r'[\s_]+', '-', s)
    return s.strip('-')


def build_proposed_filename_str(performer_names: list, path: str) -> str:
    """Build 'Performer-Name.safe_stem.ext'. Pure — no DB, no collision check."""
    ext = os.path.splitext(path)[1].lower() or '.mp4'
    stem = os.path.splitext(os.path.basename(path))[0]
    safe_stem = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', stem).strip('._')
    safe_stem = re.sub(r'_{2,}', '_', safe_stem)
    performer_part = '.'.join(n.replace(' ', '-') for n in performer_names)
    full = f"{performer_part}.{safe_stem}{ext}"
    return full if len(full) <= MAX_FILENAME else f"{performer_part}{ext}"


def build_proposed_filename(conn: sqlite3.Connection, fc_id: int,
                             performer_names: list, path: str) -> str:
    """build_proposed_filename_str + collision avoidance against DB."""
    base = build_proposed_filename_str(performer_names, path)
    base_stem, base_ext = os.path.splitext(base)
    proposed, counter = base, 2
    while True:
        row = conn.execute(
            "SELECT id FROM file_curation WHERE proposed_filename = ? AND id != ?",
            (proposed, fc_id),
        ).fetchone()
        if not row:
            return proposed
        proposed = f"{base_stem}_{counter}{base_ext}"
        counter += 1
```

- [ ] **Step 4: Run tests — confirm they pass**

```bash
cd /tmp && python3 test_auto_match.py
```

Expected:
```
  OK  normalize: underscores
  OK  normalize: 1080p
  ...
All tests passed.
```

If any test fails, fix the helper function until all pass.

---

## Task 2: Phases 1 & 2 — filename matching

**Files:**
- Modify: `/tmp/auto_match.py` (append phases 1 and 2)

- [ ] **Step 1: Write failing test for phase functions**

Append to `/tmp/test_auto_match.py`:

```python
# --- Phase function smoke tests (need helpers, no DB) ---
from auto_match import extract_new_performer_name

# Verify extract handles real filenames from the backlog
real_cases = [
    ("Freya Dee 1080p.mp4", "Freya Dee"),
    ("anabell_evans_wants_cum_on_her_big_natural_tits.mp4", "Anabell Evans"),
    ("Isa_Bella_Backstage.mp4", "Isa Bella"),
    ("Lola de Mons - Lola 28 Years Old.mp4", "Lola De Mons"),
    ("3_Cocks_Dream.mp4", None),
    ("22.06-02.10.GP2339_HD.mp4", None),
    ("MonstersOfCock.25.05.11.Miami.mp4", None),
    ("Slutty_Teen_Magda_Works_Her_Mouth.mp4", None),
]
for fname, expected in real_cases:
    result = extract_new_performer_name(fname)
    check(f"extract real: {fname[:40]}", result, expected)

if failures:
    for f in failures:
        print(f)
    sys.exit(1)
else:
    print("\nAll tests passed.")
```

Run:
```bash
cd /tmp && python3 test_auto_match.py
```

Expected: `All tests passed.` (or fix `extract_new_performer_name` until it does)

- [ ] **Step 2: Append `phase1()` and `phase2()` to `/tmp/auto_match.py`**

```python
# ---------------------------------------------------------------------------
# Phase 1: filename × existing performers
# ---------------------------------------------------------------------------

def phase1(conn: sqlite3.Connection) -> int:
    # Load all performers + aliases once
    performers = []
    for row in conn.execute("SELECT id, canonical_name FROM performer ORDER BY id"):
        aliases = [r[0] for r in conn.execute(
            "SELECT alias FROM performer_alias WHERE performer_id = ?", (row[0],)
        )]
        performers.append((row[0], row[1], aliases))

    matched = 0
    now = int(time.time())
    for fc_id, path in conn.execute(
        "SELECT id, path FROM file_curation WHERE status = 'unknown'"
    ).fetchall():
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

    conn.commit()
    return matched


# ---------------------------------------------------------------------------
# Phase 2: extract new performer from filename
# ---------------------------------------------------------------------------

def phase2(conn: sqlite3.Connection) -> tuple:
    created_names, matched = [], 0
    for fc_id, path in conn.execute(
        "SELECT id, path FROM file_curation WHERE status = 'unknown'"
    ).fetchall():
        name = extract_new_performer_name(path)
        if not name:
            continue
        slug = slugify(name)
        row = conn.execute("SELECT id FROM performer WHERE slug = ?", (slug,)).fetchone()
        if row:
            p_id = row[0]
        else:
            conn.execute(
                "INSERT INTO performer (canonical_name, slug, gender) "
                "VALUES (?, ?, 'unknown')",
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
    conn.commit()
    return matched, created_names
```

- [ ] **Step 3: Dry-run Phase 1 + 2 against container DB (read-only check)**

Write `/tmp/dryrun.py`:

```python
import sys
sys.path.insert(0, '/tmp')
import sqlite3, auto_match

conn = sqlite3.connect('/data/transcoder.db')

# Count performers in DB
n_performers = conn.execute("SELECT COUNT(*) FROM performer").fetchone()[0]
# Count aliases
n_aliases = conn.execute("SELECT COUNT(*) FROM performer_alias").fetchone()[0]
# Count unknowns
n_unknown = conn.execute("SELECT COUNT(*) FROM file_curation WHERE status='unknown'").fetchone()[0]

print(f"Performers in DB: {n_performers}")
print(f"Aliases in DB: {n_aliases}")
print(f"Unknown files: {n_unknown}")

# Preview Phase 1 matches (no writes)
performers = []
for row in conn.execute("SELECT id, canonical_name FROM performer"):
    aliases = [r[0] for r in conn.execute(
        "SELECT alias FROM performer_alias WHERE performer_id = ?", (row[0],)
    )]
    performers.append((row[0], row[1], aliases))

p1_matches = 0
for fc_id, path in conn.execute(
    "SELECT id, path FROM file_curation WHERE status = 'unknown'"
).fetchall():
    norm = auto_match.normalize_stem(path)
    hits = auto_match.match_existing_performers(norm, performers)
    if hits:
        print(f"  P1 match: {path.split('/')[-1][:50]} → {[h[1] for h in hits]}")
        p1_matches += 1

print(f"\nPhase 1 would match: {p1_matches} files")

# Preview Phase 2 extractions
p2_extract = 0
for fc_id, path in conn.execute(
    "SELECT id, path FROM file_curation WHERE status = 'unknown'"
).fetchall():
    # Skip files already matched by phase 1
    norm = auto_match.normalize_stem(path)
    if auto_match.match_existing_performers(norm, performers):
        continue
    name = auto_match.extract_new_performer_name(path)
    if name:
        print(f"  P2 new: {path.split('/')[-1][:40]} → '{name}'")
        p2_extract += 1

print(f"Phase 2 would create/match: {p2_extract} files")
conn.close()
```

Copy to container and run (read-only, no writes):

```bash
scp /tmp/auto_match.py /tmp/dryrun.py hpc02:/tmp/
ssh hpc02 "sudo docker cp /tmp/auto_match.py video-transcoder:/tmp/ && \
           sudo docker cp /tmp/dryrun.py video-transcoder:/tmp/ && \
           sudo docker exec video-transcoder python3 /tmp/dryrun.py"
```

Review the output carefully — spot-check 5–10 Phase 2 "new performer" matches to make sure the extracted names look correct (not garbage). If many extractions look wrong, adjust `STOPWORDS` or `extract_new_performer_name` before proceeding.

---

## Task 3: Phases 3 & 4 + main(), then run

**Files:**
- Modify: `/tmp/auto_match.py` (append phases 3, 4, main)

- [ ] **Step 1: Append `phase3()` and `phase4()` to `/tmp/auto_match.py`**

```python
# ---------------------------------------------------------------------------
# Phase 3: face rec fallback ≥ 0.70 — call accept_match() directly
# ---------------------------------------------------------------------------

def phase3(conn: sqlite3.Connection, threshold: float = 0.70) -> int:
    sys.path.insert(0, '/app')
    from app.face.matcher import accept_match

    # Best pending match ≥ threshold per file, rank=1 only
    rows = conn.execute("""
        SELECT fmr.id, fmr.file_curation_id, fmr.similarity
        FROM face_match_result fmr
        JOIN file_curation fc ON fc.id = fmr.file_curation_id
        WHERE fc.status = 'unknown'
          AND fmr.status = 'pending'
          AND fmr.similarity >= ?
          AND fmr.rank = 1
        ORDER BY fmr.similarity DESC
    """, (threshold,)).fetchall()

    seen_fc, matched = set(), 0
    for match_id, fc_id, sim in rows:
        if fc_id in seen_fc:
            continue
        seen_fc.add(fc_id)
        try:
            accept_match(conn, match_id)
            matched += 1
        except Exception as e:
            print(f"  [phase3] accept_match failed fc_id={fc_id}: {e}")
    return matched


# ---------------------------------------------------------------------------
# Phase 4: re-enqueue remaining unknowns for deeper face rec
# ---------------------------------------------------------------------------

def phase4(conn: sqlite3.Connection) -> int:
    unknown_ids = [r[0] for r in conn.execute(
        "SELECT id FROM file_curation WHERE status = 'unknown'"
    )]
    requeued = 0
    for fc_id in unknown_ids:
        existing = conn.execute(
            "SELECT id FROM face_recognition_job WHERE file_curation_id = ?", (fc_id,)
        ).fetchone()
        if existing:
            conn.execute(
                "UPDATE face_recognition_job "
                "SET status='queued', attempts=0, last_error=NULL, "
                "    priority=50, enqueued_at=datetime('now') "
                "WHERE file_curation_id=?",
                (fc_id,),
            )
        else:
            conn.execute(
                "INSERT INTO face_recognition_job "
                "(file_curation_id, job_type, status, priority, enqueued_at) "
                "VALUES (?, 'match_unknown', 'queued', 50, datetime('now'))",
                (fc_id,),
            )
        requeued += 1
    conn.commit()
    return requeued


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    conn = sqlite3.connect(DB_PATH)

    n_before = conn.execute(
        "SELECT COUNT(*) FROM file_curation WHERE status='unknown'"
    ).fetchone()[0]
    print(f"Unknown files before: {n_before}\n")

    print("Phase 1: filename × existing performers...")
    n1 = phase1(conn)
    print(f"  → {n1} files matched\n")

    print("Phase 2: extract new performer from filename...")
    n2, new_names = phase2(conn)
    print(f"  → {n2} files matched, {len(new_names)} new performers created")
    for name in sorted(new_names):
        print(f"     + {name}")
    print()

    print("Phase 3: face rec fallback ≥70%...")
    n3 = phase3(conn)
    print(f"  → {n3} files matched\n")

    print("Phase 4: re-enqueue remainder for deeper scan...")
    n4 = phase4(conn)
    print(f"  → {n4} files re-queued\n")

    n_after = conn.execute(
        "SELECT COUNT(*) FROM file_curation WHERE status='unknown'"
    ).fetchone()[0]
    print(f"{'─'*50}")
    print(f"Matched total:        {n1+n2+n3}")
    print(f"Re-queued:            {n4}")
    print(f"Unknown before/after: {n_before} → {n_after}")
    conn.close()


if __name__ == '__main__':
    main()
```

- [ ] **Step 2: Copy updated script to container and run**

```bash
scp /tmp/auto_match.py hpc02:/tmp/
ssh hpc02 "sudo docker cp /tmp/auto_match.py video-transcoder:/tmp/ && \
           sudo docker exec video-transcoder python3 /tmp/auto_match.py"
```

Expected output (approximate numbers):
```
Unknown files before: 1140

Phase 1: filename × existing performers...
  → N files matched

Phase 2: extract new performer from filename...
  → N files matched, N new performers created
     + Anabell Evans
     + Freya Dee
     ...

Phase 3: face rec fallback ≥70%...
  → N files matched

Phase 4: re-enqueue remainder for deeper scan...
  → N files re-queued

──────────────────────────────────────────────────
Matched total:        N
Re-queued:            N
Unknown before/after: 1140 → N
```

- [ ] **Step 3: Verify results in DB**

```bash
ssh hpc02 "sudo docker exec video-transcoder python3 -c \"
import sqlite3
conn = sqlite3.connect('/data/transcoder.db')
print('Status breakdown:')
for r in conn.execute('SELECT status, mount, COUNT(*) FROM file_curation GROUP BY status, mount ORDER BY status, mount'):
    print(f'  {r[0]:20} {r[1]:15} {r[2]}')
print()
print('New performers (source=filename):')
for r in conn.execute(\\\"SELECT DISTINCT p.canonical_name FROM file_performer fp JOIN performer p ON p.id = fp.performer_id WHERE fp.source = \\\\\\\"filename\\\\\\\" ORDER BY p.canonical_name\\\"):
    print(f'  {r[0]}')
\""
```

Spot-check: confirm approved files have non-null `proposed_filename` and that new performer names look like real people (not garbage extractions).

- [ ] **Step 4: Wait for auto-rename scheduler**

The rename scheduler runs every 30 minutes automatically. After it fires, verify files have been renamed and moved to performer folders:

```bash
ssh hpc02 "sudo docker exec video-transcoder python3 -c \"
import sqlite3
conn = sqlite3.connect('/data/transcoder.db')
for r in conn.execute(\\\"SELECT status, COUNT(*) FROM file_curation GROUP BY status\\\"):
    print(r[0], r[1])
\""
```

Expected: `approved` count drops to near 0 as the scheduler renames them to `renamed`.
