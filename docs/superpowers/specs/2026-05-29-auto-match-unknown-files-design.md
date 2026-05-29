# Auto-Match Unknown Files

**Date:** 2026-05-29
**Status:** Approved

## Goal

Process the backlog of `unknown` files (1140 total across ddMovie, intensoP1, intensoP2) by automatically assigning performers using two strategies: filename parsing and face recognition fallback. A one-time batch script run in the container clears the backlog and re-enqueues whatever remains for a deeper face rec pass.

---

## Script

Single file: `/tmp/auto_match.py` — copied into the container and run with `python3`. Not committed to the repo. Writes directly to `/data/transcoder.db`.

---

## Phase 1 — Filename × Existing Performers

For each `unknown` file:

1. Normalize the filename stem:
   - Lowercase
   - Replace `_` with space
   - Strip quality/scene markers: `1080p`, `2160p`, `720p`, `4k`, `hd`, `full hd`, `xxx`, `mp4`, and date patterns (`\d{4}-\d{2}-\d{2}`, `\d{2}\.\d{2}\.\d{2}`, etc.)
2. Load all performers from DB (`canonical_name` + all `performer_alias.alias` values).
3. For each performer, check if their name (normalized to lowercase) is a substring of the normalized stem.
4. If match found: assign performer (insert `file_performer` at position 0, source `'filename'`), build `proposed_filename` (see below), set `status = 'approved'`.

Multiple performers can match a single file (e.g. "riley-reid-mia-malkova.mp4") — insert all at positions 0, 1, 2… sorted by position in stem.

---

## Phase 2 — Extract New Performer from Filename

For files still `unknown` after Phase 1:

1. Normalize stem (same as Phase 1).
2. Split on ` - ` — take the left part as candidate.
3. Tokenize; take the first consecutive run of 1–3 tokens that:
   - Are not in the stopword list (see below)
   - Do not start with a digit
   - Are not all-caps (e.g. `XXX`, `GP2339`)
   - Have length ≥ 2 each
4. If the extracted candidate is 3–40 chars and contains at least one space (i.e. ≥ 2 words) OR is a clearly single-name performer: accept.
5. Title-case the result → `canonical_name`.
6. Slugify: lowercase, spaces → `-`, strip non-alphanumeric except `-`.
7. `INSERT OR IGNORE INTO performer (canonical_name, slug)` — skip if slug already exists (deduplication).
8. Assign performer, build `proposed_filename`, set `status = 'approved'`.

**Stopword list:**
`hot, slutty, teen, big, young, new, sexy, hard, full, scene, episode, part, vol, video, movie, clip, hd, sd, xxx, cum, fuck, sex, anal, oral, dp, bbc, milf, mature, amateur, casting, outdoor, czech, german, french, italian, spanish, russian`

**Reject patterns** (skip Phase 2, leave for Phase 3/4):
- Stem starts with a date (`\d{2}[\.\-]\d{2}`)
- Stem starts with a studio code pattern (e.g. `MonstersOfCock`, `TeamSkeet` — all-one-word CamelCase with no spaces after normalization)
- Candidate is a single token with no clear name shape

---

## Phase 3 — Face Rec Fallback ≥ 70 %

For files still `unknown` that have at least one `face_match_result` with `similarity ≥ 0.70`:

1. Take the result with the highest `similarity` where `status = 'pending'`.
2. Insert `file_performer` (position 0, source `'face'`).
3. Mark `face_match_result.status = 'accepted'`, set `resolved_at = datetime('now')`.
4. Mark all other results for this file as `superseded`.
5. Build `proposed_filename`, set `status = 'approved'`.

---

## Phase 4 — Re-enqueue Remainder

For files still `unknown` after all three phases:

```sql
UPDATE face_recognition_job
SET status = 'queued', attempts = 0, last_error = NULL
WHERE file_curation_id IN (
    SELECT id FROM file_curation WHERE status = 'unknown'
)
```

If no `face_recognition_job` row exists for a file, insert one:
```sql
INSERT OR IGNORE INTO face_recognition_job
    (file_curation_id, job_type, status, priority, enqueued_at)
VALUES (?, 'match_unknown', 'queued', 50, datetime('now'))
```

Priority `50` = deeper scan (lower number = higher priority per existing convention).

---

## proposed_filename Construction

Used in all three phases. For files without TPDB metadata (most unknowns):

```
{Performer-Name}.{sanitized_stem}.{ext}
```

- `Performer-Name`: `canonical_name` with spaces replaced by `-`
- `sanitized_stem`: original stem with unsafe chars (`<>:"/\|?*\x00-\x1f`) replaced by `_`, collapsed, stripped of leading/trailing `._`
- `ext`: lowercase original extension, default `.mp4` if absent
- If total length > 200 chars: truncate to `{Performer-Name}{ext}`
- Collision (file already exists at target path): append `_2`, `_3`, …

For files with multiple performers: `Performer1-Name.Performer2-Name.sanitized_stem.ext`

---

## Output

Script prints a summary:

```
Phase 1 (existing performer, filename):  N files
Phase 2 (new performer, filename):       N files  (M new performers created)
Phase 3 (face rec ≥70%):                N files
Phase 4 (re-enqueued for deeper scan):  N files
Total unknown remaining:                 N
```

Plus a list of new performer names created in Phase 2.

---

## Out of Scope

- TPDB enrichment (the rename scheduler handles that after status = 'approved')
- UI changes
- Collision resolution beyond `_2/_3` suffix
- Handling files where `path` no longer exists on disk (skip silently)
