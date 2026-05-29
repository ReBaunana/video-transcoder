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
