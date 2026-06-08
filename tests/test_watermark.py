"""Unit tests for app.curation.watermark pure helpers (no ffmpeg/tesseract)."""

from app.curation.watermark import (
    normalize_key,
    extract_identifiers,
    vote_identifiers,
    looks_generic,
    WatermarkResult,
)


def test_looks_generic_matches_camsite_dump_patterns():
    assert looks_generic("Ana-Lingus.1.mp4")
    assert looks_generic("Ana-Lingus.1_19.mp4")
    assert looks_generic("Supermolly777_441.mp4")
    assert looks_generic("HisExoticVixen_184.mp4")
    assert looks_generic("0hbz5cjuhydgauo0djlgs_source.mp4")
    assert looks_generic("eb1040_512_382p.mkv")
    assert looks_generic("video-62.mp4")


def test_looks_generic_rejects_proper_names():
    assert not looks_generic("Brazzers.2024-01-02.Riley-Reid.Some-Title.1080p.mp4")
    assert not looks_generic("Riley-Reid.mp4")
    assert not looks_generic("Alfiecinematic.2026-02-04.Ana-Lingus.Anal-Fuck.mp4")


def test_normalize_key_strips_scheme_www_trailing_slash_and_lowercases():
    assert normalize_key("https://www.OnlyFans.com/Ana_Lingus/") == "onlyfans.com/ana_lingus"
    assert normalize_key("Clynks.me/lingusana") == "clynks.me/lingusana"
    assert normalize_key("@Ana_Lingus11") == "@ana_lingus11"


def test_extract_platform_url():
    ids = extract_identifiers("subscribe at OnlyFans.com/Ana_Lingus today")
    assert "onlyfans.com/ana_lingus" in ids["urls"]


def test_extract_aggregator_url_real_watermark():
    ids = extract_identifiers("VIP Pages:\nclynks.me/lingusana")
    assert ids["urls"] == ["clynks.me/lingusana"]


def test_extract_handles():
    ids = extract_identifiers("@Ana_Lingus11  @lingus.ana")
    assert "@ana_lingus11" in ids["handles"]
    assert "@lingus.ana" in ids["handles"]


def test_extract_ignores_noise():
    ids = extract_identifiers("Tip 23tk for Harder  4K ULTRAHD")
    assert ids["urls"] == [] and ids["handles"] == []


def test_extract_empty():
    assert extract_identifiers("") == {"urls": [], "handles": []}
    assert extract_identifiers(None) == {"urls": [], "handles": []}


def test_vote_requires_min_agreement():
    # url in only 1 of 3 frames, min_agree=2 -> no winner
    res = vote_identifiers(["clynks.me/lingusana", "noise", "noise"], min_agree=2)
    assert res.url is None and not res.found


def test_vote_picks_majority_url_with_confidence():
    frames = ["clynks.me/lingusana", "clynks.me/lingusana", "clynks.me/lingusan", "junk"]
    res = vote_identifiers(frames, min_agree=2)
    assert res.url == "clynks.me/lingusana"
    assert res.confidence == 2 / 4
    assert res.found and res.key == "clynks.me/lingusana"


def test_vote_url_beats_handle_for_key():
    frames = [
        "onlyfans.com/ana_lingus @ana_handle",
        "onlyfans.com/ana_lingus @ana_handle",
    ]
    res = vote_identifiers(frames, min_agree=2)
    assert res.key == "onlyfans.com/ana_lingus"   # url preferred over handle
    assert res.handle == "@ana_handle"            # handle still captured


def test_vote_falls_back_to_handle_when_no_url():
    frames = ["@ana_lingus11", "@ana_lingus11", "noise"]
    res = vote_identifiers(frames, min_agree=2)
    assert res.url is None
    assert res.key == "@ana_lingus11"


def test_watermark_result_defaults():
    r = WatermarkResult()
    assert not r.found and r.key is None and r.confidence == 0.0
