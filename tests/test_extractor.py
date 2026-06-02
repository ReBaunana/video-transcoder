"""Unit tests for app.curation.extractor file discovery."""

import os

from app.curation.extractor import _iter_video_files


def test_iter_video_files_skips_transcoding_temp_files(tmp_path):
    """In-progress transcode temps ("<stem>.transcoding<ext>") must not be
    discovered as real videos — otherwise a scan running mid-encode persists
    phantom file_curation rows that can never be renamed."""
    (tmp_path / "real.mp4").write_bytes(b"x")
    (tmp_path / "clip.transcoding.mp4").write_bytes(b"x")   # active temp
    (tmp_path / "notes.txt").write_bytes(b"x")              # non-video

    found = {os.path.basename(p) for p in _iter_video_files(str(tmp_path))}

    assert found == {"real.mp4"}


def test_iter_video_files_finds_normal_videos(tmp_path):
    (tmp_path / "a.mp4").write_bytes(b"x")
    sub = tmp_path / "sub"
    sub.mkdir()
    (sub / "b.mkv").write_bytes(b"x")

    found = {os.path.basename(p) for p in _iter_video_files(str(tmp_path))}

    assert found == {"a.mp4", "b.mkv"}
