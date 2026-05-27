"""Frame extraction, face detection, and embedding storage."""

from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import cv2
import numpy as np

from app.face.model import embed_to_blob, get_face_app

log = logging.getLogger(__name__)

THUMB_DIR = Path("/data/face_thumbs")
THUMB_SIZE = (160, 160)

# Detection / quality thresholds.
MIN_DET_SCORE = 0.6
MIN_FACE_PIXELS = 64
MAX_FACES_PER_FRAME = 6  # drop crowd scenes entirely

# Seeding policy.
QUALITY_KEEP_THRESHOLD = 0.3
DEDUP_COSINE_THRESHOLD = 0.92
MAX_EMBEDDINGS_PER_FILE = 15
REFERENCE_READY_MIN = 8

FFMPEG_BIN = os.environ.get("FFMPEG_BIN", "ffmpeg")
FFMPEG_TIMEOUT_SEC = 120  # whole-window batch extract; was 20s per single frame

# Codecs that can be decoded by NVIDIA NVDEC (CUVID). Using hardware decode
# for frame extraction offloads bulk CPU decode work to the GPU decoder,
# dropping ffmpeg CPU usage from ~580% to ~30% per worker.
_CUVID_MAP: dict[str, str] = {
    "h264":       "h264_cuvid",
    "hevc":       "hevc_cuvid",
    "mpeg2video": "mpeg2_cuvid",
    "vp8":        "vp8_cuvid",
    "vp9":        "vp9_cuvid",
    "av1":        "av1_cuvid",
}

# Serialize GPU access across parallel worker threads.
# InsightFace's app.get() is NOT safe to call from multiple threads on the
# same ONNX session — concurrent CUDA inference corrupts state. CPU detection
# is also not guaranteed thread-safe inside FaceAnalysis' Python wrappers.
_gpu_lock = threading.Lock()


def ensure_thumb_dir() -> None:
    """Create /data/face_thumbs/ with mode 0o755 if missing."""
    THUMB_DIR.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(THUMB_DIR, 0o755)
    except PermissionError:
        log.debug("ensure_thumb_dir: chmod skipped (not owner)")


_WINDOW_POSITIONS = (  # 20 windows, evenly spaced 3%–97%
    0.03, 0.08, 0.13, 0.18, 0.23, 0.28, 0.33, 0.38, 0.43, 0.47,
    0.52, 0.57, 0.62, 0.67, 0.72, 0.77, 0.82, 0.87, 0.92, 0.97,
)
_WINDOW_SEC = 30.0        # length of each window — keeps NFS I/O bounded
_FRAMES_PER_WINDOW = 4    # frames per window — was 10; reduced to cut serialized GPU calls by 60%


def _sample_windows(duration_sec: float) -> list[tuple[float, float]]:
    """Return (start_t, end_t) for up to 20 windows spread across the video.

    Each window is 30 seconds long.  20 windows at evenly spaced positions
    (3%–97%) give dense temporal coverage: 200 frames total.
    """
    if duration_sec <= 10.0:
        return []
    out = []
    for pos in _WINDOW_POSITIONS:
        start = min(duration_sec * pos, 30.0) if pos < 0.1 else duration_sec * pos
        end = min(start + _WINDOW_SEC, duration_sec * 0.98)
        if end > start + 1.0:
            out.append((start, end))
    return out


def _sample_window(duration_sec: float) -> tuple[float, float, int]:
    """Kept for test/compat use. Returns first window from _sample_windows."""
    wins = _sample_windows(duration_sec)
    if not wins:
        return (0.0, 0.0, 0)
    s, e = wins[0]
    return (s, e, _FRAMES_PER_WINDOW)


def _sample_timestamps(duration_sec: float) -> list[float]:
    """Uniform timestamps from the first sample window. Kept for tests."""
    start, end, n = _sample_window(duration_sec)
    if n <= 0:
        return []
    step = (end - start) / max(n - 1, 1)
    return [start + i * step for i in range(n)]


def _probe_video_codec(video_path: str) -> str | None:
    """Return the primary video stream codec name via ffprobe, or None on error."""
    try:
        proc = subprocess.run(
            [
                FFMPEG_BIN.replace("ffmpeg", "ffprobe"),
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-of", "default=noprint_wrappers=1:nokey=1",
                video_path,
            ],
            capture_output=True,
            timeout=10,
            check=False,
        )
        if proc.returncode == 0:
            return proc.stdout.decode(errors="replace").strip().lower() or None
    except Exception:
        pass
    return None


def extract_frames(video_path: str, duration_sec: float) -> list[tuple[float, np.ndarray]]:
    """Extract frames from 8 windows spread across the full video duration.

    Each window is 30 seconds long (10 frames, one every 3s).  Eight windows
    at evenly spaced positions (5%–92%) give dense temporal coverage —
    80 frames total, ~54s over Gigabit NFS for a 2 GB 1080p HEVC file.

    Uses CUVID hardware decode when the codec is supported; falls back to
    software decode (-threads 2) per window on failure.

    Returns list of (timestamp_sec, BGR image). Empty list on any error.
    """
    windows = _sample_windows(duration_sec)
    if not windows:
        return []

    if not Path(video_path).exists():
        log.warning("extract_frames: video missing path=%s", video_path)
        return []

    codec = _probe_video_codec(video_path)
    cuvid_decoder = _CUVID_MAP.get(codec or "")
    log.debug("extract_frames: codec=%s cuvid=%s windows=%d path=%s",
              codec, cuvid_decoder, len(windows), video_path)

    all_frames: list[tuple[float, np.ndarray]] = []

    for start_t, end_t in windows:
        window = end_t - start_t
        fps = _FRAMES_PER_WINDOW / window
        tmp_dir = Path(tempfile.mkdtemp(prefix="vt_frames_"))
        try:
            def _build_cmd(use_cuvid: bool,
                           _s=start_t, _w=window, _f=fps, _d=tmp_dir) -> list[str]:
                cmd = [FFMPEG_BIN, "-nostdin", "-loglevel", "error"]
                if use_cuvid and cuvid_decoder:
                    cmd += ["-hwaccel", "cuda", "-c:v", cuvid_decoder]
                else:
                    cmd += ["-threads", "2"]
                cmd += [
                    "-ss", f"{_s:.3f}",
                    "-i", video_path,
                    "-t", f"{_w:.3f}",
                    "-vf", f"fps={_f:.6f},scale='min(1280,iw)':-2",
                    "-vsync", "vfr", "-q:v", "3", "-y",
                    str(_d / "frame_%05d.jpg"),
                ]
                return cmd

            proc = None
            try:
                proc = subprocess.run(
                    _build_cmd(use_cuvid=bool(cuvid_decoder)),
                    capture_output=True, timeout=FFMPEG_TIMEOUT_SEC, check=False,
                )
            except subprocess.TimeoutExpired:
                log.warning("extract_frames: CUVID timeout ss=%.0f path=%s", start_t, video_path)
            except FileNotFoundError:
                log.error("extract_frames: ffmpeg binary not found (%s)", FFMPEG_BIN)
                return []

            # CUVID failed with no output — retry with software decode.
            if proc is None or (
                proc.returncode != 0 and cuvid_decoder
                and not list(tmp_dir.glob("frame_*.jpg"))
            ):
                if proc is not None:
                    log.warning("extract_frames: CUVID failed (rc=%d) ss=%.0f — CPU retry",
                                proc.returncode, start_t)
                try:
                    proc = subprocess.run(
                        _build_cmd(use_cuvid=False),
                        capture_output=True, timeout=FFMPEG_TIMEOUT_SEC, check=False,
                    )
                except subprocess.TimeoutExpired:
                    log.warning("extract_frames: CPU timeout ss=%.0f path=%s", start_t, video_path)
                    continue  # skip this window, try the next

            if proc is not None and proc.returncode != 0:
                log.debug("extract_frames: ffmpeg rc=%d ss=%.0f stderr=%s",
                          proc.returncode, start_t,
                          proc.stderr[:200] if proc.stderr else b"")

            step = window / _FRAMES_PER_WINDOW
            for i, fp in enumerate(sorted(tmp_dir.glob("frame_*.jpg"))):
                img = cv2.imread(str(fp), cv2.IMREAD_COLOR)
                if img is None:
                    continue
                all_frames.append((start_t + i * step, img))

        except Exception:
            log.exception("extract_frames: unexpected failure ss=%.0f path=%s", start_t, video_path)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return all_frames


def detect_faces(img: np.ndarray) -> list[dict]:
    """Run InsightFace detection on one image.

    Returns list of {embedding, det_score, bbox, normed_embedding}.
    Returns [] for crowd scenes (>MAX_FACES_PER_FRAME).
    """
    try:
        app = get_face_app()
    except Exception:
        log.exception("detect_faces: face app unavailable")
        return []

    try:
        # Serialize ONNX session calls across worker threads — concurrent
        # CUDA inference on the same session corrupts state.
        with _gpu_lock:
            faces = app.get(img)
    except Exception:
        log.exception("detect_faces: insightface .get() failed")
        return []

    if not faces:
        return []
    if len(faces) > MAX_FACES_PER_FRAME:
        return []  # crowd scene — skip entirely

    results: list[dict] = []
    for f in faces:
        det_score = float(getattr(f, "det_score", 0.0))
        if det_score < MIN_DET_SCORE:
            continue

        bbox_raw = getattr(f, "bbox", None)
        if bbox_raw is None:
            continue
        bbox = [float(v) for v in np.asarray(bbox_raw).reshape(-1)[:4]]
        x1, y1, x2, y2 = bbox
        w, h = max(0.0, x2 - x1), max(0.0, y2 - y1)
        if w < MIN_FACE_PIXELS or h < MIN_FACE_PIXELS:
            continue

        emb = getattr(f, "embedding", None)
        if emb is None:
            continue
        emb = np.asarray(emb, dtype=np.float32).reshape(-1)

        normed = getattr(f, "normed_embedding", None)
        if normed is None:
            n = float(np.linalg.norm(emb))
            normed = (emb / n) if n > 0 else emb
        normed = np.asarray(normed, dtype=np.float32).reshape(-1)

        results.append({
            "embedding": emb,
            "normed_embedding": normed,
            "det_score": det_score,
            "bbox": bbox,
        })

    return results


def compute_quality_score(img: np.ndarray, bbox: list) -> float:
    """Blur (Laplacian variance) + bbox area score, in [0, 1]."""
    try:
        x1, y1, x2, y2 = [int(round(v)) for v in bbox[:4]]
        h_img, w_img = img.shape[:2]
        x1 = max(0, min(w_img - 1, x1))
        y1 = max(0, min(h_img - 1, y1))
        x2 = max(0, min(w_img, x2))
        y2 = max(0, min(h_img, y2))
        if x2 - x1 < 8 or y2 - y1 < 8:
            return 0.0

        crop = img[y1:y2, x1:x2]
        gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
        lap_var = float(cv2.Laplacian(gray, cv2.CV_64F).var())
        # 200 var ~= sharp; clamp.
        blur_score = max(0.0, min(1.0, lap_var / 200.0))

        face_area = (x2 - x1) * (y2 - y1)
        # 320x320 face = full score.
        area_score = max(0.0, min(1.0, face_area / (320.0 * 320.0)))

        return float(0.6 * blur_score + 0.4 * area_score)
    except Exception:
        log.debug("compute_quality_score failed", exc_info=True)
        return 0.0


def save_face_thumbnail(img: np.ndarray, bbox: list, embedding_id: int) -> str:
    """Crop face with 20% padding, resize to 160x160, save JPEG.

    Returns absolute path string.
    """
    ensure_thumb_dir()
    x1, y1, x2, y2 = [float(v) for v in bbox[:4]]
    w, h = x2 - x1, y2 - y1
    pad_x, pad_y = w * 0.20, h * 0.20

    h_img, w_img = img.shape[:2]
    x1p = int(max(0, round(x1 - pad_x)))
    y1p = int(max(0, round(y1 - pad_y)))
    x2p = int(min(w_img, round(x2 + pad_x)))
    y2p = int(min(h_img, round(y2 + pad_y)))

    if x2p - x1p < 8 or y2p - y1p < 8:
        raise ValueError(f"save_face_thumbnail: bbox too small after padding: {bbox}")

    crop = img[y1p:y2p, x1p:x2p]
    resized = cv2.resize(crop, THUMB_SIZE, interpolation=cv2.INTER_AREA)

    out_path = THUMB_DIR / f"{embedding_id}.jpg"
    ok = cv2.imwrite(str(out_path), resized, [int(cv2.IMWRITE_JPEG_QUALITY), 90])
    if not ok:
        raise IOError(f"cv2.imwrite failed for {out_path}")
    return str(out_path)


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    return float(np.dot(a, b))  # both already L2-normalized


def process_video_for_seeding(
    conn: sqlite3.Connection,
    file_curation_id: int,
    performer_id: int,
    video_path: str,
    duration: float,
) -> int:
    """Extract faces from a single-performer video and store embeddings.

    Returns number of embeddings stored.
    """
    ensure_thumb_dir()

    frames = extract_frames(video_path, duration)
    if not frames:
        log.info("seeding: no frames extracted file_id=%s", file_curation_id)
        return 0

    # Step 1+2: detect; keep only frames with exactly 1 face.
    candidates: list[dict] = []
    for t, img in frames:
        faces = detect_faces(img)
        if len(faces) != 1:
            continue
        f = faces[0]
        q = compute_quality_score(img, f["bbox"])
        if q <= QUALITY_KEEP_THRESHOLD:
            continue
        candidates.append({
            "frame_time_sec": t,
            "img": img,
            "embedding": f["embedding"],
            "normed_embedding": f["normed_embedding"],
            "det_score": f["det_score"],
            "bbox": f["bbox"],
            "quality_score": q,
        })

    if not candidates:
        log.info("seeding: no qualifying single-face frames file_id=%s", file_curation_id)
        return 0

    # Step 4: dedupe — keep higher quality of any pair with cosine > threshold.
    candidates.sort(key=lambda c: c["quality_score"], reverse=True)
    kept: list[dict] = []
    for c in candidates:
        dup = False
        for k in kept:
            if _cosine(c["normed_embedding"], k["normed_embedding"]) > DEDUP_COSINE_THRESHOLD:
                dup = True
                break
        if not dup:
            kept.append(c)
        if len(kept) >= MAX_EMBEDDINGS_PER_FILE:
            break

    # Step 6: insert.
    stored = 0
    cur = conn.cursor()
    try:
        for c in kept:
            blob = embed_to_blob(c["normed_embedding"])
            bbox_json = json.dumps([round(v, 2) for v in c["bbox"]])
            cur.execute(
                """
                INSERT INTO face_embedding
                    (performer_id, file_curation_id, source, embedding,
                     det_score, bbox, frame_time_sec, thumbnail_path, quality_score, created_at)
                VALUES (?, ?, 'video_frame', ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    performer_id,
                    file_curation_id,
                    blob,
                    c["det_score"],
                    bbox_json,
                    c["frame_time_sec"],
                    None,
                    c["quality_score"],
                    int(time.time()),
                ),
            )
            emb_id = cur.lastrowid
            try:
                thumb_path = save_face_thumbnail(c["img"], c["bbox"], emb_id)
                cur.execute(
                    "UPDATE face_embedding SET thumbnail_path = ? WHERE id = ?",
                    (thumb_path, emb_id),
                )
            except Exception:
                log.exception("seeding: thumbnail save failed emb_id=%s", emb_id)
            stored += 1

        # Step 7: update performer stats.
        cur.execute(
            "SELECT COUNT(*) FROM face_embedding WHERE performer_id = ?",
            (performer_id,),
        )
        total = int(cur.fetchone()[0])
        cur.execute(
            """
            UPDATE performer
               SET embedding_count = ?,
                   is_reference_ready = CASE WHEN ? >= ? THEN 1 ELSE is_reference_ready END
             WHERE id = ?
            """,
            (total, total, REFERENCE_READY_MIN, performer_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception(
            "seeding: DB error file_id=%s performer_id=%s",
            file_curation_id, performer_id,
        )
        raise

    log.info(
        "seeding: stored=%d performer_id=%s file_id=%s (total=%d)",
        stored, performer_id, file_curation_id, total,
    )
    return stored


def process_video_for_matching(
    conn: sqlite3.Connection,  # noqa: ARG001 — kept for symmetry / future use
    file_curation_id: int,
    video_path: str,
    duration: float,
) -> list[dict]:
    """Extract all faces from an unknown video (no DB writes).

    Returns list of dicts: {embedding, det_score, bbox, frame_time_sec, quality_score}.
    """
    frames = extract_frames(video_path, duration)
    if not frames:
        log.info("matching: no frames extracted file_id=%s", file_curation_id)
        return []

    out: list[dict] = []
    for t, img in frames:
        faces = detect_faces(img)
        for f in faces:
            q = compute_quality_score(img, f["bbox"])
            if q <= QUALITY_KEEP_THRESHOLD:
                continue
            out.append({
                "embedding": f["normed_embedding"],
                "det_score": f["det_score"],
                "bbox": f["bbox"],
                "frame_time_sec": t,
                "quality_score": q,
            })

    log.info("matching: extracted %d faces file_id=%s", len(out), file_curation_id)
    return out
