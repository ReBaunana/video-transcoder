"""Thread-safe singleton for InsightFace FaceAnalysis."""

from __future__ import annotations

import logging
import os
import threading

import numpy as np

# Set before any insightface import so its internal cache uses persistent volume.
os.environ.setdefault("INSIGHTFACE_ROOT", "/data/.insightface")

log = logging.getLogger(__name__)

_lock = threading.Lock()
_app = None  # type: ignore[var-annotated]  # FaceAnalysis | None


def get_face_app():
    """Load buffalo_l once, return singleton. Thread-safe (double-checked locking)."""
    global _app
    if _app is None:
        with _lock:
            if _app is None:
                # Import here so module import never fails if insightface is absent.
                from insightface.app import FaceAnalysis

                root = os.environ.get("INSIGHTFACE_ROOT", "/data/.insightface")
                os.makedirs(root, exist_ok=True)

                import onnxruntime as _ort
                _avail = _ort.get_available_providers()
                _use_cuda = "CUDAExecutionProvider" in _avail
                log.info(
                    "onnxruntime providers available: %s — selecting %s",
                    _avail,
                    "CUDA" if _use_cuda else "CPU",
                )

                try:
                    if not _use_cuda:
                        raise RuntimeError("CUDAExecutionProvider not in available providers")
                    fa = FaceAnalysis(
                        name="buffalo_l",
                        root=root,
                        providers=["CUDAExecutionProvider", "CPUExecutionProvider"],
                    )
                    # det_size 640x640 keeps VRAM modest on the 4GB 3050 Ti.
                    fa.prepare(ctx_id=0, det_size=(640, 640))
                    log.info("InsightFace buffalo_l loaded on NVIDIA CUDA (root=%s)", root)
                except Exception:
                    log.warning(
                        "CUDA init failed — falling back to CPU inference. "
                        "Check that cuDNN is installed and onnxruntime-gpu matches CUDA version."
                    )
                    fa = FaceAnalysis(
                        name="buffalo_l",
                        root=root,
                        providers=["CPUExecutionProvider"],
                    )
                    fa.prepare(ctx_id=-1, det_size=(640, 640))
                    log.info("InsightFace buffalo_l loaded on CPU (root=%s)", root)

                _app = fa
    return _app


def reset_face_app() -> None:
    """Drop the singleton so the next call reloads ONNX sessions.

    Used by the worker after N jobs to defragment VRAM.
    """
    global _app
    with _lock:
        old = _app
        _app = None
        if old is not None:
            try:
                # Best-effort cleanup; insightface exposes no public close().
                del old
            except Exception:
                log.debug("reset_face_app: cleanup raised", exc_info=True)
        log.info("InsightFace singleton reset")


def is_face_rec_available() -> bool:
    """Return True if insightface + onnxruntime are importable (graceful degradation)."""
    try:
        import insightface  # noqa: F401
        import onnxruntime  # noqa: F401
        return True
    except ImportError:
        return False


def embed_to_blob(embedding: np.ndarray) -> bytes:
    """L2-normalize and serialize a 512-D embedding to a BLOB."""
    arr = np.asarray(embedding, dtype=np.float32).reshape(-1)
    norm = float(np.linalg.norm(arr))
    if norm > 0.0:
        arr = arr / norm
    return arr.astype(np.float32, copy=False).tobytes()


def blob_to_embed(blob: bytes) -> np.ndarray:
    """Deserialize a BLOB back to a normalized float32 array.

    Returns a writable copy (frombuffer alone returns a read-only view).
    """
    return np.frombuffer(blob, dtype=np.float32).copy()
