"""Unit tests for the batched face-extraction orchestration (no GPU/insightface).

Exercises filtering (crowd / det_score / size) and the batch-embedding
distribution in _extract_faces_core with fake det/rec/align.
"""
import numpy as np

from app.face import extractor as ex


class FakeDet:
    """det.detect(img, ...) -> (bboxes[N,5], kpss[N,5,2]). Driven by img[0,0,0]."""
    def __init__(self, frames_dets):
        self.frames_dets = frames_dets  # key -> list of (bbox4, score)

    def detect(self, img, max_num=0, metric="default"):
        key = int(img[0, 0, 0])
        dets = self.frames_dets.get(key, [])
        if not dets:
            return np.zeros((0, 5), dtype=np.float32), np.zeros((0, 5, 2), dtype=np.float32)
        bboxes = np.array([[*b, s] for (b, s) in dets], dtype=np.float32)
        kpss = np.zeros((len(dets), 5, 2), dtype=np.float32)
        return bboxes, kpss


class FakeRec:
    """get_feat(list_of_crops) -> (M,512); each row encodes the crop's tag."""
    def __init__(self):
        self.calls = []

    def get_feat(self, crops):
        self.calls.append(len(crops))
        out = np.zeros((len(crops), 512), dtype=np.float32)
        for i, c in enumerate(crops):
            out[i, 0] = float(c)  # crop is its tag (see align_fn)
        return out


def _img(tag):
    a = np.zeros((4, 4, 3), dtype=np.uint8)
    a[0, 0, 0] = tag
    return a


def _align(img, kps):
    # return a scalar "crop" tag = frame tag * 100 + a per-call counter via kps sum
    return float(int(img[0, 0, 0]))


BIG = [0.0, 0.0, 200.0, 200.0]   # passes size gate
SMALL = [0.0, 0.0, 10.0, 10.0]   # fails MIN_FACE_PIXELS


def test_distribution_and_counts():
    frames = [(0.0, _img(1)), (1.0, _img(2)), (2.0, _img(3))]
    det = FakeDet({1: [(BIG, 0.9)], 2: [(BIG, 0.9), (BIG, 0.8)], 3: []})
    rec = FakeRec()
    res = ex._extract_faces_core(frames, det, rec, _align)
    assert [len(f) for (_t, _i, f) in res] == [1, 2, 0]
    # every surviving face got an embedding tagged with its frame
    assert res[0][2][0]["embedding"][0] == 1.0
    assert res[1][2][0]["embedding"][0] == 2.0
    assert res[1][2][1]["embedding"][0] == 2.0


def test_crowd_frame_dropped():
    crowd = [(BIG, 0.9)] * (ex.MAX_FACES_PER_FRAME + 1)
    det = FakeDet({5: crowd})
    res = ex._extract_faces_core([(0.0, _img(5))], det, FakeRec(), _align)
    assert res[0][2] == []


def test_low_score_and_small_filtered():
    det = FakeDet({7: [(BIG, 0.5), (SMALL, 0.9), (BIG, 0.95)]})  # below 0.6, too small, ok
    res = ex._extract_faces_core([(0.0, _img(7))], det, FakeRec(), _align)
    assert len(res[0][2]) == 1
    assert abs(res[0][2][0]["det_score"] - 0.95) < 1e-5


def test_batching_covers_all_when_over_batch_size(monkeypatch):
    monkeypatch.setattr(ex, "EMBED_BATCH_SIZE", 2)
    det = FakeDet({9: [(BIG, 0.9)] * 5})
    rec = FakeRec()
    res = ex._extract_faces_core([(0.0, _img(9))], det, rec, _align)
    assert len(res[0][2]) == 5
    assert rec.calls == [2, 2, 1]  # 5 crops in batches of 2
    assert all(f["embedding"] is not None for f in res[0][2])


def test_oom_fallback_splits_to_single_crops():
    """A rec that fails on batches >1 (simulating VRAM OOM) must still embed every
    crop by splitting down to singles — no silent loss."""
    det = FakeDet({1: [(BIG, 0.9)] * 4})

    class FlakyRec:
        def __init__(self):
            self.batch_sizes = []

        def get_feat(self, crops):
            self.batch_sizes.append(len(crops))
            if len(crops) > 1:
                raise RuntimeError("simulated VRAM OOM")
            out = np.zeros((1, 512), dtype=np.float32)
            out[0, 0] = float(crops[0])
            return out

    rec = FlakyRec()
    res = ex._extract_faces_core([(0.0, _img(1))], det, rec, _align)
    assert len(res[0][2]) == 4
    assert all(f["embedding"] is not None for f in res[0][2])  # nothing dropped
    assert 1 in rec.batch_sizes  # did fall back to single crops


def test_normed_embedding_is_unit_length():
    det = FakeDet({1: [(BIG, 0.9)]})

    class RecBig(FakeRec):
        def get_feat(self, crops):
            out = np.zeros((len(crops), 512), dtype=np.float32)
            out[:, 0] = 3.0
            out[:, 1] = 4.0  # norm 5
            return out

    res = ex._extract_faces_core([(0.0, _img(1))], det, RecBig(), _align)
    normed = res[0][2][0]["normed_embedding"]
    assert abs(float(np.linalg.norm(normed)) - 1.0) < 1e-6
