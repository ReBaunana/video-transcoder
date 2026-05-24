"""In-memory cosine index + face matching logic."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from collections import defaultdict

import numpy as np

from app.face.model import blob_to_embed

log = logging.getLogger(__name__)

SIMILARITY_HIGH = 0.55      # auto-suggest, green
SIMILARITY_MEDIUM = 0.42    # show as yellow, needs confirmation
MIN_FRAME_MATCHES = 3       # performer must appear in >= 3 frames
AUTO_ACCEPT_SINGLE_MATCH = False  # legacy flag — kept for compat; auto-accept uses AUTO_ACCEPT_THRESHOLD
AUTO_ACCEPT_THRESHOLD = 0.72     # rank-1 sim >= this with clear gap triggers automatic accept


class PerformerIndex:
    """In-memory face embedding index for all known performers.

    Thread-safe: read-heavy, occasional reload under a write lock.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._matrix: np.ndarray | None = None   # (N, 512) float32, L2-normalized
        self._ids: np.ndarray | None = None      # (N,) int64, row -> performer_id
        self._names: dict[int, str] = {}         # performer_id -> canonical_name
        self._loaded = False

    def load(self, conn: sqlite3.Connection) -> None:
        """Load all face_embedding rows where performer_id IS NOT NULL."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT fe.performer_id, fe.embedding, p.canonical_name
              FROM face_embedding fe
              JOIN performer p ON p.id = fe.performer_id
             WHERE fe.performer_id IS NOT NULL
               AND fe.embedding IS NOT NULL
            """
        )
        rows = cur.fetchall()

        if not rows:
            with self._lock:
                self._matrix = np.zeros((0, 512), dtype=np.float32)
                self._ids = np.zeros((0,), dtype=np.int64)
                self._names = {}
                self._loaded = True
            log.info("PerformerIndex.load: empty index")
            return

        vecs: list[np.ndarray] = []
        ids: list[int] = []
        names: dict[int, str] = {}
        for performer_id, blob, name in rows:
            try:
                v = blob_to_embed(blob)
                if v.size != 512:
                    log.warning(
                        "PerformerIndex.load: skipping wrong-size embedding (size=%d, performer_id=%s)",
                        v.size, performer_id,
                    )
                    continue
                # Defensive re-normalization.
                n = float(np.linalg.norm(v))
                if n > 0:
                    v = v / n
                vecs.append(v.astype(np.float32, copy=False))
                ids.append(int(performer_id))
                names[int(performer_id)] = name
            except Exception:
                log.exception("PerformerIndex.load: bad row performer_id=%s", performer_id)

        if not vecs:
            matrix = np.zeros((0, 512), dtype=np.float32)
            id_arr = np.zeros((0,), dtype=np.int64)
        else:
            matrix = np.vstack(vecs).astype(np.float32, copy=False)
            id_arr = np.asarray(ids, dtype=np.int64)

        with self._lock:
            self._matrix = matrix
            self._ids = id_arr
            self._names = names
            self._loaded = True

        log.info(
            "PerformerIndex.load: %d embeddings, %d performers",
            matrix.shape[0], len(names),
        )

    def reload(self, conn: sqlite3.Connection) -> None:
        """Force reload (called after new embeddings are added)."""
        self.load(conn)

    def is_loaded(self) -> bool:
        return self._loaded

    def size(self) -> int:
        with self._lock:
            return 0 if self._matrix is None else int(self._matrix.shape[0])

    def performer_count(self) -> int:
        with self._lock:
            return len(self._names)

    def match(self, query: np.ndarray, top_k: int = 20) -> list[dict]:
        """Cosine similarity search; query must be L2-normalized.

        Returns top_k dicts sorted by similarity desc.
        """
        with self._lock:
            matrix = self._matrix
            ids = self._ids
            names = self._names
            if matrix is None or ids is None or matrix.shape[0] == 0:
                return []
            # Snapshot the reference under the lock; arithmetic outside is safe.

        q = np.asarray(query, dtype=np.float32).reshape(-1)
        if q.size != matrix.shape[1]:
            log.warning("PerformerIndex.match: dim mismatch q=%d expected=%d", q.size, matrix.shape[1])
            return []

        sims = matrix @ q  # (N,)
        k = int(min(top_k, sims.shape[0]))
        if k <= 0:
            return []
        # argpartition for the top-k, then sort just those.
        idx = np.argpartition(-sims, k - 1)[:k]
        idx = idx[np.argsort(-sims[idx])]

        out: list[dict] = []
        for i in idx:
            pid = int(ids[i])
            out.append({
                "performer_id": pid,
                "performer_name": names.get(pid, ""),
                "similarity": float(sims[i]),
            })
        return out


# Module-level singleton.
_index = PerformerIndex()


def get_index() -> PerformerIndex:
    return _index


def _ensure_match_result_unique_constraint(conn: sqlite3.Connection) -> None:
    """Create a unique index for upsert if it doesn't already exist."""
    try:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_face_match_result_file_performer
              ON face_match_result (file_curation_id, performer_id)
            """
        )
    except sqlite3.OperationalError:
        log.debug("face_match_result unique index already exists or table differs", exc_info=True)


def match_video(
    conn: sqlite3.Connection,
    file_curation_id: int,
    face_embeddings: list[dict],
) -> list[dict]:
    """Match a video's face embeddings against the index; persist results."""
    idx = get_index()
    if not idx.is_loaded():
        idx.load(conn)

    total_faces = len(face_embeddings)
    if total_faces == 0 or idx.size() == 0:
        log.info(
            "match_video: nothing to match file_id=%s total_faces=%d index_size=%d",
            file_curation_id, total_faces, idx.size(),
        )
        return []

    # Per-performer accumulator.
    acc: dict[int, dict] = defaultdict(lambda: {
        "max_sim": -1.0,
        "frame_sims": [],
        "match_count": 0,
        "performer_name": "",
        "sample_face": None,  # face dict at max sim
    })

    for face in face_embeddings:
        q = face.get("embedding")
        if q is None:
            continue
        results = idx.match(np.asarray(q, dtype=np.float32), top_k=20)
        if not results:
            continue
        # Per-frame: keep best similarity per performer (avoid double counting).
        best_per_perf: dict[int, dict] = {}
        for r in results:
            pid = r["performer_id"]
            if pid not in best_per_perf or r["similarity"] > best_per_perf[pid]["similarity"]:
                best_per_perf[pid] = r
        for pid, r in best_per_perf.items():
            entry = acc[pid]
            sim = r["similarity"]
            entry["frame_sims"].append(sim)
            entry["performer_name"] = r["performer_name"]
            if sim >= SIMILARITY_MEDIUM:
                entry["match_count"] += 1
            if sim > entry["max_sim"]:
                entry["max_sim"] = sim
                entry["sample_face"] = face

    # Score candidates.
    candidates: list[dict] = []
    for pid, entry in acc.items():
        sims = sorted(entry["frame_sims"], reverse=True)
        if not sims:
            continue
        top = sims[:5] if len(sims) >= 5 else sims
        score = float(sum(top) / len(top))
        match_count = int(entry["match_count"])
        if score >= SIMILARITY_MEDIUM and match_count >= MIN_FRAME_MATCHES:
            candidates.append({
                "performer_id": pid,
                "performer_name": entry["performer_name"],
                "similarity": score,
                "match_count": match_count,
                "sample_face": entry["sample_face"],
            })

    candidates.sort(key=lambda c: c["similarity"], reverse=True)

    _ensure_match_result_unique_constraint(conn)

    now = int(time.time())
    cur = conn.cursor()
    persisted: list[dict] = []
    try:
        # Reset previously pending rows for this file so we don't leak stale ranks.
        cur.execute(
            """
            UPDATE face_match_result
               SET status = 'superseded', resolved_at = ?
             WHERE file_curation_id = ?
               AND status = 'pending'
            """,
            (now, file_curation_id),
        )

        for rank, c in enumerate(candidates, start=1):
            cur.execute(
                """
                INSERT INTO face_match_result
                    (file_curation_id, performer_id, similarity, match_count,
                     total_faces, rank, status, sample_thumb_id, created_at, resolved_at)
                VALUES (?, ?, ?, ?, ?, ?, 'pending', NULL, ?, NULL)
                ON CONFLICT(file_curation_id, performer_id) DO UPDATE SET
                    similarity = excluded.similarity,
                    match_count = excluded.match_count,
                    total_faces = excluded.total_faces,
                    rank = excluded.rank,
                    status = 'pending',
                    sample_thumb_id = excluded.sample_thumb_id,
                    created_at = excluded.created_at,
                    resolved_at = NULL
                """,
                (
                    file_curation_id,
                    c["performer_id"],
                    c["similarity"],
                    c["match_count"],
                    total_faces,
                    rank,
                    now,
                ),
            )
            persisted.append({
                "performer_id": c["performer_id"],
                "performer_name": c["performer_name"],
                "similarity": c["similarity"],
                "match_count": c["match_count"],
                "total_faces": total_faces,
                "rank": rank,
            })

        # Flip curation status when we have exactly one high-confidence candidate.
        if (
            len(candidates) >= 1
            and candidates[0]["similarity"] >= SIMILARITY_HIGH
            and (len(candidates) == 1 or candidates[1]["similarity"] < SIMILARITY_MEDIUM)
        ):
            cur.execute(
                """
                UPDATE file_curation
                   SET status = 'suggested'
                 WHERE id = ?
                   AND status = 'pending'
                """,
                (file_curation_id,),
            )

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("match_video: DB error file_id=%s", file_curation_id)
        raise

    # Auto-accept when rank-1 is high confidence and there's no plausible competitor.
    # Deferred import avoids circular load: worker.py imports matcher at module level.
    if candidates:
        top = candidates[0]
        runner_up = candidates[1] if len(candidates) > 1 else None
        if top["similarity"] >= AUTO_ACCEPT_THRESHOLD and (
            runner_up is None or runner_up["similarity"] < SIMILARITY_MEDIUM
        ):
            try:
                row = conn.execute(
                    "SELECT id FROM face_match_result WHERE file_curation_id=? AND performer_id=? AND rank=1",
                    (file_curation_id, top["performer_id"]),
                ).fetchone()
                if row is not None:
                    match_id = int(row[0])
                    accept_match(conn, match_id)
                    try:
                        from app.face.worker import enqueue_seed_for_performer
                        enqueue_seed_for_performer(conn, top["performer_id"])
                    except Exception:
                        log.exception(
                            "auto-accept: enqueue_seed_for_performer failed performer_id=%s",
                            top["performer_id"],
                        )
                    log.info(
                        "auto-accept: file_id=%s performer=%r sim=%.3f match_id=%s",
                        file_curation_id, top["performer_name"], top["similarity"], match_id,
                    )
            except Exception:
                log.exception(
                    "auto-accept: failed file_id=%s performer_id=%s",
                    file_curation_id, top["performer_id"],
                )

    log.info(
        "match_video: file_id=%s candidates=%d total_faces=%d top_sim=%.3f",
        file_curation_id,
        len(persisted),
        total_faces,
        persisted[0]["similarity"] if persisted else 0.0,
    )
    return persisted


def accept_match(conn: sqlite3.Connection, match_id: int) -> None:
    """Accept a face match suggestion."""
    now = int(time.time())
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT file_curation_id, performer_id FROM face_match_result WHERE id = ?",
            (match_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"accept_match: match_id={match_id} not found")
        file_curation_id, performer_id = int(row[0]), int(row[1])

        cur.execute(
            """
            UPDATE face_match_result
               SET status = 'accepted', resolved_at = ?
             WHERE id = ?
            """,
            (now, match_id),
        )

        cur.execute(
            """
            INSERT OR IGNORE INTO file_performer
                (file_curation_id, performer_id, position, source)
            VALUES (?, ?, 0, 'face_recognition')
            """,
            (file_curation_id, performer_id),
        )

        cur.execute(
            """
            UPDATE face_match_result
               SET status = 'superseded', resolved_at = ?
             WHERE file_curation_id = ?
               AND id != ?
               AND status = 'pending'
            """,
            (now, file_curation_id, match_id),
        )

        cur.execute(
            """
            UPDATE file_curation
               SET status = 'reviewed'
             WHERE id = ?
               AND status = 'pending'
            """,
            (file_curation_id,),
        )

        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("accept_match: failed match_id=%s", match_id)
        raise

    log.info("accept_match: match_id=%s file_id=%s performer_id=%s",
             match_id, file_curation_id, performer_id)


def reject_match(conn: sqlite3.Connection, match_id: int) -> None:
    """Mark a face match suggestion as rejected."""
    now = int(time.time())
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE face_match_result
               SET status = 'rejected', resolved_at = ?
             WHERE id = ?
            """,
            (now, match_id),
        )
        if cur.rowcount == 0:
            raise ValueError(f"reject_match: match_id={match_id} not found")
        conn.commit()
    except Exception:
        conn.rollback()
        log.exception("reject_match: failed match_id=%s", match_id)
        raise
    log.info("reject_match: match_id=%s", match_id)
