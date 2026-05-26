# app/curation/rename.py
"""Atomic rename workflow for curated video files.

Renames are performed one at a time, wrapped in a single DB transaction
plus a single filesystem `os.rename`. If the FS move succeeds but the DB
update fails, we attempt to move the file back to its original path so
state cannot diverge silently.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path


_MAX_PATH_COMPONENT = 255  # Linux NAME_MAX on ext4/btrfs/xfs


def _now_sql() -> str:
    return "datetime('now')"


def execute_rename(conn: sqlite3.Connection, file_curation_id: int) -> dict:
    """Atomically rename one approved file.

    Returns {'ok': bool, 'from': str, 'to': str, 'error': str|None}.
    """
    from_path = ""
    to_path = ""

    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        return {
            "ok": False,
            "from": "",
            "to": "",
            "error": f"db_locked: {exc}",
        }

    try:
        row = conn.execute(
            """
            SELECT path, status, proposed_filename
            FROM file_curation
            WHERE id = ?
            """,
            (int(file_curation_id),),
        ).fetchone()

        if row is None:
            conn.rollback()
            return {"ok": False, "from": "", "to": "", "error": "not_found"}

        from_path, status, proposed = row[0], row[1], row[2]

        if status != "approved":
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": "",
                "error": f"bad_status:{status}",
            }
        if not proposed:
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": "",
                "error": "no_proposed_filename",
            }

        parent = os.path.dirname(from_path)
        proposed_basename = os.path.basename(proposed)  # ignore any path injection
        to_path = os.path.join(parent, proposed_basename)

        if len(proposed_basename) > _MAX_PATH_COMPONENT:
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": to_path,
                "error": "name_too_long",
            }

        if from_path == to_path:
            conn.execute(
                """UPDATE file_curation
                      SET status = 'renamed', renamed_at = datetime('now'), updated_at = datetime('now')
                    WHERE id = ?""",
                (int(file_curation_id),),
            )
            conn.commit()
            return {"ok": True, "from": from_path, "to": to_path, "error": None}

        if not os.path.lexists(from_path):
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": to_path,
                "error": "source_missing",
            }

        if os.path.lexists(to_path) and from_path != to_path:
            # Target taken — try _2, _3, ... _99 suffix before giving up.
            stem = Path(to_path).stem
            suffix = Path(to_path).suffix
            parent_dir = os.path.dirname(to_path)
            resolved = None
            for n in range(2, 100):
                candidate = os.path.join(parent_dir, f"{stem}_{n}{suffix}")
                if not os.path.lexists(candidate):
                    resolved = candidate
                    break
            if resolved is None:
                conn.rollback()
                return {
                    "ok": False,
                    "from": from_path,
                    "to": to_path,
                    "error": "target_exists",
                }
            # Update proposed_filename in DB so it reflects what we'll actually use.
            new_basename = os.path.basename(resolved)
            conn.execute(
                "UPDATE file_curation SET proposed_filename = ? WHERE id = ?",
                (new_basename, int(file_curation_id)),
            )
            to_path = resolved

        # --- Filesystem move ---
        try:
            os.rename(from_path, to_path)
        except OSError as exc:
            conn.rollback()
            try:
                conn.execute(
                    """
                    INSERT INTO rename_log
                        (file_curation_id, from_path, to_path, success, error_message)
                    VALUES (?, ?, ?, 0, ?)
                    """,
                    (int(file_curation_id), from_path, to_path, f"os_error:{exc}"),
                )
                conn.commit()
            except Exception:
                pass
            return {
                "ok": False,
                "from": from_path,
                "to": to_path,
                "error": f"os_error:{exc}",
            }

        # --- DB updates within the open transaction ---
        try:
            conn.execute(
                """
                UPDATE file_curation
                   SET path = ?,
                       status = 'renamed',
                       renamed_at = datetime('now'),
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (to_path, int(file_curation_id)),
            )
            # Keep transcoder file_cache pointing at the new path
            try:
                conn.execute(
                    "UPDATE file_cache SET path = ? WHERE path = ?",
                    (to_path, from_path),
                )
            except sqlite3.OperationalError:
                # file_cache may not exist in dev/test DBs; ignore
                pass

            conn.execute(
                """
                INSERT INTO rename_log
                    (file_curation_id, from_path, to_path, success)
                VALUES (?, ?, ?, 1)
                """,
                (int(file_curation_id), from_path, to_path),
            )
            conn.commit()
        except Exception as db_exc:
            # DB update failed AFTER FS move succeeded — try to move back
            conn.rollback()
            revert_error = None
            try:
                os.rename(to_path, from_path)
            except OSError as revert_exc:
                revert_error = str(revert_exc)
            try:
                conn.execute(
                    """
                    INSERT INTO rename_log
                        (file_curation_id, from_path, to_path, success, error_message)
                    VALUES (?, ?, ?, 0, ?)
                    """,
                    (
                        int(file_curation_id),
                        from_path,
                        to_path,
                        f"db_error:{db_exc} | revert={revert_error or 'ok'}",
                    ),
                )
                conn.commit()
            except Exception:
                pass
            return {
                "ok": False,
                "from": from_path,
                "to": to_path,
                "error": f"db_error:{db_exc}; revert={revert_error or 'ok'}",
            }

        return {"ok": True, "from": from_path, "to": to_path, "error": None}

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return {
            "ok": False,
            "from": from_path,
            "to": to_path,
            "error": f"unexpected:{exc}",
        }


def rollback_rename(conn: sqlite3.Connection, rename_log_id: int) -> dict:
    """Reverse a previously successful rename."""
    try:
        conn.execute("BEGIN IMMEDIATE")
    except sqlite3.OperationalError as exc:
        return {"ok": False, "error": f"db_locked: {exc}"}

    try:
        row = conn.execute(
            """
            SELECT id, file_curation_id, from_path, to_path, success, rolled_back_at
            FROM rename_log
            WHERE id = ?
            """,
            (int(rename_log_id),),
        ).fetchone()

        if row is None:
            conn.rollback()
            return {"ok": False, "error": "log_not_found"}

        log_id, fc_id, orig_from, orig_to, success, rolled_back_at = row

        if not success:
            conn.rollback()
            return {"ok": False, "error": "log_marks_failure"}
        if rolled_back_at:
            conn.rollback()
            return {"ok": False, "error": "already_rolled_back"}

        if not os.path.lexists(orig_to):
            conn.rollback()
            return {"ok": False, "error": "current_file_missing"}
        if os.path.lexists(orig_from):
            conn.rollback()
            return {"ok": False, "error": "original_path_occupied"}

        try:
            os.rename(orig_to, orig_from)
        except OSError as exc:
            conn.rollback()
            return {"ok": False, "error": f"os_error:{exc}"}

        try:
            conn.execute(
                """
                UPDATE file_curation
                   SET path = ?,
                       status = 'approved',
                       renamed_at = NULL,
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (orig_from, int(fc_id)),
            )
            try:
                conn.execute(
                    "UPDATE file_cache SET path = ? WHERE path = ?",
                    (orig_from, orig_to),
                )
            except sqlite3.OperationalError:
                pass
            conn.execute(
                "UPDATE rename_log SET rolled_back_at = datetime('now') WHERE id = ?",
                (int(log_id),),
            )
            conn.commit()
        except Exception as db_exc:
            conn.rollback()
            # Try to put the file back where it was before rollback
            try:
                os.rename(orig_from, orig_to)
            except OSError:
                pass
            return {"ok": False, "error": f"db_error:{db_exc}"}

        return {"ok": True, "error": None}

    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"ok": False, "error": f"unexpected:{exc}"}


def execute_batch_rename(
    conn: sqlite3.Connection, mount: str | None = None, limit: int = 50
) -> dict:
    """Process up to `limit` approved renames serially.

    Stops on the first OS-level error so a flapping NAS mount can't churn
    through hundreds of failures. DB-level rejections (target_exists,
    bad_status, name_too_long) are counted as failures but don't abort.
    """
    params: list = []
    where = "WHERE status = 'approved' AND proposed_filename IS NOT NULL"
    if mount is not None:
        where += " AND mount = ?"
        params.append(mount)
    params.append(int(limit))

    sql = f"""
        SELECT id
        FROM file_curation
        {where}
        ORDER BY mount, path
        LIMIT ?
    """
    ids = [int(r[0]) for r in conn.execute(sql, params).fetchall()]

    ok = 0
    failed = 0
    errors: list[str] = []

    for fc_id in ids:
        result = execute_rename(conn, fc_id)
        if result["ok"]:
            ok += 1
            continue

        failed += 1
        err = result.get("error") or "unknown_error"
        errors.append(f"id={fc_id}: {err}")
        if err.startswith("os_error:") or err.startswith("db_locked"):
            # Likely FS/DB problem — stop the batch
            break

    return {"ok": ok, "failed": failed, "errors": errors}
