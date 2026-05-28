# app/curation/rename.py
"""Atomic rename + performer-folder move workflow for curated video files.

Renames are performed one at a time, wrapped in a single DB transaction
plus a single filesystem `os.rename`. If the FS move succeeds but the DB
update fails, we attempt to move the file back to its original path so
state cannot diverge silently.

For mounts listed in PERFORMER_FOLDER_MOUNTS (currently: ddMovie), a
second move follows the rename: the file is placed into a per-performer
subfolder named after the primary performer (position 0 in file_performer).
Files with no accepted performer stay in the mount root. The folder move
is non-fatal — a failure there does not roll back the rename.
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from pathlib import Path

log = logging.getLogger(__name__)

_MAX_PATH_COMPONENT = 255  # Linux NAME_MAX on ext4/btrfs/xfs

# Characters not allowed in directory names on Linux/NAS.
_UNSAFE_CHARS_RE = re.compile(r'[<>:"/\\|?*\x00-\x1f]')

# Mounts where renamed files are moved into per-performer subfolders.
PERFORMER_FOLDER_MOUNTS: frozenset[str] = frozenset({'ddMovie'})


def _now_sql() -> str:
    return "datetime('now')"


def _sanitize_folder_name(name: str) -> str:
    """Return a filesystem-safe version of a performer canonical_name.

    Strips characters illegal on Linux/NAS, collapses repeated underscores,
    trims leading/trailing punctuation. Truncated to 200 chars (well under
    NAME_MAX so the full filename inside can still fit).
    """
    if not name:
        return ""
    sanitized = _UNSAFE_CHARS_RE.sub("_", name.strip())
    sanitized = re.sub(r"_{2,}", "_", sanitized)
    sanitized = sanitized.strip("._")
    return sanitized[:200]


def _performer_folder_move(
    conn: sqlite3.Connection,
    file_curation_id: int,
    current_path: str,
    mount: str,
    rename_log_id: int,
) -> dict:
    """Move a just-renamed file into its primary performer's subfolder.

    The subfolder is created under /media/<mount>/<performer_name>/ using
    the canonical_name of the file_performer row with the lowest position.

    Always returns a dict — never raises. A failure here is non-fatal to
    the rename and is reported in the 'folder_move' key of execute_rename's
    return value.

    Return keys:
      ok      – True if no error occurred (including the no-op cases)
      moved   – True only when the file was actually moved to a new path
      to      – final path (set when moved=True or already_in_folder)
      reason  – why moved=False when ok=True ('no_performer', 'already_in_folder')
      error   – error string when ok=False
    """
    # Look up primary performer (lowest position).
    row = conn.execute(
        """
        SELECT p.canonical_name
          FROM file_performer fp
          JOIN performer p ON p.id = fp.performer_id
         WHERE fp.file_curation_id = ?
         ORDER BY fp.position ASC
         LIMIT 1
        """,
        (file_curation_id,),
    ).fetchone()

    if row is None:
        return {"ok": True, "moved": False, "reason": "no_performer"}

    folder_name = _sanitize_folder_name(row[0])
    if not folder_name:
        return {"ok": False, "moved": False, "error": "empty_folder_name"}

    mount_root = f"/media/{mount}"
    target_dir = os.path.join(mount_root, folder_name)
    basename = os.path.basename(current_path)
    target_path = os.path.join(target_dir, basename)

    # Already in the right folder — nothing to do.
    if os.path.normpath(os.path.dirname(current_path)) == os.path.normpath(target_dir):
        return {"ok": True, "moved": False, "reason": "already_in_folder", "to": current_path}

    # Resolve name collision inside the target folder.
    if os.path.lexists(target_path):
        stem = Path(target_path).stem
        suffix = Path(target_path).suffix
        resolved = None
        for n in range(2, 100):
            candidate = os.path.join(target_dir, f"{stem}_{n}{suffix}")
            if not os.path.lexists(candidate):
                resolved = candidate
                break
        if resolved is None:
            return {"ok": False, "moved": False, "error": "target_collision"}
        target_path = resolved

    # Create performer folder (no-op if already exists).
    try:
        os.makedirs(target_dir, exist_ok=True)
    except OSError as exc:
        return {"ok": False, "moved": False, "error": f"mkdir_error:{exc}"}

    # Filesystem move.
    try:
        os.rename(current_path, target_path)
    except OSError as exc:
        return {"ok": False, "moved": False, "error": f"os_error:{exc}"}

    # DB updates.  Use `with conn:` (auto-commit/rollback) so we never issue
    # a nested BEGIN IMMEDIATE on a connection that may have an implicit
    # transaction open from Python's sqlite3 isolation_level machinery.
    try:
        with conn:
            conn.execute(
                "UPDATE file_curation SET path = ?, updated_at = datetime('now') WHERE id = ?",
                (target_path, file_curation_id),
            )
            try:
                conn.execute(
                    "UPDATE file_cache SET path = ? WHERE path = ?",
                    (target_path, current_path),
                )
            except sqlite3.OperationalError:
                pass  # file_cache absent in test/dev DBs
            # Keep rename_log pointing at the actual final location so that
            # rollback_rename can find and move the file back from here.
            conn.execute(
                "UPDATE rename_log SET to_path = ? WHERE id = ?",
                (target_path, rename_log_id),
            )
    except Exception as db_exc:
        revert_error = None
        try:
            os.rename(target_path, current_path)
        except OSError as rev_exc:
            revert_error = str(rev_exc)
        error = f"db_error:{db_exc}"
        if revert_error:
            # File is physically at target_path but DB still shows current_path.
            # Operator must reconcile manually.
            log.error(
                "performer_folder_move: DB update failed AND FS revert failed — "
                "file is at %r but DB shows %r; manual fix required",
                target_path, current_path,
            )
            error += f";revert_failed:{revert_error}"
        # moved=True only when the file ended up at target_path (revert failed).
        file_is_at_target = revert_error is not None
        return {"ok": False, "moved": file_is_at_target,
                "to": target_path if file_is_at_target else None,
                "error": error}

    return {"ok": True, "moved": True, "from": current_path, "to": target_path}


def execute_rename(conn: sqlite3.Connection, file_curation_id: int) -> dict:
    """Atomically rename one approved file.

    For mounts in PERFORMER_FOLDER_MOUNTS, also moves the file into the
    performer's subfolder after the rename commits.

    Returns {
        'ok': bool,
        'from': str,          # original path
        'to': str,            # final path (post-folder-move if applicable)
        'error': str|None,
        'folder_move': dict|None,  # result of _performer_folder_move, or None
    }.
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
            "folder_move": None,
        }

    try:
        row = conn.execute(
            """
            SELECT path, status, proposed_filename, mount
            FROM file_curation
            WHERE id = ?
            """,
            (int(file_curation_id),),
        ).fetchone()

        if row is None:
            conn.rollback()
            return {
                "ok": False, "from": "", "to": "",
                "error": "not_found", "folder_move": None,
            }

        from_path, status, proposed, mount = row[0], row[1], row[2], row[3]

        if status != "approved":
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": "",
                "error": f"bad_status:{status}",
                "folder_move": None,
            }
        if not proposed:
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": "",
                "error": "no_proposed_filename",
                "folder_move": None,
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
                "folder_move": None,
            }

        if from_path == to_path:
            # Same name — still log it so rollback_rename and folder-move work.
            rl_cur = conn.execute(
                """
                INSERT INTO rename_log
                    (file_curation_id, from_path, to_path, success)
                VALUES (?, ?, ?, 1)
                """,
                (int(file_curation_id), from_path, to_path),
            )
            rename_log_id = int(rl_cur.lastrowid)
            conn.execute(
                """UPDATE file_curation
                      SET status = 'renamed', renamed_at = datetime('now'), updated_at = datetime('now')
                    WHERE id = ?""",
                (int(file_curation_id),),
            )
            conn.commit()
            folder_move = _try_folder_move(conn, file_curation_id, to_path, mount, rename_log_id)
            final_path = folder_move.get("to", to_path) if folder_move and folder_move.get("moved") else to_path
            return {
                "ok": True, "from": from_path, "to": final_path,
                "error": None, "folder_move": folder_move,
            }

        if not os.path.lexists(from_path):
            conn.rollback()
            return {
                "ok": False,
                "from": from_path,
                "to": to_path,
                "error": "source_missing",
                "folder_move": None,
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
                    "folder_move": None,
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
                "folder_move": None,
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

            rl_cur = conn.execute(
                """
                INSERT INTO rename_log
                    (file_curation_id, from_path, to_path, success)
                VALUES (?, ?, ?, 1)
                """,
                (int(file_curation_id), from_path, to_path),
            )
            rename_log_id = int(rl_cur.lastrowid)
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
                "folder_move": None,
            }

        folder_move = _try_folder_move(conn, file_curation_id, to_path, mount, rename_log_id)
        final_path = folder_move.get("to", to_path) if folder_move and folder_move.get("moved") else to_path
        return {
            "ok": True, "from": from_path, "to": final_path,
            "error": None, "folder_move": folder_move,
        }

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
            "folder_move": None,
        }


def _try_folder_move(
    conn: sqlite3.Connection,
    file_curation_id: int,
    renamed_path: str,
    mount: str | None,
    rename_log_id: int,
) -> dict | None:
    """Attempt performer folder move if mount requires it; return result or None."""
    if not mount or mount not in PERFORMER_FOLDER_MOUNTS:
        return None
    try:
        return _performer_folder_move(conn, file_curation_id, renamed_path, mount, rename_log_id)
    except Exception as exc:
        log.warning(
            "performer_folder_move unexpected error (non-fatal) fc_id=%s: %s",
            file_curation_id, exc,
        )
        return {"ok": False, "moved": False, "error": f"unexpected:{exc}"}


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

        # Same-path entry: no filesystem move ever occurred; just reset status.
        if orig_from == orig_to:
            conn.execute(
                """
                UPDATE file_curation
                   SET status = 'approved',
                       renamed_at = NULL,
                       updated_at = datetime('now')
                 WHERE id = ?
                """,
                (int(fc_id),),
            )
            conn.execute(
                "UPDATE rename_log SET rolled_back_at = datetime('now') WHERE id = ?",
                (int(log_id),),
            )
            conn.commit()
            return {"ok": True, "error": None}

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
