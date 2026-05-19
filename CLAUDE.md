# video-transcoder

## Deploy workflow
**Edit locally on Mac → push to main → done.**
- GHA (`build.yml`) auto-bumps `VERSION` (patch), builds image, pushes to `ghcr.io/rebaunana/video-transcoder:latest`
- Watchtower on hpc02 auto-pulls `:latest` and restarts the container
- Local `docker-compose.yml` uses `build: .` for local dev/testing; hpc02 uses the GHCR image
- Do NOT edit files directly on hpc02 — Watchtower overwrites on next pull

## Versioning
`VERSION` file (plain text). CI bumps patch on every push to main automatically.
For minor/major: edit `VERSION` manually before committing.

## Architecture
- `app/main.py` — FastAPI entrypoint, all API routes, scheduler loop
- `app/transcoder.py` — scan loop, ffmpeg orchestration, settings, shared state dict
- `app/database.py` — SQLite; job log + file cache
- `app/templates/index.html` — single-file GUI (Jinja2)
- `data/settings.json` — runtime settings, persisted via atomic rename (.json.tmp)
- `data/transcoder.db` — job history + file cache (survives container restart)
- `data/backups/` — rolling DB backups (default: every 24h, keep 7)

## Shared state + threading
`transcoder.state` dict is read by the web UI via polling. All writes go through `threading.Lock()` (`_lock`). Never bypass the lock.

`_stop` (hard stop) and `_soft_stop` (finish current files then stop) are `threading.Event()` objects.

## CQ / skip logic — do not break this
Each file's `nvtranscode_cq` metadata tag encodes its history:

| Tag value | Meaning |
|---|---|
| absent | Original HEVC — never re-encode |
| `original` | Confirmed original in DB cache — skip unless Re-encode Originals ON |
| `guard:26` | Output was not smaller than source at CQ 26 — skip at same CQ |
| `26` (numeric) | We encoded this at CQ 26 — re-encode if current CQ differs |

Tag survives DB reset because it's embedded in the file (ffprobe reads it back).

## Size guard
Output >= 95% of source size → keep original, mark `guard:{CQ}` in cache. Prevents bloating already-efficient files.

## NVENC concurrency
`_NVENC_SEM = BoundedSemaphore(3)` — RTX 3050 Ti max 3 concurrent NVENC sessions. Override with env `NVENC_MAX_SESSIONS`. Default workers = 2 (safe margin).

## HW decode fallback
First attempt uses CUVID decoder (e.g. `h264_cuvid`). On failure → retry once with CPU decode. Handles malformed DivX/Xvid headers.

## Corrupt detection
Duration drift > 2% between source and output → output deleted, source marked `corrupt` in cache → permanent skip.

## Temp files
Named `<stem>.transcoding<ext>`. Atomic rename to final path after verification. Leftover temps cleaned on startup.

## hpc02 specifics
- Container: `video-transcoder`, port 8267, user 1001:1001
- Media mounts (NAS via NFS): `/media/ddMovie`, `/media/intensoP1`, `/media/intensoP2`, `/media/jdownloader`, `/media/movies`, `/media/serien`, `/media/training`
- NIC counter via `/host/proc/net/dev` (bind-mount from host `/proc/1/net`) — required because container is not in network_mode:host
- After first deploy as non-root: `sudo chown -R 1001:1001 /opt/docker/video-transcoder/data`

## Known gotchas
- `threading.Lock()` on all DB writes — never remove; SQLite is not thread-safe for concurrent writes
- Reset DB wipes file cache entirely — always backup first; use Clean Jobs for routine cleanup
- Settings snapshot at transcode start (`_cq, _preset = CQ, PRESET`) — mid-job changes take effect on the next file only
- The GHA version bump commit from `github-actions[bot]` does NOT re-trigger the workflow (GitHub blocks GITHUB_TOKEN-triggered runs)
