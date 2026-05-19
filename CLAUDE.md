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
| `guard:26` | NVENC tried at CQ 26, output not 5% smaller — everyone skips |
| `guard_intel:26` | Intel VAAPI tried at CQ 26 and failed size guard — Intel skips; NVENC retries next scan |
| `26` (numeric) | We encoded this at CQ 26 — re-encode if current CQ differs |

Tag survives DB reset because it's embedded in the file (ffprobe reads it back).

## Size guard (Layer 2)
Output >= 95% of source size → keep original. NVENC stores `guard:{CQ}`, VAAPI stores `guard_intel:{CQ}`.

## Layer 1 admission control
Before encoding, bitrate vs resolution check skips files unlikely to shrink:
- 4K (>=2160p): skip if < 8000 kbps
- 1080p: skip if < 3000 kbps
- 720p: skip if < 1500 kbps
- SD: skip if < 800 kbps
Unknown bitrate passes through (let size guard decide).

## GPU backends

### NVENC (primary)
`_NVENC_SEM = BoundedSemaphore(3)` — RTX 3050 Ti max 3 concurrent sessions. Override: `NVENC_MAX_SESSIONS`.
Workers 0..WORKERS-1 are NVENC type.

### Intel VAAPI (secondary)
`_VAAPI_SEM = BoundedSemaphore(3)` — Intel iGPU (AlderLake-S GT1), `hevc_vaapi` encoder.
Workers WORKERS..WORKERS+VAAPI_WORKERS-1 are VAAPI type.
Set `VAAPI_WORKERS=2` (env) or via UI to enable. Default 0 = disabled.
Requires hpc02 docker-compose: `devices: [/dev/dri:/dev/dri]` + `group_add: ["993"]` (render GID).
Files Intel fails size guard → `guard_intel:{CQ}` → NVENC retries next scan.

## HW decode fallback
NVENC: first attempt uses CUVID decoder (e.g. `h264_cuvid`). On failure:
- If VAAPI enabled (VAAPI_WORKERS > 0) → retry with Intel VAAPI (full HW path)
- If VAAPI disabled → retry with CPU decode
Handles malformed DivX/Xvid headers.

## hpc02 docker-compose.yml (NOT in git)
File at `/opt/docker/video-transcoder/docker-compose.yml` — managed manually.
Extras vs repo template:
```yaml
devices:
  - /dev/dri:/dev/dri
group_add:
  - "993"     # render group GID — required for Intel VAAPI /dev/dri/renderD128 access
volumes:
  - /proc/1/net:/host/proc/net:ro
environment:
  - PROC_NET_DEV=/host/proc/net/dev
  - NET_IFACE=eno1
```

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
