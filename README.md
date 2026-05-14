# Video Transcoder

Scans media directories for video files in non-ideal codecs and re-encodes them to H.265 (HEVC) using NVIDIA NVENC. Runs as a Docker container with GPU passthrough and a live web dashboard.

## Features

- Scans all subdirectories under `/media` — mount as many volumes as needed
- Skips files already in HEVC or AV1; transcodes everything else (H.264, MPEG-4, WMV, VC-1, MPEG-2, etc.)
- Full GPU pipeline: NVDEC → VRAM → NVENC — no CPU roundtrip
- Verifies output before replacing: codec check + duration drift < 2%
- Replaces originals in-place — same filename and folder, Plex/Jellyfin compatible, no library rescans needed
- File cache: `path + size + mtime` in SQLite so repeated scans skip unchanged files
- Re-transcodes files if CQ setting changed since last encode (detects quality upgrades/downgrades)
- Parallel workers (1–8) configurable live from the UI
- Scheduled automatic scan (default: 03:00 daily)
- Scheduled automatic DB backup with configurable retention
- All settings persist to `/data/settings.json` — survive container restarts
- Web dashboard: live worker progress bars, space savings, codec breakdown, per-file job history with CQ column
- Semantic versioned Docker image on GHCR; Watchtower compatible

## Requirements

- Docker + Docker Compose
- NVIDIA GPU with [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) installed on the host

## Quick Start

```bash
cp .env.example .env
# edit .env (see Configuration below)
# add your media mounts to docker-compose.yml
docker compose up -d
```

Open the dashboard at `http://<host>:8267/`

## Configuration

Environment variables (`.env` file). All settings can also be changed live in the web UI and are saved to `/data/settings.json`.

| Variable | Default | Description |
|---|---|---|
| `TZ` | `UTC` | Container timezone |
| `SCHEDULE_HOUR` | `3` | Hour of day for automatic scan (0–23) |
| `FFMPEG_CQ` | `28` | NVENC Constant Quality — see CQ Guide below |
| `FFMPEG_PRESET` | `fast` | Encoding preset — see Preset Guide below |
| `FFMPEG_WORKERS` | `2` | Parallel encode jobs (1–8) |
| `DRY_RUN` | `false` | Log targets without transcoding |
| `BACKUP_INTERVAL_H` | `24` | Auto-backup interval in hours (0 = off) |
| `BACKUP_KEEP` | `7` | Number of DB backups to keep |
| `NET_IFACE` | `eno1` | NIC name(s) for the ETH stats bar (comma-separated). Change to match your host — e.g. `enp6s0`, `eth0`. |
| `PROC_NET_DEV` | `/host/proc/net/dev` | Path to read NIC counters from. Requires `- /proc/1/net:/host/proc/net:ro` volume mount in docker-compose. Note: `/proc/net` is a symlink that resolves to the container's own namespace — must use `/proc/1/net` (host PID 1) instead. |

## CQ Quality Guide

CQ (Constant Quality) controls the trade-off between file size and visual quality for `hevc_nvenc`. Lower = better quality, larger file.

| CQ | Quality | File size vs source | Recommended for |
|---|---|---|---|
| 15–22 | Near-lossless | Can be **larger** than source | Archiving only |
| 23–25 | High quality | 20–40% smaller | Quality-critical content |
| **26–28** | **Good quality** | **40–60% smaller** | **General use — recommended** |
| 29–32 | Slight quality loss | 55–70% smaller | Large collections, limited storage |
| 33+ | Noticeable artifacts | 70%+ smaller | Not recommended |

> **Note:** CQ 19 (old default) produced files that were often *larger* than the H.264 source. CQ 28 is the new default.

## Preset Guide

The NVENC preset controls how hard the encoder searches for the best compression result.

| Preset | Speed | File size | Notes |
|---|---|---|---|
| `ultrafast` | Fastest | Slightly larger | |
| `fast` | Fast | Good | **Recommended** — best throughput/quality balance |
| `medium` | 2× slower | 5–10% smaller | Diminishing returns |
| `slow` | 3× slower | Marginal gain | Not worth it for most content |
| `p1`–`p7` | p1=fast, p7=slow | — | NVENC-specific aliases |

> **Note:** Unlike software codecs (libx265), NVENC is fixed-function hardware. The quality difference between `fast` and `slow` is small — `fast` gives you much higher throughput.

## Worker Parallelization

Multiple files can be encoded in parallel. For a single NVENC unit (e.g. RTX 3050 Ti):

| Workers | NVENC load | Notes |
|---|---|---|
| 1–2 | ~40–50% | Underutilized |
| **3–4** | **~80%** | **Recommended sweet spot** |
| 5–6 | ~90–95% | Good I/O overlap |
| 7–8 | ~95–100%+ | VRAM pressure risk on 4 GB GPUs |

> **Note:** CPU usage stays low (~10–20% total) with NVENC — the GPU does all the work.

## Data & Persistence

| Path | Contents |
|---|---|
| `/data/transcoder.db` | SQLite database (job history + file cache) |
| `/data/backups/` | Automatic DB backups (`transcoder_YYYY-MM-DD_HH-MM-SS.db`) |
| `/data/settings.json` | Persisted UI settings (CQ, preset, workers, backup config) |

Mount `./data:/data` to persist everything across container restarts.

**Settings survive DB reset** — `settings.json` is never touched by Reset DB or Clean Jobs.

## Web UI

Accessible at `http://<host>:8267/`. Changes to settings take effect immediately and are saved automatically.

| Feature | Location |
|---|---|
| Start / Stop scan | Header buttons |
| Parallel workers | Header dropdown (with tooltip) |
| CQ quality slider | Settings strip — color-coded zones |
| Encoding preset | Settings strip |
| Dry Run toggle | Settings strip |
| Backup Now | Settings strip |
| Backup Settings (interval, retention) | ☰ Menu → Backup Settings |
| Clean Jobs (history only) | ☰ Menu → Clean Jobs |
| Reset DB (history + file cache) | ☰ Menu → Reset DB |
| Live worker progress bars | Shown while scan running |
| Recent Jobs with CQ column | Bottom of page |
| Filter jobs (All/Done/Failed/Running) | Jobs panel header |

## API

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Web dashboard |
| `GET` | `/api/status` | JSON: current state + live stats |
| `POST` | `/api/run` | Start a scan immediately |
| `POST` | `/api/stop` | Request stop (finishes current file) |
| `GET` | `/api/config` | Get current CQ, preset, workers, backup config |
| `POST` | `/api/config` | Update config (body: `{cq, preset, dry_run, workers, backup_interval_h, backup_keep}`) |
| `POST` | `/api/backup` | Trigger manual DB backup |
| `POST` | `/api/clean-jobs` | Delete job history (keeps file cache) |
| `POST` | `/api/reset` | Delete job history + file cache (all files re-transcoded next scan) |

## Codec Logic

| Codec | Action |
|---|---|
| `hevc`, `av1` | Skip — already ideal |
| `gif`, `png`, `mjpeg`, `unknown` | Skip — not real video |
| Everything else | Transcode to `hevc_nvenc` |

Output container: `.mkv` → `.mkv`, `.mp4`/`.m4v`/`.mov` → `.mp4`, all others → `.mkv`.

## Using a Pre-Built Image

Replace `build: .` in `docker-compose.yml`:

```yaml
image: ghcr.io/ReBaunana/video-transcoder:latest
```

Pin to a specific version:

```yaml
image: ghcr.io/ReBaunana/video-transcoder:v1.7.0
```

## Rollback

```bash
# Edit docker-compose.yml to pin image tag
docker compose pull && docker compose up -d
```

## Building Locally

```bash
docker compose build && docker compose up -d
```
