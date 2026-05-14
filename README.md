# Video Transcoder

Scans media directories for video files in non-ideal codecs and re-encodes them to H.265 (HEVC) using NVIDIA NVENC. Designed to run as a Docker container with GPU passthrough.

## Features

- Scans all subdirectories under `/media` (mount as many volumes as needed)
- Skips files already in HEVC or AV1; transcodes everything else (H.264, MPEG-4, WMV, VC-1, MPEG-2, etc.)
- Verifies output before replacing: codec check + duration drift < 2%
- Replaces originals in-place — same filename and folder (Plex/Jellyfin compatible, no rescans needed)
- File cache: tracks `path + size + mtime` in SQLite so repeated scans skip unchanged files
- Scheduled automatic scan (default: 03:00 daily, configurable)
- Web dashboard with codec breakdown, space savings, and per-file job history
- Semantic versioned Docker image on GHCR; Watchtower compatible for auto-updates

## Requirements

- Docker + Docker Compose
- NVIDIA GPU with [NVIDIA Container Toolkit](https://docs.nvidia.com/datacenter/cloud-native/container-toolkit/install-guide.html) installed on the host

## Quick Start

1. Copy `docker-compose.yml` and `.env.example` to a directory on your server:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env`:
   ```env
   TZ=Europe/Zurich
   SCHEDULE_HOUR=3      # hour (0-23) for automatic daily scan
   FFMPEG_CQ=19         # NVENC CQ value (lower = better quality, larger files)
   FFMPEG_PRESET=fast   # NVENC preset: fastest, fast, medium, slow, p1–p7
   DRY_RUN=false        # set to true to log what would be transcoded without doing it
   ```

3. Add your media mounts to `docker-compose.yml`:
   ```yaml
   volumes:
     - ./data:/data
     - /path/to/movies:/media/movies
     - /path/to/series:/media/series
   ```

4. Start the container:
   ```bash
   docker compose up -d
   ```

5. Open the dashboard at `http://<host>:8267/`

## Configuration

| Variable | Default | Description |
|---|---|---|
| `TZ` | `UTC` | Container timezone |
| `SCHEDULE_HOUR` | `3` | Hour of day for automatic scan (0–23) |
| `FFMPEG_CQ` | `19` | Constant Quality value for `hevc_nvenc` |
| `FFMPEG_PRESET` | `fast` | Encoding preset (speed vs. compression) |
| `DRY_RUN` | `false` | Log targets without transcoding |

## Data

SQLite database is stored at `/data/transcoder.db` inside the container. Mount `./data:/data` to persist it across restarts.

## API

| Method | Path | Description |
|---|---|---|
| `GET` | `/` | Web dashboard |
| `GET` | `/api/status` | JSON: current state + stats |
| `POST` | `/api/run` | Start a scan immediately |
| `POST` | `/api/stop` | Request stop (finishes current file) |

## Using a Pre-Built Image

Replace `build: .` in `docker-compose.yml` with the GHCR image:

```yaml
image: ghcr.io/YOUR_GITHUB_USERNAME/video-transcoder:latest
```

Tagged releases (e.g. `v1.0.0`) are available for pinning a specific version:

```yaml
image: ghcr.io/YOUR_GITHUB_USERNAME/video-transcoder:v1.0.0
```

## Rollback

To revert to a previous version, pin the image tag and redeploy:

```bash
# Edit docker-compose.yml: image: ghcr.io/.../video-transcoder:v1.0.0
docker compose pull && docker compose up -d
```

## Building Locally

```bash
docker compose build
docker compose up -d
```

## Codec Logic

| Codec | Action |
|---|---|
| `hevc`, `av1` | Skip (already ideal) |
| `gif`, `png`, `mjpeg`, `unknown` | Skip (not video) |
| Everything else | Transcode to `hevc_nvenc` |

Output container format follows the source: `.mkv` stays `.mkv`, `.mp4` stays `.mp4`. Other formats are remuxed to `.mkv`.
