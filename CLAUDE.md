# video-transcoder

## Versioning
Version is tracked in `VERSION` (plain text). CI auto-bumps patch on every push to main.

## Architecture
- `app/main.py` — FastAPI entrypoint, all API routes
- `app/transcoder.py` — scan loop, ffmpeg, settings
- `app/database.py` — SQLite helpers
- `app/templates/index.html` — single-file GUI
- `data/settings.json` — runtime settings
- `data/transcoder.db` — job history + file cache

## Docker / hpc02
Container runs as appuser (uid 1001).
After first deploy of non-root image:
  sudo chown -R 1001:1001 /opt/docker/video-transcoder/data

## Key behaviours
- cq='original': HEVC on first scan, skipped unless Re-encode Originals ON
- nvtranscode_cq tag: embedded in every output file, survives DB reset
- Corrupt files: duration drift >2% → codec='corrupt', permanent skip
- Disabled mounts: per-mount ⊘ button, persisted in settings.json

## Known gotchas
- Threading: all DB writes go through threading.Lock() — never remove
- Reset DB wipes file cache. Use Clean Jobs for routine cleanup.
- Always backup before Reset DB.
