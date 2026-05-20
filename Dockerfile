FROM nvidia/cuda:12.6.3-runtime-ubuntu24.04

ARG APP_VERSION=dev
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    APP_VERSION=${APP_VERSION}

RUN apt-get update && apt-get upgrade -y --no-install-recommends && apt-get install -y --no-install-recommends \
    python3 python3-pip ffmpeg tzdata \
    libva-drm2 libva2 intel-media-va-driver \
    intel-gpu-tools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY VERSION ./
COPY app/ app/

RUN groupadd --gid 1001 appuser \
    && useradd --uid 1001 --gid appuser --no-create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app

USER appuser

EXPOSE 8267
CMD ["python3", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8267"]
