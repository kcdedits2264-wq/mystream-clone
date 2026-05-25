# ─────────────────────────────────────────────────────────────────
#  StreamAdda — Dockerfile
#  Base: python:3.12-slim (Debian Bookworm)
#  FFmpeg is installed from the system package manager.
#  Works on: Render (Free), Koyeb (Free), Railway, Fly.io free tier.
# ─────────────────────────────────────────────────────────────────

FROM python:3.12-slim

# System deps: ffmpeg + clean up in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends ffmpeg && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN useradd -m -u 1000 streamadda
WORKDIR /app

# Install Python deps first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY --chown=streamadda:streamadda . .

USER streamadda

# Render / Koyeb inject $PORT at runtime; default 8000
ENV PORT=8000
ENV FLASK_ENV=production
ENV PYTHONUNBUFFERED=1

EXPOSE 8000

# Gunicorn: 1 worker (free tier RAM), threaded for SSE connections
CMD gunicorn app:app \
    --bind "0.0.0.0:${PORT}" \
    --workers 1 \
    --threads 16 \
    --timeout 0 \
    --keep-alive 65 \
    --log-level info \
    --access-logfile - \
    --error-logfile -
