"""
StreamAdda Clone — Flask Backend
Manages FFmpeg streaming processes with real-time SSE log streaming.
Optimized for free-tier cloud deployment (Render, Koyeb, Hugging Face Spaces).
"""

import os
import sys
import uuid
import time
import signal
import logging
import threading
import subprocess
from datetime import datetime, timezone
from queue import Queue, Empty
from flask import Flask, request, jsonify, render_template, Response, stream_with_context

# ─── Logging ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ─── App Setup ────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "streamadda-dev-secret-change-in-prod")

# ─── In-memory State ──────────────────────────────────────────────────────────
# slot_id → {process, name, status, started_at, log_queue, thread}
streams: dict[str, dict] = {}
streams_lock = threading.Lock()

RTMP_BASES = {
    "youtube": "rtmp://a.rtmp.youtube.com/live2/",
    "facebook": "rtmps://live-api-s.facebook.com:443/rtmp/",
    "custom": "",
}

MAX_LOG_LINES = 300   # per slot
LOG_QUEUE_MAX = 500


# ─── Helpers ──────────────────────────────────────────────────────────────────

def find_ffmpeg() -> str:
    """Locate ffmpeg binary: env override → system PATH → common locations."""
    env_path = os.environ.get("FFMPEG_PATH")
    if env_path and os.path.isfile(env_path):
        return env_path
    for candidate in ["ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"]:
        try:
            subprocess.run(
                [candidate, "-version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            return candidate
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue
    raise RuntimeError(
        "FFmpeg not found. Install it or set the FFMPEG_PATH environment variable."
    )


def build_ffmpeg_command(
    ffmpeg_bin: str,
    video_url: str,
    rtmp_url: str,
    video_bitrate: str = "2500k",
    audio_bitrate: str = "128k",
    resolution: str = "1280x720",
    loop: bool = True,
) -> list[str]:
    """
    Build an FFmpeg command that:
    - Reads from a direct URL (no local disk required)
    - Loops infinitely if loop=True
    - Re-muxes to flv/RTMP
    - Stays within RAM limits (no copy to disk)
    """
    width, height = resolution.split("x") if "x" in resolution else ("1280", "720")

    cmd = [ffmpeg_bin]

    # Reconnect on drop (important for long-running streams)
    cmd += [
        "-reconnect", "1",
        "-reconnect_at_eof", "1",
        "-reconnect_streamed", "1",
        "-reconnect_delay_max", "30",
    ]

    if loop:
        cmd += ["-stream_loop", "-1"]

    # Read at native frame rate (critical for RTMP)
    cmd += ["-re"]

    # Input: direct URL, no local download
    cmd += ["-i", video_url]

    # Video encoding
    cmd += [
        "-vf", f"scale={width}:{height}:force_original_aspect_ratio=decrease,"
               f"pad={width}:{height}:(ow-iw)/2:(oh-ih)/2:black",
        "-c:v", "libx264",
        "-preset", "veryfast",       # low CPU on free tiers
        "-tune", "zerolatency",
        "-b:v", video_bitrate,
        "-maxrate", video_bitrate,
        "-bufsize", str(int(video_bitrate.replace("k", "")) * 2) + "k",
        "-g", "60",                  # keyframe every 2 s at 30 fps
        "-keyint_min", "30",
    ]

    # Audio encoding
    cmd += [
        "-c:a", "aac",
        "-b:a", audio_bitrate,
        "-ar", "44100",
        "-ac", "2",
    ]

    # Output: RTMP stream
    cmd += [
        "-f", "flv",
        "-flvflags", "no_duration_filesize",
        rtmp_url,
    ]

    return cmd


def _read_ffmpeg_stderr(slot_id: str, process: subprocess.Popen):
    """Background thread: read FFmpeg stderr and push lines to the log queue."""
    with streams_lock:
        slot = streams.get(slot_id)
    if not slot:
        return

    log_queue: Queue = slot["log_queue"]
    log_lines: list = slot["log_lines"]

    try:
        for raw_line in iter(process.stderr.readline, b""):
            line = raw_line.decode("utf-8", errors="replace").rstrip()
            if not line:
                continue
            timestamped = f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] {line}"
            log_lines.append(timestamped)
            if len(log_lines) > MAX_LOG_LINES:
                log_lines.pop(0)
            log_queue.put(timestamped)
            if log_queue.qsize() > LOG_QUEUE_MAX:
                try:
                    log_queue.get_nowait()
                except Empty:
                    pass
    except Exception as exc:
        logger.warning("Log reader error for slot %s: %s", slot_id, exc)
    finally:
        # Mark stream as offline when process exits
        with streams_lock:
            slot = streams.get(slot_id)
            if slot and slot.get("status") == "online":
                slot["status"] = "offline"
                slot["ended_at"] = datetime.now(timezone.utc).isoformat()
        log_queue.put(f"[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] ── FFmpeg process ended ──")
        logger.info("Slot %s stream ended.", slot_id)


# ─── API Routes ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/streams", methods=["GET"])
def list_streams():
    """Return status of all slots."""
    with streams_lock:
        result = {}
        for sid, slot in streams.items():
            uptime = None
            if slot["status"] == "online" and slot.get("started_at"):
                delta = datetime.now(timezone.utc) - slot["started_at"]
                uptime = int(delta.total_seconds())
            result[sid] = {
                "id": sid,
                "name": slot["name"],
                "status": slot["status"],
                "uptime": uptime,
                "rtmp_url": slot.get("rtmp_url", ""),
                "resolution": slot.get("resolution", ""),
                "video_bitrate": slot.get("video_bitrate", ""),
                "audio_bitrate": slot.get("audio_bitrate", ""),
                "loop": slot.get("loop", True),
                "started_at": slot["started_at"].isoformat() if slot.get("started_at") else None,
            }
    return jsonify(result)


@app.route("/api/streams", methods=["POST"])
def start_stream():
    """Start a new FFmpeg stream slot."""
    data = request.get_json(force=True)

    name = (data.get("name") or "Unnamed Stream").strip()
    video_url = (data.get("video_url") or "").strip()
    stream_key = (data.get("stream_key") or "").strip()
    platform = (data.get("platform") or "youtube").lower()
    custom_rtmp = (data.get("custom_rtmp") or "").strip()
    video_bitrate = (data.get("video_bitrate") or "2500k").strip()
    audio_bitrate = (data.get("audio_bitrate") or "128k").strip()
    resolution = (data.get("resolution") or "1280x720").strip()
    loop = bool(data.get("loop", True))

    if not video_url:
        return jsonify({"error": "video_url is required"}), 400
    if not stream_key and not custom_rtmp:
        return jsonify({"error": "stream_key or custom_rtmp is required"}), 400

    # Build RTMP target
    if custom_rtmp:
        rtmp_url = custom_rtmp
    else:
        base = RTMP_BASES.get(platform, RTMP_BASES["youtube"])
        rtmp_url = base + stream_key

    try:
        ffmpeg_bin = find_ffmpeg()
    except RuntimeError as exc:
        return jsonify({"error": str(exc)}), 500

    cmd = build_ffmpeg_command(
        ffmpeg_bin, video_url, rtmp_url,
        video_bitrate=video_bitrate,
        audio_bitrate=audio_bitrate,
        resolution=resolution,
        loop=loop,
    )

    logger.info("Starting stream '%s' → %s", name, rtmp_url)
    logger.info("CMD: %s", " ".join(cmd))

    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            preexec_fn=os.setsid if sys.platform != "win32" else None,
        )
    except Exception as exc:
        logger.error("Failed to start FFmpeg: %s", exc)
        return jsonify({"error": f"Failed to start FFmpeg: {exc}"}), 500

    slot_id = str(uuid.uuid4())
    log_queue: Queue = Queue(maxsize=LOG_QUEUE_MAX)
    log_lines: list = []

    slot = {
        "id": slot_id,
        "name": name,
        "status": "online",
        "process": process,
        "started_at": datetime.now(timezone.utc),
        "ended_at": None,
        "rtmp_url": rtmp_url,
        "video_url": video_url,
        "video_bitrate": video_bitrate,
        "audio_bitrate": audio_bitrate,
        "resolution": resolution,
        "loop": loop,
        "log_queue": log_queue,
        "log_lines": log_lines,
    }

    with streams_lock:
        streams[slot_id] = slot

    # Start stderr reader thread
    reader = threading.Thread(
        target=_read_ffmpeg_stderr,
        args=(slot_id, process),
        daemon=True,
        name=f"log-reader-{slot_id[:8]}",
    )
    reader.start()
    slot["reader_thread"] = reader

    return jsonify({
        "id": slot_id,
        "name": name,
        "status": "online",
        "rtmp_url": rtmp_url,
        "message": "Stream started successfully.",
    }), 201


@app.route("/api/streams/<slot_id>/stop", methods=["POST"])
def stop_stream(slot_id: str):
    """Gracefully terminate an FFmpeg process."""
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return jsonify({"error": "Stream not found"}), 404
        if slot["status"] != "online":
            return jsonify({"error": "Stream is not running"}), 400

    process: subprocess.Popen = slot["process"]

    try:
        if sys.platform != "win32":
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()
        try:
            process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            if sys.platform != "win32":
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                process.kill()
    except ProcessLookupError:
        pass  # Already dead — that's fine
    except Exception as exc:
        logger.warning("Error stopping slot %s: %s", slot_id, exc)

    with streams_lock:
        slot["status"] = "offline"
        slot["ended_at"] = datetime.now(timezone.utc).isoformat()

    logger.info("Slot %s stopped.", slot_id)
    return jsonify({"id": slot_id, "status": "offline", "message": "Stream stopped."})


@app.route("/api/streams/<slot_id>", methods=["DELETE"])
def delete_stream(slot_id: str):
    """Stop (if running) and remove a slot entirely."""
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return jsonify({"error": "Stream not found"}), 404

    if slot["status"] == "online":
        # Re-use stop logic
        stop_stream(slot_id)

    with streams_lock:
        streams.pop(slot_id, None)

    return jsonify({"id": slot_id, "deleted": True})


@app.route("/api/streams/<slot_id>/logs", methods=["GET"])
def get_log_history(slot_id: str):
    """Return buffered log lines for a slot."""
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return jsonify({"error": "Stream not found"}), 404
        lines = list(slot["log_lines"])
    return jsonify({"id": slot_id, "lines": lines})


@app.route("/api/streams/<slot_id>/events")
def stream_events(slot_id: str):
    """
    Server-Sent Events endpoint — pushes FFmpeg log lines in real time.
    Client: new EventSource('/api/streams/<id>/events')
    """
    with streams_lock:
        slot = streams.get(slot_id)
        if not slot:
            return Response("data: {\"error\": \"not found\"}\n\n",
                            mimetype="text/event-stream", status=404)

    log_queue: Queue = slot["log_queue"]

    def generate():
        yield "retry: 3000\n\n"  # auto-reconnect every 3 s
        # First, flush existing buffered lines
        with streams_lock:
            history = list(slot.get("log_lines", []))
        for line in history:
            yield f"data: {line}\n\n"

        # Then stream live
        while True:
            with streams_lock:
                current = streams.get(slot_id)
            if not current:
                break
            try:
                line = log_queue.get(timeout=1.0)
                yield f"data: {line}\n\n"
            except Empty:
                yield ": heartbeat\n\n"  # keep connection alive

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",   # Nginx: disable buffering
            "Connection": "keep-alive",
        },
    )


@app.route("/api/health")
def health():
    try:
        ffmpeg_bin = find_ffmpeg()
        ffmpeg_ok = True
    except RuntimeError:
        ffmpeg_bin = None
        ffmpeg_ok = False

    with streams_lock:
        active = sum(1 for s in streams.values() if s["status"] == "online")

    return jsonify({
        "status": "ok",
        "ffmpeg": ffmpeg_ok,
        "ffmpeg_path": ffmpeg_bin,
        "active_streams": active,
        "total_slots": len(streams),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    debug = os.environ.get("FLASK_DEBUG", "0") == "1"
    logger.info("StreamAdda starting on port %d (debug=%s)", port, debug)
    app.run(host="0.0.0.0", port=port, debug=debug, threaded=True)
