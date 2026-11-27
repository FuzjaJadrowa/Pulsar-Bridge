import json
import os
import shutil
import subprocess
import sys
import threading
from collections import OrderedDict

_PROBE_CACHE_MAX = 256
_probe_cache = OrderedDict()
_probe_cache_lock = threading.Lock()

def _file_fingerprint(path):
    try:
        st = os.stat(path)
        return (os.path.realpath(path), st.st_mtime_ns, st.st_size)
    except OSError:
        return None

def resolve_ffprobe_path(ffprobe_path=None, ffmpeg_path=None):
    if ffprobe_path:
        candidate = str(ffprobe_path).strip()
        if candidate and os.path.isfile(candidate):
            return candidate
    if ffmpeg_path:
        ffmpeg_dir = os.path.dirname(str(ffmpeg_path).strip())
        if ffmpeg_dir:
            exe_name = "ffprobe.exe" if sys.platform == "win32" else "ffprobe"
            sibling = os.path.join(ffmpeg_dir, exe_name)
            if os.path.isfile(sibling):
                return sibling
    return shutil.which("ffprobe")

def _parse_float(value):
    if value is None:
        return None
    try:
        parsed = float(value)
        return parsed if parsed >= 0 else None
    except (TypeError, ValueError):
        return None


def _parse_int(value):
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def parse_ffprobe_output(raw_json):
    if not raw_json:
        return None
    data = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    if not isinstance(data, dict):
        return None

    fmt = data.get("format") if isinstance(data.get("format"), dict) else {}
    raw_streams = data.get("streams") if isinstance(data.get("streams"), list) else []

    streams = []
    for raw in raw_streams:
        if not isinstance(raw, dict):
            continue
        disposition = raw.get("disposition") if isinstance(raw.get("disposition"), dict) else {}
        tags = raw.get("tags") if isinstance(raw.get("tags"), dict) else {}
        codec_type = str(raw.get("codec_type") or "").strip().lower()
        codec_name = str(raw.get("codec_name") or "").strip().lower()
        stream = {
            "index": _parse_int(raw.get("index")),
            "codec_type": codec_type,
            "codec_name": codec_name,
            "profile": raw.get("profile"),
            "pix_fmt": raw.get("pix_fmt"),
            "time_base": raw.get("time_base"),
            "width": _parse_int(raw.get("width")),
            "height": _parse_int(raw.get("height")),
            "channels": _parse_int(raw.get("channels")),
            "sample_rate": _parse_int(raw.get("sample_rate")),
            "is_attached_pic": bool(disposition.get("attached_pic") == 1),
            "tags": {str(k): str(v) for k, v in tags.items() if v is not None},
        }
        streams.append(stream)

    fmt_tags = fmt.get("tags") if isinstance(fmt.get("tags"), dict) else {}
    return {
        "format_name": str(fmt.get("format_name") or "").strip().lower(),
        "duration_seconds": _parse_float(fmt.get("duration")),
        "bit_rate": _parse_int(fmt.get("bit_rate")),
        "tags": {str(k): str(v) for k, v in fmt_tags.items() if v is not None},
        "streams": streams,
    }

def probe_media(path, ffprobe_path=None, ffmpeg_path=None, timeout=15):
    if not path or not os.path.isfile(path):
        return None

    fp = _file_fingerprint(path)
    if fp is not None:
        with _probe_cache_lock:
            cached = _probe_cache.get(fp)
            if cached is not None:
                _probe_cache.move_to_end(fp)
                return cached

    binary = resolve_ffprobe_path(ffprobe_path=ffprobe_path, ffmpeg_path=ffmpeg_path)
    if not binary:
        return None

    creationflags = 0x08000000 if sys.platform == "win32" else 0  # CREATE_NO_WINDOW
    try:
        completed = subprocess.run(
            [
                binary,
                "-v",
                "error",
                "-show_streams",
                "-show_format",
                "-of",
                "json",
                path,
            ],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
            creationflags=creationflags,
        )
    except Exception:
        return None

    if completed.returncode != 0 or not completed.stdout.strip():
        return None

    try:
        parsed = parse_ffprobe_output(completed.stdout)
    except Exception:
        return None

    if parsed is not None and fp is not None:
        with _probe_cache_lock:
            _probe_cache[fp] = parsed
            _probe_cache.move_to_end(fp)
            while len(_probe_cache) > _PROBE_CACHE_MAX:
                _probe_cache.popitem(last=False)

    return parsed