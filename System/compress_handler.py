import json
import os
import time

from System.ffmpeg_runner import run_ffmpeg_with_progress

def _emit(payload):
    print(json.dumps(payload), flush=True)

class _ProgressEmitter:
    def __init__(self, task_id, min_interval=0.25):
        self.task_id = task_id
        self.min_interval = min_interval
        self.last_emit = 0.0
        self.start_time = time.monotonic()

    def emit(self, percent, status="processing", force=False, eta_seconds=None):
        if percent is None:
            return
        try:
            percent_val = float(percent)
        except Exception:
            return
        if percent_val < 0:
            percent_val = 0.0
        if percent_val > 100:
            percent_val = 100.0
        now = time.monotonic()
        if not force and self.last_emit and (now - self.last_emit) < self.min_interval:
            return
        eta_payload = None
        if eta_seconds is not None:
            try:
                eta_payload = float(eta_seconds)
                if eta_payload < 0:
                    eta_payload = 0.0
            except Exception:
                eta_payload = None
        if eta_payload is None and 0 < percent_val < 100:
            elapsed = now - self.start_time
            if elapsed > 0:
                total = elapsed / (percent_val / 100.0)
                eta_payload = max(0.0, total - elapsed)
        payload = {
            "type": "progress",
            "id": self.task_id,
            "percent": percent_val,
            "status": status
        }
        if eta_payload is not None:
            payload["eta_seconds"] = eta_payload
        _emit(payload)
        self.last_emit = now

def _parse_time_to_seconds(value):
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit():
        return float(raw)
    parts = raw.split(":")
    if not parts:
        return None
    try:
        parts = [float(p) for p in parts]
    except Exception:
        return None
    total = 0.0
    for idx, part in enumerate(reversed(parts)):
        total += part * (60 ** idx)
    return total

def _resolve_progress_percent(payload, total_seconds):
    if payload is None:
        return None, None
    if isinstance(payload, (int, float)):
        return float(payload), None
    if not isinstance(payload, dict):
        return None, None
    percent = None
    elapsed = None
    if "out_time_ms" in payload:
        try:
            elapsed = float(payload["out_time_ms"]) / 1000.0
        except Exception:
            elapsed = None
    if elapsed is None and "out_time_us" in payload:
        try:
            elapsed = float(payload["out_time_us"]) / 1_000_000.0
        except Exception:
            elapsed = None
    if elapsed is None and "out_time" in payload:
        elapsed = _parse_time_to_seconds(payload.get("out_time"))
    if elapsed is None and "time" in payload:
        elapsed = _parse_time_to_seconds(payload.get("time"))

    for key in ("percent", "percentage"):
        if key in payload:
            try:
                percent = float(payload[key])
            except Exception:
                percent = None
            break
    if percent is None and "progress" in payload and isinstance(payload["progress"], (int, float)):
        percent = float(payload["progress"])
    if percent is not None and percent <= 1.0 and (payload.get("percent_is_ratio") or payload.get("progress_is_ratio")):
        percent *= 100.0
    if total_seconds and total_seconds > 0:
        if elapsed is not None:
            if percent is None:
                percent = (elapsed / total_seconds) * 100.0
            eta_seconds = max(0.0, total_seconds - elapsed)
            return percent, eta_seconds
    return percent, None

class CompressHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    def run(self, args, payload=None):
        progress = _ProgressEmitter(self.task_id)
        try:
            if payload is None:
                payload = None
                if args:
                    try:
                        payload = json.loads(args[0])
                    except Exception:
                        payload = {}
                if payload is None:
                    payload = {}

            input_path = str(payload.get("input_path") or "").strip()
            output_path = str(payload.get("output_path") or "").strip()
            category = str(payload.get("category") or "").strip().lower()
            ffmpeg_path = str(payload.get("ffmpeg_path") or "").strip()
            ffmpeg_args = payload.get("ffmpeg_args")
            if not input_path:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Input path is missing."
                })
                return
            if not os.path.isfile(input_path):
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "File not found."
                })
                return
            if category not in ("video", "audio", "image"):
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported compression category."
                })
                return
            if not output_path:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Output path is missing."
                })
                return
            if not ffmpeg_path:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "FFmpeg path is missing."
                })
                return
            if not isinstance(ffmpeg_args, list) or not ffmpeg_args:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "FFmpeg args are missing."
                })
                return

            total_seconds = None
            try:
                if payload.get("source_duration_seconds") is not None:
                    total_seconds = float(payload.get("source_duration_seconds"))
            except Exception:
                total_seconds = None

            progress.emit(0, force=True)

            def on_progress(data):
                percent, eta_seconds = _resolve_progress_percent(data, total_seconds)
                if percent is not None:
                    progress.emit(percent, eta_seconds=eta_seconds)

            ret = run_ffmpeg_with_progress(self.task_id, ffmpeg_path, ffmpeg_args, on_progress)
            if ret != 0:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "FFmpeg compression failed."
                })
                return

            _emit({
                "type": "finished",
                "id": self.task_id,
                "success": True,
                "output_path": output_path
            })
        except Exception as e:
            _emit({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": str(e)
            })