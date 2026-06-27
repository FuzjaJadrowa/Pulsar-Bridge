import json
import time
import os
import threading

_emit_lock = threading.Lock()

def emit_json(payload):
    with _emit_lock:
        print(json.dumps(payload), flush=True)

class ProgressEmitter:
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
            payload["eta"] = int(eta_payload)
        emit_json(payload)
        self.last_emit = now

def parse_time_to_seconds(value):
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

def resolve_progress_percent(payload, total_seconds):
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
        elapsed = parse_time_to_seconds(payload.get("out_time"))
    if elapsed is None and "time" in payload:
        elapsed = parse_time_to_seconds(payload.get("time"))

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

def is_safe_path(base_dir, target_path):
    abs_base = os.path.abspath(base_dir)
    abs_target = os.path.abspath(target_path)
    return os.path.commonpath([abs_base]) == os.path.commonpath([abs_base, abs_target])