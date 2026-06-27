import json
import os
import time

from System.ffmpeg_runner import run_ffmpeg_with_progress
from System.utils import emit_json, ProgressEmitter, resolve_progress_percent

_emit = emit_json
_ProgressEmitter = ProgressEmitter
_resolve_progress_percent = resolve_progress_percent

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