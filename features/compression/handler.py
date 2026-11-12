import json
import os
import time
import tempfile
import glob

from core.ffmpeg.runner import run_ffmpeg_with_progress
from core.utils import emit_json, ProgressEmitter, resolve_progress_percent

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
                from core.media.extensions import _detect_extension, _detect_category
                ext, forced_cat = _detect_extension(input_path)
                detected_cat = forced_cat or _detect_category(ext)
                if not detected_cat or detected_cat not in ("video", "audio", "image"):
                    out_ext, out_forced = _detect_extension(output_path)
                    detected_cat = out_forced or _detect_category(out_ext)
                if detected_cat in ("video", "audio", "image"):
                    category = detected_cat
                else:
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

            pass_log = None
            try:
                if payload.get("two_pass") and len(ffmpeg_args) > 1:
                    descriptor, pass_log = tempfile.mkstemp(prefix="pulsar-compress-pass-")
                    os.close(descriptor)
                    os.remove(pass_log)
                    base_args = ffmpeg_args[:-1]
                    first_args = base_args + [
                        "-an", "-pass", "1", "-passlogfile", pass_log, "-f", "null", "-"
                    ]
                    second_args = base_args + [
                        "-pass", "2", "-passlogfile", pass_log, output_path
                    ]

                    def on_first_pass(data):
                        percent, eta_seconds = _resolve_progress_percent(data, total_seconds)
                        if percent is not None:
                            progress.emit(percent * 0.5, eta_seconds=eta_seconds)

                    ret = run_ffmpeg_with_progress(
                        self.task_id, ffmpeg_path, first_args, on_first_pass
                    )
                    if ret == 0:
                        def on_second_pass(data):
                            percent, eta_seconds = _resolve_progress_percent(data, total_seconds)
                            if percent is not None:
                                progress.emit(50.0 + percent * 0.5, eta_seconds=eta_seconds)

                        ret = run_ffmpeg_with_progress(
                            self.task_id, ffmpeg_path, second_args, on_second_pass
                        )
                else:
                    ret = run_ffmpeg_with_progress(
                        self.task_id, ffmpeg_path, ffmpeg_args, on_progress
                    )
            finally:
                if pass_log:
                    for path in glob.glob(pass_log + "*"):
                        try:
                            os.remove(path)
                        except OSError:
                            pass

            if ret != 0:
                detail = getattr(ret, "stderr", "").strip()
                error = f"FFmpeg compression failed (exit code {int(ret)})."
                if detail:
                    error = f"{error} {detail[-3000:]}"
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": error
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