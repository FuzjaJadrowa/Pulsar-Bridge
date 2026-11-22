import glob
import json
import os
import tempfile

try:
    from PIL import Image
except Exception:
    Image = None

from core.ffmpeg.runner import run_ffmpeg_with_progress
from core.tasking import TaskCancelledError, run_in_worker_process
from core.utils import emit_json, ProgressEmitter, parse_ffmpeg_speed, resolve_progress_percent
from core.worker import worker_compress_animated_image

_emit = emit_json
_ProgressEmitter = ProgressEmitter
_resolve_progress_percent = resolve_progress_percent


def _detect_animated_image(path):
    if not Image or not path or not os.path.isfile(path):
        return False
    try:
        with Image.open(path) as img:
            frame_count = int(getattr(img, "n_frames", 1) or 1)
            return bool(getattr(img, "is_animated", False)) or frame_count > 1
    except Exception:
        return False


class CompressHandler:
    def __init__(self, task_id, cancel_token=None, task_context=None):
        self.task_id = task_id
        self.cancel_token = cancel_token or (task_context.cancel_token if task_context else None)
        self.task_context = task_context

    def _emit_terminal(self, payload):
        if self.task_context is not None:
            self.task_context.emit_terminal(payload)
        else:
            _emit(payload)

    def run(self, args, payload=None):
        progress = _ProgressEmitter(self.task_id)
        try:
            if self.cancel_token is not None:
                self.cancel_token.check()

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
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Input path is missing.",
                })
                return
            if not os.path.isfile(input_path):
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "File not found.",
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
                    self._emit_terminal({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "Unsupported compression category.",
                    })
                    return
            if not output_path:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Output path is missing.",
                })
                return

            if category == "image" and (
                bool(payload.get("is_animated")) or _detect_animated_image(input_path)
            ):
                output_format = str(payload.get("output_format") or "").strip().lstrip(".").lower()
                if not output_format:
                    _, ext = os.path.splitext(output_path)
                    output_format = ext.lstrip(".").lower()
                run_in_worker_process(
                    worker_compress_animated_image,
                    (
                        input_path,
                        output_path,
                        output_format,
                        payload.get("compress_mode"),
                        payload.get("target_percent"),
                        payload.get("target_size_bytes"),
                        payload.get("source_size_bytes"),
                        payload.get("crf"),
                    ),
                    cancel_token=self.cancel_token,
                    progress_callback=progress.emit,
                )
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": True,
                    "output_path": output_path,
                })
                return

            if not ffmpeg_path:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "FFmpeg path is missing.",
                })
                return
            if not isinstance(ffmpeg_args, list) or not ffmpeg_args:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "FFmpeg args are missing.",
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
                            scaled_eta = None
                            if eta_seconds is not None:
                                effective_total = total_seconds or data.get("duration_seconds")
                                speed = parse_ffmpeg_speed(data.get("speed"))
                                if effective_total and effective_total > 0 and speed:
                                    scaled_eta = eta_seconds + (float(effective_total) / speed)
                            progress.emit(percent * 0.5, eta_seconds=scaled_eta)

                    ret = run_ffmpeg_with_progress(
                        self.task_id,
                        ffmpeg_path,
                        first_args,
                        on_first_pass,
                        cancel_token=self.cancel_token,
                    )
                    if self.cancel_token is not None:
                        self.cancel_token.check()
                    if ret == 0:
                        def on_second_pass(data):
                            percent, eta_seconds = _resolve_progress_percent(data, total_seconds)
                            if percent is not None:
                                progress.emit(50.0 + percent * 0.5, eta_seconds=eta_seconds)

                        ret = run_ffmpeg_with_progress(
                            self.task_id,
                            ffmpeg_path,
                            second_args,
                            on_second_pass,
                            cancel_token=self.cancel_token,
                        )
                else:
                    ret = run_ffmpeg_with_progress(
                        self.task_id,
                        ffmpeg_path,
                        ffmpeg_args,
                        on_progress,
                        cancel_token=self.cancel_token,
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
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": error,
                })
                return

            self._emit_terminal({
                "type": "finished",
                "id": self.task_id,
                "success": True,
                "output_path": output_path,
            })
        except TaskCancelledError:
            if self.task_context is not None:
                raise
            self._emit_terminal({"type": "cancelled", "id": self.task_id})
        except Exception as e:
            self._emit_terminal({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": str(e),
            })