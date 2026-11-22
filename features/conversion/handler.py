import os
import json
import shutil
import tempfile

try:
    from hachoir.parser import createParser
    from hachoir.metadata import extractMetadata
except Exception:
    createParser = None
    extractMetadata = None

try:
    from PIL import Image
    from PIL import ImageSequence
except Exception:
    Image = None
    ImageSequence = None

try:
    import pillow_heif
    pillow_heif.register_heif_opener()
except Exception:
    pillow_heif = None

try:
    import py7zr
except Exception:
    py7zr = None

try:
    import rarfile
except Exception:
    rarfile = None

try:
    from fontTools.ttLib import TTFont
except Exception:
    TTFont = None

try:
    import brotli
except Exception:
    brotli = None

from core.ffmpeg.runner import run_ffmpeg_with_progress
from core.media.probe import probe_media
from core.tasking import TaskCancelledError, run_in_worker_process
from core.utils import emit_json, ProgressEmitter, resolve_progress_percent
from core.worker import (
    worker_convert_archive,
    worker_convert_font,
    worker_convert_image,
    worker_convert_svg,
)

_emit = emit_json
_ProgressEmitter = ProgressEmitter


def _format_duration(seconds):
    if seconds is None:
        return None
    try:
        total = int(float(seconds))
    except (TypeError, ValueError):
        return None
    if total < 0:
        total = 0
    hrs = total // 3600
    mins = (total % 3600) // 60
    secs = total % 60
    if hrs > 0:
        return f"{hrs:02d}:{mins:02d}:{secs:02d}"
    return f"{mins:02d}:{secs:02d}"


def _extract_duration(path):
    if not createParser or not extractMetadata:
        return None
    try:
        parser = createParser(path)
        if not parser:
            return None
        try:
            metadata = extractMetadata(parser)
        finally:
            try:
                parser.close()
            except Exception:
                pass
        if not metadata:
            return None
        duration = metadata.get("duration")
        if not duration:
            return None
        return duration.total_seconds()
    except Exception:
        return None


from core.media.extensions import (
    VIDEO_EXTENSIONS,
    AUDIO_EXTENSIONS,
    IMAGE_EXTENSIONS,
    ARCHIVE_EXTENSIONS,
    FONT_EXTENSIONS,
    _detect_extension,
    _detect_category,
)


def _extract_image_info(path):
    if not Image:
        return None, None, False, 1
    try:
        with Image.open(path) as img:
            width, height = img.size[0], img.size[1]
            frame_count = int(getattr(img, "n_frames", 1) or 1)
            is_animated = bool(getattr(img, "is_animated", False)) or frame_count > 1
            return width, height, is_animated, frame_count
    except Exception:
        return None, None, False, 1


def _parse_metadata_args(args):
    ffprobe_path = None
    file_path = None
    idx = 0
    while idx < len(args):
        arg = args[idx]
        if arg == "--ffprobe-location" and idx + 1 < len(args):
            ffprobe_path = args[idx + 1]
            idx += 2
            continue
        file_path = arg
        idx += 1
    return file_path, ffprobe_path


class ConvertMetadataHandler:
    def __init__(self, task_id, cancel_token=None, task_context=None):
        self.task_id = task_id
        self.cancel_token = cancel_token or (task_context.cancel_token if task_context else None)
        self.task_context = task_context

    def _emit_terminal(self, payload):
        if self.task_context is not None:
            self.task_context.emit_terminal(payload)
        else:
            _emit(payload)

    def run(self, args):
        try:
            if self.cancel_token is not None:
                self.cancel_token.check()

            if not args:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "code": "MISSING_PATH",
                    "error": "No file path provided",
                })
                return

            path, ffprobe_path = _parse_metadata_args(args)
            if not path or not os.path.isfile(path):
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "code": "FILE_NOT_FOUND",
                    "error": "File not found",
                })
                return

            ext, forced_category = _detect_extension(path)
            category = forced_category or _detect_category(ext)
            if not category:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "code": "UNSUPPORTED_FORMAT",
                    "error": "Unsupported format",
                })
                return

            size_bytes = os.path.getsize(path)

            duration_seconds = None
            duration_string = None
            probed_media = None
            if category in ("video", "audio"):
                probed_media = probe_media(path, ffprobe_path=ffprobe_path)
                if self.cancel_token is not None:
                    self.cancel_token.check()
                if probed_media and probed_media.get("duration_seconds") is not None:
                    duration_seconds = probed_media["duration_seconds"]
                else:
                    duration_seconds = _extract_duration(path)
                duration_string = _format_duration(duration_seconds)

            width = None
            height = None
            is_animated = False
            frame_count = 1
            if category == "image":
                width, height, is_animated, frame_count = _extract_image_info(path)

            if self.cancel_token is not None:
                self.cancel_token.check()

            base_name = os.path.basename(path)
            if ext:
                ext_suffix = f".{ext}"
                if base_name.lower().endswith(ext_suffix):
                    base_name = base_name[: -len(ext_suffix)]

            data = {
                "path": path,
                "name": base_name,
                "extension": ext or "",
                "category": category,
                "size_bytes": size_bytes,
            }

            if duration_seconds is not None:
                data["duration_seconds"] = duration_seconds
            if duration_string:
                data["duration_string"] = duration_string
            if width is not None:
                data["width"] = width
            if height is not None:
                data["height"] = height
            if category == "image":
                data["is_animated"] = is_animated
                data["frame_count"] = frame_count

            if probed_media is not None:
                streams = probed_media.get("streams") or []
                data["format_name"] = probed_media.get("format_name") or ""
                data["streams"] = streams
                video_streams = [
                    s for s in streams
                    if s.get("codec_type") == "video" and not s.get("is_attached_pic")
                ]
                audio_streams = [
                    s for s in streams
                    if s.get("codec_type") == "audio"
                ]
                if video_streams:
                    if video_streams[0].get("codec_name"):
                        data["video_codec"] = video_streams[0]["codec_name"]
                    if data.get("width") is None and video_streams[0].get("width"):
                        data["width"] = video_streams[0]["width"]
                    if data.get("height") is None and video_streams[0].get("height"):
                        data["height"] = video_streams[0]["height"]
                if audio_streams and audio_streams[0].get("codec_name"):
                    data["audio_codec"] = audio_streams[0]["codec_name"]

            self._emit_terminal({
                "type": "metadata",
                "id": self.task_id,
                "success": True,
                "data": data,
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


class ConvertHandler:
    def __init__(self, task_id, cancel_token=None, task_context=None):
        self.task_id = task_id
        self.cancel_token = cancel_token or (task_context.cancel_token if task_context else None)
        self.task_context = task_context

    def _emit_terminal(self, payload):
        if self.task_context is not None:
            self.task_context.emit_terminal(payload)
        else:
            _emit(payload)

    @staticmethod
    def _normalize_format(value):
        if not value:
            return ""
        raw = str(value).strip().lower()
        while raw.startswith("."):
            raw = raw[1:]
        return raw

    @staticmethod
    def _resolve_output_format(payload, output_path):
        fmt = ConvertHandler._normalize_format(payload.get("output_format"))
        if fmt:
            return fmt
        if output_path:
            _, ext = os.path.splitext(output_path)
            return ext.lstrip(".").lower()
        return ""

    @staticmethod
    def _detect_archive_format(path):
        ext, _ = _detect_extension(path)
        return ext

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
            if not input_path:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Input path is missing.",
                })
                return
            if category and category not in ("image", "archive", "font", "video", "audio"):
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported conversion category.",
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
            if not os.path.isfile(input_path):
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "File not found.",
                })
                return

            output_format = self._resolve_output_format(payload, output_path)
            if not output_format:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Output format is missing.",
                })
                return
            if not category:
                if output_format in VIDEO_EXTENSIONS:
                    category = "video"
                elif output_format in AUDIO_EXTENSIONS:
                    category = "audio"
                elif output_format in IMAGE_EXTENSIONS:
                    category = "image"
                elif output_format in ARCHIVE_EXTENSIONS or output_format in ("tar.gz", "tar.bz2", "tar.xz"):
                    category = "archive"
                elif output_format in FONT_EXTENSIONS:
                    category = "font"
            if category == "image" and not Image:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Pillow is not available.",
                })
                return
            if output_format in ("psd", "ai"):
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported output format.",
                })
                return
            if output_format in ("heic", "heif") and pillow_heif is None:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "HEIC/HEIF requires pillow-heif.",
                })
                return

            if category == "archive":
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                input_format = self._detect_archive_format(input_path) or ""
                temp_dir = tempfile.mkdtemp(prefix="pulsar-archive-")
                try:
                    run_in_worker_process(
                        worker_convert_archive,
                        (input_path, output_path, input_format, output_format, temp_dir),
                        cancel_token=self.cancel_token,
                        progress_callback=progress.emit,
                    )
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": True,
                    "output_path": output_path,
                })
                return

            if category == "font":
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                run_in_worker_process(
                    worker_convert_font,
                    (input_path, output_path, output_format),
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

            if category in ("video", "audio"):
                ffmpeg_path = str(payload.get("ffmpeg_path") or "").strip()
                ffmpeg_args = payload.get("ffmpeg_args")
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
                    percent, eta_seconds = resolve_progress_percent(data, total_seconds)
                    if percent is not None:
                        progress.emit(percent, eta_seconds=eta_seconds)

                ret = run_ffmpeg_with_progress(
                    self.task_id,
                    ffmpeg_path,
                    ffmpeg_args,
                    on_progress,
                    cancel_token=self.cancel_token,
                )
                if ret != 0:
                    detail = getattr(ret, "stderr", "").strip()
                    error = f"FFmpeg conversion failed (exit code {int(ret)})."
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
                return

            if output_format == "svg":
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                run_in_worker_process(
                    worker_convert_svg,
                    (
                        input_path,
                        output_path,
                        payload.get("image_width"),
                        payload.get("image_height"),
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

            registered = Image.registered_extensions()
            ext_key = f".{output_format.lower()}"
            if ext_key not in registered and output_format in ("heic", "heif") and ".heif" in registered:
                ext_key = ".heif"
            if ext_key not in registered:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported output format.",
                })
                return
            target_format = registered[ext_key]
            if target_format not in Image.SAVE and target_format not in Image.SAVE_ALL:
                self._emit_terminal({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported output format.",
                })
                return

            run_in_worker_process(
                worker_convert_image,
                (
                    input_path,
                    output_path,
                    output_format,
                    target_format,
                    payload.get("image_width"),
                    payload.get("image_height"),
                    payload.get("image_quality"),
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