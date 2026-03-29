import os
import json
import time
import io
import base64
import shutil
import tempfile
import zipfile
import tarfile
import gzip
import bz2
import lzma

try:
    from hachoir.parser import createParser
    from hachoir.metadata import extractMetadata
except Exception:
    createParser = None
    extractMetadata = None

try:
    from PIL import Image
except Exception:
    Image = None

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


VIDEO_EXTENSIONS = {
    "3gp", "avi", "flv", "m4v", "mkv", "mov", "mp4", "ogm", "ogv", "ogx", "ts", "vob", "webm", "wmv"
}

AUDIO_EXTENSIONS = {
    "aac", "aiff", "alac", "flac", "m4a", "midi", "mp3", "oga", "ogg", "opus", "wav", "weba", "wma"
}

IMAGE_EXTENSIONS = {
    "avif", "bmp", "gif", "heic", "heif", "ico", "jfif", "jpe", "jpeg",
    "jpg", "png", "svg", "tga", "tif", "tiff", "webp", "icns"
}

ARCHIVE_EXTENSIONS = {
    "7z", "bz2", "gz", "rar", "tar", "tbz2", "tgz", "txz", "xz", "zip"
}

FONT_EXTENSIONS = {
    "otf", "ttf", "woff", "woff2"
}

MULTI_EXTENSIONS = {
    ".tar.gz": ("tar.gz", "archive"),
    ".tar.bz2": ("tar.bz2", "archive"),
    ".tar.xz": ("tar.xz", "archive")
}

def _emit(payload):
    print(json.dumps(payload), flush=True)

class _ProgressEmitter:
    def __init__(self, task_id, min_interval=0.25):
        self.task_id = task_id
        self.min_interval = min_interval
        self.last_emit = 0.0

    def emit(self, percent, status="processing", force=False):
        if percent is None:
            return
        now = time.monotonic()
        if not force and self.last_emit and (now - self.last_emit) < self.min_interval:
            return
        _emit({
            "type": "progress",
            "id": self.task_id,
            "percent": float(percent),
            "status": status
        })
        self.last_emit = now

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
        duration = metadata.get('duration')
        if not duration:
            return None
        return duration.total_seconds()
    except Exception:
        return None

def _detect_extension(path):
    name = os.path.basename(path)
    lowered = name.lower()
    for suffix, result in MULTI_EXTENSIONS.items():
        if lowered.endswith(suffix):
            return result
    _, ext = os.path.splitext(lowered)
    if not ext:
        return None, None
    return ext.lstrip("."), None

def _detect_category(ext):
    if not ext:
        return None
    if ext in VIDEO_EXTENSIONS:
        return "video"
    if ext in AUDIO_EXTENSIONS:
        return "audio"
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in ARCHIVE_EXTENSIONS:
        return "archive"
    if ext in FONT_EXTENSIONS:
        return "font"
    return None

def _extract_image_size(path):
    if not Image:
        return None, None
    try:
        with Image.open(path) as img:
            return img.size[0], img.size[1]
    except Exception:
        return None, None

class ConvertMetadataHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    def run(self, args):
        try:
            if not args:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "No file path provided"
                })
                return

            path = args[-1]
            if not os.path.isfile(path):
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "File not found"
                })
                return

            ext, forced_category = _detect_extension(path)
            category = forced_category or _detect_category(ext)
            if not category:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported format"
                })
                return

            size_bytes = os.path.getsize(path)

            duration_seconds = None
            duration_string = None
            if category in ("video", "audio"):
                duration_seconds = _extract_duration(path)
                duration_string = _format_duration(duration_seconds)
            width = None
            height = None
            if category == "image":
                width, height = _extract_image_size(path)

            base_name = os.path.basename(path)
            if ext:
                ext_suffix = f".{ext}"
                if base_name.lower().endswith(ext_suffix):
                    base_name = base_name[: -len(ext_suffix)]

            payload = {
                "type": "metadata",
                "id": self.task_id,
                "success": True,
                "data": {
                    "path": path,
                    "name": base_name,
                    "extension": ext or "",
                    "category": category,
                    "size_bytes": size_bytes
                }
            }

            if duration_seconds is not None:
                payload["data"]["duration_seconds"] = duration_seconds
            if duration_string:
                payload["data"]["duration_string"] = duration_string
            if width is not None:
                payload["data"]["width"] = width
            if height is not None:
                payload["data"]["height"] = height

            _emit(payload)
        except Exception as e:
            _emit({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": str(e)
            })


class ConvertHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    @staticmethod
    def _parse_int(value):
        try:
            parsed = int(value)
        except Exception:
            return None
        return parsed if parsed > 0 else None

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
    def _resolve_target_size(original, width, height):
        orig_w, orig_h = original
        if width and height:
            return width, height
        if width and orig_w > 0:
            scaled_h = max(1, round(orig_h * (width / orig_w)))
            return width, scaled_h
        if height and orig_h > 0:
            scaled_w = max(1, round(orig_w * (height / orig_h)))
            return scaled_w, height
        return orig_w, orig_h

    @staticmethod
    def _ensure_rgb(img):
        if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
            alpha = img.convert("RGBA")
            bg = Image.new("RGB", alpha.size, (255, 255, 255))
            bg.paste(alpha, mask=alpha.split()[-1])
            return bg
        if img.mode != "RGB":
            return img.convert("RGB")
        return img

    @staticmethod
    def _build_save_kwargs(fmt, quality, size):
        kwargs = {}
        quality_val = ConvertHandler._parse_int(quality)
        if fmt in ("JPEG", "WEBP", "AVIF", "HEIF", "HEIC") and quality_val:
            kwargs["quality"] = max(1, min(100, quality_val))
        if fmt == "PNG" and quality_val:
            compress_level = int(round((100 - max(1, min(100, quality_val))) * 9 / 100))
            kwargs["compress_level"] = max(0, min(9, compress_level))
            kwargs["optimize"] = True
        if fmt == "ICO" and size:
            kwargs["sizes"] = [size]
        return kwargs

    @staticmethod
    def _detect_archive_format(path):
        ext, forced = _detect_extension(path)
        if forced == "archive":
            return ext
        return ext

    @staticmethod
    def _archive_requires_single_file(fmt):
        return fmt in ("gz", "bz2", "xz")

    @staticmethod
    def _resolve_tar_mode(fmt, for_write=True):
        write = "w" if for_write else "r"
        if fmt in ("tar.gz", "tgz"):
            return f"{write}:gz"
        if fmt in ("tar.bz2", "tbz2"):
            return f"{write}:bz2"
        if fmt in ("tar.xz", "txz"):
            return f"{write}:xz"
        if fmt == "tar":
            return f"{write}:"
        return None

    @staticmethod
    def _safe_relpath(path, base):
        rel = os.path.relpath(path, base)
        return rel.replace("\\", "/")

    def _emit_step_progress(self, progress, base, span, current, total):
        if total <= 0:
            progress.emit(base + span)
            return
        percent = base + (span * (current / total))
        progress.emit(percent)

    def _extract_zip(self, input_path, work_dir, progress):
        with zipfile.ZipFile(input_path) as zf:
            infos = zf.infolist()
            total = sum(info.file_size for info in infos) or len(infos)
            done = 0
            for info in infos:
                zf.extract(info, work_dir)
                done += info.file_size or 1
                self._emit_step_progress(progress, 15, 40, done, total)

    def _extract_tar(self, input_path, work_dir, mode, progress):
        with tarfile.open(input_path, mode) as tf:
            members = tf.getmembers()
            total = sum(m.size for m in members) or len(members)
            done = 0
            for member in members:
                tf.extract(member, work_dir)
                done += member.size or 1
                self._emit_step_progress(progress, 15, 40, done, total)

    def _extract_7z(self, input_path, work_dir, progress):
        if not py7zr:
            raise Exception("py7zr is not available.")
        with py7zr.SevenZipFile(input_path, mode="r") as zf:
            items = zf.list()
            total = sum(item.uncompressed for item in items if item.uncompressed) or len(items)
            zf.extractall(work_dir)
            done = total if total else len(items)
            self._emit_step_progress(progress, 15, 40, done, total if total else len(items))

    def _extract_rar(self, input_path, work_dir, progress):
        if not rarfile:
            raise Exception("rarfile is not available.")
        with rarfile.RarFile(input_path) as rf:
            infos = rf.infolist()
            total = sum(info.file_size for info in infos) or len(infos)
            done = 0
            for info in infos:
                rf.extract(info, work_dir)
                done += info.file_size or 1
                self._emit_step_progress(progress, 15, 40, done, total)

    def _extract_single(self, input_path, work_dir, fmt, progress):
        opener = gzip.open if fmt == "gz" else bz2.open if fmt == "bz2" else lzma.open
        total = os.path.getsize(input_path) or 1
        name = os.path.basename(input_path)
        for suffix in (".gz", ".bz2", ".xz"):
            if name.lower().endswith(suffix):
                name = name[: -len(suffix)]
                break
        output_path = os.path.join(work_dir, name or "payload")
        done = 0
        with opener(input_path, "rb") as src, open(output_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
                done += len(chunk)
                self._emit_step_progress(progress, 15, 40, done, total)

    def _pack_zip(self, work_dir, output_path, progress):
        files = []
        for root, _, names in os.walk(work_dir):
            for name in names:
                full = os.path.join(root, name)
                files.append(full)
        total = sum(os.path.getsize(p) for p in files) or len(files)
        done = 0
        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for full in files:
                rel = self._safe_relpath(full, work_dir)
                zf.write(full, rel)
                done += os.path.getsize(full) or 1
                self._emit_step_progress(progress, 60, 35, done, total)

    def _pack_tar(self, work_dir, output_path, mode, progress):
        files = []
        for root, _, names in os.walk(work_dir):
            for name in names:
                files.append(os.path.join(root, name))
        total = sum(os.path.getsize(p) for p in files) or len(files)
        done = 0
        with tarfile.open(output_path, mode) as tf:
            for full in files:
                rel = self._safe_relpath(full, work_dir)
                tf.add(full, arcname=rel)
                done += os.path.getsize(full) or 1
                self._emit_step_progress(progress, 60, 35, done, total)

    def _pack_7z(self, work_dir, output_path, progress):
        if not py7zr:
            raise Exception("py7zr is not available.")
        files = []
        for root, _, names in os.walk(work_dir):
            for name in names:
                files.append(os.path.join(root, name))
        total = sum(os.path.getsize(p) for p in files) or len(files)
        with py7zr.SevenZipFile(output_path, "w") as zf:
            zf.writeall(work_dir, arcname=".")
        self._emit_step_progress(progress, 60, 35, total, total)

    def _pack_single(self, work_dir, output_path, fmt, progress):
        files = []
        for root, _, names in os.walk(work_dir):
            for name in names:
                files.append(os.path.join(root, name))
        if len(files) != 1:
            raise Exception("Single-file archive requires exactly one file.")
        input_path = files[0]
        opener = gzip.open if fmt == "gz" else bz2.open if fmt == "bz2" else lzma.open
        total = os.path.getsize(input_path) or 1
        done = 0
        with open(input_path, "rb") as src, opener(output_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                dst.write(chunk)
                done += len(chunk)
                self._emit_step_progress(progress, 60, 35, done, total)

    def _convert_archive(self, input_path, output_path, output_format, progress):
        input_format = self._detect_archive_format(input_path) or ""
        output_format = output_format.lower()
        temp_dir = tempfile.mkdtemp(prefix="pulsar-archive-")
        try:
            if input_format in ("zip",):
                self._extract_zip(input_path, temp_dir, progress)
            elif input_format in ("7z",):
                self._extract_7z(input_path, temp_dir, progress)
            elif input_format in ("rar",):
                self._extract_rar(input_path, temp_dir, progress)
            elif self._resolve_tar_mode(input_format, for_write=False):
                mode = self._resolve_tar_mode(input_format, for_write=False)
                self._extract_tar(input_path, temp_dir, mode, progress)
            elif input_format in ("gz", "bz2", "xz"):
                self._extract_single(input_path, temp_dir, input_format, progress)
            else:
                raise Exception("Unsupported input archive format.")

            if output_format == "zip":
                self._pack_zip(temp_dir, output_path, progress)
            elif output_format == "7z":
                self._pack_7z(temp_dir, output_path, progress)
            else:
                tar_mode = self._resolve_tar_mode(output_format, for_write=True)
                if tar_mode:
                    self._pack_tar(temp_dir, output_path, tar_mode, progress)
                elif output_format in ("gz", "bz2", "xz"):
                    self._pack_single(temp_dir, output_path, output_format, progress)
                else:
                    raise Exception("Unsupported output archive format.")
            progress.emit(100, force=True)
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def _convert_font(self, input_path, output_path, output_format, progress):
        if not TTFont:
            raise Exception("fontTools is not available.")
        fmt = output_format.lower()
        if fmt not in ("ttf", "otf", "woff", "woff2"):
            raise Exception("Unsupported output font format.")
        if fmt == "woff2" and brotli is None:
            raise Exception("WOFF2 requires brotli.")
        progress.emit(0, force=True)
        font = TTFont(input_path, recalcBBoxes=True, recalcTimestamp=False)
        progress.emit(35)
        if fmt in ("woff", "woff2"):
            font.flavor = fmt
        else:
            font.flavor = None
        progress.emit(70)
        font.save(output_path)
        progress.emit(100, force=True)

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
            if not input_path:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Input path is missing."
                })
                return
            if category and category not in ("image", "archive", "font"):
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported conversion category."
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
            if not os.path.isfile(input_path):
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "File not found."
                })
                return

            output_format = self._resolve_output_format(payload, output_path)
            if not output_format:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Output format is missing."
                })
                return
            if not category:
                if output_format in IMAGE_EXTENSIONS:
                    category = "image"
                elif output_format in ARCHIVE_EXTENSIONS or output_format in ("tar.gz", "tar.bz2", "tar.xz"):
                    category = "archive"
                elif output_format in FONT_EXTENSIONS:
                    category = "font"
            if category == "image" and not Image:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Pillow is not available."
                })
                return
            if output_format in ("psd", "ai"):
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported output format."
                })
                return
            if output_format in ("heic", "heif") and pillow_heif is None:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "HEIC/HEIF requires pillow-heif."
                })
                return

            if category == "archive":
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                self._convert_archive(input_path, output_path, output_format, progress)
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": True,
                    "output_path": output_path
                })
                return

            if category == "font":
                output_dir = os.path.dirname(output_path)
                if output_dir:
                    os.makedirs(output_dir, exist_ok=True)
                self._convert_font(input_path, output_path, output_format, progress)
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": True,
                    "output_path": output_path
                })
                return

            if output_format == "svg":
                progress.emit(0, force=True)
                with Image.open(input_path) as img:
                    progress.emit(15)
                    original_size = img.size
                    target_width = self._parse_int(payload.get("image_width"))
                    target_height = self._parse_int(payload.get("image_height"))
                    target_size = self._resolve_target_size(original_size, target_width, target_height)
                    if target_size != original_size:
                        img = img.resize(target_size, Image.LANCZOS)
                        progress.emit(55)
                    else:
                        progress.emit(35)
                    if img.mode not in ("RGBA", "RGB"):
                        img = img.convert("RGBA")
                    buffer = io.BytesIO()
                    img.save(buffer, format="PNG")
                    progress.emit(80)
                    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
                    width, height = target_size
                    svg_payload = (
                        '<?xml version="1.0" encoding="UTF-8"?>\n'
                        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" '
                        f'viewBox="0 0 {width} {height}">'
                        f'<image href="data:image/png;base64,{encoded}" width="{width}" height="{height}" />'
                        '</svg>'
                    )
                    with open(output_path, "w", encoding="utf-8") as handle:
                        handle.write(svg_payload)
                    progress.emit(100, force=True)

                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": True,
                    "output_path": output_path
                })
                return

            registered = Image.registered_extensions()
            ext_key = f".{output_format.lower()}"
            if ext_key not in registered and output_format in ("heic", "heif") and ".heif" in registered:
                ext_key = ".heif"
            if ext_key not in registered:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported output format."
                })
                return
            target_format = registered[ext_key]
            if target_format not in Image.SAVE and target_format not in Image.SAVE_ALL:
                _emit({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "Unsupported output format."
                })
                return

            output_dir = os.path.dirname(output_path)
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)

            progress.emit(0, force=True)
            with Image.open(input_path) as img:
                progress.emit(15)
                original_size = img.size
                target_width = self._parse_int(payload.get("image_width"))
                target_height = self._parse_int(payload.get("image_height"))
                target_size = self._resolve_target_size(original_size, target_width, target_height)
                if target_size != original_size:
                    img = img.resize(target_size, Image.LANCZOS)
                    progress.emit(55)
                else:
                    progress.emit(35)

                if target_format in ("JPEG", "JPG"):
                    img = self._ensure_rgb(img)

                save_kwargs = self._build_save_kwargs(target_format, payload.get("image_quality"), target_size)
                progress.emit(80)
                img.save(output_path, format=target_format, **save_kwargs)
                progress.emit(100, force=True)

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