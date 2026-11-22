import base64
import bz2
import gzip
import io
import lzma
import os
import stat
import tarfile
import zipfile

try:
    from PIL import Image, ImageSequence
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

from core.utils import is_safe_path


def _parse_int(value):
    try:
        parsed = int(value)
    except Exception:
        return None
    return parsed if parsed > 0 else None


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


def _ensure_rgb(img):
    if img.mode in ("RGBA", "LA") or (img.mode == "P" and "transparency" in img.info):
        alpha = img.convert("RGBA")
        bg = Image.new("RGB", alpha.size, (255, 255, 255))
        bg.paste(alpha, mask=alpha.split()[-1])
        return bg
    if img.mode != "RGB":
        return img.convert("RGB")
    return img


def _build_save_kwargs(fmt, quality, size):
    kwargs = {}
    quality_val = _parse_int(quality)
    if fmt in ("JPEG", "WEBP", "AVIF", "HEIF", "HEIC") and quality_val:
        kwargs["quality"] = max(1, min(100, quality_val))
    if fmt == "PNG" and quality_val:
        compress_level = int(round((100 - max(1, min(100, quality_val))) * 9 / 100))
        kwargs["compress_level"] = max(0, min(9, compress_level))
        kwargs["optimize"] = True
    if fmt == "ICO" and size:
        kwargs["sizes"] = [size]
    return kwargs


def _emit_step(progress_cb, base, span, current, total):
    if progress_cb is None:
        return
    if total <= 0:
        progress_cb(base + span)
        return
    progress_cb(base + (span * (current / total)))


def _safe_relpath(path, base):
    rel = os.path.relpath(path, base)
    return rel.replace("\\", "/")


def _validate_archive_entries(entries, size_getter):
    if len(entries) > 100_000:
        raise Exception("Archive contains too many entries (limit: 100000).")
    total = sum(max(0, int(size_getter(entry) or 0)) for entry in entries)
    if total > 100 * 1024 * 1024 * 1024:
        raise Exception("Archive expands beyond the 100 GiB safety limit.")
    return total or len(entries)


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


def worker_convert_svg(input_path, output_path, image_width, image_height, progress_cb=None):
    if not Image:
        raise Exception("Pillow is not available.")
    if progress_cb:
        progress_cb(0, force=True)
    with Image.open(input_path) as img:
        if getattr(img, "is_animated", False):
            raise Exception("Animated images cannot be converted to SVG without losing frames.")
        if progress_cb:
            progress_cb(15)
        original_size = img.size
        target_width = _parse_int(image_width)
        target_height = _parse_int(image_height)
        target_size = _resolve_target_size(original_size, target_width, target_height)
        if target_size != original_size:
            img = img.resize(target_size, Image.LANCZOS)
            if progress_cb:
                progress_cb(55)
        elif progress_cb:
            progress_cb(35)
        if img.mode not in ("RGBA", "RGB"):
            img = img.convert("RGBA")
        buffer = io.BytesIO()
        img.save(buffer, format="PNG")
        if progress_cb:
            progress_cb(80)
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
        if progress_cb:
            progress_cb(100, force=True)
    return output_path


def worker_convert_image(
    input_path,
    output_path,
    output_format,
    target_format,
    image_width,
    image_height,
    image_quality,
    progress_cb=None,
):
    if not Image:
        raise Exception("Pillow is not available.")
    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    if progress_cb:
        progress_cb(0, force=True)
    with Image.open(input_path) as img:
        if progress_cb:
            progress_cb(15)
        original_size = img.size
        target_width = _parse_int(image_width)
        target_height = _parse_int(image_height)
        target_size = _resolve_target_size(original_size, target_width, target_height)
        save_kwargs = _build_save_kwargs(target_format, image_quality, target_size)
        frame_count = int(getattr(img, "n_frames", 1))
        if frame_count > 1 and target_format not in Image.SAVE_ALL:
            raise Exception(f"{output_format.upper()} does not support animated image output.")

        frames = []
        durations = []
        if frame_count > 1:
            for frame in ImageSequence.Iterator(img):
                current = frame.copy()
                if current.size != target_size:
                    current = current.resize(target_size, Image.LANCZOS)
                if target_format in ("JPEG", "JPG"):
                    current = _ensure_rgb(current)
                frames.append(current)
                durations.append(frame.info.get("duration", img.info.get("duration", 0)))
        else:
            current = img.copy()
            if current.size != target_size:
                current = current.resize(target_size, Image.LANCZOS)
            if target_format in ("JPEG", "JPG"):
                current = _ensure_rgb(current)

        if progress_cb:
            progress_cb(55 if target_size != original_size else 35)
            progress_cb(80)
        if frame_count > 1:
            save_kwargs["save_all"] = True
            save_kwargs["append_images"] = frames[1:]
            if target_format in ("GIF", "PNG", "WEBP"):
                save_kwargs["duration"] = durations
                if "loop" in img.info:
                    save_kwargs["loop"] = img.info["loop"]
            try:
                frames[0].save(output_path, format=target_format, **save_kwargs)
            finally:
                for frame in frames:
                    frame.close()
        else:
            try:
                current.save(output_path, format=target_format, **save_kwargs)
            finally:
                current.close()
        if progress_cb:
            progress_cb(100, force=True)
    return output_path


def _resolve_animated_quality_score(compress_mode, target_percent, target_size_bytes, source_size_bytes, crf):
    """Map compression settings to a 1..100 quality score for animated images."""
    mode = str(compress_mode or "percent").strip().lower()
    if mode == "quality":
        try:
            crf_val = max(0, min(51, int(crf if crf is not None else 26)))
        except (TypeError, ValueError):
            crf_val = 26
        return max(10, min(100, int(round(100.0 - (crf_val / 51.0) * 85.0))))
    if mode == "size" and target_size_bytes and source_size_bytes and source_size_bytes > 0:
        pct = (float(target_size_bytes) / float(source_size_bytes)) * 100.0
        return max(10, min(100, int(round(pct))))
    try:
        pct = float(target_percent if target_percent is not None else 60.0)
    except (TypeError, ValueError):
        pct = 60.0
    return max(10, min(100, int(round(pct))))


def _quantize_rgba_for_gif(rgba_frame, max_colors):
    """Quantize an RGBA frame to palette mode while preserving full transparency."""
    colors = max(16, min(255, int(max_colors)))
    alpha = rgba_frame.getchannel("A")
    transparent_mask = Image.eval(alpha, lambda a: 255 if a < 128 else 0)
    has_transparency = transparent_mask.getbbox() is not None

    if has_transparency:
        # Reserve palette index 255 for transparency.
        rgb = rgba_frame.convert("RGB")
        quantized = rgb.quantize(colors=min(colors, 255), method=Image.Quantize.MEDIANCUT)
        quantized.paste(255, mask=transparent_mask)
        return quantized, 255

    rgb = rgba_frame.convert("RGB")
    quantized = rgb.quantize(colors=min(colors + 1, 256), method=Image.Quantize.MEDIANCUT)
    return quantized, None


def worker_compress_animated_image(
    input_path,
    output_path,
    output_format,
    compress_mode,
    target_percent,
    target_size_bytes,
    source_size_bytes,
    crf,
    progress_cb=None,
):
    """Compress an animated image (GIF, WebP, APNG, multi-frame TIFF) preserving all frames,
    per-frame durations, loop count, disposal, and transparency (C04).
    """
    if not Image or not ImageSequence:
        raise Exception("Pillow is not available.")

    output_dir = os.path.dirname(output_path)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    if progress_cb:
        progress_cb(0, force=True)

    fmt_clean = str(output_format or "").strip().lstrip(".").lower()
    registered = Image.registered_extensions()
    ext_key = f".{fmt_clean}" if fmt_clean else os.path.splitext(output_path)[1].lower()
    target_format = registered.get(ext_key)

    with Image.open(input_path) as img:
        if not target_format:
            target_format = img.format
        frame_count = int(getattr(img, "n_frames", 1))
        if frame_count > 1 and target_format not in Image.SAVE_ALL:
            raise Exception(
                f"Animated {(fmt_clean or str(target_format)).upper()} compression is not supported without losing frames."
            )

        quality_score = _resolve_animated_quality_score(
            compress_mode, target_percent, target_size_bytes, source_size_bytes, crf
        )

        frames = []
        durations = []
        disposals = []
        default_duration = img.info.get("duration", 100)
        loop = img.info.get("loop", 0)
        background = img.info.get("background")
        gif_transparency_index = None

        # Map quality 10..100 to 32..255 colors for GIF palette optimization.
        gif_colors = max(32, min(255, int(round(32 + (quality_score / 100.0) * 223))))

        try:
            for idx, raw_frame in enumerate(ImageSequence.Iterator(img)):
                frame_duration = raw_frame.info.get("duration", default_duration)
                durations.append(frame_duration)
                disposal = getattr(raw_frame, "disposal_method", raw_frame.info.get("disposal", 2))
                disposals.append(disposal)

                if target_format == "GIF":
                    rgba = raw_frame.convert("RGBA")
                    quantized, trans_idx = _quantize_rgba_for_gif(rgba, gif_colors)
                    rgba.close()
                    if trans_idx is not None:
                        gif_transparency_index = trans_idx
                    frames.append(quantized)
                elif target_format in ("WEBP", "PNG"):
                    # Keep full RGBA to preserve alpha channel across all frames.
                    rgba = raw_frame.convert("RGBA")
                    frames.append(rgba)
                else:
                    frames.append(raw_frame.copy())

                _emit_step(progress_cb, 10, 65, idx + 1, max(1, frame_count))

            if progress_cb:
                progress_cb(80)

            save_kwargs = {
                "save_all": True,
                "append_images": frames[1:],
            }

            if target_format == "GIF":
                save_kwargs["duration"] = durations
                save_kwargs["loop"] = loop
                save_kwargs["optimize"] = True
                if any(d is not None for d in disposals):
                    save_kwargs["disposal"] = [d if d is not None else 2 for d in disposals]
                if gif_transparency_index is not None:
                    save_kwargs["transparency"] = gif_transparency_index
                elif background is not None:
                    save_kwargs["background"] = background
            elif target_format == "WEBP":
                save_kwargs["duration"] = durations
                save_kwargs["loop"] = loop
                save_kwargs["quality"] = quality_score
                save_kwargs["alpha_quality"] = max(50, quality_score)
                save_kwargs["method"] = 4
                save_kwargs["minimize_size"] = True
                if isinstance(background, (list, tuple)) and len(background) == 4:
                    save_kwargs["background"] = tuple(int(c) for c in background)
                else:
                    save_kwargs["background"] = (0, 0, 0, 0)
            elif target_format == "PNG":
                compress_level = max(0, min(9, int(round((100 - quality_score) * 9 / 100))))
                save_kwargs["duration"] = durations
                save_kwargs["loop"] = loop
                save_kwargs["optimize"] = True
                save_kwargs["compress_level"] = compress_level
                if any(d is not None for d in disposals):
                    save_kwargs["disposal"] = [d if d is not None else 0 for d in disposals]

            frames[0].save(output_path, format=target_format, **save_kwargs)
        finally:
            for frame in frames:
                try:
                    frame.close()
                except Exception:
                    pass

    if progress_cb:
        progress_cb(100, force=True)
    return output_path


def worker_convert_archive(input_path, output_path, input_format, output_format, temp_dir, progress_cb=None):
    input_format = (input_format or "").lower()
    output_format = (output_format or "").lower()

    if input_format == "zip":
        with zipfile.ZipFile(input_path) as zf:
            infos = zf.infolist()
            total = _validate_archive_entries(infos, lambda info: info.file_size)
            done = 0
            for info in infos:
                target_path = os.path.join(temp_dir, info.filename)
                if not is_safe_path(temp_dir, target_path):
                    raise Exception(f"Unsafe path in archive: {info.filename}")
                mode = (info.external_attr >> 16) & 0xFFFF
                if stat.S_ISLNK(mode):
                    raise Exception(f"Symbolic links are not supported in archives: {info.filename}")
                zf.extract(info, temp_dir)
                done += info.file_size or 1
                _emit_step(progress_cb, 15, 40, done, total)
    elif input_format == "7z":
        if not py7zr:
            raise Exception("py7zr is not available.")
        with py7zr.SevenZipFile(input_path, mode="r") as zf:
            items = zf.list()
            total = _validate_archive_entries(items, lambda item: item.uncompressed)
            for item in items:
                target_path = os.path.join(temp_dir, item.filename)
                if not is_safe_path(temp_dir, target_path):
                    raise Exception(f"Unsafe path in archive: {item.filename}")
            zf.extractall(temp_dir)
            _emit_step(progress_cb, 15, 40, total, total)
    elif input_format == "rar":
        if not rarfile:
            raise Exception("rarfile is not available.")
        with rarfile.RarFile(input_path) as rf:
            infos = rf.infolist()
            total = _validate_archive_entries(infos, lambda info: info.file_size)
            done = 0
            for info in infos:
                target_path = os.path.join(temp_dir, info.filename)
                if not is_safe_path(temp_dir, target_path):
                    raise Exception(f"Unsafe path in archive: {info.filename}")
                is_symlink = getattr(info, "is_symlink", None)
                if callable(is_symlink) and is_symlink():
                    raise Exception(f"Symbolic links are not supported in archives: {info.filename}")
                rf.extract(info, temp_dir)
                done += info.file_size or 1
                _emit_step(progress_cb, 15, 40, done, total)
    elif _resolve_tar_mode(input_format, for_write=False):
        mode = _resolve_tar_mode(input_format, for_write=False)
        with tarfile.open(input_path, mode) as tf:
            members = tf.getmembers()
            total = _validate_archive_entries(members, lambda member: member.size)
            done = 0
            for member in members:
                target_path = os.path.join(temp_dir, member.name)
                if not is_safe_path(temp_dir, target_path):
                    raise Exception(f"Unsafe path in archive: {member.name}")
                if not (member.isfile() or member.isdir()):
                    raise Exception(f"Links and special files are not supported in archives: {member.name}")
                tf.extract(member, temp_dir)
                done += member.size or 1
                _emit_step(progress_cb, 15, 40, done, total)
    elif input_format in ("gz", "bz2", "xz"):
        opener = gzip.open if input_format == "gz" else bz2.open if input_format == "bz2" else lzma.open
        total = os.path.getsize(input_path) or 1
        name = os.path.basename(input_path)
        for suffix in (".gz", ".bz2", ".xz"):
            if name.lower().endswith(suffix):
                name = name[: -len(suffix)]
                break
        extracted_path = os.path.join(temp_dir, name or "payload")
        done = 0
        with opener(input_path, "rb") as src, open(extracted_path, "wb") as dst:
            while True:
                chunk = src.read(1024 * 1024)
                if not chunk:
                    break
                done += len(chunk)
                if done > 100 * 1024 * 1024 * 1024:
                    raise Exception("Archive expands beyond the 100 GiB safety limit.")
                dst.write(chunk)
                _emit_step(progress_cb, 15, 40, done, total)
    else:
        raise Exception("Unsupported input archive format.")

    if output_format == "zip":
        files = []
        for root, _, names in os.walk(temp_dir):
            for name in names:
                files.append(os.path.join(root, name))
        total = sum(os.path.getsize(p) for p in files) or len(files)
        done = 0
        with zipfile.ZipFile(output_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for full in files:
                rel = _safe_relpath(full, temp_dir)
                zf.write(full, rel)
                done += os.path.getsize(full) or 1
                _emit_step(progress_cb, 60, 35, done, total)
    elif output_format == "7z":
        if not py7zr:
            raise Exception("py7zr is not available.")
        files = []
        for root, _, names in os.walk(temp_dir):
            for name in names:
                files.append(os.path.join(root, name))
        total = sum(os.path.getsize(p) for p in files) or len(files)
        with py7zr.SevenZipFile(output_path, "w") as zf:
            zf.writeall(temp_dir, arcname=".")
        _emit_step(progress_cb, 60, 35, total, total)
    else:
        tar_mode = _resolve_tar_mode(output_format, for_write=True)
        if tar_mode:
            files = []
            for root, _, names in os.walk(temp_dir):
                for name in names:
                    files.append(os.path.join(root, name))
            total = sum(os.path.getsize(p) for p in files) or len(files)
            done = 0
            with tarfile.open(output_path, tar_mode) as tf:
                for full in files:
                    rel = _safe_relpath(full, temp_dir)
                    tf.add(full, arcname=rel)
                    done += os.path.getsize(full) or 1
                    _emit_step(progress_cb, 60, 35, done, total)
        elif output_format in ("gz", "bz2", "xz"):
            files = []
            for root, _, names in os.walk(temp_dir):
                for name in names:
                    files.append(os.path.join(root, name))
            if len(files) != 1:
                raise Exception("Single-file archive requires exactly one file.")
            single_input = files[0]
            opener = gzip.open if output_format == "gz" else bz2.open if output_format == "bz2" else lzma.open
            total = os.path.getsize(single_input) or 1
            done = 0
            with open(single_input, "rb") as src, opener(output_path, "wb") as dst:
                while True:
                    chunk = src.read(1024 * 1024)
                    if not chunk:
                        break
                    dst.write(chunk)
                    done += len(chunk)
                    _emit_step(progress_cb, 60, 35, done, total)
        else:
            raise Exception("Unsupported output archive format.")

    if progress_cb:
        progress_cb(100, force=True)
    return output_path

def worker_convert_font(input_path, output_path, output_format, progress_cb=None):
    if not TTFont:
        raise Exception("fontTools is not available.")
    fmt = output_format.lower()
    if fmt not in ("ttf", "otf", "woff", "woff2"):
        raise Exception("Unsupported output font format.")
    if fmt == "woff2" and brotli is None:
        raise Exception("WOFF2 requires brotli.")
    if progress_cb:
        progress_cb(0, force=True)
    font = TTFont(input_path, recalcBBoxes=True, recalcTimestamp=False)
    if progress_cb:
        progress_cb(35)
    if fmt in ("woff", "woff2"):
        font.flavor = fmt
    else:
        font.flavor = None
    if progress_cb:
        progress_cb(70)
    font.save(output_path)
    if progress_cb:
        progress_cb(100, force=True)
    return output_path