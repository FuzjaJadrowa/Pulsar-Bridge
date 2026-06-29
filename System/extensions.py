import os

try:
    from hachoir.parser import createParser
except Exception:
    createParser = None

try:
    from PIL import Image
except Exception:
    Image = None

VIDEO_EXTENSIONS = {
    "avi", "flv", "m4v", "mkv", "mov", "mp4", "ogm", "ogv", "ogx", "ts", "vob", "webm", "wmv"
}

AUDIO_EXTENSIONS = {
    "aac", "aiff", "flac", "m4a", "mp3", "oga", "ogg", "opus", "wav", "wma"
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


def _probe_file_content(path):
    if not os.path.isfile(path):
        return None, None
    try:
        with open(path, "rb") as f:
            header = f.read(64)
    except Exception:
        header = b""

    if header:
        if header.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png", "image"
        if header.startswith(b"\xff\xd8\xff"):
            return "jpg", "image"
        if header.startswith(b"GIF87a") or header.startswith(b"GIF89a"):
            return "gif", "image"
        if header.startswith(b"RIFF") and len(header) >= 12 and header[8:12] == b"WEBP":
            return "webp", "image"
        if header.startswith(b"RIFF") and len(header) >= 12 and header[8:12] == b"WAVE":
            return "wav", "audio"
        if header.startswith(b"BM"):
            return "bmp", "image"
        if header.startswith(b"II\x2a\x00") or header.startswith(b"MM\x00\x2a"):
            return "tiff", "image"
        if header.startswith(b"PK\x03\x04") or header.startswith(b"PK\x05\x06"):
            return "zip", "archive"
        if header.startswith(b"7z\xbc\xaf\x27\x1c"):
            return "7z", "archive"
        if header.startswith(b"Rar!\x1a\x07"):
            return "rar", "archive"
        if header.startswith(b"\x1f\x8b"):
            return "gz", "archive"
        if header.startswith(b"BZh"):
            return "bz2", "archive"
        if header.startswith(b"\xfd7zXZ\x00"):
            return "xz", "archive"
        if header.startswith(b"\x00\x01\x00\x00") or header.startswith(b"true"):
            return "ttf", "font"
        if header.startswith(b"OTTO"):
            return "otf", "font"
        if header.startswith(b"wOFF"):
            return "woff", "font"
        if header.startswith(b"wOF2"):
            return "woff2", "font"
        if header.startswith(b"fLaC"):
            return "flac", "audio"
        if header.startswith(b"OggS"):
            return "ogg", "audio"
        if header.startswith(b"ID3") or (len(header) >= 2 and header[0] == 0xFF and (header[1] & 0xE0) == 0xE0):
            return "mp3", "audio"
        if len(header) >= 12 and header[4:8] == b"ftyp":
            brand = header[8:12]
            if brand in (b"M4A ", b"M4B ", b"M4P "):
                return "m4a", "audio"
            return "mp4", "video"
        if header.startswith(b"\x1a\x45\xdf\xa3"):
            return "mkv", "video"

    if Image:
        try:
            with Image.open(path) as img:
                fmt = (img.format or "").lower()
                if fmt == "jpeg":
                    fmt = "jpg"
                if fmt in IMAGE_EXTENSIONS:
                    return fmt, "image"
        except Exception:
            pass

    if createParser:
        try:
            parser = createParser(path)
            if parser:
                try:
                    mime = parser.mime_type
                    if mime:
                        if mime.startswith("video/"):
                            return "mp4", "video"
                        if mime.startswith("audio/"):
                            return "mp3", "audio"
                        if mime.startswith("image/"):
                            return "jpg", "image"
                finally:
                    try:
                        parser.close()
                    except Exception:
                        pass
        except Exception:
            pass

    return None, None


def _detect_extension(path):
    name = os.path.basename(path)
    lowered = name.lower()
    for suffix, result in MULTI_EXTENSIONS.items():
        if lowered.endswith(suffix):
            return result
    _, ext = os.path.splitext(lowered)
    if ext:
        ext_clean = ext.lstrip(".")
        cat = _detect_category(ext_clean)
        if cat:
            return ext_clean, cat
        probed_ext, probed_cat = _probe_file_content(path)
        if probed_cat:
            return probed_ext or ext_clean, probed_cat
        return ext_clean, None

    probed_ext, probed_cat = _probe_file_content(path)
    if probed_cat:
        return probed_ext, probed_cat
    return None, None


def _detect_category(ext):
    if not ext:
        return None
    ext = ext.lower().lstrip(".")
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