import json
import re
import urllib.parse
import urllib.request

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
_MAX_TRACKS = 200


def is_deezer_url(url):
    if not url:
        return False
    lowered = str(url).lower()
    return (
        "deezer.com" in lowered
        or "deezer.page.link" in lowered
        or "dzr.page.link" in lowered
        or "link.deezer.com" in lowered
        or "dzr.fm" in lowered
    )


def _fetch_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        text = response.read().decode("utf-8", errors="replace")
        return json.loads(text), response.geturl()


def _fetch_url(url, timeout=6):
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.geturl()


def _resolve_redirect(url):
    try:
        final_url = _fetch_url(url, timeout=6)
        return final_url or url
    except Exception:
        return url


def parse_deezer_url(raw_url):
    if not raw_url:
        return None
    url = str(raw_url).strip()
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None

    host = (parsed.hostname or "").lower()
    if host.endswith("deezer.page.link") or host.endswith("dzr.page.link") or host.endswith("link.deezer.com") or host.endswith("dzr.fm"):
        return parse_deezer_url(_resolve_redirect(url))

    if not host.endswith("deezer.com"):
        return None

    parts = [p for p in parsed.path.split("/") if p]
    if len(parts) < 2:
        return None
    item_type = parts[0]
    item_id = _extract_trailing_id(parts[1])
    if not item_id:
        return None
    if item_type not in {"track", "album", "playlist"}:
        return None
    return {"type": item_type, "id": item_id, "url": url}


def _extract_trailing_id(value):
    match = re.search(r"(\d+)$", str(value))
    return match.group(1) if match else None


def _fetch_tracklist(url):
    tracks = []
    next_url = url
    while next_url and len(tracks) < _MAX_TRACKS:
        try:
            data, _ = _fetch_json(next_url, timeout=8)
        except Exception:
            break
        batch = data.get("data") or []
        tracks.extend(batch)
        next_url = data.get("next")
    return tracks[:_MAX_TRACKS]


def _build_track_payload(item):
    if not item:
        return None
    title = item.get("title")
    artist = (item.get("artist") or {}).get("name")
    duration = item.get("duration")
    url = item.get("link")
    return {
        "title": title,
        "artist": artist,
        "duration_ms": duration * 1000 if isinstance(duration, int) else None,
        "deezer_url": url
    }


def _build_payload(parsed):
    if not parsed:
        return None
    item_type = parsed["type"]
    item_id = parsed["id"]
    base_url = parsed.get("url")

    if item_type == "track":
        try:
            track, _ = _fetch_json(f"https://api.deezer.com/track/{item_id}")
        except Exception:
            return None
        payload_track = _build_track_payload(track)
        return {
            "type": "track",
            "url": base_url,
            "title": track.get("title"),
            "author": (track.get("artist") or {}).get("name"),
            "author_url": (track.get("artist") or {}).get("link"),
            "thumbnail": (track.get("album") or {}).get("cover_xl") or (track.get("album") or {}).get("cover_big"),
            "tracks": [payload_track] if payload_track else []
        }

    if item_type == "album":
        try:
            album, _ = _fetch_json(f"https://api.deezer.com/album/{item_id}")
        except Exception:
            return None
        tracks_data = album.get("tracks", {}).get("data") or []
        tracklist_url = album.get("tracklist")
        if tracklist_url and len(tracks_data) < album.get("nb_tracks", 0):
            tracks_data = _fetch_tracklist(tracklist_url)
        tracks = [_build_track_payload(item) for item in tracks_data]
        tracks = [t for t in tracks if t]
        return {
            "type": "album",
            "url": base_url,
            "title": album.get("title"),
            "author": (album.get("artist") or {}).get("name"),
            "author_url": (album.get("artist") or {}).get("link"),
            "thumbnail": album.get("cover_xl") or album.get("cover_big"),
            "tracks": tracks
        }

    if item_type == "playlist":
        try:
            playlist, _ = _fetch_json(f"https://api.deezer.com/playlist/{item_id}")
        except Exception:
            return None
        tracks_data = playlist.get("tracks", {}).get("data") or []
        tracklist_url = playlist.get("tracklist")
        if tracklist_url and (playlist.get("nb_tracks", 0) > len(tracks_data)):
            tracks_data = _fetch_tracklist(tracklist_url)
        tracks = [_build_track_payload(item) for item in tracks_data]
        tracks = [t for t in tracks if t]
        creator = playlist.get("creator") or {}
        return {
            "type": "playlist",
            "url": base_url,
            "title": playlist.get("title"),
            "author": creator.get("name"),
            "author_url": creator.get("link"),
            "thumbnail": playlist.get("picture_xl") or playlist.get("picture_big"),
            "tracks": tracks
        }

    return None


def build_youtube_queries(payload):
    if not payload:
        return []
    tracks = payload.get("tracks") or []
    queries = []
    for track in tracks:
        title = (track.get("title") or "").strip()
        artist = (track.get("artist") or "").strip()
        if not title and not artist:
            continue
        if title and artist:
            query = f"{artist} - {title}"
        else:
            query = title or artist
        queries.append(f"ytsearch1:{query} audio")
    return queries


def resolve_deezer_for_download(urls):
    resolved = []
    for url in urls:
        if not is_deezer_url(url):
            resolved.append(url)
            continue
        parsed = parse_deezer_url(url)
        payload = _build_payload(parsed)
        queries = build_youtube_queries(payload)
        if queries:
            resolved.extend(queries)
        else:
            resolved.append(url)
    return resolved


def resolve_deezer_for_metadata(url):
    if not is_deezer_url(url):
        return None
    parsed = parse_deezer_url(url)
    payload = _build_payload(parsed)
    if not payload:
        return None
    queries = build_youtube_queries(payload)
    return {
        "deezer": payload,
        "yt_query": queries[0] if queries else None
    }