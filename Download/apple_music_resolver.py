import json
import re
import urllib.parse
import urllib.request

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"


class AppleMusicUnsupportedError(Exception):
    pass


def is_apple_music_url(url):
    if not url:
        return False
    lowered = str(url).lower()
    return "music.apple.com" in lowered or "itunes.apple.com" in lowered or "apple.co/" in lowered


def _fetch_url_text(url, timeout=6):
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace"), response.geturl()


def _resolve_redirect(url):
    try:
        _, final_url = _fetch_url_text(url, timeout=6)
        return final_url or url
    except Exception:
        return url


def _normalize_artwork(url):
    if not url:
        return None
    return re.sub(r"/\d+x\d+bb", "/600x600bb", url)


def _itunes_lookup(item_id, country=None, entity=None):
    if not item_id:
        return None
    params = {"id": str(item_id)}
    if country:
        params["country"] = country
    if entity:
        params["entity"] = entity
    query = urllib.parse.urlencode(params)
    url = f"https://itunes.apple.com/lookup?{query}"
    try:
        text, _ = _fetch_url_text(url, timeout=8)
        return json.loads(text)
    except Exception:
        return None


def _fetch_oembed(url):
    try:
        oembed_url = "https://embed.music.apple.com/oembed?url=" + urllib.parse.quote(url)
        text, _ = _fetch_url_text(oembed_url, timeout=6)
        return json.loads(text)
    except Exception:
        return None


def parse_apple_music_url(raw_url):
    if not raw_url:
        return None
    url = str(raw_url).strip()
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None

    host = (parsed.hostname or "").lower()
    if host.endswith("apple.co"):
        return parse_apple_music_url(_resolve_redirect(url))

    if not (host.endswith("music.apple.com") or host.endswith("itunes.apple.com")):
        return None

    parts = [p for p in parsed.path.split("/") if p]
    country = None
    if parts and len(parts[0]) == 2:
        country = parts[0]
        parts = parts[1:]
    if not parts:
        return None

    kind = parts[0]
    if kind == "playlist":
        return {"type": "playlist", "url": url, "country": country}

    if kind == "album":
        collection_id = _extract_trailing_id(parts[-1])
        track_id = urllib.parse.parse_qs(parsed.query).get("i", [None])[0]
        if track_id:
            return {"type": "track", "url": url, "country": country, "track_id": track_id, "collection_id": collection_id}
        return {"type": "album", "url": url, "country": country, "collection_id": collection_id}

    if kind == "song":
        track_id = _extract_trailing_id(parts[-1])
        return {"type": "track", "url": url, "country": country, "track_id": track_id}

    return None


def _extract_trailing_id(value):
    match = re.search(r"(\d+)$", str(value))
    return match.group(1) if match else None


def _build_tracks_from_itunes(results):
    tracks = []
    for item in results or []:
        if item.get("wrapperType") != "track":
            continue
        title = item.get("trackName")
        artist = item.get("artistName")
        duration = item.get("trackTimeMillis")
        url = item.get("trackViewUrl")
        if not title:
            continue
        tracks.append({
            "title": title,
            "artist": artist,
            "duration_ms": duration,
            "apple_music_url": url
        })
    return tracks


def _build_payload(parsed):
    if not parsed:
        return None

    if parsed["type"] == "playlist":
        return {"error": "unsupported link"}

    country = parsed.get("country")
    oembed = _fetch_oembed(parsed.get("url"))

    title = None
    author = None
    author_url = None
    thumbnail = None
    tracks = []

    if parsed["type"] == "track":
        lookup = _itunes_lookup(parsed.get("track_id"), country=country)
        if lookup and lookup.get("results"):
            track_item = next((r for r in lookup["results"] if r.get("wrapperType") == "track"), None)
            if track_item:
                title = track_item.get("trackName")
                author = track_item.get("artistName")
                author_url = track_item.get("artistViewUrl")
                thumbnail = _normalize_artwork(track_item.get("artworkUrl100"))
                tracks = _build_tracks_from_itunes([track_item])

    if parsed["type"] == "album":
        lookup = _itunes_lookup(parsed.get("collection_id"), country=country, entity="song")
        if lookup and lookup.get("results"):
            collection = next((r for r in lookup["results"] if r.get("wrapperType") == "collection"), None)
            if collection:
                title = collection.get("collectionName")
                author = collection.get("artistName")
                author_url = collection.get("artistViewUrl")
                thumbnail = _normalize_artwork(collection.get("artworkUrl100"))
            tracks = _build_tracks_from_itunes(lookup.get("results"))

    if oembed:
        title = title or oembed.get("title")
        author = author or oembed.get("author_name")
        author_url = author_url or oembed.get("author_url")
        thumbnail = thumbnail or oembed.get("thumbnail_url")

    if not tracks and title:
        tracks = [{"title": title, "artist": author, "duration_ms": None, "apple_music_url": parsed.get("url")}]

    return {
        "type": parsed["type"],
        "url": parsed.get("url"),
        "title": title,
        "author": author,
        "author_url": author_url,
        "thumbnail": thumbnail,
        "tracks": tracks
    }


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


def resolve_apple_music_for_download(urls):
    resolved = []
    for url in urls:
        if not is_apple_music_url(url):
            resolved.append(url)
            continue
        parsed = parse_apple_music_url(url)
        if parsed and parsed.get("type") == "playlist":
            raise AppleMusicUnsupportedError("unsupported link")
        payload = _build_payload(parsed)
        if payload and payload.get("error"):
            raise AppleMusicUnsupportedError(payload.get("error"))
        queries = build_youtube_queries(payload)
        if queries:
            resolved.extend(queries)
        else:
            resolved.append(url)
    return resolved


def resolve_apple_music_for_metadata(url):
    if not is_apple_music_url(url):
        return None
    parsed = parse_apple_music_url(url)
    if parsed and parsed.get("type") == "playlist":
        return {"error": "unsupported link"}
    payload = _build_payload(parsed)
    if not payload:
        return None
    if payload.get("error"):
        return {"error": payload.get("error")}
    queries = build_youtube_queries(payload)
    return {
        "apple_music": payload,
        "yt_query": queries[0] if queries else None
    }