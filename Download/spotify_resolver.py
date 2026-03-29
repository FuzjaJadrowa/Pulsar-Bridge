import json
import re
import urllib.parse
import urllib.request

_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
_SPOTIFY_CACHE = {}
_SPOTIFY_CACHE_ORDER = []
_SPOTIFY_CACHE_LIMIT = 24


def is_spotify_url(url):
    if not url:
        return False
    lowered = str(url).lower()
    return "spotify:" in lowered or "spotify.com" in lowered or "spotify.link" in lowered


def _cache_get(key):
    if key in _SPOTIFY_CACHE:
        if key in _SPOTIFY_CACHE_ORDER:
            _SPOTIFY_CACHE_ORDER.remove(key)
        _SPOTIFY_CACHE_ORDER.append(key)
        return _SPOTIFY_CACHE[key]
    return None


def _cache_set(key, value):
    if key in _SPOTIFY_CACHE:
        _SPOTIFY_CACHE[key] = value
        if key in _SPOTIFY_CACHE_ORDER:
            _SPOTIFY_CACHE_ORDER.remove(key)
        _SPOTIFY_CACHE_ORDER.append(key)
        return
    _SPOTIFY_CACHE[key] = value
    _SPOTIFY_CACHE_ORDER.append(key)
    while len(_SPOTIFY_CACHE_ORDER) > _SPOTIFY_CACHE_LIMIT:
        oldest = _SPOTIFY_CACHE_ORDER.pop(0)
        _SPOTIFY_CACHE.pop(oldest, None)


def _fetch_url_text(url, timeout=6):
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace"), response.geturl()


def _resolve_spotify_redirect(url):
    try:
        _, final_url = _fetch_url_text(url, timeout=6)
        return final_url or url
    except Exception:
        return url


def _normalize_spotify_path(path):
    path = re.sub(r"^/intl-[a-z]{2}/", "/", path)
    parts = [p for p in path.split("/") if p]
    if parts and parts[0] == "embed":
        parts = parts[1:]
    return parts


def parse_spotify_url(raw_url):
    if not raw_url:
        return None
    url = str(raw_url).strip()
    allowed_types = {"track", "playlist", "album", "artist", "episode", "show"}
    if url.startswith("spotify:"):
        parts = url.split(":")
        if len(parts) >= 3:
            if parts[1] not in allowed_types:
                return None
            return {"type": parts[1], "id": parts[2], "url": build_spotify_url(parts[1], parts[2])}
        return None

    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return None

    host = (parsed.hostname or "").lower()
    if host.endswith("spotify.link"):
        return parse_spotify_url(_resolve_spotify_redirect(url))

    if not host.endswith("spotify.com"):
        return None

    parts = _normalize_spotify_path(parsed.path)
    if len(parts) < 2:
        return None

    item_type = parts[0]
    item_id = parts[1]
    if item_type not in allowed_types:
        return None
    return {"type": item_type, "id": item_id, "url": build_spotify_url(item_type, item_id)}


def build_spotify_url(item_type, item_id):
    if not item_type or not item_id:
        return None
    return f"https://open.spotify.com/{item_type}/{item_id}"


def _spotify_uri_to_url(uri):
    if not uri or not isinstance(uri, str):
        return None
    if not uri.startswith("spotify:"):
        return None
    parts = uri.split(":")
    if len(parts) < 3:
        return None
    return build_spotify_url(parts[1], parts[2])


def _pick_best_image(sources):
    if not sources:
        return None
    def score(src):
        h = src.get("height") or src.get("maxHeight") or 0
        w = src.get("width") or src.get("maxWidth") or 0
        return (h * w, h, w)
    best = max(sources, key=score)
    return best.get("url")


def _fetch_spotify_oembed(url):
    try:
        oembed_url = "https://open.spotify.com/oembed?url=" + urllib.parse.quote(url)
        text, _ = _fetch_url_text(oembed_url, timeout=6)
        return json.loads(text)
    except Exception:
        return None


def _extract_next_data(html):
    marker = '__NEXT_DATA__" type="application/json">'
    start = html.find(marker)
    if start == -1:
        return None
    start += len(marker)
    end = html.find("</script>", start)
    if end == -1:
        return None
    json_str = html[start:end]
    try:
        return json.loads(json_str)
    except Exception:
        return None


def _fetch_spotify_embed_entity(item_type, item_id):
    if not item_type or not item_id:
        return None
    embed_url = f"https://open.spotify.com/embed/{item_type}/{item_id}"
    try:
        html, _ = _fetch_url_text(embed_url, timeout=8)
    except Exception:
        return None
    data = _extract_next_data(html)
    if not data:
        return None
    try:
        return data["props"]["pageProps"]["state"]["data"]["entity"]
    except Exception:
        return None


def _build_tracks_from_entity(entity, fallback_url=None):
    tracks = []
    if not entity:
        return tracks

    track_list = entity.get("trackList") or []
    if track_list:
        for item in track_list:
            title = item.get("title") or item.get("name")
            artist = item.get("subtitle")
            duration = item.get("duration")
            uri = item.get("uri")
            track_url = _spotify_uri_to_url(uri) or fallback_url
            if title:
                tracks.append({
                    "title": title,
                    "artist": artist,
                    "duration_ms": duration,
                    "spotify_url": track_url
                })
        return tracks

    if entity.get("type") == "track":
        title = entity.get("title") or entity.get("name")
        artists = entity.get("artists") or []
        artist_names = [a.get("name") for a in artists if a.get("name")]
        artist = artist_names[0] if artist_names else None
        duration = entity.get("duration")
        uri = entity.get("uri")
        track_url = _spotify_uri_to_url(uri) or fallback_url
        if title:
            tracks.append({
                "title": title,
                "artist": artist,
                "duration_ms": duration,
                "spotify_url": track_url
            })
    return tracks


def _build_spotify_payload(raw_url):
    parsed = parse_spotify_url(raw_url)
    if not parsed:
        return None

    canonical_url = parsed["url"]
    cached = _cache_get(canonical_url)
    if cached:
        return cached

    oembed = _fetch_spotify_oembed(canonical_url) if canonical_url else None
    entity = _fetch_spotify_embed_entity(parsed["type"], parsed["id"])

    title = None
    author = None
    author_url = None
    thumbnail = None

    if oembed:
        title = oembed.get("title") or title
        author = oembed.get("author_name") or author
        author_url = oembed.get("author_url") or author_url
        thumbnail = oembed.get("thumbnail_url") or thumbnail

    if entity:
        title = title or entity.get("title") or entity.get("name")
        if not author:
            if entity.get("type") == "track":
                artists = entity.get("artists") or []
                artist_names = [a.get("name") for a in artists if a.get("name")]
                author = artist_names[0] if artist_names else None
            else:
                author = entity.get("subtitle") or author
        if not author_url:
            related = entity.get("relatedEntityUri")
            author_url = _spotify_uri_to_url(related) or author_url

        if not thumbnail:
            if entity.get("visualIdentity") and entity["visualIdentity"].get("image"):
                thumbnail = _pick_best_image(entity["visualIdentity"]["image"])
            elif entity.get("coverArt") and entity["coverArt"].get("sources"):
                thumbnail = _pick_best_image(entity["coverArt"]["sources"])

    tracks = _build_tracks_from_entity(entity, canonical_url)
    if not tracks and title:
        tracks = [{"title": title, "artist": author, "duration_ms": None, "spotify_url": canonical_url}]

    payload = {
        "type": parsed["type"],
        "url": canonical_url,
        "title": title,
        "author": author,
        "author_url": author_url,
        "thumbnail": thumbnail,
        "tracks": tracks
    }
    _cache_set(canonical_url, payload)
    return payload


def build_youtube_queries(spotify_payload):
    if not spotify_payload:
        return []
    tracks = spotify_payload.get("tracks") or []
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


def resolve_spotify_for_download(urls):
    resolved = []
    for url in urls:
        if not is_spotify_url(url):
            resolved.append(url)
            continue
        payload = _build_spotify_payload(url)
        if not payload:
            resolved.append(url)
            continue
        queries = build_youtube_queries(payload)
        if queries:
            resolved.extend(queries)
        else:
            resolved.append(url)
    return resolved


def resolve_spotify_for_metadata(url):
    if not is_spotify_url(url):
        return None
    payload = _build_spotify_payload(url)
    if not payload:
        return None
    queries = build_youtube_queries(payload)
    return {
        "spotify": payload,
        "yt_query": queries[0] if queries else None
    }