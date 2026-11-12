import base64
import json
import re
import unicodedata
from difflib import SequenceMatcher
from urllib.parse import urlsplit

from yt_dlp.extractor.common import InfoExtractor
from yt_dlp.utils import ExtractorError

def normalize(value):
    value = unicodedata.normalize('NFKD', str(value or '').casefold().replace('ł', 'l'))
    value = ''.join(c for c in value if not unicodedata.combining(c))
    return ' '.join(re.findall(r'\w+', value.replace('_', ' ')))

def versions(value):
    text = normalize(value)
    patterns = {
        'live': r'\blive\b', 'remix': r'\b(remix|rework|bootleg)\b',
        'cover': r'\bcover\b', 'karaoke': r'\bkaraoke\b',
        'instrumental': r'\binstrumental\b', 'acoustic': r'\bacoustic\b',
        'slowed': r'\b(slowed|slowed down)\b',
        'sped': r'\b(sped up|speed up|nightcore)\b',
        'extended': r'\bextended\b', 'radio': r'\bradio edit\b',
    }
    return {key for key, pattern in patterns.items() if re.search(pattern, text)}

def candidate_score(track, candidate):
    title = normalize(track.get('title'))
    artist = normalize(track.get('artist'))
    actual = normalize(candidate.get('track') or candidate.get('title'))
    if not title or not artist or not actual or candidate.get('is_live'):
        return None
    expected_versions = versions(title.replace(artist, ''))
    actual_versions = versions(actual.replace(artist, ''))
    if expected_versions != actual_versions:
        return None
    artist_fields = ' '.join(normalize(candidate.get(key)) for key in
                             ('artist', 'uploader', 'channel', 'title'))
    artists = track.get('artists') or [track.get('artist')]
    if not all(set(normalize(name).split()) <= set(artist_fields.split()) for name in artists):
        return None
    clean = actual.replace(artist, '').strip()
    clean = re.sub(r'\b(official|audio|video|lyrics|lyric|visualizer|hd|hq|4k|music)\b', '', clean)
    clean = ' '.join(clean.split())
    coverage = len(set(title.split()) & set(clean.split())) / len(set(title.split()))
    similarity = SequenceMatcher(None, title, clean).ratio()
    if coverage < .85 or similarity < .65:
        return None
    score = 65 * similarity + 20 * coverage
    duration_ms = track.get('duration_ms')
    duration = candidate.get('duration')
    if isinstance(duration_ms, (int, float)) and duration_ms > 0:
        if not isinstance(duration, (int, float)) or duration <= 0:
            return None
        delta = abs(duration - duration_ms / 1000)
        tolerance = max(6, min(15, duration_ms / 1000 * .04))
        if delta > tolerance:
            return None
        score += 15 * (1 - delta / tolerance)
    if normalize(candidate.get('channel') or candidate.get('uploader')).endswith(' topic'):
        score += 3
    return score

def choose_candidate(track, candidates):
    ranked = [(score, candidate) for candidate in candidates
              if candidate and (score := candidate_score(track, candidate)) is not None]
    return max(ranked, key=lambda item: item[0])[1] if ranked else None


class MusicCatalogIE(InfoExtractor):
    _SOURCE = None
    _COLLECTION_TYPES = {'album', 'playlist', 'artist', 'show'}

    def _fetch_text(self, url, timeout=None):
        page, response = self._download_webpage_handle(url, None)
        return page, response.url

    def _fetch_json(self, url, timeout=None):
        text, final_url = self._fetch_text(url)
        return self._parse_json(text, None), final_url

    def _fetch_redirect(self, url, timeout=None):
        return self._fetch_text(url)[1]

    def _catalog_payload(self, url):
        raise NotImplementedError

    def _real_extract(self, url):
        payload = self._catalog_payload(url)
        if not payload or payload.get('error'):
            raise ExtractorError(
                f'{self.IE_NAME}: {payload.get("error") if payload else "Unable to read music metadata"}',
                expected=True)
        tracks = payload.get('tracks') or []
        summary = {key: payload.get(key) for key in
                   ('type', 'url', 'title', 'author', 'author_url', 'thumbnail')}
        summary['track_count'] = len(tracks)
        collection = payload.get('type') in self._COLLECTION_TYPES
        entries = []
        for track in tracks:
            title = (track.get('title') or '').strip()
            artist = (track.get('artist') or '').strip()
            if not title and not artist:
                continue
            query = f'{artist} - {title}' if title and artist else title or artist
            metadata = {
                'title': title or query, 'fulltitle': title or query,
                'uploader': artist or None, 'channel': artist or None,
                'uploader_url': payload.get('author_url'),
                'thumbnail': payload.get('thumbnail'),
                'webpage_url': track.get(f'{self._SOURCE}_url') or payload.get('url') or url,
                self._SOURCE: summary,
            }
            if collection and self.get_param('pulsar_metadata'):
                metadata.update({
                    'title': payload.get('title') or metadata['title'],
                    'fulltitle': payload.get('title') or metadata['title'],
                    'uploader': payload.get('author') or metadata['uploader'],
                    'channel': payload.get('author') or metadata['channel'],
                    'webpage_url': payload.get('url') or url,
                })
            data = {'query': f'{query} audio', 'metadata': metadata}
            if self._SOURCE == 'spotify':
                data['track'] = track
            token = base64.urlsafe_b64encode(json.dumps(data).encode()).decode().rstrip('=')
            entries.append({
                '_type': 'url_transparent', 'ie_key': PulsarMusicSearchIE.ie_key(),
                'url': f'pulsar-music-search:{token}', **metadata,
            })
            if self.get_param('pulsar_metadata'):
                break
        if not entries:
            raise ExtractorError(f'{self.IE_NAME}: no tracks could be resolved.', expected=True)
        if not collection or self.get_param('pulsar_metadata'):
            return entries[0]
        return self.playlist_result(
            entries, playlist_id=urlsplit(payload.get('url') or url).path.rstrip('/').rsplit('/', 1)[-1],
            playlist_title=payload.get('title'), webpage_url=payload.get('url') or url,
            thumbnail=payload.get('thumbnail'), uploader=payload.get('author'),
            **{self._SOURCE: summary})

class PulsarMusicSearchIE(InfoExtractor):
    IE_NAME = 'pulsar:music-search'
    _VALID_URL = r'pulsar-music-search:(?P<data>[A-Za-z0-9_-]+)$'

    @classmethod
    def _match_id(cls, url):
        return 'music-match'

    def _real_extract(self, url):
        try:
            token = self._match_valid_url(url).group('data')
            data = json.loads(base64.urlsafe_b64decode(token + '=' * (-len(token) % 4)))
            query, metadata = data['query'], data['metadata']
            if not isinstance(query, str) or not isinstance(metadata, dict):
                raise ValueError('Invalid search data')
        except (ValueError, TypeError, KeyError) as exc:
            raise ExtractorError('Invalid music search entry.', expected=True) from exc
        track = data.get('track')
        limit = 10 if track else 1
        search = self._downloader.extract_info(f'ytsearch{limit}:{query}', download=False, process=False)
        candidates = [entry for entry in (search or {}).get('entries', []) if entry]
        first = choose_candidate(track, candidates) if track else next(iter(candidates), None)
        if not first:
            raise ExtractorError(f'No sufficiently reliable YouTube match found for {query}.', expected=True)
        target = first.get('webpage_url') or first.get('url')
        if not target and first.get('id'):
            target = f'https://www.youtube.com/watch?v={first["id"]}'
        if not target:
            raise ExtractorError('YouTube search returned no video URL.', expected=True)
        return {**metadata, '_type': 'url_transparent', 'url': target, 'ie_key': first.get('ie_key')}