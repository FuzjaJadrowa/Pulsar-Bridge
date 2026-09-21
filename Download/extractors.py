import yt_dlp

from Download.apple_music_resolver import AppleMusicIE
from Download.deezer_resolver import DeezerIE
from Download.music_extractor import PulsarMusicSearchIE
from Download.spotify_resolver import SpotifyIE
from Download.vider_resolver import ViderIE

BRIDGE_EXTRACTORS = (SpotifyIE, AppleMusicIE, DeezerIE, ViderIE, PulsarMusicSearchIE)

def create_resolver_ydl(options):
    ydl = yt_dlp.YoutubeDL(options, auto_init=False)
    for extractor in BRIDGE_EXTRACTORS:
        ydl.add_info_extractor(extractor())
    ydl.add_default_info_extractors()
    return ydl


def metadata_fallback(ydl, url):
    for extractor in BRIDGE_EXTRACTORS:
        if extractor.suitable(url):
            return getattr(ydl.get_info_extractor(extractor.ie_key()), 'metadata_fallback', None)
    return None
