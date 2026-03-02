import yt_dlp
import json
from System.ytmusic_search import YTMusicSearchHandler
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_popen_patch import patch_ffmpeg_popen_for_progress
from System.spotify_resolver import resolve_spotify_for_download, resolve_spotify_for_metadata, is_spotify_url
from System.apple_music_resolver import resolve_apple_music_for_download, resolve_apple_music_for_metadata, AppleMusicUnsupportedError, is_apple_music_url
from System.deezer_resolver import resolve_deezer_for_download, resolve_deezer_for_metadata, is_deezer_url

class BridgeLogger:
    def __init__(self, task_id):
        self.task_id = task_id
        self.ffmpeg_parser = FFMpegOutputParser()
        self.last_error = None
        self.last_warning = None

    def debug(self, msg):
        ffmpeg_data = self.ffmpeg_parser.parse_progress_line(msg)
        if ffmpeg_data:
            payload = {
                "type": "progress_ffmpeg",
                "id": self.task_id,
                "status": "processing"
            }
            payload.update(ffmpeg_data)
            print(json.dumps(payload), flush=True)
        elif not msg.startswith('[download] '):
            pass

    def info(self, msg):
        pass

    def warning(self, msg):
        self.last_warning = msg
        print(json.dumps({
            "type": "log",
            "level": "warning",
            "id": self.task_id,
            "message": msg
        }), flush=True)

    def error(self, msg):
        self.last_error = msg
        print(json.dumps({
            "type": "log",
            "level": "error",
            "id": self.task_id,
            "message": msg
        }), flush=True)


class DownloadHandler:
    def __init__(self, task_id):
        self.task_id = task_id
        self.expected_playlist_count = None
        self.current_playlist_index = 1

    @staticmethod
    def _parse_int(value):
        try:
            return int(value)
        except Exception:
            return None

    def _extract_playlist_progress(self, d):
        info = d.get('info_dict') or {}
        index = (
            d.get('playlist_index')
            or info.get('playlist_index')
            or info.get('playlist_autonumber')
        )
        count = (
            d.get('playlist_count')
            or info.get('playlist_count')
            or info.get('n_entries')
            or info.get('playlist_size')
        )

        index = self._parse_int(index)
        count = self._parse_int(count)

        if index is None and count is None and self.expected_playlist_count:
            return self.current_playlist_index, self.expected_playlist_count
        if index is None and count is None:
            return 1, 1
        if index is None and count is not None:
            index = min(1, count)
        if count is None and index is not None:
            count = max(1, index)
        return index, count

    def _progress_hook(self, d):
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)

            percent = 0
            if total > 0:
                percent = (downloaded / total) * 100

            item_index, item_count = self._extract_playlist_progress(d)

            msg = {
                "type": "progress",
                "id": self.task_id,
                "percent": percent,
                "eta": d.get('eta', 0),
                "speed": d.get('speed', 0),
                "filename": d.get('filename', ''),
                "item_index": item_index,
                "item_count": item_count,
                "status": "downloading"
            }
            print(json.dumps(msg), flush=True)

        elif d['status'] == 'finished':
            print(json.dumps({
                "type": "status",
                "id": self.task_id,
                "msg": "File downloaded, starting post-processing..."
            }), flush=True)
            if self.expected_playlist_count and self.current_playlist_index < self.expected_playlist_count:
                self.current_playlist_index += 1

    def run(self, args_list):
        logger = BridgeLogger(self.task_id)
        try:
            extra_args = ["--remote-components", "ejs:github"]
            final_args = extra_args + args_list

            parsed_args = yt_dlp.parse_options(final_args)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]
            original_urls = list(urls)

            try:
                urls = resolve_spotify_for_download(urls)
                urls = resolve_apple_music_for_download(urls)
                urls = resolve_deezer_for_download(urls)
            except AppleMusicUnsupportedError as e:
                raise Exception(str(e))

            expanded_from_non_yt = any(
                is_spotify_url(u) or is_apple_music_url(u) or is_deezer_url(u)
                for u in original_urls
            )
            if expanded_from_non_yt and len(urls) > 1:
                self.expected_playlist_count = len(urls)
                self.current_playlist_index = 1

            if 'progress_hooks' not in ydl_opts:
                ydl_opts['progress_hooks'] = []
            ydl_opts['progress_hooks'].append(self._progress_hook)

            ydl_opts['logger'] = logger
            ydl_opts['no_color'] = True
            ydl_opts['ignoreerrors'] = False

            with patch_ffmpeg_popen_for_progress(self.task_id):
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    retcode = ydl.download(urls)

            if retcode != 0:
                raise Exception(f"yt-dlp exited with error code {retcode}")

            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": True
            }), flush=True)

        except SystemExit:
            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": "Cancelled"
            }), flush=True)
        except Exception as e:
            error_msg = logger.last_error or str(e)
            if "yt-dlp exited with error code" in error_msg:
                error_msg = "Download failed."

            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": error_msg
            }), flush=True)

class MetadataHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    def _filter_metadata(self, info, force_subtitle_langs=False):
        keys_to_keep = [
            'id', 'title', 'fulltitle', 'thumbnail', 'description',
            'uploader', 'uploader_id', 'uploader_url',
            'upload_date', 'duration', 'duration_string',
            'view_count', 'like_count', 'comment_count',
            'age_limit', 'is_live', 'was_live', 'availability',
            'channel', 'channel_follower_count', 'webpage_url', 'spotify', 'apple_music', 'deezer'
        ]
        filtered = {k: info.get(k) for k in keys_to_keep if k in info}

        if 'formats' in info:
            clean_formats = []
            for f in info['formats']:
                clean_f = {
                    'format_id': f.get('format_id'),
                    'ext': f.get('ext'),
                    'resolution': f.get('resolution'),
                    'filesize': f.get('filesize'),
                    'filesize_approx': f.get('filesize_approx'),
                    'vcodec': f.get('vcodec'),
                    'acodec': f.get('acodec'),
                    'note': f.get('format_note'),
                }
                clean_formats.append(clean_f)
            filtered['formats'] = clean_formats

        if 'subtitles' in info and info['subtitles']:
            filtered['subtitles_langs'] = list(info['subtitles'].keys())
        elif force_subtitle_langs:
            filtered['subtitles_langs'] = []

        if 'automatic_captions' in info and info['automatic_captions']:
            filtered['auto_captions_langs'] = list(info['automatic_captions'].keys())
        elif force_subtitle_langs:
            filtered['auto_captions_langs'] = []

        return filtered

    @staticmethod
    def _build_youtube_url(entry):
        if not entry:
            return None
        url = entry.get('webpage_url') or entry.get('url')
        if isinstance(url, str) and url.startswith("http"):
            return url
        video_id = entry.get('id') if isinstance(entry, dict) else None
        if video_id:
            return f"https://www.youtube.com/watch?v={video_id}"
        return None

    def _ensure_full_info(self, ydl, info):
        if not info:
            return info

        if info.get('_type') == 'playlist' and info.get('entries'):
            first = next((e for e in info['entries'] if e), None)
            if not first:
                return info
            if isinstance(first, dict):
                if first.get('_type') in ('url', 'url_transparent') or (
                    not first.get('subtitles') and not first.get('automatic_captions')
                ):
                    url = self._build_youtube_url(first)
                    if url:
                        try:
                            return ydl.extract_info(url, download=False)
                        except Exception:
                            return first
            return first

        if info.get('_type') in ('url', 'url_transparent') or (
            not info.get('subtitles') and not info.get('automatic_captions')
        ):
            url = self._build_youtube_url(info)
            if url:
                try:
                    return ydl.extract_info(url, download=False)
                except Exception:
                    return info

        return info

    @staticmethod
    def _is_youtube_playlist_url(url):
        if not isinstance(url, str):
            return False
        lowered = url.lower()
        if "list=" not in lowered:
            return False
        return ("youtube.com" in lowered) or ("youtu.be" in lowered) or ("music.youtube.com" in lowered)

    def run(self, args):
        logger = BridgeLogger(self.task_id)
        try:
            extra_args = ["--remote-components", "ejs:github"]
            final_args = extra_args + args

            parsed_args = yt_dlp.parse_options(final_args)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]

            if not urls:
                print(json.dumps({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "No URL provided for metadata"
                }), flush=True)
                return

            spotify_meta = None
            apple_meta = None
            deezer_meta = None
            force_subs_output = False

            if is_spotify_url(urls[0]):
                spotify_payload = resolve_spotify_for_metadata(urls[0])
                if not spotify_payload:
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "unsupported link"
                    }), flush=True)
                    return
                spotify_meta = spotify_payload.get("spotify")
                resolved = spotify_payload.get("yt_query")
                if not resolved and spotify_meta:
                    artist = (spotify_meta.get("author") or "").strip()
                    title = (spotify_meta.get("title") or "").strip()
                    if title or artist:
                        query = f"{artist} - {title}" if title and artist else (title or artist)
                        resolved = f"ytsearch1:{query} audio"
                if resolved:
                    urls = [resolved]
                else:
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "unable to resolve youtube query"
                    }), flush=True)
                    return
                force_subs_output = True

            elif is_apple_music_url(urls[0]):
                apple_payload = resolve_apple_music_for_metadata(urls[0])
                if not apple_payload:
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "unsupported link"
                    }), flush=True)
                    return
                if apple_payload.get("error"):
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": apple_payload.get("error")
                    }), flush=True)
                    return
                apple_meta = apple_payload.get("apple_music")
                resolved = apple_payload.get("yt_query")
                if resolved:
                    urls = [resolved]
                else:
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "unable to resolve youtube query"
                    }), flush=True)
                    return
                force_subs_output = True

            elif is_deezer_url(urls[0]):
                deezer_payload = resolve_deezer_for_metadata(urls[0])
                if not deezer_payload:
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "unsupported link"
                    }), flush=True)
                    return
                if deezer_payload.get("error"):
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": deezer_payload.get("error")
                    }), flush=True)
                    return
                deezer_meta = deezer_payload.get("deezer")
                resolved = deezer_payload.get("yt_query")
                if resolved:
                    urls = [resolved]
                else:
                    print(json.dumps({
                        "type": "finished",
                        "id": self.task_id,
                        "success": False,
                        "error": "unable to resolve youtube query"
                    }), flush=True)
                    return
                force_subs_output = True

            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'logger': logger,
                'simulate': True,
                'skip_download': True,
                'writesubtitles': True,
                'writeautomaticsub': True,
                **ydl_opts,
            }

            if args:
                try:
                    parsed_args = yt_dlp.parse_options(final_args)
                    user_opts = parsed_args[3]
                    ydl_opts.update(user_opts)

                    ydl_opts.update({
                        'quiet': True,
                        'no_warnings': True,
                        'simulate': True,
                        'skip_download': True,
                        'logger': logger,
                        'extract_flat': False,
                        'writesubtitles': True,
                        'writeautomaticsub': True
                    })
                except Exception as e:
                    print(json.dumps({"type": "error", "message": f"Args error: {str(e)}"}), flush=True)
                    return

            if self._is_youtube_playlist_url(urls[0]):
                ydl_opts['extract_flat'] = 'in_playlist'
                ydl_opts['playlistend'] = 1
                ydl_opts['playlist_items'] = '1'

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(urls[0], download=False)

                info = self._ensure_full_info(ydl, info)
                if not info:
                    raise Exception("No metadata extracted")

                clean_info = ydl.sanitize_info(info)
                minimized_info = self._filter_metadata(clean_info, force_subtitle_langs=force_subs_output)

            if spotify_meta:
                tracks = spotify_meta.get("tracks") or []
                primary_track = tracks[0] if tracks else {}
                meta_type = (spotify_meta.get("type") or "").lower()
                if meta_type in ("playlist", "album", "artist", "show"):
                    title = spotify_meta.get("title")
                    author = spotify_meta.get("author")
                else:
                    title = primary_track.get("title") or spotify_meta.get("title")
                    author = primary_track.get("artist") or spotify_meta.get("author")
                author_url = spotify_meta.get("author_url")
                thumbnail = spotify_meta.get("thumbnail")
                if meta_type in ("playlist", "album", "artist", "show"):
                    spotify_url = spotify_meta.get("url")
                else:
                    spotify_url = primary_track.get("spotify_url") or spotify_meta.get("url")

                if title:
                    minimized_info["title"] = title
                    minimized_info["fulltitle"] = title
                if author:
                    minimized_info["uploader"] = author
                    minimized_info["channel"] = author
                if author_url:
                    minimized_info["uploader_url"] = author_url
                if thumbnail:
                    minimized_info["thumbnail"] = thumbnail
                if spotify_url:
                    minimized_info["webpage_url"] = spotify_url
                minimized_info["spotify"] = {
                    "type": spotify_meta.get("type"),
                    "url": spotify_meta.get("url"),
                    "title": spotify_meta.get("title"),
                    "author": spotify_meta.get("author"),
                    "author_url": author_url,
                    "thumbnail": thumbnail,
                    "track_count": len(tracks)
                }

            if apple_meta:
                tracks = apple_meta.get("tracks") or []
                primary_track = tracks[0] if tracks else {}
                meta_type = (apple_meta.get("type") or "").lower()
                if meta_type in ("album", "playlist", "artist"):
                    title = apple_meta.get("title")
                    author = apple_meta.get("author")
                else:
                    title = primary_track.get("title") or apple_meta.get("title")
                    author = primary_track.get("artist") or apple_meta.get("author")
                author_url = apple_meta.get("author_url")
                thumbnail = apple_meta.get("thumbnail")
                if meta_type in ("album", "playlist", "artist"):
                    apple_url = apple_meta.get("url")
                else:
                    apple_url = primary_track.get("apple_music_url") or apple_meta.get("url")

                if title:
                    minimized_info["title"] = title
                    minimized_info["fulltitle"] = title
                if author:
                    minimized_info["uploader"] = author
                    minimized_info["channel"] = author
                if author_url:
                    minimized_info["uploader_url"] = author_url
                if thumbnail:
                    minimized_info["thumbnail"] = thumbnail
                if apple_url:
                    minimized_info["webpage_url"] = apple_url
                minimized_info["apple_music"] = {
                    "type": apple_meta.get("type"),
                    "url": apple_meta.get("url"),
                    "title": apple_meta.get("title"),
                    "author": apple_meta.get("author"),
                    "author_url": author_url,
                    "thumbnail": thumbnail,
                    "track_count": len(tracks)
                }

            if deezer_meta:
                tracks = deezer_meta.get("tracks") or []
                primary_track = tracks[0] if tracks else {}
                meta_type = (deezer_meta.get("type") or "").lower()
                if meta_type in ("album", "playlist", "artist"):
                    title = deezer_meta.get("title")
                    author = deezer_meta.get("author")
                else:
                    title = primary_track.get("title") or deezer_meta.get("title")
                    author = primary_track.get("artist") or deezer_meta.get("author")
                author_url = deezer_meta.get("author_url")
                thumbnail = deezer_meta.get("thumbnail")
                if meta_type in ("album", "playlist", "artist"):
                    deezer_url = deezer_meta.get("url")
                else:
                    deezer_url = primary_track.get("deezer_url") or deezer_meta.get("url")

                if title:
                    minimized_info["title"] = title
                    minimized_info["fulltitle"] = title
                if author:
                    minimized_info["uploader"] = author
                    minimized_info["channel"] = author
                if author_url:
                    minimized_info["uploader_url"] = author_url
                if thumbnail:
                    minimized_info["thumbnail"] = thumbnail
                if deezer_url:
                    minimized_info["webpage_url"] = deezer_url
                minimized_info["deezer"] = {
                    "type": deezer_meta.get("type"),
                    "url": deezer_meta.get("url"),
                    "title": deezer_meta.get("title"),
                    "author": deezer_meta.get("author"),
                    "author_url": author_url,
                    "thumbnail": thumbnail,
                    "track_count": len(tracks)
                }

            print(json.dumps({
                "type": "metadata",
                "id": self.task_id,
                "success": True,
                "data": minimized_info
            }), flush=True)

        except Exception as e:
            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": logger.last_error or str(e)
            }), flush=True)


class SearchHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    @staticmethod
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
            return f"{hrs}:{mins:02d}:{secs:02d}"
        return f"{mins:02d}:{secs:02d}"

    @staticmethod
    def _parse_ytmusic_search(url):
        if not isinstance(url, str):
            return None, None
        lowered = url.lower()
        if not lowered.startswith("ytmsearch"):
            return None, None
        prefix, _, query = url.partition(":")
        digits = "".join([c for c in prefix if c.isdigit()])
        limit = None
        if digits:
            try:
                limit = int(digits)
            except Exception:
                limit = None
        if limit is None:
            limit = 10
        return query.strip(), max(1, min(limit, 50))

    def run(self, args):
        logger = BridgeLogger(self.task_id)
        try:
            extra_args = ["--remote-components", "ejs:github"]
            final_args = extra_args + args

            parsed_args = yt_dlp.parse_options(final_args)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]

            if not urls:
                print(json.dumps({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "No query provided for search"
                }), flush=True)
                return

            query, limit = self._parse_ytmusic_search(urls[0])
            if query is not None:
                ytm_handler = YTMusicSearchHandler(self.task_id)
                ytm_handler.run([query, str(limit)])
                return

            override_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'logger': logger,
                'simulate': True,
                'skip_download': True,
            }
            ydl_opts.update(override_opts)

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(urls[0], download=False)

                raw_entries = info.get('entries', []) if info.get('_type') in ['playlist', 'multi_video'] else [info]

                results = []
                for entry in raw_entries:
                    if not entry:
                        continue

                    thumbnail_url = entry.get('thumbnail')
                    if not thumbnail_url and entry.get('thumbnails'):
                        thumbnail_url = entry['thumbnails'][-1].get('url')

                    duration = entry.get('duration')
                    duration_string = entry.get('duration_string') or self._format_duration(duration)

                    results.append({
                        'id': entry.get('id'),
                        'title': entry.get('title'),
                        'uploader': entry.get('uploader') or entry.get('channel'),
                        'duration': duration,
                        'duration_string': duration_string,
                        'thumbnail': thumbnail_url,
                        'url': entry.get('url') or entry.get('webpage_url')
                    })

            print(json.dumps({
                "type": "search_results",
                "id": self.task_id,
                "success": True,
                "data": results
            }), flush=True)

        except Exception as e:
            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": logger.last_error or str(e)
            }), flush=True)