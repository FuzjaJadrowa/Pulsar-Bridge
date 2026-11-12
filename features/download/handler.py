import yt_dlp
import json
import os
import shlex
from features.download.ytmusic_search import YTMusicSearchHandler
from core.ffmpeg.popen_patch import patch_ffmpeg_popen_for_progress
from features.download.extractors import create_resolver_ydl, metadata_fallback
from core.logging import BridgeLogger
from core.utils import emit_json

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
        index = d.get('playlist_index') or info.get('playlist_index')
        count = d.get('playlist_count') or info.get('playlist_count')

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

            percent = 0.0
            if total > 0:
                percent = (downloaded / total) * 100
            elif d.get('fragment_count') and d.get('fragment_index'):
                percent = (d['fragment_index'] / d['fragment_count']) * 100

            item_index, item_count = self._extract_playlist_progress(d)
            eta = d.get('eta')
            if eta is None:
                eta = 0

            msg = {
                "type": "progress",
                "id": self.task_id,
                "percent": round(percent * (0.70 if getattr(self, 'has_transcode', False) else 1.0), 2),
                "eta": eta,
                "eta_seconds": eta,
                "speed": d.get('speed', 0) or 0,
                "filename": d.get('filename', ''),
                "item_index": item_index,
                "item_count": item_count,
                "status": "downloading",
                "stage": "downloading"
            }
            emit_json(msg)

        elif d['status'] == 'finished':
            emit_json({
                "type": "status",
                "id": self.task_id,
                "msg": "File downloaded, starting post-processing...",
                "stage": "converting"
            })
            if self.expected_playlist_count and self.current_playlist_index < self.expected_playlist_count:
                self.current_playlist_index += 1

    def run(self, args_list):
        logger = BridgeLogger(self.task_id)
        try:
            parsed_args = yt_dlp.parse_options(args_list)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]
            self.has_transcode = any(arg in args_list for arg in ('--recode-video', '-x', '--audio-format', '--postprocessor-args'))
            if 'progress_hooks' not in ydl_opts:
                ydl_opts['progress_hooks'] = []
            ydl_opts['progress_hooks'].append(self._progress_hook)

            ydl_opts['logger'] = logger
            ydl_opts['no_color'] = True
            ydl_opts['ignoreerrors'] = False

            with patch_ffmpeg_popen_for_progress(self.task_id, self.has_transcode):
                with create_resolver_ydl(ydl_opts) as ydl:
                    retcode = ydl.download(urls)

            if retcode != 0:
                raise Exception(f"yt-dlp exited with error code {retcode}")

            emit_json({
                "type": "finished",
                "id": self.task_id,
                "success": True
            })

        except SystemExit:
            emit_json({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": "Cancelled"
            })
        except Exception as e:
            error_msg = logger.last_error or str(e)
            if "yt-dlp exited with error code" in error_msg:
                error_msg = "Download failed."
            emit_json({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": error_msg
            })

    def run_ffmpeg_direct(self, args_list):
        from core.ffmpeg.runner import run_ffmpeg_with_progress

        logger = BridgeLogger(self.task_id)
        try:
            parsed = yt_dlp.parse_options(args_list)
            urls, ydl_opts = parsed[2], parsed[3]
            if not urls:
                raise ValueError("No URL provided")
            ydl_opts.update({'quiet': True, 'no_warnings': True, 'logger': logger, 'skip_download': True})
            ffmpeg_path = ydl_opts.get('ffmpeg_location') or 'ffmpeg'
            selector_spec = ydl_opts.get('format') or 'bestvideo+bestaudio/best'
            output_template = ydl_opts.get('outtmpl') or '%(title)s [%(id)s].%(ext)s'
            with create_resolver_ydl(ydl_opts) as ydl:
                info = ydl.extract_info(urls[0], download=False)
                if not info:
                    raise ValueError("Could not resolve media metadata")
                if info.get('_type') in ('playlist', 'multi_video'):
                    emit_json({'type': 'status', 'id': self.task_id, 'stage': 'downloading', 'msg': 'Using yt-dlp for playlist compatibility'})
                    return self.run(args_list)
                selected = list(ydl.build_format_selector(selector_spec)(info))
                if not selected:
                    raise ValueError("No matching formats found")
                chosen = selected[0]
                inputs = chosen.get('requested_formats') or [chosen]
                prepared_info = dict(info)
                prepared_info.update(chosen)
                output_path = ydl.prepare_filename(prepared_info)
                target_ext = None
                for index, arg in enumerate(args_list[:-1]):
                    if arg == '--recode-video':
                        target_ext = args_list[index + 1]
                    elif arg == '--audio-format':
                        audio_ext = args_list[index + 1]
                        target_ext = {'vorbis': 'ogg', 'alac': 'm4a', 'aac': 'm4a', 'mp3': 'mp3', 'opus': 'opus', 'flac': 'flac', 'wav': 'wav'}.get(audio_ext, audio_ext)
                if target_ext:
                    stem, _ = os.path.splitext(output_path)
                    output_path = f"{stem}.{target_ext}"
                os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)

                command_args = ['-hide_banner', '-y']
                for fmt in inputs:
                    headers = fmt.get('http_headers') or {}
                    if headers:
                        command_args.extend(['-headers', ''.join(f'{key}: {value}\r\n' for key, value in headers.items())])
                    command_args.extend(['-i', fmt.get('url') or fmt.get('manifest_url') or ''])
                if len(inputs) > 1:
                    command_args.extend(['-map', '0:v:0?', '-map', '1:a:0?'])
                else:
                    command_args.extend(['-map', '0:v:0?', '-map', '0:a:0?'])
                if '-x' in args_list:
                    command_args.append('-vn')
                if '--audio-quality' in args_list:
                    quality_index = args_list.index('--audio-quality')
                    if quality_index + 1 < len(args_list):
                        command_args.extend(['-b:a', args_list[quality_index + 1]])

                extra = []
                for index, arg in enumerate(args_list[:-1]):
                    if arg == '--postprocessor-args':
                        value = args_list[index + 1]
                        if ':' in value:
                            value = value.split(':', 1)[1]
                        extra.extend(shlex.split(value))
                command_args.extend(extra)
                command_args.append(output_path)

                duration = float(info.get('duration') or 0)
                def on_progress(data):
                    percent, eta = 0.0, None
                    elapsed = data.get('out_time')
                    if duration > 0 and elapsed:
                        try:
                            h, m, s = elapsed.split(':')
                            seconds = int(h) * 3600 + int(m) * 60 + float(s)
                            percent = min(100.0, seconds * 100 / duration)
                            speed = float(str(data.get('speed', '0x')).rstrip('x'))
                            eta = max(0.0, (duration - seconds) / speed) if speed > 0 else None
                        except (ValueError, TypeError):
                            pass
                    payload = {'type': 'progress', 'id': self.task_id, 'stage': 'converting', 'status': 'converting', 'percent': percent}
                    if eta is not None:
                        payload['eta_seconds'] = eta
                    emit_json(payload)

                result = run_ffmpeg_with_progress(self.task_id, ffmpeg_path, command_args, on_progress)
                if result != 0:
                    raise RuntimeError(getattr(result, 'stderr', '') or f'FFmpeg exited with code {result}')
                emit_json({'type': 'finished', 'id': self.task_id, 'success': True, 'filepath': output_path})
        except Exception as exc:
            emit_json({'type': 'finished', 'id': self.task_id, 'success': False, 'error': str(exc)})

class DownloadMetadataHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    def _filter_metadata(self, info, force_subtitle_langs=False):
        keys_to_keep = [
            'id', 'title', 'fulltitle', 'thumbnail', 'description',
            'uploader', 'uploader_id', 'uploader_url',
            'upload_date', 'duration', 'duration_string',
            'view_count', 'like_count', 'comment_count',
            'age_limit', 'is_live', 'was_live', 'availability',
            'channel', 'channel_follower_count', 'webpage_url', 'spotify', 'apple_music', 'deezer',
            'resolver', 'resolver_warning'
        ]
        filtered = {k: info.get(k) for k in keys_to_keep if k in info}

        if 'formats' in info:
            clean_formats = []
            for f in info['formats']:
                clean_f = {
                    'format_id': f.get('format_id'),
                    'ext': f.get('ext'),
                    'resolution': f.get('resolution'),
                    'fps': f.get('fps'),
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
            if first and isinstance(first, dict):
                if first.get('_type') in ('url', 'url_transparent') and not first.get('title'):
                    url = self._build_youtube_url(first)
                    if url:
                        try:
                            return ydl.extract_info(url, download=False)
                        except Exception:
                            return first
                return first

        if info.get('_type') in ('url', 'url_transparent') and not info.get('title'):
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
            parsed_args = yt_dlp.parse_options(args)
            urls, ydl_opts = parsed_args[2], parsed_args[3]
            if not urls:
                raise Exception("No URL provided for metadata")
            ydl_opts.update({
                'quiet': True, 'no_warnings': True, 'logger': logger,
                'simulate': True, 'skip_download': True, 'ignoreerrors': False,
                'extract_flat': False, 'writesubtitles': True, 'writeautomaticsub': True,
                'pulsar_metadata': True,
            })
            if self._is_youtube_playlist_url(urls[0]):
                ydl_opts.update({'extract_flat': 'in_playlist', 'playlistend': 1, 'playlist_items': '1'})

            with create_resolver_ydl(ydl_opts) as ydl:
                extraction_error = None
                try:
                    info = self._ensure_full_info(ydl, ydl.extract_info(urls[0], download=False))
                    if not info:
                        raise Exception("No metadata extracted")
                    force_subs = any(info.get(source) for source in ('spotify', 'apple_music', 'deezer'))
                    minimized_info = self._filter_metadata(ydl.sanitize_info(info), force_subtitle_langs=force_subs)
                except Exception as exc:
                    extraction_error = exc

                fallback = metadata_fallback(ydl, urls[0])
                if extraction_error is not None:
                    if not fallback:
                        raise extraction_error
                    minimized_info = {
                        'formats': [], 'subtitles_langs': [], 'auto_captions_langs': [],
                        'resolver_warning': logger.last_error or str(extraction_error),
                    }
                if fallback:
                    for field in ('id', 'title', 'thumbnail', 'webpage_url', 'description', 'duration'):
                        if fallback.get(field) is not None:
                            minimized_info[field] = fallback[field]
                    if fallback.get('title'):
                        minimized_info['fulltitle'] = fallback['title']
                    minimized_info['resolver'] = {
                        'source': fallback.get('source'),
                        'player_host': fallback.get('player_host') or fallback.get('resolved_host'),
                    }

            emit_json({'type': 'metadata', 'id': self.task_id, 'success': True, 'data': minimized_info})
        except SystemExit:
            emit_json({'type': 'finished', 'id': self.task_id, 'success': False, 'error': 'Cancelled'})
        except Exception as exc:
            emit_json({
                'type': 'finished', 'id': self.task_id, 'success': False,
                'error': logger.last_error or str(exc),
            })


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
            parsed_args = yt_dlp.parse_options(args)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]

            if not urls:
                emit_json({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "No query provided for search"
                })
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

            emit_json({
                "type": "search_results",
                "id": self.task_id,
                "success": True,
                "data": results
            })

        except SystemExit:
            emit_json({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": "Cancelled"
            })
        except Exception as e:
            emit_json({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": logger.last_error or str(e)
            })