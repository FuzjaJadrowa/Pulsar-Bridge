import yt_dlp
import json
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_popen_patch import patch_ffmpeg_popen_for_progress
from System.spotify_resolver import resolve_spotify_url

class BridgeLogger:
    def __init__(self, task_id):
        self.task_id = task_id
        self.ffmpeg_parser = FFMpegOutputParser()

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
        print(json.dumps({
            "type": "log",
            "level": "warning",
            "id": self.task_id,
            "message": msg
        }), flush=True)

    def error(self, msg):
        print(json.dumps({
            "type": "log",
            "level": "error",
            "id": self.task_id,
            "message": msg
        }), flush=True)


class DownloadHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    def _progress_hook(self, d):
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)

            percent = 0
            if total > 0:
                percent = (downloaded / total) * 100

            msg = {
                "type": "progress",
                "id": self.task_id,
                "percent": percent,
                "eta": d.get('eta', 0),
                "speed": d.get('speed', 0),
                "filename": d.get('filename', ''),
                "status": "downloading"
            }
            print(json.dumps(msg), flush=True)

        elif d['status'] == 'finished':
            print(json.dumps({
                "type": "status",
                "id": self.task_id,
                "msg": "File downloaded, starting post-processing..."
            }), flush=True)

    def run(self, args_list):
        try:
            extra_args = ["--remote-components", "ejs:github"]
            final_args = extra_args + args_list

            parsed_args = yt_dlp.parse_options(final_args)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]

            urls = [resolve_spotify_url(u) for u in urls]

            if 'progress_hooks' not in ydl_opts:
                ydl_opts['progress_hooks'] = []
            ydl_opts['progress_hooks'].append(self._progress_hook)

            ydl_opts['logger'] = BridgeLogger(self.task_id)
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
            error_msg = str(e)
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

    def _filter_metadata(self, info):
        keys_to_keep = [
            'id', 'title', 'fulltitle', 'thumbnail', 'description',
            'uploader', 'uploader_id', 'uploader_url',
            'upload_date', 'duration', 'duration_string',
            'view_count', 'like_count', 'comment_count',
            'age_limit', 'is_live', 'was_live', 'availability',
            'channel', 'channel_follower_count', 'webpage_url'
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

        if 'automatic_captions' in info and info['automatic_captions']:
            filtered['auto_captions_langs'] = list(info['automatic_captions'].keys())

        return filtered

    def run(self, args):
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

            urls = [resolve_spotify_url(u) for u in urls]

            ydl_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': False,
                'logger': BridgeLogger(self.task_id),
                'simulate': True,
                'skip_download': True,
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
                        'logger': BridgeLogger(self.task_id)
                    })
                except Exception as e:
                    print(json.dumps({"type": "error", "message": f"Args error: {str(e)}"}), flush=True)
                    return

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(urls[0], download=False)

                if info and info.get('_type') == 'playlist' and info.get('entries'):
                    info = info['entries'][0]

                clean_info = ydl.sanitize_info(info)
                minimized_info = self._filter_metadata(clean_info)

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
                "error": str(e)
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

    def run(self, args):
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

            override_opts = {
                'quiet': True,
                'no_warnings': True,
                'extract_flat': True,
                'logger': BridgeLogger(self.task_id),
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
                "error": str(e)
            }), flush=True)