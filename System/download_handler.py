import yt_dlp
import json
import sys

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
                "filename": d.get('filename', '')
            }
            print(json.dumps(msg), flush=True)

        elif d['status'] == 'finished':
            print(json.dumps({
                "type": "status",
                "id": self.task_id,
                "msg": "Przetwarzanie (FFmpeg)..."
            }), flush=True)

    def run(self, args_list):
        try:
            parsed_args = yt_dlp.parse_options(args_list)

            ydl_opts = parsed_args[3]
            urls = parsed_args[2]

            if 'progress_hooks' not in ydl_opts:
                ydl_opts['progress_hooks'] = []
            ydl_opts['progress_hooks'].append(self._progress_hook)

            ydl_opts['quiet'] = True
            ydl_opts['no_warnings'] = True

            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download(urls)

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
            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": str(e)
            }), flush=True)