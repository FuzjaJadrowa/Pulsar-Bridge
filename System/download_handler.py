import yt_dlp
import json
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_popen_patch import patch_ffmpeg_popen_for_progress

class BridgeLogger:
    def __init__(self, task_id):
        self.task_id = task_id
        self.ffmpeg_parser = FFMpegOutputParser()

    def debug(self, msg):
        ffmpeg_data = self.ffmpeg_parser.parse_progress_line(msg)
        if ffmpeg_data:
            payload = {
                "type": "progress",
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
            parsed_args = yt_dlp.parse_options(args_list)
            ydl_opts = parsed_args[3]
            urls = parsed_args[2]

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