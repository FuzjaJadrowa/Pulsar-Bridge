import yt_dlp
import json
import sys
import os


class DownloadHandler:
    def _progress_hook(self, d):
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate') or 0
            downloaded = d.get('downloaded_bytes', 0)

            percent = 0
            if total > 0:
                percent = (downloaded / total) * 100

            eta = d.get('eta', 0)
            speed = d.get('speed', 0)

            msg = {
                "type": "progress",
                "percent": percent,
                "eta": eta,
                "speed": speed,
                "filename": d.get('filename', '')
            }
            print(json.dumps(msg), flush=True)

        elif d['status'] == 'finished':
            print(json.dumps({"type": "status", "msg": "Processing..."}), flush=True)

    def run(self, data):
        url = data.get("url")
        args = data.get("args", {})
        path = args.get("path", ".")

        ydl_opts = {
            'outtmpl': f'{path}/%(title)s.%(ext)s',
            'progress_hooks': [self._progress_hook],
            'quiet': True,
            'no_warnings': True,
        }

        if args.get("audioOnly"):
            ydl_opts['format'] = 'bestaudio/best'
            ydl_opts['postprocessors'] = [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': args.get('audioFormat', 'mp3'),
                'preferredquality': args.get('audioQuality', '192').replace('k', ''),
            }]
        else:
            v_fmt = args.get("videoFormat", "mp4")
            if v_fmt == "mp4":
                ydl_opts['format'] = 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best'
            else:
                ydl_opts['format'] = f'bestvideo+bestaudio/best'
                ydl_opts['merge_output_format'] = v_fmt

        if args.get("cookiesBrowser") and args.get("cookiesBrowser") != "None":
            ydl_opts['cookiesfrombrowser'] = (args.get("cookiesBrowser").lower(), None, None, None)

        if args.get("downloadSubs"):
            ydl_opts['writesubtitles'] = True
            if args.get("subsLang"):
                ydl_opts['subtitleslangs'] = [args.get("subsLang")]
            else:
                ydl_opts['writeautomaticsub'] = True

        if args.get("ffmpegLocation"):
            ydl_opts['ffmpeg_location'] = args.get("ffmpegLocation")

        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:

                ydl.download([url])

            print(json.dumps({"type": "finished", "success": True}), flush=True)

        except Exception as e:
            print(json.dumps({"type": "finished", "success": False, "error": str(e)}), flush=True)