import json
import threading
import subprocess
import os
from contextlib import contextmanager

import yt_dlp.downloader.external as yt_external
from System.ffmpeg_output_parser import FFMpegOutputParser

@contextmanager
def patch_ffmpeg_popen_for_progress(task_id):
    original_popen = yt_external.Popen
    parser = FFMpegOutputParser()

    class BridgeFFmpegPopen(original_popen):
        def __init__(self, args, *remaining, **kwargs):
            self._stderr_thread = None
            self._stderr_tail = []

            cmd0 = ""
            if isinstance(args, (list, tuple)) and args:
                cmd0 = str(args[0])
            elif isinstance(args, str):
                cmd0 = args.split(" ", 1)[0]

            is_ffmpeg = os.path.basename(cmd0).lower() in {"ffmpeg", "ffmpeg.exe", "avconv", "avconv.exe"}

            if is_ffmpeg:
                kwargs.setdefault("stderr", subprocess.PIPE)
                kwargs.setdefault("stdout", subprocess.DEVNULL)
                kwargs.setdefault("text", False)
                kwargs.setdefault("bufsize", 0)

            super().__init__(args, *remaining, **kwargs)

            if is_ffmpeg and self.stderr is not None:
                self._stderr_thread = threading.Thread(target=self._consume_ffmpeg_stderr, daemon=True)
                self._stderr_thread.start()

        def _consume_ffmpeg_stderr(self):
            buffer = bytearray()
            while True:
                chunk = self.stderr.read(1)
                if not chunk:
                    if buffer:
                        self._handle_stderr_fragment(buffer.decode("utf-8", errors="replace"))
                    break

                if chunk in (b"\r", b"\n"):
                    if buffer:
                        self._handle_stderr_fragment(buffer.decode("utf-8", errors="replace"))
                        buffer.clear()
                else:
                    buffer.extend(chunk)

        def _handle_stderr_fragment(self, fragment):
            line = fragment.strip()
            if not line:
                return

            self._stderr_tail.append(line)
            if len(self._stderr_tail) > 30:
                self._stderr_tail = self._stderr_tail[-30:]

            ffmpeg_data = parser.parse_progress_line(line)
            if ffmpeg_data:
                payload = {
                    "type": "progress",
                    "id": task_id,
                    "status": "processing",
                }
                payload.update(ffmpeg_data)
                print(json.dumps(payload), flush=True)

        def wait(self, timeout=None):
            ret = super().wait(timeout=timeout)
            if self._stderr_thread is not None:
                self._stderr_thread.join(timeout=1.0)
            return ret

    yt_external.Popen = BridgeFFmpegPopen
    try:
        yield
    finally:
        yt_external.Popen = original_popen