import json
import threading
import subprocess
import os
import time
from contextlib import contextmanager

import yt_dlp.downloader.external as yt_external
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_runner import kill_ffmpeg_for_task
from System.utils import emit_json

_active_popens = {}
_popens_lock = threading.Lock()
_thread_local = threading.local()
_original_popen = yt_external.Popen
_is_patched = False
_patch_lock = threading.Lock()

def kill_processes_for_task(task_id):
    with _popens_lock:
        popens = _active_popens.pop(task_id, [])
    for p in popens:
        try:
            p.kill()
        except Exception:
            pass
    kill_ffmpeg_for_task(task_id)

class GlobalBridgeFFmpegPopen(_original_popen):
    def __init__(self, args, *remaining, **kwargs):
        self._stderr_thread = None
        self.task_id = getattr(_thread_local, "task_id", None)
        self.last_emit_time = 0.0
        self.parser = FFMpegOutputParser()

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
            kwargs.setdefault("bufsize", 1024 * 64)

        super().__init__(args, *remaining, **kwargs)

        if self.task_id:
            with _popens_lock:
                if self.task_id not in _active_popens:
                    _active_popens[self.task_id] = []
                _active_popens[self.task_id].append(self)

        if is_ffmpeg and self.stderr is not None:
            self._stderr_thread = threading.Thread(target=self._consume_ffmpeg_stderr, daemon=True)
            self._stderr_thread.start()

    def _consume_ffmpeg_stderr(self):
        buffer = bytearray()
        while True:
            try:
                chunk = self.stderr.read(4096)
            except Exception:
                break
            if not chunk:
                if buffer:
                    self._handle_stderr_fragment(buffer.decode("utf-8", errors="replace"))
                break
            buffer.extend(chunk)
            while b"\r" in buffer or b"\n" in buffer:
                idx_r = buffer.find(b"\r")
                idx_n = buffer.find(b"\n")
                if idx_r == -1:
                    split_idx = idx_n
                elif idx_n == -1:
                    split_idx = idx_r
                else:
                    split_idx = min(idx_r, idx_n)
                
                line = buffer[:split_idx]
                del buffer[:split_idx + 1]
                if line:
                    self._handle_stderr_fragment(line.decode("utf-8", errors="replace"))

    def _handle_stderr_fragment(self, fragment):
        if not self.task_id:
            return
        line = fragment.strip()
        if not line:
            return

        now = time.monotonic()
        if (now - self.last_emit_time) < 0.2:
            return

        ffmpeg_data = self.parser.parse_progress_line(line)
        if ffmpeg_data:
            self.last_emit_time = now
            payload = {
                "type": "progress_ffmpeg",
                "id": self.task_id,
                "status": "processing",
            }
            payload.update(ffmpeg_data)
            emit_json(payload)

    def wait(self, timeout=None):
        ret = super().wait(timeout=timeout)
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1.0)

        if self.task_id:
            with _popens_lock:
                if self.task_id in _active_popens and self in _active_popens[self.task_id]:
                    _active_popens[self.task_id].remove(self)

        return ret

def _ensure_patched():
    global _is_patched
    with _patch_lock:
        if not _is_patched:
            yt_external.Popen = GlobalBridgeFFmpegPopen
            try:
                import yt_dlp.utils
                yt_dlp.utils.Popen = GlobalBridgeFFmpegPopen
            except Exception:
                pass
            try:
                import yt_dlp.postprocessor.ffmpeg
                yt_dlp.postprocessor.ffmpeg.Popen = GlobalBridgeFFmpegPopen
            except Exception:
                pass
            _is_patched = True

@contextmanager
def patch_ffmpeg_popen_for_progress(task_id):
    _ensure_patched()
    _thread_local.task_id = task_id
    try:
        yield
    finally:
        _thread_local.task_id = None
        kill_processes_for_task(task_id)