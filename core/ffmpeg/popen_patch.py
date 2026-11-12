import json
import threading
import subprocess
import os
import time
import weakref
from contextlib import contextmanager

import yt_dlp.downloader.external as yt_external
from core.ffmpeg.output_parser import FFMpegOutputParser
from core.ffmpeg.runner import kill_ffmpeg_for_task
from core.utils import emit_json

_active_popens = {}
_popens_lock = threading.Lock()
_thread_local = threading.local()
_original_popen = yt_external.Popen
_is_patched = False
_patch_lock = threading.Lock()
_task_transcodes = set()
_task_transcodes_lock = threading.Lock()

def kill_processes_for_task(task_id):
    with _popens_lock:
        popens_refs = _active_popens.pop(task_id, [])
    for ref in popens_refs:
        p = ref()
        if p is not None:
            try:
                p.kill()
            except Exception:
                pass
    kill_ffmpeg_for_task(task_id)

class GlobalBridgeFFmpegPopen(_original_popen):
    def __init__(self, args, *remaining, **kwargs):
        self._stderr_thread = None
        self._stderr_capture = []
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
            import sys
            if sys.platform == "win32":
                kwargs.setdefault("creationflags", 0x00008000) # ABOVE_NORMAL_PRIORITY_CLASS

        super().__init__(args, *remaining, **kwargs)

        if self.task_id:
            with _popens_lock:
                if self.task_id not in _active_popens:
                    _active_popens[self.task_id] = []
                _active_popens[self.task_id].append(weakref.ref(self))

        if is_ffmpeg and self.stderr is not None:
            self._stderr_thread = threading.Thread(target=self._consume_ffmpeg_stderr, daemon=True)
            self._stderr_thread.start()

    def _consume_ffmpeg_stderr(self):
        buffer = bytearray()
        while True:
            try:
                chunk = os.read(self.stderr.fileno(), 4096)
            except Exception:
                break
            if not chunk:
                if buffer:
                    fragment = buffer.decode("utf-8", errors="replace")
                    self._stderr_capture.append(fragment)
                    self._handle_stderr_fragment(fragment)
                break
            if isinstance(chunk, str):
                chunk = chunk.encode("utf-8", errors="replace")
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
                    fragment = line.decode("utf-8", errors="replace")
                    self._stderr_capture.append(fragment + "\n")
                    self._handle_stderr_fragment(fragment)

    @classmethod
    def run(cls, *args, timeout=None, input=None, **kwargs):
        command = args[0] if args else kwargs.get("args")
        executable = command[0] if isinstance(command, (list, tuple)) and command else command
        is_ffmpeg = os.path.basename(str(executable or "")).lower() in {
            "ffmpeg", "ffmpeg.exe", "avconv", "avconv.exe"
        }
        if not is_ffmpeg:
            return _original_popen.run(*args, timeout=timeout, input=input, **kwargs)

        text_mode = bool(kwargs.get("text") or kwargs.get("universal_newlines"))
        if input is not None and kwargs.get("stdin") is None:
            kwargs["stdin"] = subprocess.PIPE
        with cls(*args, **kwargs) as proc:
            if input is not None:
                proc.stdin.write(input)
                proc.stdin.close()
            stdout = proc.stdout.read() if proc.stdout is not None else ("" if text_mode else b"")
            proc.wait(timeout=timeout)
            stderr_text = "".join(proc._stderr_capture)
            stderr = stderr_text if text_mode else stderr_text.encode("utf-8", errors="replace")
            default = "" if text_mode else b""
            return stdout or default, stderr or default, proc.returncode

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
                "type": "progress",
                "id": self.task_id,
                "stage": "converting",
            }
            with _task_transcodes_lock:
                has_transcode = self.task_id in _task_transcodes
            if has_transcode and "percent" in ffmpeg_data:
                ffmpeg_data["percent"] = str(70.0 + float(ffmpeg_data["percent"]) * 0.30)
                ffmpeg_data["stage"] = "converting"
            payload.update(ffmpeg_data)
            emit_json(payload)

    def wait(self, timeout=None):
        ret = super().wait(timeout=timeout)
        if self._stderr_thread is not None:
            self._stderr_thread.join(timeout=1.0)

        if self.task_id:
            with _popens_lock:
                if self.task_id in _active_popens:
                    _active_popens[self.task_id] = [
                        ref for ref in _active_popens[self.task_id]
                        if ref() is not None and ref() is not self
                    ]

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
def patch_ffmpeg_popen_for_progress(task_id, has_transcode=False):
    _ensure_patched()
    _thread_local.task_id = task_id
    if has_transcode:
        with _task_transcodes_lock:
            _task_transcodes.add(task_id)
    try:
        yield
    finally:
        _thread_local.task_id = None
        with _task_transcodes_lock:
            _task_transcodes.discard(task_id)
        kill_processes_for_task(task_id)