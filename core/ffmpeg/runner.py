import subprocess
from collections import deque
import threading
import time
import re
from core.utils import emit_json

try:
    from better_ffmpeg_progress import FFMpegProgress as _FFMpegProgress
except Exception:
    _FFMpegProgress = None

_active_ffmpeg = {}
_ffmpeg_lock = threading.Lock()

def register_ffmpeg(task_id, process):
    if not task_id or process is None:
        return False
    with _ffmpeg_lock:
        _active_ffmpeg[task_id] = process
    return True

def kill_ffmpeg_for_task(task_id):
    with _ffmpeg_lock:
        proc = _active_ffmpeg.pop(task_id, None)
    if not proc:
        return
    try:
        proc.kill()
    except Exception:
        pass

def kill_all_ffmpeg():
    with _ffmpeg_lock:
        items = list(_active_ffmpeg.items())
        _active_ffmpeg.clear()
    for task_id, proc in items:
        try:
            proc.kill()
        except Exception:
            pass

class FFmpegResult(int):
    def __new__(cls, returncode, stderr=""):
        result = int.__new__(cls, returncode)
        result.stderr = stderr
        return result

def _parse_progress_line(line):
    data = {}
    for raw in line.splitlines():
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        data[key.strip()] = value.strip()
    return data

def _run_ffmpeg_manual(task_id, cmd, progress_callback, timeout=None):
    import sys
    creationflags = 0
    if sys.platform == "win32":
        creationflags = 0x00008000  # ABOVE_NORMAL_PRIORITY_CLASS

    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
        errors="replace",
        creationflags=creationflags
    )
    if not register_ffmpeg(task_id, proc):
        proc.kill()
        return 1

    stderr_output = deque(maxlen=128)
    detected_duration = {"seconds": None}
    duration_pattern = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")

    def consume_stderr():
        for line in proc.stderr:
            stderr_output.append(line)
            match = duration_pattern.search(line)
            if match:
                hours, minutes, seconds = match.groups()
                detected_duration["seconds"] = int(hours) * 3600 + int(minutes) * 60 + float(seconds)

    stderr_thread = threading.Thread(target=consume_stderr, daemon=True)
    stderr_thread.start()

    for line in proc.stdout:
        data = _parse_progress_line(line)
        if not data:
            continue
        if detected_duration["seconds"] is not None:
            data["duration_seconds"] = detected_duration["seconds"]
        progress_callback(data)
        if data.get("progress") == "end":
            break

    try:
        returncode = proc.wait(timeout=timeout)
        stderr_thread.join(timeout=1)
        return FFmpegResult(returncode, "".join(stderr_output)[-4000:])
    finally:
        kill_ffmpeg_for_task(task_id)

def run_ffmpeg_with_progress(task_id, ffmpeg_path, args, progress_callback):
    cmd = [ffmpeg_path] + args + ["-progress", "pipe:1", "-nostats"]

    if _FFMpegProgress and hasattr(_FFMpegProgress, "run_command_with_progress"):
        registered = False
        try:
            runner = _FFMpegProgress(cmd)
            for payload in runner.run_command_with_progress():
                proc = getattr(runner, "process", None)
                if proc is not None and not registered:
                    registered = register_ffmpeg(task_id, proc)
                if proc is None and not registered:
                    raise RuntimeError("FFmpeg process not attached.")
                progress_callback(payload)
            proc = getattr(runner, "process", None)
            if proc is not None and not registered:
                registered = register_ffmpeg(task_id, proc)
            if proc is None and not registered:
                raise RuntimeError("FFmpeg process not attached.")
            ret = proc.returncode if proc is not None else 0
            return FFmpegResult(ret)
        except Exception:
            pass
        finally:
            if registered:
                kill_ffmpeg_for_task(task_id)

    return _run_ffmpeg_manual(task_id, cmd, progress_callback)