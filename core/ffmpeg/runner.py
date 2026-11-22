import subprocess
from collections import deque
import threading
import re
from core.tasking import TaskCancelledError

_active_ffmpeg = {}
_ffmpeg_lock = threading.Lock()


def register_ffmpeg(task_id, process):
    if not task_id or process is None:
        return False
    with _ffmpeg_lock:
        _active_ffmpeg[task_id] = process
    return True


def unregister_ffmpeg(task_id, process=None):
    with _ffmpeg_lock:
        existing = _active_ffmpeg.get(task_id)
        if existing is not None and (process is None or existing is process):
            _active_ffmpeg.pop(task_id, None)


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
    for _, proc in items:
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


def _run_ffmpeg_manual(task_id, cmd, progress_callback, timeout=None, cancel_token=None):
    import sys

    if cancel_token is not None:
        cancel_token.check()

    creationflags = 0
    if sys.platform == "win32":
        creationflags = 0x00008000 | 0x08000000  # ABOVE_NORMAL_PRIORITY_CLASS | CREATE_NO_WINDOW

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
        creationflags=creationflags,
    )
    if not register_ffmpeg(task_id, proc):
        proc.kill()
        try:
            proc.wait(timeout=5)
        except Exception:
            pass
        return FFmpegResult(1, "")

    def kill_proc():
        try:
            proc.kill()
        except Exception:
            pass

    if cancel_token is not None:
        cancel_token.register_callback(kill_proc)

    stderr_output = deque(maxlen=128)
    detected_duration = {"seconds": None}
    duration_pattern = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")

    def consume_stderr():
        if proc.stderr is None:
            return
        for line in proc.stderr:
            stderr_output.append(line)
            match = duration_pattern.search(line)
            if match:
                hours, minutes, seconds = match.groups()
                detected_duration["seconds"] = int(hours) * 3600 + int(minutes) * 60 + float(seconds)

    stderr_thread = threading.Thread(target=consume_stderr, daemon=True)
    stderr_thread.start()

    try:
        block = {}
        if proc.stdout is not None:
            for line in proc.stdout:
                if cancel_token is not None and cancel_token.is_cancelled:
                    break
                data = _parse_progress_line(line)
                if not data:
                    continue
                block.update(data)
                progress_state = data.get("progress")
                if progress_state in ("continue", "end"):
                    if detected_duration["seconds"] is not None:
                        block["duration_seconds"] = detected_duration["seconds"]
                    progress_callback(dict(block))
                    block.clear()
                    if progress_state == "end":
                        break
        if block and not (cancel_token is not None and cancel_token.is_cancelled):
            if detected_duration["seconds"] is not None:
                block["duration_seconds"] = detected_duration["seconds"]
            progress_callback(dict(block))

        returncode = proc.wait(timeout=timeout)
        stderr_thread.join(timeout=1)
        if cancel_token is not None and cancel_token.is_cancelled:
            raise TaskCancelledError("Cancelled")
        return FFmpegResult(returncode, "".join(stderr_output)[-4000:])
    finally:
        if cancel_token is not None:
            cancel_token.unregister_callback(kill_proc)
        unregister_ffmpeg(task_id, proc)
        if proc.poll() is None:
            kill_proc()
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
        stderr_thread.join(timeout=1)
        for pipe in (proc.stdout, proc.stderr):
            if pipe is not None:
                try:
                    pipe.close()
                except Exception:
                    pass

def run_ffmpeg_with_progress(task_id, ffmpeg_path, args, progress_callback, cancel_token=None):
    cmd = [ffmpeg_path] + args + ["-progress", "pipe:1", "-nostats"]
    return _run_ffmpeg_manual(task_id, cmd, progress_callback, cancel_token=cancel_token)