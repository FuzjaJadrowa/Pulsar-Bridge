import json
import subprocess
import threading
import time

try:
    from better_ffmpeg_progress import FFMpegProgress as _FFMpegProgress
except Exception:
    _FFMpegProgress = None

_active_ffmpeg = {}

def register_ffmpeg(task_id, process):
    if not task_id or process is None:
        return False
    _active_ffmpeg[task_id] = process
    return True

def kill_ffmpeg_for_task(task_id):
    proc = _active_ffmpeg.pop(task_id, None)
    if not proc:
        return
    try:
        proc.kill()
    except Exception:
        pass

def kill_all_ffmpeg():
    for task_id, proc in list(_active_ffmpeg.items()):
        try:
            proc.kill()
        except Exception:
            pass
        _active_ffmpeg.pop(task_id, None)

def _emit(payload):
    print(json.dumps(payload), flush=True)

def _parse_progress_line(line):
    data = {}
    for raw in line.splitlines():
        if "=" not in raw:
            continue
        key, value = raw.split("=", 1)
        data[key.strip()] = value.strip()
    return data

def _run_ffmpeg_manual(task_id, cmd, progress_callback, timeout=None):
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    if not register_ffmpeg(task_id, proc):
        proc.kill()
        return 1

    def consume_stderr():
        for _ in proc.stderr:
            pass

    stderr_thread = threading.Thread(target=consume_stderr, daemon=True)
    stderr_thread.start()

    for line in proc.stdout:
        data = _parse_progress_line(line)
        if not data:
            continue
        progress_callback(data)
        if data.get("progress") == "end":
            break

    try:
        return proc.wait(timeout=timeout)
    finally:
        kill_ffmpeg_for_task(task_id)

def run_ffmpeg_with_progress(task_id, ffmpeg_path, args, progress_callback):
    cmd = [ffmpeg_path] + args + ["-progress", "pipe:1", "-nostats"]

    if _FFMpegProgress and hasattr(_FFMpegProgress, "run_command_with_progress"):
        try:
            runner = _FFMpegProgress(cmd)
            registered = False
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
            if registered:
                kill_ffmpeg_for_task(task_id)
            return ret
        except Exception:
            pass

    return _run_ffmpeg_manual(task_id, cmd, progress_callback)