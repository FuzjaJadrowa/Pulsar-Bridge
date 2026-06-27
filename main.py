import sys
import json
import traceback
import threading
import time
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_popen_patch import kill_processes_for_task
from System.ffmpeg_runner import kill_all_ffmpeg
from System.killable_thread import KillableThread
from System.utils import emit_json

def setup_windows_job_object():
    if sys.platform != "win32":
        return
    try:
        import ctypes
        from ctypes import wintypes
        
        kernel32 = ctypes.windll.kernel32
        
        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", wintypes.LARGE_INTEGER),
                ("PerJobUserTimeLimit", wintypes.LARGE_INTEGER),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_ulonglong),
                ("WriteOperationCount", ctypes.c_ulonglong),
                ("OtherOperationCount", ctypes.c_ulonglong),
                ("ReadTransferCount", ctypes.c_ulonglong),
                ("WriteTransferCount", ctypes.c_ulonglong),
                ("OtherTransferCount", ctypes.c_ulonglong),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE = 0x2000
        JobObjectExtendedLimitInformation = 9

        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

        res = kernel32.SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            ctypes.byref(info),
            ctypes.sizeof(info)
        )
        if res:
            process = kernel32.GetCurrentProcess()
            kernel32.AssignProcessToJobObject(job, process)
    except Exception:
        pass

active_tasks = {}
active_tasks_lock = threading.Lock()

class RateLimitedStdout:
    def __init__(self, stream, min_interval=0.4, throttled_types=None):
        self.stream = stream
        self.min_interval = float(min_interval)
        self.throttled_types = set(throttled_types or ())
        self.buffer = ""
        self.lock = threading.Lock()
        self.next_allowed = {}

    def clear_task(self, task_id):
        with self.lock:
            self.next_allowed.pop(task_id, None)

    def _should_throttle(self, payload):
        task_id = payload.get("id")
        if not task_id:
            return False
        msg_type = payload.get("type")
        if msg_type not in self.throttled_types:
            return False
        if msg_type == "log" and str(payload.get("level", "")).lower() == "error":
            return False
        now = time.monotonic()
        next_allowed = self.next_allowed.get(task_id, 0.0)
        if now < next_allowed:
            return True
        self.next_allowed[task_id] = now + self.min_interval
        return False

    def write(self, data):
        if not data:
            return 0
        with self.lock:
            self.buffer += data
            while "\n" in self.buffer:
                line, self.buffer = self.buffer.split("\n", 1)
                if line == "":
                    self.stream.write("\n")
                    continue
                
                line_str = line.strip()
                should_skip = False
                if line_str.startswith("{") and line_str.endswith("}"):
                    if '"progress"' in line_str or '"progress_ffmpeg"' in line_str or '"log"' in line_str:
                        try:
                            payload = json.loads(line_str)
                            if payload and self._should_throttle(payload):
                                should_skip = True
                        except Exception:
                            pass
                if should_skip:
                    continue
                self.stream.write(line + "\n")
            self.stream.flush()
        return len(data)

    def flush(self):
        with self.lock:
            if self.buffer:
                line_str = self.buffer.strip()
                should_skip = False
                if line_str.startswith("{") and line_str.endswith("}"):
                    if '"progress"' in line_str or '"progress_ffmpeg"' in line_str or '"log"' in line_str:
                        try:
                            payload = json.loads(line_str)
                            if payload and self._should_throttle(payload):
                                should_skip = True
                        except Exception:
                            pass
                if not should_skip:
                    self.stream.write(self.buffer)
                self.buffer = ""
            self.stream.flush()

    def isatty(self):
        if hasattr(self.stream, "isatty"):
            return self.stream.isatty()
        return False

    def writable(self):
        if hasattr(self.stream, "writable"):
            return self.stream.writable()
        return True

rate_limited_stdout = None

class BridgeLogger:
    def __init__(self, task_id):
        self.task_id = task_id
        self.ffmpeg_parser = FFMpegOutputParser()
        self.last_error = None
        self.last_warning = None

    def debug(self, msg):
        ffmpeg_data = self.ffmpeg_parser.parse_progress_line(msg)
        if ffmpeg_data:
            payload = {
                "type": "progress_ffmpeg",
                "id": self.task_id,
                "status": "processing"
            }
            payload.update(ffmpeg_data)
            emit_json(payload)
        elif not msg.startswith('[download] '):
            pass

    def info(self, msg):
        pass

    def warning(self, msg):
        self.last_warning = msg
        emit_json({
            "type": "log",
            "level": "warning",
            "id": self.task_id,
            "message": msg
        })

    def error(self, msg):
        self.last_error = msg
        emit_json({
            "type": "log",
            "level": "error",
            "id": self.task_id,
            "message": msg
        })

def run_task_with_cleanup(task_id, target_fn, *args, **kwargs):
    try:
        target_fn(*args, **kwargs)
    finally:
        with active_tasks_lock:
            active_tasks.pop(task_id, None)
        if rate_limited_stdout:
            rate_limited_stdout.clear_task(task_id)

def main():
    global rate_limited_stdout
    setup_windows_job_object()

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    rate_limited_stdout = RateLimitedStdout(sys.stdout, min_interval=0.4, throttled_types={"progress", "progress_ffmpeg", "log"})
    sys.stdout = rate_limited_stdout

    from System.download_handler import DownloadHandler, DownloadMetadataHandler, SearchHandler
    from System.convert_handler import ConvertMetadataHandler, ConvertHandler
    from System.compress_handler import CompressHandler

    emit_json({"type": "ready", "message": "Bridge is ready"})

    try:
        for line in sys.stdin:
            try:
                if not line.strip():
                    continue

                data = json.loads(line)
                command = data.get("command")
                task_id = data.get("id")

                if command in ("download", "metadata_d", "metadata_c", "metadata", "convert", "compress", "search"):
                    if not task_id:
                        emit_json({"type": "error", "message": "No ID provided"})
                        continue

                    args = data.get("args", [])
                    payload = data.get("payload", None)

                    if command == "download":
                        handler = DownloadHandler(task_id)
                        t = KillableThread(target=run_task_with_cleanup, args=(task_id, handler.run, args), daemon=True)
                    elif command in ("metadata_d", "metadata"):
                        handler = DownloadMetadataHandler(task_id)
                        t = KillableThread(target=run_task_with_cleanup, args=(task_id, handler.run, args), daemon=True)
                    elif command == "metadata_c":
                        handler = ConvertMetadataHandler(task_id)
                        t = KillableThread(target=run_task_with_cleanup, args=(task_id, handler.run, args), daemon=True)
                    elif command == "convert":
                        handler = ConvertHandler(task_id)
                        t = KillableThread(target=run_task_with_cleanup, args=(task_id, handler.run, args, payload), daemon=True)
                    elif command == "compress":
                        handler = CompressHandler(task_id)
                        t = KillableThread(target=run_task_with_cleanup, args=(task_id, handler.run, args, payload), daemon=True)
                    elif command == "search":
                        handler = SearchHandler(task_id)
                        t = KillableThread(target=run_task_with_cleanup, args=(task_id, handler.run, args), daemon=True)

                    with active_tasks_lock:
                        active_tasks[task_id] = t
                    t.start()

                elif command == "cancel":
                    kill_processes_for_task(task_id)
                    with active_tasks_lock:
                        thread = active_tasks.get(task_id)
                        if thread:
                            active_tasks.pop(task_id, None)
                    if rate_limited_stdout:
                        rate_limited_stdout.clear_task(task_id)
                    if thread and thread.is_alive():
                        thread.terminate()
                        emit_json({"type": "cancelled", "id": task_id})
                    else:
                        emit_json({"type": "error", "message": "Task not found"})

                elif command == "exit":
                    break

            except json.JSONDecodeError:
                emit_json({"type": "error", "message": "Invalid JSON"})
            except Exception as e:
                msg = f"Global Error: {str(e)}\n{traceback.format_exc()}"
                emit_json({"type": "error", "message": msg})
    finally:
        with active_tasks_lock:
            active_ids = list(active_tasks.keys())
            active_tasks.clear()

        for active_id in active_ids:
            kill_processes_for_task(active_id)

        kill_all_ffmpeg()

if __name__ == "__main__":
    main()