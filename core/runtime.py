import sys
import json
import threading
import time
from core.ffmpeg.popen_patch import kill_processes_for_task
from core.ffmpeg.runner import kill_all_ffmpeg
from core.tasking import CancelToken, TaskCancelledError, TaskContext
from core.utils import emit_json


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
            ctypes.sizeof(info),
        )
        if res:
            process = kernel32.GetCurrentProcess()
            kernel32.AssignProcessToJobObject(job, process)
    except Exception:
        pass


class TaskHandle:
    def __init__(self, task_id, context: TaskContext, thread: threading.Thread):
        self.task_id = task_id
        self.context = context
        self.cancel_token = context.cancel_token
        self.thread = thread

    def is_alive(self) -> bool:
        return self.thread.is_alive()

    def request_cancel(self):
        self.cancel_token.cancel()
        kill_processes_for_task(self.task_id)


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
            for key in [key for key in self.next_allowed if key[0] == task_id]:
                self.next_allowed.pop(key, None)

    def _should_throttle(self, payload):
        task_id = payload.get("id")
        if not task_id:
            return False
        msg_type = payload.get("type")
        if msg_type not in self.throttled_types:
            return False
        if msg_type == "log" and str(payload.get("level", "")).lower() in ("warning", "error"):
            return False
        if msg_type == "progress":
            try:
                if float(payload.get("percent", 0)) >= 100:
                    return False
            except (TypeError, ValueError):
                pass
        now = time.monotonic()
        throttle_key = (task_id, msg_type)
        next_allowed = self.next_allowed.get(throttle_key, 0.0)
        if now < next_allowed:
            return True
        self.next_allowed[throttle_key] = now + self.min_interval
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
PROTOCOL_VERSION = 1
TASK_COMMANDS = (
    "download",
    "download_ffmpeg",
    "metadata_d",
    "metadata_c",
    "metadata",
    "convert",
    "compress",
    "search",
)


def _emit_task_error(task_id, code, message):
    emit_json({
        "type": "finished",
        "id": task_id,
        "success": False,
        "code": code,
        "error": message,
        "message": message,
    })


def run_task_with_cleanup(task_context: TaskContext, target_fn, *args, **kwargs):
    task_id = task_context.task_id
    cancelled = False
    unexpected_error = None
    try:
        target_fn(*args, **kwargs)
    except TaskCancelledError:
        cancelled = True
    except SystemExit:
        cancelled = True
    except Exception as exc:
        unexpected_error = str(exc)
    finally:
        kill_processes_for_task(task_id)
        with active_tasks_lock:
            current = active_tasks.get(task_id)
            if current is not None and current.context is task_context:
                active_tasks.pop(task_id, None)
        if rate_limited_stdout:
            rate_limited_stdout.clear_task(task_id)

        if not task_context.terminal_emitted:
            if cancelled or task_context.cancel_token.is_cancelled:
                task_context.emit_terminal({
                    "type": "cancelled",
                    "id": task_id,
                })
            elif unexpected_error is not None:
                task_context.emit_terminal({
                    "type": "finished",
                    "id": task_id,
                    "success": False,
                    "code": "INTERNAL_ERROR",
                    "error": unexpected_error,
                    "message": unexpected_error,
                })
            else:
                task_context.emit_terminal({
                    "type": "finished",
                    "id": task_id,
                    "success": False,
                    "code": "MISSING_TERMINAL_EVENT",
                    "error": "Task exited without emitting a terminal response.",
                    "message": "Task exited without emitting a terminal response.",
                })


def main():
    global rate_limited_stdout
    setup_windows_job_object()

    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")
    rate_limited_stdout = RateLimitedStdout(
        sys.stdout,
        min_interval=0.4,
        throttled_types={"progress", "progress_ffmpeg", "log"},
    )
    sys.stdout = rate_limited_stdout

    from features.download.handler import DownloadHandler, DownloadMetadataHandler, SearchHandler
    from features.conversion.handler import ConvertMetadataHandler, ConvertHandler
    from features.compression.handler import CompressHandler

    emit_json({"type": "ready", "protocol_version": PROTOCOL_VERSION})

    try:
        for line in sys.stdin:
            task_id = None
            command = None
            try:
                if not line.strip():
                    continue

                data = json.loads(line)
                if not isinstance(data, dict):
                    emit_json({
                        "type": "error",
                        "code": "INVALID_COMMAND",
                        "error": "Command must be a JSON object",
                        "message": "Command must be a JSON object",
                    })
                    continue

                command = data.get("command")
                task_id = data.get("id")
                if not isinstance(command, str):
                    emit_json({
                        "type": "error",
                        "id": task_id if isinstance(task_id, str) else None,
                        "code": "INVALID_COMMAND",
                        "error": "Command must be a string",
                        "message": "Command must be a string",
                    })
                    continue

                if command in TASK_COMMANDS:
                    if not isinstance(task_id, str) or not task_id.strip():
                        emit_json({
                            "type": "error",
                            "code": "INVALID_TASK_ID",
                            "error": "Task ID must be a non-empty string",
                            "message": "Task ID must be a non-empty string",
                        })
                        continue

                    args = data.get("args", [])
                    payload = data.get("payload", None)
                    if not isinstance(args, list) or not all(isinstance(arg, str) for arg in args):
                        _emit_task_error(task_id, "INVALID_ARGS", "Args must be a list of strings")
                        continue
                    if payload is not None and not isinstance(payload, dict):
                        _emit_task_error(task_id, "INVALID_PAYLOAD", "Payload must be a JSON object")
                        continue

                    with active_tasks_lock:
                        existing = active_tasks.get(task_id)
                        if existing and existing.is_alive():
                            _emit_task_error(
                                task_id,
                                "TASK_ALREADY_RUNNING",
                                "A task with this ID is already running",
                            )
                            continue

                    cancel_token = CancelToken()
                    task_context = TaskContext(task_id, emit_json, cancel_token=cancel_token)

                    if command == "download":
                        handler = DownloadHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run
                        target_args = (args,)
                    elif command == "download_ffmpeg":
                        handler = DownloadHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run_ffmpeg_direct
                        target_args = (args,)
                    elif command in ("metadata_d", "metadata"):
                        handler = DownloadMetadataHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run
                        target_args = (args,)
                    elif command == "metadata_c":
                        handler = ConvertMetadataHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run
                        target_args = (args,)
                    elif command == "convert":
                        handler = ConvertHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run
                        target_args = (args, payload)
                    elif command == "compress":
                        handler = CompressHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run
                        target_args = (args, payload)
                    elif command == "search":
                        handler = SearchHandler(task_id, cancel_token=cancel_token, task_context=task_context)
                        target_fn = handler.run
                        target_args = (args,)

                    thread = threading.Thread(
                        target=run_task_with_cleanup,
                        args=(task_context, target_fn, *target_args),
                        daemon=True,
                    )
                    handle = TaskHandle(task_id, task_context, thread)

                    with active_tasks_lock:
                        existing = active_tasks.get(task_id)
                        if existing and existing.is_alive():
                            _emit_task_error(
                                task_id,
                                "TASK_ALREADY_RUNNING",
                                "A task with this ID is already running",
                            )
                            continue
                        active_tasks[task_id] = handle
                        try:
                            thread.start()
                        except Exception as start_exc:
                            active_tasks.pop(task_id, None)
                            _emit_task_error(task_id, "START_FAILED", str(start_exc))
                            continue

                elif command == "cancel":
                    with active_tasks_lock:
                        handle = active_tasks.get(task_id)
                    if handle and handle.is_alive():
                        handle.request_cancel()
                    else:
                        kill_processes_for_task(task_id)
                        if rate_limited_stdout:
                            rate_limited_stdout.clear_task(task_id)
                        emit_json({
                            "type": "error",
                            "command": "cancel",
                            "id": task_id if isinstance(task_id, str) else None,
                            "code": "TASK_NOT_FOUND",
                            "message": "Task not found",
                        })

                elif command == "exit":
                    break
                else:
                    msg = f"Unknown command: {command}"
                    emit_json({
                        "type": "error",
                        "id": task_id if isinstance(task_id, str) else None,
                        "code": "UNKNOWN_COMMAND",
                        "error": msg,
                        "message": msg,
                    })

            except json.JSONDecodeError:
                emit_json({
                    "type": "error",
                    "code": "INVALID_JSON",
                    "error": "Invalid JSON",
                    "message": "Invalid JSON",
                })
            except Exception as e:
                if command in TASK_COMMANDS and isinstance(task_id, str) and task_id.strip():
                    _emit_task_error(task_id, "DISPATCH_ERROR", str(e))
                else:
                    emit_json({
                        "type": "error",
                        "id": task_id if isinstance(task_id, str) else None,
                        "code": "INTERNAL_ERROR",
                        "error": str(e),
                        "message": str(e),
                    })
    finally:
        with active_tasks_lock:
            handles = list(active_tasks.values())
            active_tasks.clear()

        for handle in handles:
            handle.request_cancel()

        kill_all_ffmpeg()

        deadline = time.monotonic() + 2.0
        for handle in handles:
            remaining = max(0.0, deadline - time.monotonic())
            if remaining > 0 and handle.is_alive():
                handle.thread.join(timeout=remaining)

if __name__ == "__main__":
    main()