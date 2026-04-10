import sys
import json
import traceback
import threading
import time
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_popen_patch import kill_processes_for_task
from System.ffmpeg_runner import kill_all_ffmpeg
from System.killable_thread import KillableThread

active_tasks = {}

class RateLimitedStdout:
    def __init__(self, stream, min_interval=0.4, throttled_types=None):
        self.stream = stream
        self.min_interval = float(min_interval)
        self.throttled_types = set(throttled_types or ())
        self.buffer = ""
        self.lock = threading.Lock()
        self.next_allowed = {}

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
                payload = None
                try:
                    payload = json.loads(line)
                except Exception:
                    payload = None
                if payload and self._should_throttle(payload):
                    continue
                self.stream.write(line + "\n")
            self.stream.flush()
        return len(data)

    def flush(self):
        with self.lock:
            if self.buffer:
                payload = None
                try:
                    payload = json.loads(self.buffer)
                except Exception:
                    payload = None
                if not (payload and self._should_throttle(payload)):
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

def emit_json(payload):
    print(json.dumps(payload), flush=True)

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

def main():
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    sys.stdout = RateLimitedStdout(sys.stdout, min_interval=0.4, throttled_types={"progress", "progress_ffmpeg", "log"})

    from System.download_handler import DownloadHandler, DownloadMetadataHandler, SearchHandler
    from System.convert_handler import ConvertMetadataHandler, ConvertHandler
    from System.compress_handler import CompressHandler

    emit_json({"type": "ready", "message": "Bridge is ready"})

    for line in sys.stdin:
        try:
            if not line.strip():
                continue

            data = json.loads(line)
            command = data.get("command")
            task_id = data.get("id")

            if command == "download":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                handler = DownloadHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "metadata_d":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                handler = DownloadMetadataHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "metadata_c":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                handler = ConvertMetadataHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            # to delete in future
            elif command == "metadata":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                handler = DownloadMetadataHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "convert":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                payload = data.get("payload", None)
                handler = ConvertHandler(task_id)
                t = KillableThread(target=handler.run, args=(args, payload), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "compress":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                payload = data.get("payload", None)
                handler = CompressHandler(task_id)
                t = KillableThread(target=handler.run, args=(args, payload), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "metadata":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])
                handler = DownloadMetadataHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "search":
                if not task_id:
                    emit_json({"type": "error", "message": "No ID provided"})
                    continue

                args = data.get("args", [])

                handler = SearchHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "cancel":
                kill_processes_for_task(task_id)
                thread = active_tasks[task_id]
                if thread.is_alive():
                    thread.terminate()
                    del active_tasks[task_id]
                    emit_json({"type": "cancelled", "id": task_id})
                else:
                    emit_json({"type": "error", "message": "Task not found"})

            elif command == "exit":
                for active_id in list(active_tasks.keys()):
                    kill_processes_for_task(active_id)
                    thread = active_tasks.get(active_id)
                    if thread and thread.is_alive():
                        thread.terminate()
                    active_tasks.pop(active_id, None)
                kill_all_ffmpeg()
                sys.exit(0)

        except json.JSONDecodeError:
            emit_json({"type": "error", "message": "Invalid JSON"})
        except Exception as e:
            msg = f"Global Error: {str(e)}\n{traceback.format_exc()}"
            emit_json({"type": "error", "message": msg})

if __name__ == "__main__":
    main()