import json
import sys
import traceback
from datetime import datetime

from System.download_handler import DownloadHandler, MetadataHandler, SearchHandler
from System.ytmusic_search import YTMusicSearchHandler
from System.ffmpeg_popen_patch import kill_processes_for_task
from System.killable_thread import KillableThread

active_tasks = {}


def _timestamp():
    return datetime.now().strftime("%H:%M:%S")


def _debug(msg):
    sys.__stdout__.write(f"[{_timestamp()}][DEBUG] {msg}\n")
    sys.__stdout__.flush()


def _format_json_line(payload, raw):
    parts = []
    for key in ("type", "status", "event", "id"):
        if key in payload:
            parts.append(f"{key}={payload.get(key)}")
    if "percent" in payload:
        try:
            parts.append(f"percent={float(payload.get('percent')):.2f}")
        except Exception:
            parts.append(f"percent={payload.get('percent')}")
    if "eta" in payload:
        parts.append(f"eta={payload.get('eta')}")
    if "eta_seconds" in payload:
        parts.append(f"eta_seconds={payload.get('eta_seconds')}")
    if "speed" in payload:
        parts.append(f"speed={payload.get('speed')}")
    if "filename" in payload:
        parts.append(f"file={payload.get('filename')}")
    if "message" in payload:
        parts.append(f"message={payload.get('message')}")
    if "error" in payload:
        parts.append(f"error={payload.get('error')}")

    summary = " ".join(parts) if parts else "event"
    return f"[{_timestamp()}][EVENT] {summary} | raw={raw}"


class DebugStdout:
    def __init__(self, target):
        self._target = target
        self._buffer = ""

    def write(self, data):
        if not data:
            return 0
        self._buffer += data
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            self._emit(line)
        return len(data)

    def flush(self):
        if self._buffer:
            self._emit(self._buffer)
            self._buffer = ""
        self._target.flush()

    def _emit(self, line):
        if line == "":
            self._target.write("\n")
            return
        trimmed = line.strip()
        try:
            payload = json.loads(trimmed)
            output = _format_json_line(payload, trimmed)
        except Exception:
            output = f"[{_timestamp()}][LOG] {trimmed}"
        self._target.write(output + "\n")
        self._target.flush()


def main():
    sys.stdout = DebugStdout(sys.__stdout__)
    sys.stderr = sys.__stderr__

    if sys.platform == "win32":
        import io
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    _debug("Bridge debug mode ready. Waiting for commands on stdin.")

    for line in sys.stdin:
        try:
            if not line.strip():
                continue

            data = json.loads(line)
            command = data.get("command")
            task_id = data.get("id")
            args = data.get("args", [])

            _debug(f"Received command={command} id={task_id} args={args}")

            if command == "download":
                if not task_id:
                    _debug("No ID provided for download command.")
                    continue
                handler = DownloadHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()
                _debug(f"DownloadHandler started for id={task_id}")

            elif command == "metadata":
                if not task_id:
                    _debug("No ID provided for metadata command.")
                    continue
                handler = MetadataHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()
                _debug(f"MetadataHandler started for id={task_id}")

            elif command == "search":
                if not task_id:
                    _debug("No ID provided for search command.")
                    continue
                handler = SearchHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()
                _debug(f"SearchHandler started for id={task_id}")

            elif command == "ytmusic_search":
                if not task_id:
                    _debug("No ID provided for ytmusic search command.")
                    continue
                handler = YTMusicSearchHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()
                _debug(f"YTMusicSearchHandler started for id={task_id}")

            elif command == "cancel":
                if not task_id:
                    _debug("No ID provided for cancel command.")
                    continue
                kill_processes_for_task(task_id)
                thread = active_tasks.get(task_id)
                if thread and thread.is_alive():
                    thread.terminate()
                    del active_tasks[task_id]
                    _debug(f"Cancelled task id={task_id}")
                else:
                    _debug(f"Task not found for cancel id={task_id}")

            elif command == "exit":
                _debug("Exit command received. Shutting down.")
                sys.exit(0)

            else:
                _debug(f"Unknown command: {command}")

        except json.JSONDecodeError:
            _debug("Invalid JSON received on stdin.")
        except Exception as e:
            msg = f"Global Error: {str(e)}\n{traceback.format_exc()}"
            _debug(msg)


if __name__ == "__main__":
    main()