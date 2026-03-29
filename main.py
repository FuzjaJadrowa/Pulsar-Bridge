import sys
import json
import traceback
import threading
from System.ffmpeg_output_parser import FFMpegOutputParser
from System.ffmpeg_popen_patch import kill_processes_for_task
from System.killable_thread import KillableThread

active_tasks = {}

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

    from System.download_handler import DownloadHandler, DownloadMetadataHandler, SearchHandler
    from System.convert_handler import ConvertMetadataHandler, ConvertHandler

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
                sys.exit(0)

        except json.JSONDecodeError:
            emit_json({"type": "error", "message": "Invalid JSON"})
        except Exception as e:
            msg = f"Global Error: {str(e)}\n{traceback.format_exc()}"
            emit_json({"type": "error", "message": msg})

if __name__ == "__main__":
    main()