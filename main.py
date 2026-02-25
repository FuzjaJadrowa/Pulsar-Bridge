import os
import sys
import json
import traceback
from System.download_handler import DownloadHandler, MetadataHandler, SearchHandler
from System.ytmusic_search import YTMusicSearchHandler
from System.ffmpeg_popen_patch import kill_processes_for_task
from System.killable_thread import KillableThread

active_tasks = {}

def main():
    if sys.platform == "win32":
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stdin = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    print(json.dumps({"type": "ready", "message": "Bridge is ready"}), flush=True)

    for line in sys.stdin:
        try:
            if not line.strip():
                continue

            data = json.loads(line)
            command = data.get("command")
            task_id = data.get("id")

            if command == "download":
                if not task_id:
                    print(json.dumps({"type": "error", "message": "No ID provided"}), flush=True)
                    continue

                args = data.get("args", [])
                handler = DownloadHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()


            elif command == "metadata":
                if not task_id:
                    print(json.dumps({"type": "error", "message": "No ID provided"}), flush=True)
                    continue

                args = data.get("args", [])

                handler = MetadataHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "search":
                if not task_id:
                    print(json.dumps({"type": "error", "message": "No ID provided"}), flush=True)
                    continue

                args = data.get("args", [])

                handler = SearchHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "ytmusic_search":
                if not task_id:
                    print(json.dumps({"type": "error", "message": "No ID provided"}), flush=True)
                    continue

                args = data.get("args", [])

                handler = YTMusicSearchHandler(task_id)
                t = KillableThread(target=handler.run, args=(args,), daemon=True)
                active_tasks[task_id] = t
                t.start()

            elif command == "cancel":
                kill_processes_for_task(task_id)
                thread = active_tasks[task_id]
                if thread.is_alive():
                    thread.terminate()
                    del active_tasks[task_id]
                    print(json.dumps({"type": "cancelled", "id": task_id}), flush=True)
                else:
                    print(json.dumps({"type": "error", "message": "Task not found"}), flush=True)

            elif command == "exit":
                sys.exit(0)

        except json.JSONDecodeError:
            print(json.dumps({"type": "error", "message": "Invalid JSON"}), flush=True)
        except Exception as e:
            msg = f"Global Error: {str(e)}\n{traceback.format_exc()}"
            print(json.dumps({"type": "error", "message": msg}), flush=True)

if __name__ == "__main__":
    main()