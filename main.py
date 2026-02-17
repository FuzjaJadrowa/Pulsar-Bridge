import os
import sys
import json
import traceback
from System.download_handler import DownloadHandler, MetadataHandler
from System.killable_thread import KillableThread

active_tasks = {}


def main():
    user_home = os.path.expanduser("~")
    deno_path = os.path.join(user_home, ".deno", "bin")

    if os.path.exists(deno_path) and deno_path not in os.environ["PATH"]:
        os.environ["PATH"] += os.pathsep + deno_path

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

            elif command == "cancel":
                if task_id in active_tasks:
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