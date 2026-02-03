import sys
import json
import traceback
import threading
from System.download_handler import DownloadHandler


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

            if command == "download":
                handler = DownloadHandler()
                t = threading.Thread(target=handler.run, args=(data,), daemon=True)
                t.start()

            elif command == "exit":
                sys.exit(0)

        except json.JSONDecodeError:
            print(json.dumps({"type": "error", "message": "Invalid JSON received"}), flush=True)
        except Exception as e:
            error_msg = f"Global Error: {str(e)}\n{traceback.format_exc()}"
            print(json.dumps({"type": "error", "message": error_msg}), flush=True)


if __name__ == "__main__":
    main()