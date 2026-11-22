import json
import os
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from core.tasking import CancelToken, TaskCancelledError, TaskContext, run_in_worker_process

BRIDGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN_SCRIPT = os.path.join(BRIDGE_ROOT, "main.py")

def _blocking_sleep_worker(pid_file, duration, progress_cb=None):
    with open(pid_file, "w", encoding="utf-8") as handle:
        handle.write(str(os.getpid()))
    if progress_cb:
        progress_cb(10, force=True)
    time.sleep(duration)
    return "completed"

def _is_pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if sys.platform == "win32":
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        kernel32 = ctypes.windll.kernel32
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            if kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code)):
                return exit_code.value == STILL_ACTIVE
            return False
        finally:
            kernel32.CloseHandle(handle)
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


class CancellationTests(unittest.TestCase):
    def test_cancel_token_callbacks_and_single_terminal_event_guard(self):
        token = CancelToken()
        called = []
        token.register_callback(lambda: called.append("first"))
        token.cancel()
        token.register_callback(lambda: called.append("late"))
        self.assertEqual(called, ["first", "late"])
        with self.assertRaises(TaskCancelledError):
            token.check()

        emitted = []
        ctx = TaskContext("task-1", emitted.append, cancel_token=token)
        self.assertTrue(ctx.emit_terminal({"type": "cancelled", "id": "task-1"}))
        self.assertFalse(ctx.emit_terminal({"type": "finished", "id": "task-1", "success": False}))
        self.assertEqual(len(emitted), 1)
        self.assertEqual(emitted[0]["type"], "cancelled")

    def test_worker_process_is_confirmed_dead_before_cancel_returns(self):
        with tempfile.TemporaryDirectory(prefix="pulsar-q02-") as tmp:
            pid_file = os.path.join(tmp, "worker.pid")
            token = CancelToken()
            started = threading.Event()

            def on_progress(percent, **_kwargs):
                if percent is not None:
                    started.set()

            def cancel_after_start():
                if started.wait(timeout=10):
                    token.cancel()

            canceller = threading.Thread(target=cancel_after_start, daemon=True)
            canceller.start()

            t0 = time.monotonic()
            with self.assertRaises(TaskCancelledError):
                run_in_worker_process(
                    _blocking_sleep_worker,
                    (pid_file, 30.0),
                    cancel_token=token,
                    progress_callback=on_progress,
                )
            elapsed = time.monotonic() - t0
            canceller.join(timeout=2)

            self.assertLess(elapsed, 10.0)
            self.assertTrue(os.path.isfile(pid_file))
            with open(pid_file, "r", encoding="utf-8") as handle:
                worker_pid = int(handle.read().strip())
            self.assertFalse(
                _is_pid_alive(worker_pid),
                f"Worker PID {worker_pid} must be terminated before cancellation completes (Q02).",
            )

    def test_bridge_cancel_ffmpeg_emits_single_cancelled_event_after_process_exit(self):
        ffmpeg_bin = shutil.which("ffmpeg")
        if not ffmpeg_bin:
            self.skipTest("ffmpeg is not available on PATH")

        with tempfile.TemporaryDirectory(prefix="pulsar-q02-ffmpeg-") as tmp:
            dummy_input = os.path.join(tmp, "dummy.mp4")
            with open(dummy_input, "wb") as handle:
                handle.write(b"placeholder")
            output_path = os.path.join(tmp, "out.mp4")

            proc = subprocess.Popen(
                [sys.executable, "-u", MAIN_SCRIPT],
                cwd=BRIDGE_ROOT,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
            )
            try:
                ready = json.loads(proc.stdout.readline())
                self.assertEqual(ready.get("type"), "ready")

                task_id = "cancel-ffmpeg-1"
                cmd = {
                    "command": "convert",
                    "id": task_id,
                    "payload": {
                        "input_path": dummy_input,
                        "output_path": output_path,
                        "output_format": "mp4",
                        "category": "video",
                        "source_duration_seconds": 60.0,
                        "ffmpeg_path": ffmpeg_bin,
                        "ffmpeg_args": [
                            "-hide_banner",
                            "-y",
                            "-re",
                            "-f",
                            "lavfi",
                            "-i",
                            "testsrc=duration=60:size=64x64:rate=10",
                            "-c:v",
                            "libx264",
                            output_path,
                        ],
                    },
                }
                proc.stdin.write(json.dumps(cmd) + "\n")
                proc.stdin.flush()

                first_event = json.loads(proc.stdout.readline())
                self.assertEqual(first_event.get("id"), task_id)

                proc.stdin.write(json.dumps({"command": "cancel", "id": task_id}) + "\n")
                proc.stdin.flush()

                terminal_events = []
                deadline = time.monotonic() + 10.0
                while time.monotonic() < deadline:
                    line = proc.stdout.readline()
                    if not line:
                        break
                    ev = json.loads(line)
                    if ev.get("id") == task_id and ev.get("type") in ("cancelled", "finished"):
                        terminal_events.append(ev)
                        break

                self.assertEqual(len(terminal_events), 1)
                self.assertEqual(terminal_events[0]["type"], "cancelled")

                proc.stdin.write(json.dumps({"command": "exit"}) + "\n")
                proc.stdin.flush()
                remaining = proc.stdout.read()
                for line in remaining.splitlines():
                    if not line.strip():
                        continue
                    ev = json.loads(line)
                    if ev.get("id") == task_id:
                        self.assertNotIn(
                            ev.get("type"),
                            ("cancelled", "finished"),
                            f"Duplicate terminal event emitted after cancel: {ev}",
                        )
                self.assertEqual(proc.wait(timeout=10), 0)
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=5)
                for pipe in (proc.stdin, proc.stdout, proc.stderr):
                    if pipe:
                        pipe.close()

if __name__ == "__main__":
    unittest.main()