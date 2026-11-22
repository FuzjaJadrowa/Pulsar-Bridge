import multiprocessing
import queue
import threading
import time


class TaskCancelledError(Exception):
    """Raised cooperatively when a Bridge task is cancelled."""


class CancelToken:
    """Thread-safe cooperative cancellation token with immediate callback dispatch."""

    def __init__(self):
        self._event = threading.Event()
        self._lock = threading.Lock()
        self._callbacks = []

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def check(self):
        if self._event.is_set():
            raise TaskCancelledError("Cancelled")

    def register_callback(self, callback):
        invoke_now = False
        with self._lock:
            if self._event.is_set():
                invoke_now = True
            elif callback not in self._callbacks:
                self._callbacks.append(callback)
        if invoke_now:
            try:
                callback()
            except Exception:
                pass

    def unregister_callback(self, callback):
        with self._lock:
            try:
                self._callbacks.remove(callback)
            except ValueError:
                pass

    def cancel(self):
        with self._lock:
            already_set = self._event.is_set()
            self._event.set()
            callbacks = list(self._callbacks)
        if already_set and not callbacks:
            return
        for callback in callbacks:
            try:
                callback()
            except Exception:
                pass


class TaskContext:
    """Per-task lifecycle guard ensuring cooperative cancel and a single terminal event."""

    def __init__(self, task_id, emit_fn, cancel_token=None):
        self.task_id = task_id
        self.cancel_token = cancel_token or CancelToken()
        self._emit_fn = emit_fn
        self._terminal_lock = threading.Lock()
        self._terminal_emitted = False

    @property
    def terminal_emitted(self) -> bool:
        with self._terminal_lock:
            return self._terminal_emitted

    def emit_terminal(self, payload: dict) -> bool:
        with self._terminal_lock:
            if self._terminal_emitted:
                return False
            self._terminal_emitted = True
        self._emit_fn(payload)
        return True


def _worker_entrypoint(target, args, conn):
    try:
        def report_progress(percent, status="processing", force=False, eta_seconds=None):
            try:
                conn.send({
                    "kind": "progress",
                    "percent": percent,
                    "status": status,
                    "force": force,
                    "eta_seconds": eta_seconds,
                })
            except Exception:
                pass

        result = target(*args, progress_cb=report_progress)
        conn.send({"kind": "result", "value": result})
    except BaseException as exc:
        try:
            conn.send({
                "kind": "error",
                "type": type(exc).__name__,
                "message": str(exc),
            })
        except Exception:
            pass
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _terminate_process_confirmed(proc, grace_seconds=0.5):
    """Terminate a child process and block until the OS confirms it has exited."""
    if proc is None:
        return
    try:
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=grace_seconds)
        if proc.is_alive():
            kill_fn = getattr(proc, "kill", None)
            if callable(kill_fn):
                kill_fn()
            else:
                proc.terminate()
            proc.join(timeout=2.0)
    except Exception:
        pass
    finally:
        try:
            if not proc.is_alive():
                proc.join(timeout=0.5)
        except Exception:
            pass


def run_in_worker_process(target, args, cancel_token=None, progress_callback=None):
    if cancel_token is not None:
        cancel_token.check()

    ctx = multiprocessing.get_context("spawn")
    parent_conn, child_conn = ctx.Pipe(duplex=False)
    proc = ctx.Process(
        target=_worker_entrypoint,
        args=(target, args, child_conn),
        daemon=True,
    )

    terminate_lock = threading.Lock()
    terminated_by_cancel = {"value": False}

    def on_cancel():
        with terminate_lock:
            terminated_by_cancel["value"] = True
            try:
                if proc.is_alive():
                    proc.terminate()
            except Exception:
                pass

    proc.start()
    child_conn.close()

    if cancel_token is not None:
        cancel_token.register_callback(on_cancel)

    result_payload = None
    error_payload = None

    try:
        while True:
            if cancel_token is not None and cancel_token.is_cancelled:
                _terminate_process_confirmed(proc)
                raise TaskCancelledError("Cancelled")

            has_data = False
            try:
                has_data = parent_conn.poll(0.05)
            except (EOFError, OSError):
                break

            if has_data:
                try:
                    msg = parent_conn.recv()
                except (EOFError, OSError):
                    break

                if not isinstance(msg, dict):
                    continue
                kind = msg.get("kind")
                if kind == "progress" and progress_callback is not None:
                    progress_callback(
                        msg.get("percent"),
                        status=msg.get("status", "processing"),
                        force=bool(msg.get("force", False)),
                        eta_seconds=msg.get("eta_seconds"),
                    )
                elif kind == "result":
                    result_payload = msg
                    break
                elif kind == "error":
                    error_payload = msg
                    break
            elif not proc.is_alive():
                while True:
                    try:
                        if not parent_conn.poll(0):
                            break
                        msg = parent_conn.recv()
                    except (EOFError, OSError):
                        break
                    if isinstance(msg, dict):
                        if msg.get("kind") == "result":
                            result_payload = msg
                        elif msg.get("kind") == "error":
                            error_payload = msg
                break
    finally:
        if cancel_token is not None:
            cancel_token.unregister_callback(on_cancel)
        try:
            parent_conn.close()
        except Exception:
            pass
        if proc.is_alive():
            if (cancel_token is not None and cancel_token.is_cancelled) or terminated_by_cancel["value"]:
                _terminate_process_confirmed(proc)
            else:
                proc.join(timeout=2.0)
                if proc.is_alive():
                    _terminate_process_confirmed(proc)
        else:
            proc.join(timeout=0.5)
        try:
            proc.close()
        except Exception:
            pass

    if (cancel_token is not None and cancel_token.is_cancelled) or terminated_by_cancel["value"]:
        raise TaskCancelledError("Cancelled")

    if error_payload is not None:
        raise RuntimeError(error_payload.get("message") or "Worker process failed.")

    if result_payload is not None:
        return result_payload.get("value")

    raise RuntimeError(f"Worker process exited unexpectedly (exit code {proc.exitcode}).")