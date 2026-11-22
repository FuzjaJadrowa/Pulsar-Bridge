import json
import os
import subprocess
import sys
import unittest

BRIDGE_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAIN_SCRIPT = os.path.join(BRIDGE_ROOT, "main.py")

class RuntimeProtocolTests(unittest.TestCase):
    def _spawn_bridge(self):
        return subprocess.Popen(
            [sys.executable, "-u", MAIN_SCRIPT],
            cwd=BRIDGE_ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
        )

    def _read_json(self, proc):
        line = proc.stdout.readline()
        self.assertTrue(line, "Bridge stdout closed unexpectedly")
        return json.loads(line)

    def _send_json(self, proc, payload):
        proc.stdin.write(json.dumps(payload) + "\n")
        proc.stdin.flush()

    def test_ready_handshake_and_normalized_terminal_errors(self):
        proc = self._spawn_bridge()
        try:
            ready = self._read_json(proc)
            self.assertEqual(ready.get("type"), "ready")
            self.assertEqual(ready.get("protocol_version"), 1)

            proc.stdin.write("{not-valid-json\n")
            proc.stdin.flush()
            invalid_json = self._read_json(proc)
            self.assertEqual(invalid_json.get("type"), "error")
            self.assertEqual(invalid_json.get("code"), "INVALID_JSON")

            self._send_json(proc, {"command": "convert", "id": "req-bad-args", "args": "not-a-list"})
            bad_args = self._read_json(proc)
            self.assertEqual(bad_args.get("type"), "finished")
            self.assertEqual(bad_args.get("id"), "req-bad-args")
            self.assertIs(bad_args.get("success"), False)
            self.assertEqual(bad_args.get("code"), "INVALID_ARGS")

            self._send_json(proc, {"command": "compress", "id": "req-bad-payload", "payload": ["nope"]})
            bad_payload = self._read_json(proc)
            self.assertEqual(bad_payload.get("type"), "finished")
            self.assertEqual(bad_payload.get("id"), "req-bad-payload")
            self.assertIs(bad_payload.get("success"), False)
            self.assertEqual(bad_payload.get("code"), "INVALID_PAYLOAD")

            self._send_json(proc, {"command": "does_not_exist", "id": "req-unknown"})
            unknown = self._read_json(proc)
            self.assertEqual(unknown.get("type"), "error")
            self.assertEqual(unknown.get("id"), "req-unknown")
            self.assertEqual(unknown.get("code"), "UNKNOWN_COMMAND")

            self._send_json(proc, {"command": "cancel", "id": "missing-task"})
            missing = self._read_json(proc)
            self.assertEqual(missing.get("type"), "error")
            self.assertEqual(missing.get("command"), "cancel")
            self.assertEqual(missing.get("code"), "TASK_NOT_FOUND")

            self._send_json(proc, {"command": "exit"})
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