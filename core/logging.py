from core.ffmpeg.output_parser import FFMpegOutputParser
from core.utils import emit_json


class BridgeLogger:
    def __init__(self, task_id, cancel_token=None):
        self.task_id = task_id
        self.cancel_token = cancel_token
        self.ffmpeg_parser = FFMpegOutputParser()
        self.last_error = None
        self.last_warning = None

    def _check_cancel(self):
        if self.cancel_token is not None:
            self.cancel_token.check()

    def debug(self, msg):
        self._check_cancel()
        ffmpeg_data = self.ffmpeg_parser.parse_progress_line(msg)
        if ffmpeg_data:
            payload = {
                "type": "progress",
                "id": self.task_id,
                "stage": "converting",
            }
            payload.update(ffmpeg_data)
            emit_json(payload)
        elif not msg.startswith('[download] '):
            pass

    def info(self, msg):
        self._check_cancel()

    def warning(self, msg):
        self._check_cancel()
        self.last_warning = msg
        emit_json({
            "type": "log",
            "level": "warning",
            "id": self.task_id,
            "message": msg,
        })

    def error(self, msg):
        self._check_cancel()
        self.last_error = msg
        emit_json({
            "type": "log",
            "level": "error",
            "id": self.task_id,
            "message": msg,
        })