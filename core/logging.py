from core.ffmpeg.output_parser import FFMpegOutputParser
from core.utils import emit_json

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