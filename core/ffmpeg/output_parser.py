import re
from typing import Dict, Optional

class FFMpegOutputParser:
    _DURATION = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")
    _PROGRESS_PATTERN = re.compile(
        r"frame=\s*(?P<frame>\d+)"
        r"(?:\s+fps=\s*(?P<fps>[\d.]+))?"
        r"(?:\s+q=\s*(?P<q>-?[\d.]+))?"
        r"(?:\s+size=\s*(?P<size>\S+))?"
        r"\s+time=(?P<time>\d{2}:\d{2}:\d{2}\.\d+)"
        r"(?:\s+bitrate=\s*(?P<bitrate>[^\s]+))?"
        r"(?:\s+speed=\s*(?P<speed>[^\s]+))?"
    )

    def __init__(self):
        self.duration = None

    def parse_progress_line(self, message: str) -> Optional[Dict[str, str]]:
        duration = self._DURATION.search(message)
        if duration:
            h, m, s = duration.groups()
            self.duration = int(h) * 3600 + int(m) * 60 + float(s)
        if "frame=" not in message or "time=" not in message:
            return None

        match = self._PROGRESS_PATTERN.search(message)
        if not match:
            return None

        result = {k: v for k, v in match.groupdict().items() if v is not None}
        result["stage"] = "converting"
        if self.duration and self.duration > 0:
            seconds = self._time_seconds(result.get("time", "00:00:00"))
            result["percent"] = str(min(100.0, max(0.0, seconds * 100.0 / self.duration)))
            try:
                rate = float(result.get("speed", "").rstrip("x"))
                if rate > 0:
                    result["eta_seconds"] = str(max(0.0, (self.duration - seconds) / rate))
            except ValueError:
                pass
        return result

    @staticmethod
    def _time_seconds(value):
        try:
            h, m, s = value.split(":")
            return int(h) * 3600 + int(m) * 60 + float(s)
        except (ValueError, AttributeError):
            return 0.0