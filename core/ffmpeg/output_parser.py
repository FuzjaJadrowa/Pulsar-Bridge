import re
from typing import Dict, Optional

class FFMpegOutputParser:
    _DURATION = re.compile(r"Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)")
    _PROGRESS_FIELDS = re.compile(
        r"(?:^|\s)(?P<key>frame|fps|q|size|Lsize|time|bitrate|speed)=\s*(?P<value>[^\s]+)"
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

        fields = {match.group("key"): match.group("value") for match in self._PROGRESS_FIELDS.finditer(message)}
        if "frame" not in fields or "time" not in fields:
            return None

        result = {key: value for key, value in fields.items() if key not in ("Lsize",)}
        if "Lsize" in fields:
            result["size"] = fields["Lsize"]
        result["stage"] = "converting"
        seconds = self._time_seconds(result.get("time"))
        if seconds is None:
            return None
        if seconds is not None and self.duration and self.duration > 0:
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
            return None