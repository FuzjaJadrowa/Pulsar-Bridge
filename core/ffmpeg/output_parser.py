import re
from typing import Dict, Optional

class FFMpegOutputParser:
    _PROGRESS_PATTERN = re.compile(
        r"frame=\s*(?P<frame>\d+)"
        r"(?:\s+fps=\s*(?P<fps>[\d.]+))?"
        r"(?:\s+q=\s*(?P<q>-?[\d.]+))?"
        r"(?:\s+size=\s*(?P<size>\S+))?"
        r"\s+time=(?P<time>\d{2}:\d{2}:\d{2}\.\d+)"
        r"(?:\s+bitrate=\s*(?P<bitrate>[^\s]+))?"
        r"(?:\s+speed=\s*(?P<speed>[^\s]+))?"
    )

    def parse_progress_line(self, message: str) -> Optional[Dict[str, str]]:
        if "frame=" not in message or "time=" not in message:
            return None

        match = self._PROGRESS_PATTERN.search(message)
        if not match:
            return None

        result = {k: v for k, v in match.groupdict().items() if v is not None}
        return result