import json


class YTMusicSearchHandler:
    def __init__(self, task_id):
        self.task_id = task_id

    @staticmethod
    def _parse_limit(value):
        try:
            limit = int(value)
        except Exception:
            return 10
        return max(1, min(limit, 50))

    @staticmethod
    def _duration_to_seconds(duration_str):
        if not duration_str:
            return None
        parts = str(duration_str).strip().split(':')
        if not parts or any(not p.isdigit() for p in parts):
            return None
        total = 0
        for part in parts:
            total = total * 60 + int(part)
        return total

    @staticmethod
    def _format_duration(seconds):
        if seconds is None:
            return None
        try:
            total = int(float(seconds))
        except (TypeError, ValueError):
            return None
        if total < 0:
            total = 0
        hrs = total // 3600
        mins = (total % 3600) // 60
        secs = total % 60
        if hrs > 0:
            return f"{hrs}:{mins:02d}:{secs:02d}"
        return f"{mins:02d}:{secs:02d}"

    @staticmethod
    def _pick_thumbnail(thumbnails):
        if isinstance(thumbnails, list) and thumbnails:
            for item in reversed(thumbnails):
                url = item.get("url") if isinstance(item, dict) else None
                if url:
                    return url
        return None

    @staticmethod
    def _join_artists(artists):
        if not isinstance(artists, list):
            return None
        names = []
        for artist in artists:
            if isinstance(artist, dict):
                name = artist.get("name")
                if name:
                    names.append(name)
        return ", ".join(names) if names else None

    def run(self, args):
        try:
            query = str(args[0]).strip() if args else ""
            if not query:
                print(json.dumps({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": "No query provided for YT Music search"
                }), flush=True)
                return

            limit = self._parse_limit(args[1]) if len(args) > 1 else 10

            try:
                from ytmusicapi import YTMusic
            except Exception as e:
                print(json.dumps({
                    "type": "finished",
                    "id": self.task_id,
                    "success": False,
                    "error": f"ytmusicapi not available: {str(e)}"
                }), flush=True)
                return

            yt = YTMusic()
            results = yt.search(query, filter="songs", limit=limit) or []
            if not results:
                results = yt.search(query, filter="videos", limit=limit) or []

            output = []
            for entry in results:
                if not isinstance(entry, dict):
                    continue
                video_id = entry.get("videoId")
                if not video_id:
                    continue

                title = entry.get("title") or entry.get("name")
                artists = self._join_artists(entry.get("artists"))
                author = artists or entry.get("artist") or entry.get("author") or entry.get("channel")

                duration_str = entry.get("duration")
                duration_seconds = entry.get("duration_seconds")
                if duration_seconds is None:
                    duration_seconds = self._duration_to_seconds(duration_str)
                if duration_str is None and duration_seconds is not None:
                    duration_str = self._format_duration(duration_seconds)

                thumbnail_url = self._pick_thumbnail(entry.get("thumbnails"))
                url = f"https://music.youtube.com/watch?v={video_id}"

                output.append({
                    "id": video_id,
                    "title": title,
                    "uploader": author,
                    "duration": duration_seconds,
                    "duration_string": duration_str,
                    "thumbnail": thumbnail_url,
                    "url": url
                })
                if len(output) >= limit:
                    break

            print(json.dumps({
                "type": "search_results",
                "id": self.task_id,
                "success": True,
                "data": output
            }), flush=True)

        except Exception as e:
            print(json.dumps({
                "type": "finished",
                "id": self.task_id,
                "success": False,
                "error": str(e)
            }), flush=True)