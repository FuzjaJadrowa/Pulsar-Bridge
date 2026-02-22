import json
import urllib.request
import urllib.parse

def resolve_spotify_url(url):
    if "spotify.com" in url or "spotify:" in url:
        try:
            oembed_url = "https://open.spotify.com/oembed?url=" + urllib.parse.quote(url)
            req = urllib.request.Request(oembed_url, headers={'User-Agent': 'Mozilla/5.0'})

            with urllib.request.urlopen(req, timeout=5) as response:
                data = json.loads(response.read().decode())
                title = data.get("title", "")
                author = data.get("author_name", "")

                if title or author:
                    return f"ytsearch1:{author} {title}".strip()
        except Exception:
            pass

    return url