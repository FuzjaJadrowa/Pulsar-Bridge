import base64
import html as html_lib
import ipaddress
import re
import urllib.error
import urllib.parse
import urllib.request
from yt_dlp.extractor.common import InfoExtractor
from yt_dlp.networking.exceptions import HTTPError
from yt_dlp.utils import ExtractorError, smuggle_url

_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
)
_VIDER_ROOTS = ("vider.info", "vider.pl", "vider.love", "vider.net")
_CAPTCHA_MARKERS = ("recaptcha", "g-recaptcha", "hcaptcha", "captcha")


class ViderResolveError(Exception):
    pass


class ViderCaptchaRequiredError(ViderResolveError):
    pass


class ViderAccessBlockedError(ViderResolveError):
    pass


def _host_matches(host):
    host = (host or "").lower().rstrip(".")
    return any(host == root or host.endswith("." + root) for root in _VIDER_ROOTS)


def is_vider_url(url):
    if not url:
        return False
    try:
        parsed = urllib.parse.urlparse(str(url).strip())
    except Exception:
        return False
    return parsed.scheme in ("http", "https") and _host_matches(parsed.hostname)


def _looks_like_captcha(text):
    lowered = (text or "").lower()
    return any(marker in lowered for marker in _CAPTCHA_MARKERS)


def _read_error_body(exc):
    try:
        raw = exc.read(256 * 1024)
        return raw.decode("utf-8", errors="replace")
    except Exception:
        return ""


def _fetch_text(url, *, referer=None, timeout=10, captcha_on_404=False, cookiejar=None):
    headers = {
        "User-Agent": _USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
    }
    if referer:
        headers["Referer"] = referer

    request = urllib.request.Request(url, headers=headers)
    if cookiejar is not None:
        try:
            cookiejar.add_cookie_header(request)
        except Exception:
            pass
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read(2 * 1024 * 1024)
            charset = response.headers.get_content_charset() or "utf-8"
            text = raw.decode(charset, errors="replace")
            if _looks_like_captcha(text):
                raise ViderCaptchaRequiredError(
                    "Vider requires CAPTCHA for this request."
                )
            return text, response.geturl(), response.headers
    except urllib.error.HTTPError as exc:
        body = _read_error_body(exc)
        if (captcha_on_404 and exc.code == 404) or _looks_like_captcha(body):
            raise ViderCaptchaRequiredError(
                "Vider requires CAPTCHA for this request."
            ) from exc
        if exc.code in (403, 429):
            raise ViderAccessBlockedError(
                f"Vider access blocked (HTTP {exc.code})."
            ) from exc
        raise ViderResolveError(f"Vider returned HTTP {exc.code} while resolving the video.") from exc
    except ViderResolveError:
        raise
    except Exception as exc:
        raise ViderResolveError(f"Unable to resolve Vider: {exc}") from exc


def _extract_video_id(url):
    parsed = urllib.parse.urlparse(str(url).strip())
    path = urllib.parse.unquote(parsed.path or "")

    patterns = (
        r"/(?:vid)/([^/?#]+)",
        r"/(?:embed/video)/([^/?#]+)",
        r"/(?:video)/([^/?#]+)/",
    )
    for pattern in patterns:
        match = re.search(pattern, path, flags=re.IGNORECASE)
        if match:
            return match.group(1)
    return None


def _decode_base64(value):
    if not value:
        return None
    text = value.strip()
    padding = "=" * ((4 - len(text) % 4) % 4)
    for decoder in (base64.b64decode, base64.urlsafe_b64decode):
        try:
            decoded = decoder((text + padding).encode("ascii")).decode("utf-8", errors="strict")
            if decoded.startswith(("http://", "https://")):
                return decoded
        except Exception:
            continue
    return None


def _decode_video_url(raw_value):
    if not raw_value:
        return None
    value = html_lib.unescape(raw_value).strip()
    if value.startswith(("http://", "https://")):
        return value

    if value.startswith("="):
        decoded = _decode_base64(value[::-1])
        if decoded:
            return decoded

    return _decode_base64(value)


def _strip_tags(value):
    if not value:
        return None
    value = re.sub(r"<[^>]+>", " ", value)
    value = html_lib.unescape(value)
    value = re.sub(r"\s+", " ", value).strip()
    return value or None


def _extract_title(page_html):
    if not page_html:
        return None

    meta_patterns = (
        r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:title["\']',
        r'<meta[^>]+name=["\']twitter:title["\'][^>]+content=["\']([^"\']+)["\']',
    )
    for pattern in meta_patterns:
        match = re.search(pattern, page_html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            title = _strip_tags(match.group(1))
            if title:
                return title

    h1 = re.search(r'<h1\b[^>]*>(.*?)</h1>', page_html, flags=re.IGNORECASE | re.DOTALL)
    if h1:
        title = _strip_tags(h1.group(1))
        if title:
            return title

    title_tag = re.search(r'<title\b[^>]*>(.*?)</title>', page_html, flags=re.IGNORECASE | re.DOTALL)
    if title_tag:
        title = _strip_tags(title_tag.group(1))
        if title:
            title = re.sub(r"\s*[|\-–—]\s*Vider(?:\.info|\.pl|\.love|\.net)?\s*$", "", title, flags=re.IGNORECASE)
            return title.strip() or None
    return None


def _extract_thumbnail(page_html, base_url):
    if not page_html:
        return None
    patterns = (
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
    )
    for pattern in patterns:
        match = re.search(pattern, page_html, flags=re.IGNORECASE | re.DOTALL)
        if match:
            value = html_lib.unescape(match.group(1)).strip()
            if value:
                return urllib.parse.urljoin(base_url, value)
    return None


def _is_safe_http_url(url):
    try:
        parsed = urllib.parse.urlparse(url)
    except Exception:
        return False
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return False

    host = parsed.hostname.lower().rstrip(".")
    if host == "localhost" or host.endswith(".localhost"):
        return False
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return True
    return not (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved or ip.is_multicast)


def resolve_vider_url(url, timeout=10, cookiejar=None, fetch_text=None):
    if not is_vider_url(url):
        return None

    original_url = str(url).strip()
    fetch_text = fetch_text or _fetch_text
    parsed = urllib.parse.urlparse(original_url)
    original_host = parsed.hostname or "vider.info"

    video_id = _extract_video_id(original_url)
    if not video_id:
        raise ViderResolveError("Unsupported Vider URL: video ID was not found.")

    quoted_id = urllib.parse.quote(video_id, safe="+,._-")

    candidate_hosts = [original_host]
    if cookiejar is not None and original_host.lower() != "vider.pl":
        candidate_hosts.append("vider.pl")

    last_error = None
    embed_html = None
    page_url = None
    embed_url = None
    resolved_host = None

    for host in candidate_hosts:
        if host == original_host and parsed.path.lower().startswith("/vid/"):
            candidate_page_url = original_url
        else:
            candidate_page_url = f"https://{host}/vid/{quoted_id}"
        candidate_embed_url = f"https://{host}/embed/video/{quoted_id}"

        try:
            candidate_embed_html, _, _ = fetch_text(
                candidate_embed_url,
                referer=candidate_page_url,
                timeout=timeout,
                captcha_on_404=True,
                cookiejar=cookiejar,
            )
            embed_html = candidate_embed_html
            page_url = candidate_page_url
            embed_url = candidate_embed_url
            resolved_host = host
            break
        except (ViderCaptchaRequiredError, ViderAccessBlockedError, ViderResolveError) as exc:
            last_error = exc
            continue

    if embed_html is None:
        if isinstance(last_error, ViderCaptchaRequiredError):
            if cookiejar is None:
                raise ViderCaptchaRequiredError(
                    "Vider requires browser cookies. Retry with --cookies-from-browser setting."
                ) from last_error
            raise ViderCaptchaRequiredError(
                f"Vider CAPTCHA is not solved. Open link in the browser and solve the CAPTCHA."
            ) from last_error
        if last_error:
            raise last_error
        raise ViderResolveError("Unable to resolve Vider player.")

    media_url = None
    match = re.search(r'data-video-url\s*=\s*["\']([^"\']+)["\']', embed_html, flags=re.IGNORECASE)
    if match:
        media_url = _decode_video_url(match.group(1))

    if not media_url and "," not in video_id:
        media_url = f"https://stream.vider.info/video/{quoted_id}/v.mp4?uid=0"

    if not media_url:
        raise ViderResolveError("Vider page was reachable, but no video URL was exposed by the player.")
    if not _is_safe_http_url(media_url):
        raise ViderResolveError("Vider returned an unsafe or unsupported media URL.")

    title = _extract_title(embed_html)
    thumbnail = _extract_thumbnail(embed_html, embed_url)

    if not title or not thumbnail:
        try:
            page_html, final_page_url, _ = fetch_text(
                page_url,
                timeout=timeout,
                cookiejar=cookiejar,
            )
            title = title or _extract_title(page_html)
            thumbnail = thumbnail or _extract_thumbnail(page_html, final_page_url)
        except ViderResolveError:
            pass

    return {
        "id": video_id,
        "title": title or f"Vider {video_id}",
        "thumbnail": thumbnail,
        "webpage_url": page_url,
        "media_url": media_url,
        "resolved_host": resolved_host,
        "http_headers": {
            "User-Agent": _USER_AGENT,
            "Referer": page_url,
            "Accept": "*/*",
            "Accept-Language": "pl-PL,pl;q=0.9,en-US;q=0.8,en;q=0.7",
        },
        "source": "vider",
    }


def resolve_vider_for_download(urls, cookiejar=None):
    resolved = []
    contexts = []
    for url in urls:
        if not is_vider_url(url):
            resolved.append(url)
            continue
        payload = resolve_vider_url(url, cookiejar=cookiejar)
        resolved.append(payload["media_url"])
        contexts.append(payload)
    return resolved, contexts


def resolve_vider_for_metadata(url, cookiejar=None):
    if not is_vider_url(url):
        return None
    return resolve_vider_url(url, cookiejar=cookiejar)

class ViderIE(InfoExtractor):
    IE_NAME = 'pulsar:vider'
    _VALID_URL = r'https?://(?:[\w-]+\.)?vider\.(?:info|pl|love|net)/(?P<id>[^?#]+)'

    def _fetch_text(self, url, *, referer=None, captcha_on_404=False, **kwargs):
        headers = {'User-Agent': _USER_AGENT}
        if referer:
            headers['Referer'] = referer
        try:
            page, response = self._download_webpage_handle(url, None, headers=headers)
        except ExtractorError as exc:
            status = exc.cause.status if isinstance(exc.cause, HTTPError) else None
            if captcha_on_404 and status == 404:
                raise ViderCaptchaRequiredError('Vider requires CAPTCHA for this request.') from exc
            if status in (403, 429):
                raise ViderAccessBlockedError(f'Vider access blocked (HTTP {status}).') from exc
            raise ViderResolveError(str(exc)) from exc
        if _looks_like_captcha(page):
            raise ViderCaptchaRequiredError('Vider requires CAPTCHA for this request.')
        return page, response.url, response.headers

    def _real_extract(self, url):
        try:
            jar = self._downloader.cookiejar
            payload = resolve_vider_url(url, cookiejar=jar if len(jar) else None,
                                        fetch_text=self._fetch_text)
        except ViderResolveError as exc:
            raise ExtractorError(str(exc), expected=True) from exc
        self.metadata_fallback = payload
        return {
            '_type': 'url_transparent', 'ie_key': 'Generic',
            'url': smuggle_url(payload['media_url'], {
                'referer': payload['http_headers']['Referer'], 'force_videoid': payload['id'],
            }),
            'id': payload['id'], 'title': payload['title'],
            'thumbnail': payload.get('thumbnail'), 'webpage_url': payload['webpage_url'],
            'http_headers': payload['http_headers'],
            'resolver': {'source': 'vider', 'player_host': payload['resolved_host']},
        }