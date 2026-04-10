from playwright.sync_api import sync_playwright


MEDIA_EXTENSIONS = (".m3u8", ".mp4", ".webm", ".ts")


def is_direct_media_url(url):
    if not isinstance(url, str):
        return False
    lowered = url.lower()
    return any(ext in lowered for ext in MEDIA_EXTENSIONS)


def extract_custom_stream_url(page_url, click_x=400, click_y=300, initial_wait_ms=3000, probe_rounds=12, probe_wait_ms=1000):
    if not isinstance(page_url, str) or not page_url.startswith("http"):
        return page_url
    if is_direct_media_url(page_url):
        return page_url

    found_link = page_url

    def intercept_request(request):
        nonlocal found_link
        request_url = request.url
        lowered = request_url.lower()
        if not any(ext in lowered for ext in MEDIA_EXTENSIONS):
            return
        if found_link == page_url or "master.m3u8" in lowered:
            found_link = request_url

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()
        page.on("request", intercept_request)
        page.goto(page_url, wait_until="domcontentloaded")
        page.wait_for_timeout(initial_wait_ms)
        page.mouse.click(click_x, click_y)
        for _ in range(probe_rounds):
            if found_link != page_url and "master.m3u8" in found_link.lower():
                break
            page.wait_for_timeout(probe_wait_ms)
        browser.close()

    return found_link