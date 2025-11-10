#!/usr/bin/env python3
"""Download product images referenced in the curated catalogue."""

from __future__ import annotations

import argparse
import re
import ssl
import time
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus, unquote, urljoin, urlsplit
from urllib.error import HTTPError, URLError
from urllib.request import HTTPCookieProcessor, HTTPSHandler, Request, build_opener

USER_AGENT = "Mozilla/5.0 (compatible; GearloomImageFetcher/1.0; +https://github.com/)"
ROOT = Path(__file__).resolve().parents[1]
STATIC_DIR = ROOT / "static"
SSL_CONTEXT = ssl._create_unverified_context()
COOKIE_OPENER = build_opener(HTTPSHandler(context=SSL_CONTEXT), HTTPCookieProcessor())

import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from seed_data.product_catalog import PRODUCT_CATALOG


def _http_get(url: str, headers: Optional[dict[str, str]] = None) -> bytes:
    final_headers = {"User-Agent": USER_AGENT}
    if headers:
        final_headers.update({k: v for k, v in headers.items() if v})
    req = Request(url, headers=final_headers)
    last_error: Optional[Exception] = None
    for attempt in range(3):
        try:
            with COOKIE_OPENER.open(req, timeout=30) as resp:  # type: ignore[arg-type]
                return resp.read()
        except (TimeoutError, URLError) as exc:  # type: ignore[attr-defined]
            last_error = exc
            time.sleep(1 + attempt)
            continue
        except HTTPError as exc:
            last_error = exc
            if exc.code in {429, 503} and attempt < 2:
                time.sleep(1 + attempt)
                continue
            raise
    if last_error:
        raise last_error
    raise RuntimeError(f"Failed to fetch {url}")


def _extract_image_url(html: bytes, base_url: str) -> Optional[str]:
    text = html.decode("utf-8", errors="ignore")
    patterns = [
        r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+property=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<meta[^>]+name=["\']twitter:image["\'][^>]+content=["\']([^"\']+)["\']',
        r'<link[^>]+rel=["\']image_src["\'][^>]+href=["\']([^"\']+)["\']',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            candidate = match.group(1).strip()
            if candidate:
                return urljoin(base_url, candidate)
    return None


def _bing_image(query: str) -> Optional[str]:
    search_url = f"https://r.jina.ai/https://www.bing.com/images/search?q={quote_plus(query)}"
    last_error = None
    for attempt in range(3):
        try:
            text = _http_get(search_url).decode("utf-8", errors="ignore")
            break
        except HTTPError as exc:  # type: ignore[attr-defined]
            last_error = exc
            if exc.code in {429, 503} and attempt < 2:
                time.sleep(1 + attempt)
                continue
            raise
    else:
        if last_error:
            raise last_error
        return None
    thumb_match = re.search(r"https://th\.bing\.com/th/id/[^\s\)\"']+", text, flags=re.IGNORECASE)
    if thumb_match:
        return thumb_match.group(0)
    match = re.search(r"mediaurl=(https%3a%2f%2f[^&]+)", text, flags=re.IGNORECASE)
    if not match:
        return None
    encoded_url = match.group(1)
    return unquote(encoded_url)


def download_image(record: dict[str, object], *, force: bool = False) -> None:
    image_rel = Path(str(record["image_path"]))
    destination = STATIC_DIR / image_rel
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not force:
        print(f"[skip] {image_rel}")
        return

    fallback_query = f"{record.get('brand', '')} {record['name']}"
    used_fallback = False

    direct_url = record.get("image_url")  # type: ignore[assignment]
    if direct_url:
        image_url = str(direct_url)
    else:
        product_url = str(record.get("product_url") or "")
        if not product_url:
            raise RuntimeError(f"No product_url for {record['name']}")
        image_url = None
        try:
            html = _http_get(product_url)
            image_url = _extract_image_url(html, product_url)
        except Exception:
            image_url = None
        if not image_url:
            image_url = _bing_image(fallback_query)
            used_fallback = True
        if not image_url:
            raise RuntimeError(f"Could not locate an image for {record['name']}")

    fallback_tries = 0
    while True:
        print(f"[fetch] {image_rel} ← {image_url}")
        referer = None
        try:
            parsed = urlsplit(image_url)  # type: ignore[name-defined]
        except Exception:
            parsed = None
        if parsed and parsed.scheme and parsed.netloc:
            referer = f"{parsed.scheme}://{parsed.netloc}/"
        headers = {"Referer": referer} if referer else None
        try:
            blob = _http_get(image_url, headers=headers)
            destination.write_bytes(blob)
            print(f"[saved] {image_rel} ← {image_url}")
            break
        except HTTPError as exc:
            if not used_fallback and fallback_tries < 3:
                fallback_tries += 1
                new_url = _bing_image(f"{fallback_query} product photo")
                if not new_url:
                    raise
                print(f"[retry] switching to fallback source → {new_url}")
                image_url = new_url
                used_fallback = True
                continue
            if used_fallback and exc.code in {403, 404} and fallback_tries < 3:
                fallback_tries += 1
                new_url = _bing_image(f"{fallback_query} product photo {fallback_tries}")
                if not new_url:
                    raise
                print(f"[retry] swapping image source → {new_url}")
                image_url = new_url
                continue
            raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--force", action="store_true", help="Re-download assets even if they exist")
    args = parser.parse_args()

    for record in PRODUCT_CATALOG:
        try:
            download_image(record, force=args.force)
        except Exception as exc:
            print(f"[error] {record['name']}: {exc}")
            raise


if __name__ == "__main__":
    main()
