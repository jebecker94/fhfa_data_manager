"""Download utilities for scraping web pages and downloading files."""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import Iterable, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

from fhfa_data_manager.config import ensure_parent_dir

logger = logging.getLogger(__name__)


def download_from_page(
    base_url: str,
    download_dir: Path,
    allowed_extensions: Iterable[str],
    included_substrings: Optional[Iterable[str]] = None,
    pause_seconds: int = 5,
    *,
    overwrite: bool = False,
) -> None:
    """Scrape page for links and download files that match filters.

    - allowed_extensions: case-insensitive (e.g., ['.zip'])
    - included_substrings: optional case-insensitive filename substrings to include
    """
    download_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(base_url)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a', href=True)

    allowed_lower = tuple(ext.lower() for ext in allowed_extensions)
    substrings_lower = tuple(s.lower() for s in included_substrings) if included_substrings else tuple()

    for link in links:
        if not isinstance(link, Tag):
            continue
        href_value = link.get('href')
        if not isinstance(href_value, str) or not href_value:
            continue
        file_url = urljoin(base_url, href_value)
        parsed = urlparse(file_url)
        file_name = os.path.basename(parsed.path)
        if not file_name:
            continue

        _, ext = os.path.splitext(file_name)
        if ext.lower() not in allowed_lower:
            continue

        if substrings_lower and all(s not in file_name.lower() for s in substrings_lower):
            continue

        file_path = download_dir / file_name
        if file_path.exists() and not overwrite:
            continue

        try:
            logger.info('Downloading %s...', file_url)
            file_response = requests.get(file_url)
            file_response.raise_for_status()
            ensure_parent_dir(file_path)
            with open(file_path, 'wb') as f:
                f.write(file_response.content)
            logger.info('Saved to %s', file_path)
            time.sleep(pause_seconds)
        except requests.RequestException as e:
            logger.error('Failed to download %s: %s', file_url, e)

