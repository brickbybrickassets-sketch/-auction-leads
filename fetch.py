"""
utils.py — Shared helpers: logging, retries, rate limiting, robots.txt checks,
HTTP session factory, phone normalization, directory setup.
"""
from __future__ import annotations

import json
import logging
import os
import random
import re
import time
import urllib.robotparser
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import requests
from scraper.config import (
    DATA_DIR,
    DASHBOARD_DIR,
    EXPORTS_DIR,
    LOGS_DIR,
    UPLOAD_DIR,
    MAX_DELAY_SECONDS,
    MAX_RETRIES,
    MIN_DELAY_SECONDS,
    REQUEST_TIMEOUT,
    USER_AGENTS,
)

# ─── Logging ───────────────────────────────────────────────────────────────────

def get_logger(name: str = "auction_leads") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        fmt = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
        logger.setLevel(logging.INFO)
    return logger


log = get_logger()


# ─── Directory Setup ───────────────────────────────────────────────────────────

def ensure_dirs() -> None:
    for d in [DATA_DIR, DASHBOARD_DIR, EXPORTS_DIR, LOGS_DIR, UPLOAD_DIR]:
        Path(d).mkdir(parents=True, exist_ok=True)


# ─── Rate Limiting ─────────────────────────────────────────────────────────────

def random_delay(min_s: float = MIN_DELAY_SECONDS, max_s: float = MAX_DELAY_SECONDS) -> None:
    delay = random.uniform(min_s, max_s)
    log.debug(f"Sleeping {delay:.2f}s")
    time.sleep(delay)


# ─── HTTP Session ──────────────────────────────────────────────────────────────

def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "DNT": "1",
    })
    return session


def safe_get(
    session: requests.Session,
    url: str,
    retries: int = MAX_RETRIES,
    timeout: int = REQUEST_TIMEOUT,
    **kwargs,
) -> Optional[requests.Response]:
    """GET with retry and rate-limit awareness. Returns None on failure."""
    for attempt in range(1, retries + 1):
        try:
            random_delay()
            resp = session.get(url, timeout=timeout, **kwargs)
            if resp.status_code == 200:
                return resp
            if resp.status_code in (403, 429):
                log.warning(f"Rate-limited ({resp.status_code}) on {url}. Backing off.")
                time.sleep(min(60 * attempt, 180))
            else:
                log.warning(f"HTTP {resp.status_code} on {url} (attempt {attempt})")
        except requests.RequestException as exc:
            log.error(f"Request error on {url} (attempt {attempt}): {exc}")
        if attempt < retries:
            time.sleep(5 * attempt)
    return None


# ─── robots.txt Checker ────────────────────────────────────────────────────────

@lru_cache(maxsize=64)
def can_fetch(base_url: str, path: str = "/") -> bool:
    """Check robots.txt. Returns True (allowed) if robots.txt is unreachable."""
    robots_url = f"{base_url.rstrip('/')}/robots.txt"
    rp = urllib.robotparser.RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        allowed = rp.can_fetch("*", path)
        if not allowed:
            log.warning(f"robots.txt disallows path={path} on {base_url}")
        return allowed
    except Exception:
        return True  # Fail open — cannot verify, proceed cautiously


# ─── Phone Utilities ───────────────────────────────────────────────────────────

PHONE_RE = re.compile(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}")


def normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    return raw.strip()


def extract_phones(text: str) -> list[str]:
    found = PHONE_RE.findall(text)
    return [normalize_phone(p) for p in found]


# ─── JSON Helpers ──────────────────────────────────────────────────────────────

def load_json(path: str, default: Any = None) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def save_json(path: str, data: Any, indent: int = 2) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, default=str)


# ─── Address Helpers ───────────────────────────────────────────────────────────

def clean_address(addr: str) -> str:
    return re.sub(r"\s+", " ", addr.strip().upper())


def is_absentee(prop_zip: str, mail_zip: str) -> bool:
    if not prop_zip or not mail_zip:
        return False
    return prop_zip.strip()[:5] != mail_zip.strip()[:5]


# ─── Record ID ─────────────────────────────────────────────────────────────────

def make_record_id(address: str, auction_date: str) -> str:
    slug = re.sub(r"[^a-z0-9]", "_", f"{address}_{auction_date}".lower())
    return slug[:80]


# ─── Timestamp ─────────────────────────────────────────────────────────────────

def now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


# ─── Safe Value Extractor ──────────────────────────────────────────────────────

def safe_str(val: Any, default: str = "") -> str:
    if val is None:
        return default
    return str(val).strip()


def safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(str(val).replace(",", "").replace("$", ""))
    except (ValueError, TypeError):
        return default
