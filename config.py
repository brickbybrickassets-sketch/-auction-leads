"""
enrich_owner.py — Owner identification from free public sources.

Sources used (all publicly accessible, no login required):
  1. Cook County Assessor open data API (Socrata)
  2. Cook County Recorder open data API (Socrata)
  3. Illinois SOS business registry (manual/public lookups only)
  4. Google/Bing public search snippets as last resort

COMPLIANCE:
  - All API calls use published public APIs.
  - No login, no CAPTCHA bypass, no paywall.
  - Rate limited and robots-checked.
"""
from __future__ import annotations

import re
from typing import Optional

import requests

from scraper.config import (
    COOK_COUNTY_ASSESSOR_API,
    COOK_COUNTY_RECORDER_API,
    STATE,
)
from scraper.utils import (
    clean_address,
    get_logger,
    make_session,
    random_delay,
    safe_get,
    safe_str,
)

log = get_logger("enrich_owner")

ENTITY_KEYWORDS = [
    "llc", "l.l.c", "inc", "corp", "trust", "land trust", "properties",
    "holdings", "realty", "investment", "enterprises", "group", "fund",
    "capital", "partners", "associates", "management", "solutions",
]


def is_entity(name: str) -> bool:
    n = name.lower()
    return any(kw in n for kw in ENTITY_KEYWORDS)


def split_owner_name(full_name: str) -> tuple[str, str]:
    """Best-effort split of 'LAST FIRST' or 'FIRST LAST' style names."""
    parts = full_name.strip().split()
    if len(parts) == 0:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    # County records often store as "LAST FIRST MI" or "FIRST LAST"
    # Heuristic: if all-caps, assume LAST FIRST
    if full_name == full_name.upper():
        last = parts[0].title()
        first = " ".join(p.title() for p in parts[1:])
    else:
        first = parts[0]
        last = " ".join(parts[1:])
    return first, last


# ─── Cook County Assessor API ─────────────────────────────────────────────────

def _query_cook_assessor(address: str, pin: str = "") -> Optional[dict]:
    """
    Query Cook County Assessor open data for property owner.
    Returns raw API record dict or None.
    Docs: https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json
    """
    session = make_session()
    params: dict = {"$limit": 5}

    if pin:
        pin_clean = re.sub(r"[^\d]", "", pin)
        if pin_clean:
            params["$where"] = f"pin='{pin_clean}'"
    elif address:
        # Socrata full-text search on address
        # Strip unit numbers for better match
        addr_clean = re.sub(r"(apt|unit|#|ste|suite)\s*\w+", "", address, flags=re.I)
        addr_clean = addr_clean.strip()
        params["$q"] = addr_clean

    random_delay(1.5, 3.0)
    resp = safe_get(session, COOK_COUNTY_ASSESSOR_API, params=params)
    if not resp:
        return None

    try:
        data = resp.json()
        if data:
            return data[0]  # return first matching record
    except Exception as exc:
        log.error(f"Assessor API parse error: {exc}")
    return None


def _extract_assessor_fields(api_row: dict) -> dict:
    """Map Cook County Assessor API fields → our owner fields."""
    return {
        "owner_name": safe_str(api_row.get("taxpayer_name") or api_row.get("owner_name") or ""),
        "mail_address": safe_str(api_row.get("taxpayer_address") or api_row.get("mail_address") or ""),
        "mail_city": safe_str(api_row.get("taxpayer_city") or ""),
        "mail_state": safe_str(api_row.get("taxpayer_state") or ""),
        "mail_zip": safe_str(api_row.get("taxpayer_zip") or ""),
        "parcel_number": safe_str(api_row.get("pin") or ""),
    }


# ─── Cook County Recorder API ─────────────────────────────────────────────────

def _query_cook_recorder(pin: str) -> Optional[dict]:
    """
    Query Cook County Recorder open data for deed/transfer records.
    """
    if not pin:
        return None
    session = make_session()
    pin_clean = re.sub(r"[^\d]", "", pin)
    params = {
        "$where": f"pin='{pin_clean}'",
        "$order": "document_date DESC",
        "$limit": 1,
    }
    random_delay(1.5, 3.0)
    resp = safe_get(session, COOK_COUNTY_RECORDER_API, params=params)
    if not resp:
        return None
    try:
        data = resp.json()
        if data:
            return data[0]
    except Exception as exc:
        log.error(f"Recorder API parse error: {exc}")
    return None


# ─── Google/Bing Snippet Fallback ─────────────────────────────────────────────

def _google_owner_search(address: str, city: str, state: str) -> Optional[str]:
    """
    Use Google/Bing public search to find owner name snippets.
    Only reads publicly visible result snippets — no scraping of full pages.
    """
    from scraper.utils import can_fetch
    
    query = f'"{address}" "{city} {state}" property owner'
    bing_search = f"https://www.bing.com/search?q={requests.utils.quote(query)}"

    if not can_fetch("https://www.bing.com", "/search"):
        return None

    session = make_session()
    random_delay(3, 6)
    resp = safe_get(session, bing_search)
    if not resp:
        return None

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(resp.text, "html.parser")
    # Read only the plain-text snippets from search results
    for snippet in soup.select(".b_caption p, .b_algoSlug"):
        text = snippet.get_text(" ", strip=True)
        # Look for patterns like "Owner: John Smith" or "Property owner: ..."
        match = re.search(
            r"(?:owner|owned by|taxpayer)[:\s]+([A-Z][a-z]+ [A-Z][a-z]+)", text, re.I
        )
        if match:
            return match.group(1).strip()
    return None


# ─── Main Enrichment Function ─────────────────────────────────────────────────

def enrich_owner(
    property_address: str,
    property_city: str,
    property_state: str,
    property_zip: str,
    parcel_number: str = "",
    county: str = "",
) -> dict:
    """
    Attempt to identify property owner from public records.
    Returns dict with owner fields or empty strings.
    """
    result = {
        "owner_name": "",
        "owner_first_name": "",
        "owner_last_name": "",
        "owner_is_entity": False,
        "mail_address": "",
        "mail_city": "",
        "mail_state": "",
        "mail_zip": "",
        "parcel_number": parcel_number,
        "enrichment_source": "",
    }

    addr = clean_address(property_address)
    county_lower = county.lower()

    # ── Step 1: Cook County Assessor (primary for Cook County, IL) ────────────
    if "cook" in county_lower or property_state.upper() == "IL":
        try:
            api_row = _query_cook_assessor(addr, parcel_number)
            if api_row:
                fields = _extract_assessor_fields(api_row)
                if fields.get("owner_name"):
                    result.update(fields)
                    result["enrichment_source"] = "Cook County Assessor API"
                    log.info(f"Owner found via Assessor API: {result['owner_name']}")
        except Exception as exc:
            log.error(f"Assessor enrichment error: {exc}")

    # ── Step 2: Cook County Recorder (deed data, supplements assessor) ────────
    if not result["owner_name"] and result.get("parcel_number"):
        try:
            rec_row = _query_cook_recorder(result["parcel_number"])
            if rec_row:
                grantee = safe_str(rec_row.get("grantee_name") or "")
                if grantee:
                    result["owner_name"] = grantee
                    result["enrichment_source"] = "Cook County Recorder API"
                    log.info(f"Owner found via Recorder API: {grantee}")
        except Exception as exc:
            log.error(f"Recorder enrichment error: {exc}")

    # ── Step 3: Google/Bing snippet fallback ──────────────────────────────────
    if not result["owner_name"]:
        try:
            found = _google_owner_search(addr, property_city, property_state)
            if found:
                result["owner_name"] = found
                result["enrichment_source"] = "Search engine snippet"
                log.info(f"Owner found via search snippet: {found}")
        except Exception as exc:
            log.error(f"Search snippet error: {exc}")

    # ── Post-processing ────────────────────────────────────────────────────────
    if result["owner_name"]:
        result["owner_is_entity"] = is_entity(result["owner_name"])
        if not result["owner_is_entity"]:
            first, last = split_owner_name(result["owner_name"])
            result["owner_first_name"] = first
            result["owner_last_name"] = last

    return result
