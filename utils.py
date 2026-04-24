"""
skiptrace_free.py — Phone number enrichment using free public people-search sites.

COMPLIANCE RULES ENFORCED:
  - Only reads publicly visible information (no login, no payment, no CAPTCHA bypass).
  - Respects robots.txt before fetching any site.
  - Rate-limited with randomised delays.
  - Does not automate spam — data is for lawful real estate outreach only.
  - If a site blocks access or requires login, skips gracefully.
  - Returns only what is visible without authentication.

Sources:
  1. TruePeopleSearch (public name/address search)
  2. FastPeopleSearch (public name/city search)
  3. SearchPeopleFree (public search)
  4. Whitepages free preview (public snippet only)
"""
from __future__ import annotations

import re
from typing import Optional

from rapidfuzz import fuzz

from scraper.config import (
    FASTPEOPLESEARCH_BASE,
    SEARCHPEOPLEFREE_BASE,
    TRUEPEOPLESEARCH_BASE,
)
from scraper.utils import (
    can_fetch,
    extract_phones,
    get_logger,
    make_session,
    normalize_phone,
    random_delay,
    safe_get,
)

log = get_logger("skiptrace_free")

# ─── Confidence Scoring Constants ─────────────────────────────────────────────

CONF_EXACT_NAME = 40
CONF_EXACT_ADDRESS = 25
CONF_MAILING_ADDRESS = 20
CONF_SAME_CITY_STATE = 15
CONF_MOBILE_LABELED = 10
CONF_MULTI_SOURCE = 5
CONF_NAME_MISMATCH = -20
CONF_OLD_ADDRESS = -20
CONF_UNRELATED = -30


# ─── Shared Parser Helpers ────────────────────────────────────────────────────

def _get_soup(url: str, session=None):
    from bs4 import BeautifulSoup
    s = session or make_session()
    resp = safe_get(s, url)
    if not resp:
        return None
    block_signals = [
        "sign in", "log in", "captcha", "access denied",
        "are you a robot", "cloudflare", "just a moment",
        "verify you are human", "please enable cookies",
    ]
    html_lower = resp.text.lower()
    if any(sig in html_lower for sig in block_signals):
        log.warning(f"Access blocked on {url}. Skipping.")
        return None
    return BeautifulSoup(resp.text, "html.parser")


def _name_similarity(name1: str, name2: str) -> int:
    """0–100 fuzzy similarity score between two names."""
    return fuzz.token_set_ratio(name1.lower(), name2.lower())


# ─── TruePeopleSearch ─────────────────────────────────────────────────────────

def _search_truepeoplesearch(
    owner_name: str, city: str, state: str, address: str = ""
) -> list[dict]:
    """
    Searches TruePeopleSearch public name lookup.
    Only collects what is visible without login/payment.
    """
    base = TRUEPEOPLESEARCH_BASE
    if not can_fetch(base, "/results"):
        return []

    session = make_session()
    # URL pattern: /results?name=John+Smith&citystatezip=Chicago+IL
    query_name = owner_name.replace(" ", "+")
    location = f"{city}+{state}".replace(" ", "+")
    url = f"{base}/results?name={query_name}&citystatezip={location}"

    random_delay(3, 7)
    soup = _get_soup(url, session)
    if not soup:
        return []

    results = []
    # TruePeopleSearch lists people in cards with class "card-summary"
    for card in soup.select(".card-summary, .people-card, [data-detail-link]")[:5]:
        try:
            card_text = card.get_text(" ", strip=True)
            phones = extract_phones(card_text)
            # Get name from card
            name_el = card.select_one("h2, .name, .card-name")
            found_name = name_el.get_text(strip=True) if name_el else ""
            # Get address from card
            addr_el = card.select_one(".address, .location, .card-address")
            found_addr = addr_el.get_text(strip=True) if addr_el else ""
            # Get phone type hint
            is_mobile = "mobile" in card_text.lower() or "cell" in card_text.lower()

            for phone in phones:
                results.append({
                    "phone": phone,
                    "found_name": found_name,
                    "found_address": found_addr,
                    "is_mobile": is_mobile,
                    "source": "TruePeopleSearch",
                })
        except Exception as exc:
            log.debug(f"TruePeopleSearch card parse error: {exc}")

    log.info(f"TruePeopleSearch: {len(results)} phone candidates for '{owner_name}'")
    return results


# ─── FastPeopleSearch ─────────────────────────────────────────────────────────

def _search_fastpeoplesearch(
    owner_name: str, city: str, state: str
) -> list[dict]:
    base = FASTPEOPLESEARCH_BASE
    if not can_fetch(base, "/"):
        return []

    session = make_session()
    # FastPeopleSearch URL: /name/john-smith/chicago-il
    name_slug = re.sub(r"[^a-z0-9]+", "-", owner_name.lower()).strip("-")
    city_slug = re.sub(r"[^a-z0-9]+", "-", f"{city}-{state}".lower()).strip("-")
    url = f"{base}/name/{name_slug}/{city_slug}"

    random_delay(3, 7)
    soup = _get_soup(url, session)
    if not soup:
        return []

    results = []
    for card in soup.select(".card, .person-card, .result-card")[:5]:
        try:
            card_text = card.get_text(" ", strip=True)
            phones = extract_phones(card_text)
            name_el = card.select_one("h2, h3, .full-name, .name")
            found_name = name_el.get_text(strip=True) if name_el else ""
            is_mobile = "mobile" in card_text.lower() or "cell" in card_text.lower()

            for phone in phones:
                results.append({
                    "phone": phone,
                    "found_name": found_name,
                    "found_address": "",
                    "is_mobile": is_mobile,
                    "source": "FastPeopleSearch",
                })
        except Exception as exc:
            log.debug(f"FastPeopleSearch card parse error: {exc}")

    log.info(f"FastPeopleSearch: {len(results)} phone candidates for '{owner_name}'")
    return results


# ─── SearchPeopleFree ─────────────────────────────────────────────────────────

def _search_searchpeoplefree(
    owner_name: str, city: str, state: str
) -> list[dict]:
    base = SEARCHPEOPLEFREE_BASE
    if not can_fetch(base, "/"):
        return []

    session = make_session()
    parts = owner_name.strip().split()
    if len(parts) < 2:
        return []
    first = parts[0]
    last = " ".join(parts[1:])
    url = f"{base}/find/{first.lower()}-{last.lower()}/{city.lower()}-{state.lower()}"

    random_delay(3, 7)
    soup = _get_soup(url, session)
    if not soup:
        return []

    results = []
    for card in soup.select(".card, .person, .search-result")[:5]:
        try:
            card_text = card.get_text(" ", strip=True)
            phones = extract_phones(card_text)
            name_el = card.select_one("h2, h3, .name")
            found_name = name_el.get_text(strip=True) if name_el else ""
            is_mobile = "mobile" in card_text.lower()

            for phone in phones:
                results.append({
                    "phone": phone,
                    "found_name": found_name,
                    "found_address": "",
                    "is_mobile": is_mobile,
                    "source": "SearchPeopleFree",
                })
        except Exception as exc:
            log.debug(f"SearchPeopleFree parse error: {exc}")

    log.info(f"SearchPeopleFree: {len(results)} phone candidates for '{owner_name}'")
    return results


# ─── Confidence Scorer ────────────────────────────────────────────────────────

def _score_candidate(
    candidate: dict,
    owner_name: str,
    property_address: str,
    property_city: str,
    property_state: str,
    mail_address: str = "",
    mail_city: str = "",
    seen_phones: dict[str, int] = None,
) -> int:
    """Calculate confidence score for a single phone candidate."""
    score = 0
    found_name = candidate.get("found_name", "")
    found_addr = candidate.get("found_address", "")

    # Name match
    if owner_name and found_name:
        sim = _name_similarity(owner_name, found_name)
        if sim >= 85:
            score += CONF_EXACT_NAME
        elif sim >= 60:
            score += CONF_SAME_CITY_STATE
        elif sim < 40 and found_name:
            score += CONF_NAME_MISMATCH

    # Address match
    if property_address and found_addr:
        addr_sim = _name_similarity(property_address, found_addr)
        if addr_sim >= 80:
            score += CONF_EXACT_ADDRESS
        elif mail_address and _name_similarity(mail_address, found_addr) >= 80:
            score += CONF_MAILING_ADDRESS
        elif property_city and property_city.lower() in found_addr.lower():
            score += CONF_SAME_CITY_STATE

    # Mobile bonus
    if candidate.get("is_mobile"):
        score += CONF_MOBILE_LABELED

    # Multi-source confirmation
    if seen_phones and candidate.get("phone") in seen_phones:
        score += CONF_MULTI_SOURCE

    return max(0, score)


# ─── Best Phone Selector ──────────────────────────────────────────────────────

def select_best_phone(
    candidates: list[dict],
    owner_name: str,
    property_address: str,
    property_city: str,
    property_state: str,
    mail_address: str = "",
    mail_city: str = "",
) -> dict:
    """
    From all collected candidates, select the best phone and return enriched dict.
    """
    if not candidates:
        return {
            "best_phone": "",
            "phone_type": "",
            "phone_confidence": 0,
            "phone_source": "",
            "alt_phone_1": "",
            "alt_phone_2": "",
            "contact_confidence_notes": "No phone candidates found.",
        }

    # Count phone frequency across sources (for multi-source bonus)
    phone_counts: dict[str, int] = {}
    for c in candidates:
        p = c.get("phone", "")
        if p:
            phone_counts[p] = phone_counts.get(p, 0) + 1

    # Score each candidate
    scored: list[tuple[int, dict]] = []
    for c in candidates:
        conf = _score_candidate(
            c, owner_name, property_address, property_city,
            property_state, mail_address, mail_city, phone_counts
        )
        # Multi-source bonus applied post-scoring
        if phone_counts.get(c.get("phone", ""), 0) > 1:
            conf += CONF_MULTI_SOURCE
        scored.append((conf, c))

    scored.sort(key=lambda x: x[0], reverse=True)

    # Deduplicate by phone number, keeping highest score
    seen_phones: set[str] = set()
    ranked: list[tuple[int, dict]] = []
    for conf, c in scored:
        p = c.get("phone", "")
        if p and p not in seen_phones:
            seen_phones.add(p)
            ranked.append((conf, c))

    best_conf, best = ranked[0]
    alts = [c for _, c in ranked[1:3]]

    notes_parts = []
    if best_conf >= 70:
        notes_parts.append("High confidence match.")
    elif best_conf >= 40:
        notes_parts.append("Moderate confidence match.")
    else:
        notes_parts.append("Low confidence — manual review recommended.")

    notes_parts.append(f"Source: {best.get('source', 'unknown')}.")
    if best.get("is_mobile"):
        notes_parts.append("Labeled as mobile number.")

    return {
        "best_phone": best.get("phone", ""),
        "phone_type": "mobile" if best.get("is_mobile") else "unknown",
        "phone_confidence": min(100, best_conf),
        "phone_source": best.get("source", ""),
        "alt_phone_1": alts[0].get("phone", "") if len(alts) > 0 else "",
        "alt_phone_2": alts[1].get("phone", "") if len(alts) > 1 else "",
        "contact_confidence_notes": " ".join(notes_parts),
    }


# ─── Main Skip Trace Function ─────────────────────────────────────────────────

def skiptrace_owner(
    owner_name: str,
    property_address: str,
    property_city: str,
    property_state: str,
    property_zip: str,
    mail_address: str = "",
    mail_city: str = "",
    mail_state: str = "",
) -> dict:
    """
    Run free skip trace pipeline for a single owner.
    Returns phone enrichment dict.
    """
    if not owner_name or owner_name.strip() == "":
        return {
            "best_phone": "",
            "phone_type": "",
            "phone_confidence": 0,
            "phone_source": "",
            "alt_phone_1": "",
            "alt_phone_2": "",
            "contact_confidence_notes": "No owner name to search.",
        }

    city = property_city or mail_city or ""
    state = property_state or mail_state or ""

    all_candidates: list[dict] = []

    # Source 1: TruePeopleSearch
    try:
        results = _search_truepeoplesearch(owner_name, city, state, property_address)
        all_candidates.extend(results)
    except Exception as exc:
        log.error(f"TruePeopleSearch error for '{owner_name}': {exc}")

    # Source 2: FastPeopleSearch
    try:
        results = _search_fastpeoplesearch(owner_name, city, state)
        all_candidates.extend(results)
    except Exception as exc:
        log.error(f"FastPeopleSearch error for '{owner_name}': {exc}")

    # Source 3: SearchPeopleFree
    try:
        results = _search_searchpeoplefree(owner_name, city, state)
        all_candidates.extend(results)
    except Exception as exc:
        log.error(f"SearchPeopleFree error for '{owner_name}': {exc}")

    log.info(
        f"Skip trace for '{owner_name}': {len(all_candidates)} total candidates "
        f"from {len(set(c['source'] for c in all_candidates if c.get('source')))} sources"
    )

    return select_best_phone(
        all_candidates,
        owner_name,
        property_address,
        property_city,
        property_state,
        mail_address,
        mail_city,
    )
