"""
export.py — Export pipeline: JSON records, auction CSV, GHL CSV, skiptrace review CSV.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from scraper.config import (
    AUCTION_LEADS_CSV,
    DASHBOARD_JSON,
    GHL_EXPORT_CSV,
    HIGH_CONFIDENCE_PHONE_THRESHOLD,
    LOOKAHEAD_DAYS,
    LOOKBACK_DAYS,
    RECORDS_JSON,
    RUN_LOG_JSON,
    SKIPTRACE_CSV,
)
from scraper.utils import get_logger, now_iso, save_json

log = get_logger("export")

# ─── Field Lists ───────────────────────────────────────────────────────────────

AUCTION_CSV_COLUMNS = [
    "auction_id", "property_url", "property_address", "property_city",
    "property_state", "property_zip", "county", "auction_date", "auction_time",
    "auction_type", "opening_bid", "estimated_resale_value", "property_type",
    "beds", "baths", "square_feet", "lot_size", "year_built",
    "occupancy_status", "foreclosure_status", "parcel_number", "case_number",
    "seller", "owner_name", "owner_first_name", "owner_last_name",
    "owner_is_entity", "mail_address", "mail_city", "mail_state", "mail_zip",
    "best_phone", "phone_type", "phone_confidence", "phone_source",
    "alt_phone_1", "alt_phone_2", "contact_confidence_notes",
    "flags", "score", "notes", "source", "fetched_at",
]

GHL_CSV_COLUMNS = [
    "First Name", "Last Name", "Phone", "Mailing Address", "Mailing City",
    "Mailing State", "Mailing Zip", "Property Address", "Property City",
    "Property State", "Property Zip", "Lead Type", "Auction Type",
    "Auction Date", "Opening Bid", "Estimated Value", "Seller Score",
    "Motivated Seller Flags", "Phone Confidence", "Source", "Property URL", "Notes",
]

SKIPTRACE_COLUMNS = [
    "record_id", "property_address", "property_city", "property_state",
    "property_zip", "owner_name", "owner_is_entity",
    "mail_address", "mail_city", "mail_state", "mail_zip",
    "best_phone", "phone_confidence", "phone_source", "alt_phone_1", "alt_phone_2",
    "contact_confidence_notes", "score", "flags",
]


# ─── Helpers ───────────────────────────────────────────────────────────────────

def _flags_str(flags) -> str:
    if isinstance(flags, list):
        return "; ".join(flags)
    return str(flags)


def _records_to_df(records: list[dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        row = dict(r)
        row["flags"] = _flags_str(r.get("flags", []))
        rows.append(row)
    return pd.DataFrame(rows)


# ─── Auction Leads CSV ────────────────────────────────────────────────────────

def export_auction_csv(records: list[dict]) -> None:
    Path(AUCTION_LEADS_CSV).parent.mkdir(parents=True, exist_ok=True)
    df = _records_to_df(records)
    cols = [c for c in AUCTION_CSV_COLUMNS if c in df.columns]
    df[cols].to_csv(AUCTION_LEADS_CSV, index=False)
    log.info(f"Exported {len(df)} records → {AUCTION_LEADS_CSV}")


# ─── GoHighLevel Export CSV ───────────────────────────────────────────────────

def export_ghl_csv(records: list[dict]) -> None:
    Path(GHL_EXPORT_CSV).parent.mkdir(parents=True, exist_ok=True)
    rows = []
    for r in records:
        rows.append({
            "First Name": r.get("owner_first_name", ""),
            "Last Name": r.get("owner_last_name", ""),
            "Phone": r.get("best_phone", ""),
            "Mailing Address": r.get("mail_address", r.get("property_address", "")),
            "Mailing City": r.get("mail_city", r.get("property_city", "")),
            "Mailing State": r.get("mail_state", r.get("property_state", "")),
            "Mailing Zip": r.get("mail_zip", r.get("property_zip", "")),
            "Property Address": r.get("property_address", ""),
            "Property City": r.get("property_city", ""),
            "Property State": r.get("property_state", ""),
            "Property Zip": r.get("property_zip", ""),
            "Lead Type": "Auction",
            "Auction Type": r.get("auction_type", ""),
            "Auction Date": r.get("auction_date", ""),
            "Opening Bid": r.get("opening_bid", ""),
            "Estimated Value": r.get("estimated_resale_value", ""),
            "Seller Score": r.get("score", 0),
            "Motivated Seller Flags": _flags_str(r.get("flags", [])),
            "Phone Confidence": r.get("phone_confidence", 0),
            "Source": r.get("source", ""),
            "Property URL": r.get("property_url", ""),
            "Notes": r.get("notes", ""),
        })
    df = pd.DataFrame(rows, columns=GHL_CSV_COLUMNS)
    df.to_csv(GHL_EXPORT_CSV, index=False)
    log.info(f"Exported {len(df)} GHL records → {GHL_EXPORT_CSV}")


# ─── Skip Trace Review CSV ────────────────────────────────────────────────────

def export_skiptrace_csv(records: list[dict]) -> None:
    """Export records that need manual skip trace review (no phone or low confidence)."""
    Path(SKIPTRACE_CSV).parent.mkdir(parents=True, exist_ok=True)
    needs_review = [
        r for r in records
        if not r.get("best_phone") or r.get("phone_confidence", 0) < 40
    ]
    if not needs_review:
        log.info("No records need skip trace review.")
        pd.DataFrame(columns=SKIPTRACE_COLUMNS).to_csv(SKIPTRACE_CSV, index=False)
        return
    df = _records_to_df(needs_review)
    cols = [c for c in SKIPTRACE_COLUMNS if c in df.columns]
    df[cols].to_csv(SKIPTRACE_CSV, index=False)
    log.info(f"Exported {len(needs_review)} skiptrace-review records → {SKIPTRACE_CSV}")


# ─── JSON Export ─────────────────────────────────────────────────────────────

def export_json(records: list[dict]) -> None:
    today = date.today()
    date_range = (
        f"{(today - timedelta(days=LOOKBACK_DAYS)).isoformat()} to "
        f"{(today + timedelta(days=LOOKAHEAD_DAYS)).isoformat()}"
    )
    with_owner = sum(1 for r in records if r.get("owner_name"))
    with_phone = sum(1 for r in records if r.get("best_phone"))
    high_conf = sum(
        1 for r in records
        if r.get("phone_confidence", 0) >= HIGH_CONFIDENCE_PHONE_THRESHOLD
    )

    payload = {
        "fetched_at": now_iso(),
        "source": "Auction.com + public enrichment",
        "date_range": date_range,
        "total": len(records),
        "with_owner": with_owner,
        "with_phone": with_phone,
        "high_confidence_phone": high_conf,
        "records": records,
    }

    save_json(RECORDS_JSON, payload)
    save_json(DASHBOARD_JSON, payload)
    log.info(f"Exported JSON: {len(records)} records → {RECORDS_JSON} + {DASHBOARD_JSON}")


# ─── Run Log ──────────────────────────────────────────────────────────────────

def write_run_log(
    total: int,
    with_owner: int,
    with_phone: int,
    high_conf: int,
    errors: list[str],
    duration_seconds: float,
) -> None:
    Path(RUN_LOG_JSON).parent.mkdir(parents=True, exist_ok=True)
    log_entry = {
        "run_at": now_iso(),
        "total_records": total,
        "with_owner": with_owner,
        "with_phone": with_phone,
        "high_confidence_phone": high_conf,
        "errors": errors,
        "duration_seconds": round(duration_seconds, 2),
    }

    # Append to existing log
    try:
        with open(RUN_LOG_JSON, "r", encoding="utf-8") as f:
            existing = json.load(f)
            if not isinstance(existing, list):
                existing = [existing]
    except Exception:
        existing = []

    existing.append(log_entry)
    # Keep last 30 runs
    existing = existing[-30:]
    save_json(RUN_LOG_JSON, existing)
    log.info(f"Run log written → {RUN_LOG_JSON}")


# ─── Master Export ────────────────────────────────────────────────────────────

def export_all(records: list[dict]) -> None:
    """Run all exports."""
    export_json(records)
    export_auction_csv(records)
    export_ghl_csv(records)
    export_skiptrace_csv(records)
