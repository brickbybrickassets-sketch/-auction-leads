"""
fetch.py — Main orchestrator for the Auction Lead Collection System.

Execution flow:
  1. Collect auction records from Auction.com (CSV upload or public page).
  2. Enrich each record with owner data from public county records.
  3. Run free skip trace to find phone numbers.
  4. Score each lead (0–100) and assign flags.
  5. Export: JSON, CSV, GHL CSV, skiptrace review CSV, run log.

Run:
  python scraper/fetch.py
  python scraper/fetch.py --dry-run    # skip enrichment, score only
  python scraper/fetch.py --no-phone   # skip skip-trace
"""
from __future__ import annotations

import argparse
import json
import sys
import time
import traceback
from pathlib import Path

# Ensure project root is on the path when running as a script
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scraper.auction_com import collect_auction_records
from scraper.config import HIGH_CONFIDENCE_PHONE_THRESHOLD, RECORDS_JSON
from scraper.enrich_owner import enrich_owner
from scraper.export import export_all, write_run_log
from scraper.scoring import score_all
from scraper.skiptrace_free import skiptrace_owner
from scraper.utils import ensure_dirs, get_logger, load_json, now_iso, safe_str

log = get_logger("fetch")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auction Lead Collection System")
    parser.add_argument("--dry-run", action="store_true", help="Skip enrichment; score existing records only")
    parser.add_argument("--no-phone", action="store_true", help="Skip skip-trace phone lookup")
    parser.add_argument("--resume", action="store_true", help="Resume from last saved records.json")
    parser.add_argument("--limit", type=int, default=0, help="Process only N records (testing)")
    return parser.parse_args()


def record_to_dict(rec) -> dict:
    """Convert AuctionRecord (Pydantic) or dict to plain dict."""
    if hasattr(rec, "dict"):
        return rec.dict()
    return dict(rec)


def save_partial(records: list[dict]) -> None:
    """Save progress after each batch to avoid losing work on crash."""
    from scraper.utils import save_json
    save_json(RECORDS_JSON, {
        "fetched_at": now_iso(),
        "source": "partial",
        "total": len(records),
        "records": records,
    })


def main() -> None:
    args = parse_args()
    start_time = time.time()
    errors: list[str] = []

    ensure_dirs()
    log.info("=" * 60)
    log.info("Auction Lead Collection System — Starting")
    log.info(f"Options: dry_run={args.dry_run}, no_phone={args.no_phone}, resume={args.resume}")
    log.info("=" * 60)

    # ── Step 1: Collect Records ───────────────────────────────────────────────
    raw_records: list[dict] = []

    if args.resume:
        log.info("Resuming from saved records.json ...")
        saved = load_json(RECORDS_JSON, {})
        raw_records = saved.get("records", [])
        log.info(f"Resumed {len(raw_records)} records from disk.")
    else:
        log.info("Collecting auction records ...")
        try:
            auction_records = collect_auction_records()
            raw_records = [record_to_dict(r) for r in auction_records]
            log.info(f"Collected {len(raw_records)} auction records.")
        except Exception as exc:
            log.error(f"Fatal error in collection: {exc}")
            errors.append(f"collection: {exc}")
            # Try to continue with empty set rather than crash
            raw_records = []

    if args.limit > 0:
        raw_records = raw_records[:args.limit]
        log.info(f"Limiting to {args.limit} records (--limit flag).")

    if not raw_records:
        log.warning(
            "No records collected. "
            "Please place a manually exported Auction.com CSV in the uploads/ directory "
            "and re-run. See README.md for instructions."
        )

    # ── Step 2 & 3: Owner Enrichment + Skip Trace ────────────────────────────
    if not args.dry_run:
        total = len(raw_records)
        for i, record in enumerate(raw_records):
            try:
                log.info(f"[{i+1}/{total}] Enriching: {record.get('property_address', '?')}")

                # Owner enrichment (county records)
                if not record.get("owner_name"):
                    owner_data = enrich_owner(
                        property_address=safe_str(record.get("property_address")),
                        property_city=safe_str(record.get("property_city")),
                        property_state=safe_str(record.get("property_state")),
                        property_zip=safe_str(record.get("property_zip")),
                        parcel_number=safe_str(record.get("parcel_number")),
                        county=safe_str(record.get("county")),
                    )
                    record.update({
                        k: v for k, v in owner_data.items()
                        if k not in ("enrichment_source",)
                    })
                    if owner_data.get("enrichment_source"):
                        record["notes"] = (
                            safe_str(record.get("notes")) + 
                            f" [Owner source: {owner_data['enrichment_source']}]"
                        ).strip()

                # Skip trace (phone lookup)
                if not args.no_phone and not record.get("best_phone"):
                    owner_name = safe_str(record.get("owner_name"))
                    if owner_name:
                        phone_data = skiptrace_owner(
                            owner_name=owner_name,
                            property_address=safe_str(record.get("property_address")),
                            property_city=safe_str(record.get("property_city")),
                            property_state=safe_str(record.get("property_state")),
                            property_zip=safe_str(record.get("property_zip")),
                            mail_address=safe_str(record.get("mail_address")),
                            mail_city=safe_str(record.get("mail_city")),
                            mail_state=safe_str(record.get("mail_state")),
                        )
                        record.update(phone_data)

                # Save partial progress every 10 records
                if (i + 1) % 10 == 0:
                    save_partial(raw_records)
                    log.info(f"Partial save at record {i+1}")

            except KeyboardInterrupt:
                log.warning("Interrupted — saving partial progress ...")
                save_partial(raw_records)
                sys.exit(0)
            except Exception as exc:
                tb = traceback.format_exc()
                err_msg = f"Enrichment error on record {i}: {exc}"
                log.error(err_msg)
                log.debug(tb)
                errors.append(err_msg)
                # Never crash entire run — continue with next record
                continue

    # ── Step 4: Scoring ───────────────────────────────────────────────────────
    log.info("Scoring all records ...")
    try:
        raw_records = score_all(raw_records)
        log.info("Scoring complete.")
    except Exception as exc:
        log.error(f"Scoring failed: {exc}")
        errors.append(f"scoring: {exc}")

    # ── Step 5: Export ────────────────────────────────────────────────────────
    log.info("Exporting all outputs ...")
    try:
        export_all(raw_records)
    except Exception as exc:
        log.error(f"Export failed: {exc}")
        errors.append(f"export: {exc}")

    # ── Summary ───────────────────────────────────────────────────────────────
    duration = time.time() - start_time
    with_owner = sum(1 for r in raw_records if r.get("owner_name"))
    with_phone = sum(1 for r in raw_records if r.get("best_phone"))
    high_conf = sum(
        1 for r in raw_records
        if r.get("phone_confidence", 0) >= HIGH_CONFIDENCE_PHONE_THRESHOLD
    )

    write_run_log(
        total=len(raw_records),
        with_owner=with_owner,
        with_phone=with_phone,
        high_conf=high_conf,
        errors=errors,
        duration_seconds=duration,
    )

    log.info("=" * 60)
    log.info(f"Run complete in {duration:.1f}s")
    log.info(f"  Total records   : {len(raw_records)}")
    log.info(f"  With owner      : {with_owner}")
    log.info(f"  With phone      : {with_phone}")
    log.info(f"  High confidence : {high_conf}")
    log.info(f"  Errors          : {len(errors)}")
    log.info("=" * 60)

    if errors:
        log.warning(f"{len(errors)} non-fatal errors occurred. Check logs.")
    else:
        log.info("All tasks completed without errors.")


if __name__ == "__main__":
    main()
