"""
config.py — Central configuration for the Auction Lead Collection System.
All tunable parameters live here. Override via environment variables or .env file.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ─── Target Geography ──────────────────────────────────────────────────────────
STATE = os.getenv("TARGET_STATE", "IL")
COUNTIES = os.getenv("TARGET_COUNTIES", "Cook").split(",")

# ─── Auction Filters ───────────────────────────────────────────────────────────
AUCTION_TYPES = [
    "foreclosure",
    "bank_owned",
    "reo",
    "trustee_sale",
    "sheriff_sale",
    "occupied",
    "vacant",
    "online_auction",
    "live_auction",
]

LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "30"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))

# ─── Input / Output Paths ──────────────────────────────────────────────────────
DATA_DIR = "data"
DASHBOARD_DIR = "dashboard"
EXPORTS_DIR = "exports"
LOGS_DIR = "logs"
UPLOAD_DIR = "uploads"  # place manually downloaded Auction.com CSVs here

RECORDS_JSON = os.path.join(DATA_DIR, "records.json")
DASHBOARD_JSON = os.path.join(DASHBOARD_DIR, "records.json")
AUCTION_LEADS_CSV = os.path.join(EXPORTS_DIR, "auction_leads.csv")
GHL_EXPORT_CSV = os.path.join(EXPORTS_DIR, "ghl_auction_export.csv")
SKIPTRACE_CSV = os.path.join(EXPORTS_DIR, "skiptrace_review.csv")
RUN_LOG_JSON = os.path.join(LOGS_DIR, "run_log.json")

# ─── Rate Limiting ─────────────────────────────────────────────────────────────
MIN_DELAY_SECONDS = float(os.getenv("MIN_DELAY", "2.5"))
MAX_DELAY_SECONDS = float(os.getenv("MAX_DELAY", "7.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

# ─── Scoring Thresholds ────────────────────────────────────────────────────────
HIGH_CONFIDENCE_PHONE_THRESHOLD = 70
HIGH_SCORE_THRESHOLD = 60

# ─── Playwright ────────────────────────────────────────────────────────────────
HEADLESS = os.getenv("HEADLESS", "true").lower() == "true"
BROWSER_TIMEOUT_MS = int(os.getenv("BROWSER_TIMEOUT_MS", "30000"))

# ─── Auction.com ───────────────────────────────────────────────────────────────
AUCTION_COM_BASE = "https://www.auction.com"
AUCTION_COM_SEARCH = "https://www.auction.com/search"

# ─── Free Enrichment Sources ───────────────────────────────────────────────────
TRUEPEOPLESEARCH_BASE = "https://www.truepeoplesearch.com"
FASTPEOPLESEARCH_BASE = "https://www.fastpeoplesearch.com"
SEARCHPEOPLEFREE_BASE = "https://www.searchpeoplefree.com"

# ─── Cook County APIs ──────────────────────────────────────────────────────────
COOK_COUNTY_ASSESSOR_API = "https://datacatalog.cookcountyil.gov/resource/tx2p-k2g9.json"
COOK_COUNTY_RECORDER_API = "https://datacatalog.cookcountyil.gov/resource/iqm2-xekr.json"
ILLINOIS_SOSAPI = "https://www.ilsos.gov/corporatellc/"  # manual fallback only

# ─── User-Agent Pool ───────────────────────────────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]
