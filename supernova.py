# ============================================================================
# SuperClaw 30.0 — APEX + HUMANIZER + FLYWHEEL EDITION
# Real-time X firehose | On-chain whales | Swarm-bound archetypes |
# Local discriminator | Cross-engagement | Timezone peak | Elastic limits |
# RAG injection | A/B testing | Bayesian tuning | Embedding dedup |
# Paper-trading bridge | SVG generation | Meme engineering
# ============================================================================

import os, re, json, time, math, socket, random, hashlib, subprocess, sqlite3, threading
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, Counter

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from openai import OpenAI
from flask import Flask, jsonify, render_template_string

try:
    from playwright.sync_api import sync_playwright
except ImportError:
    print("=" * 60)
    print("SuperClaw needs Playwright.  pip install playwright")
    print("Then run:  playwright install chromium")
    print("=" * 60)
    raise SystemExit(2)

# ============================================================================
# SECTION 1 — CONFIG
# ============================================================================

BINANCE_BASE    = "https://api.binance.com"
BINANCE_FUTURES = "https://fapi.binance.com"
DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "").strip()

BINANCE_SQUARE_API_URL = "https://www.binance.com/bapi/composite/v1/public/pgc/openApi/content/add"
BINANCE_SQUARE_OPENAPI_KEY = os.getenv("BINANCE_SQUARE_OPENAPI_KEY", "").strip()
USE_SQUARE_OPENAPI = bool(BINANCE_SQUARE_OPENAPI_KEY)

RUN_IN_BACKGROUND = False
BROWSER_WINDOW_POS  = "100,100"
BROWSER_WINDOW_SIZE = "1280,900"

DEBUG_PORT = 9222
SQUARE_URL = "https://www.binance.com/en/square"

SQUARE_DAILY_LIMIT           = 60
DAILY_ACTION_TARGET          = 32
DAILY_POST_TARGET            = 22
DAILY_REPLY_TARGET           = 10
MIN_POST_INTERVAL_SECONDS    = 900
POST_INTERVAL_JITTER_SECONDS = 240
MIN_REPLY_INTERVAL_SECONDS   = 300
SCAN_INTERVAL_SECONDS        = 45

ARCHETYPE_A1A2_CAP = 0.30
ARCHETYPE_TRUST_MIN = 0.55

NARRATIVE_REFRESH_SECONDS = 180
NARRATIVE_MIN_SCORE       = 0.35
NARRATIVE_TOP_N           = 12

ENGAGEMENT_WATCH_MINUTES  = 35
ENGAGEMENT_REPLY_DEADLINE = 30 * 60
ENGAGEMENT_MAX_REPLIES    = 6

LANG_ARBITRAGE_ENABLED = True
LANG_TARGETS = ["vi", "ar", "zh"]
LANG_DELAY_MINUTES = {"vi": 18, "ar": 25, "zh": 15}

DIVERSIFICATION = {
    "news":           (4, 7),
    "trade_plan":     (3, 5),
    "diary":          (1, 3),
    "curator":        (2, 4),
    "educational":    (2, 3),
    "signal":         (5, 8),
    "whale":          (1, 3),
    "onchain":        (1, 2),
    "market":         (1, 2),
    "coin_analysis":  (1, 2),
    "gainers":        (1, 2),
    "quiz":           (0, 1),
    "recap":          (0, 1),
}

SIGNAL_THRESHOLD_STRONG  = 78
SIGNAL_THRESHOLD         = 62
SIGNAL_THRESHOLD_RELAXED = 48
MIN_QUOTE_VOLUME         = 6_000_000
ENGAGEMENT_MIN_QUOTE_VOLUME = 8_000_000
ENGAGEMENT_MIN_CHANGE_PCT   = 1.2
RELAXED_MIN_QUOTE_VOLUME    = 3_000_000
RELAXED_MIN_CHANGE_PCT      = 0.6
TOP_CANDIDATES              = 120

MIN_POST_QUALITY      = 60
MIN_TRUST_SCORE       = 55
IDEAL_POST_LENGTH_MIN = 80
IDEAL_POST_LENGTH_MAX = 220

EARN_PER_1K_VIEWS = 0.0035
VIEW_TIERS        = [1000, 5000, 10000]
TARGET_VIEWS      = 1000
VIEW_LEARN_DAYS   = 21
VIEW_MIN_SAMPLE   = 30

HUMAN_WPM_LO            = 260
HUMAN_WPM_HI            = 520
HUMAN_TYPO_RATE         = 0.008
HUMAN_BURST_PAUSE_RATE  = 0.020
HUMAN_MOUSE_STEPS_LO    = 18
HUMAN_MOUSE_STEPS_HI    = 42
HUMAN_PRE_CLICK_PAUSE   = (0.35, 1.10)
HUMAN_PRE_PUBLISH_READ  = True
HUMAN_IDLE_JITTER       = True
HUMAN_SCROLL_AROUND     = True
HUMAN_POST_TYPING_PAUSE = (0.8, 2.4)

# ---------- V30.0 CONFIG ----------
# L1: X firehose + on-chain whales
X_FIREHOSE_ENABLED     = True
X_FIREHOSE_SOURCES     = [
    "https://nitter.net/search/rss?f=tweets&q=%24BTC+OR+%24ETH+OR+%24SOL",
    "https://nitter.net/search/rss?f=tweets&q=crypto+listing+OR+hack",
    "https://nitter.net/search/rss?f=tweets&q=binance+listing",
]
X_FIREHOSE_POLL_SEC    = 30
X_FIREHOSE_MAX_AGE_SEC = 240

ONCHAIN_RPC_ENABLED    = True
ONCHAIN_RPC_URL        = os.getenv("ONCHAIN_RPC_URL", "https://eth.llamarpc.com")
WHALE_WALLETS          = [
    "0x28c6c06298d514db089934071355e5743bf21d60",
    "0x21a31ee1afc51d94c2efccaa2092ad1028285549",
    "0xdfd5293d8e347dfe9b3743b0f0f6b8b7f7c7de5c",
    "0x47ac0fb4f2d84898e4d9e7b4dab3c24507a6d503",
]
WHALE_MIN_USD          = 5_000_000
WHALE_POLL_SEC         = 45

# L2: Persistent archetype binding per swarm node
SWARM_NODE_ID          = os.getenv("SWARM_NODE_ID", "node-alpha")
ARCHETYPE_BINDING_FILE = f"archetype_binding_{SWARM_NODE_ID}.json"

# L3: Local discriminator
DISCRIMINATOR_MODEL_PATH  = f"discriminator_{SWARM_NODE_ID}.pkl"
DISCRIMINATOR_MIN_SAMPLES = 40
DISCRIMINATOR_THRESHOLD   = 0.62

# L4: Swarm cross-engagement
SWARM_ENABLED          = True
SWARM_PEER_NODES       = [n.strip() for n in
                          os.getenv("SWARM_PEER_NODES", "").split(",") if n.strip()]
SWARM_REPLY_MIN_DELAY  = 180
SWARM_REPLY_MAX_DELAY  = 900

# L5: Timezone peak mapping
LANG_PEAK_TZ = {
    "vi": {"tz": "Asia/Ho_Chi_Minh", "peak_hours": [20, 21, 22, 23]},
    "ar": {"tz": "Asia/Riyadh",      "peak_hours": [21, 22, 23,  0]},
    "zh": {"tz": "Asia/Shanghai",    "peak_hours": [21, 22, 23,  0]},
}

# L6: Elastic limits
DYNAMIC_ELASTIC_ENABLED = True
ELASTIC_MIN_MULT        = 0.40
ELASTIC_MAX_MULT        = 1.85

# RAG
RAG_TOP_N          = 3
RAG_LOOKBACK_HOURS = 48

# A/B testing
AB_ENGAGEMENT_VIEWS_W    = 1.0
AB_ENGAGEMENT_LIKES_W    = 50.0
AB_ENGAGEMENT_COMMENTS_W = 200.0
AB_ENGAGEMENT_SHARES_W   = 300.0
AB_MIN_SAMPLES           = 30

# Bayesian tuning
BAYES_TUNE_ENABLED = True
BAYES_N_INIT       = 8
BAYES_N_ITER       = 25

# Paper trading
PAPER_TRADE_ENABLED    = True
BINANCE_TESTNET_BASE   = "https://testnet.binance.vision"
BINANCE_TESTNET_KEY    = os.getenv("BINANCE_TESTNET_KEY", "").strip()
BINANCE_TESTNET_SECRET = os.getenv("BINANCE_TESTNET_SECRET", "").strip()

# SVG
SVG_RENDER_ENABLED = True
SVG_OUTPUT_DIR     = Path("svg_out")
SVG_BRAND_COLOR    = "#F0B90B"
SVG_BG_COLOR       = "#0B0F14"

# Memes
MEME_ENABLED   = True
MEME_LANGS     = ["vi", "tr"]
MEME_VLM_MODEL = "openai/gpt-4o-mini"

PERSONAS = {
    "trader":     {"name": "Trader",     "voice": "Blunt, first-person, action-oriented. Short sentences. Trading slang."},
    "analyst":    {"name": "Analyst",    "voice": "Data-first, methodical. References levels, volume ratios, timeframes precisely."},
    "contrarian": {"name": "Contrarian", "voice": "Skeptical, challenges consensus. Opens with popular view, then dismantles it."},
    "educator":   {"name": "Educator",   "voice": "Patient, concrete examples. Explains the why. Never signals."},
    "degen":      {"name": "Degen",      "voice": "High-energy, informal, one strong emoji max. Still specific."},
    "stoic":      {"name": "Stoic",      "voice": "Calm, principle-driven. Zen observations about risk, patience, process."},
    "diarist":    {"name": "Diarist",    "voice": "Honest, vulnerable, first-person PNL. Shares losses openly. No advice."},
    "curator":    {"name": "Curator",    "voice": "Helpful filter. Curated lists, translated news, actionable non-obvious info."},
}

PERSONA_ROUTING = {
    "signal":          ["trader", "analyst", "contrarian", "degen"],
    "trade_plan":      ["analyst", "trader", "stoic"],
    "diary":           ["diarist"],
    "curator":         ["curator"],
    "educational":     ["educator", "stoic", "analyst"],
    "market_analysis": ["analyst", "stoic", "contrarian"],
    "coin_analysis":   ["analyst", "educator", "contrarian"],
    "gainers":         ["degen", "trader"],
    "whale":           ["analyst", "contrarian", "trader"],
    "result":          ["trader", "degen", "stoic"],
    "news":            ["trader", "analyst", "curator"],
    "onchain":         ["analyst", "educator", "stoic"],
    "quiz":            ["educator"],
    "recap":           ["stoic", "analyst", "trader"],
}

BANNED_AI_TELLS = [
    "in the ever-evolving", "in the dynamic world", "navigating the",
    "it's important to note", "it is important to note", "it's worth noting",
    "keep in mind that", "as we can see", "let's dive in", "delve into",
    "in conclusion", "to sum up", "the bottom line is", "game-changer",
    "paradigm shift", "synergy", "leverage the", "unlock the", "harness the power",
    "at the end of the day", "here's the thing", "first and foremost",
    "take a deep dive", "unpack this", "in today's", "buckle up",
]

FORBIDDEN_GENERIC_PHRASES = {
    "something is brewing", "watch what happens next", "just got interesting",
    "getting interesting", "don't sleep on", "this is huge", "next 100x",
    "guaranteed", "to the moon", "easy money", "100% sure", "cannot lose",
    "risk free", "nfa", "dyor",
}

TEMPLATE_WORDS = {
    "long","short","signal","entry","tp1","tp2","tp3","sl","setup","target",
    "targets","leverage","risk","trade","trading","price","coin","reached","hit",
}

_EMOJI_RE   = re.compile(r"[\U0001F300-\U0001FAFF\u2600-\u27BF]")
_CASHTAG_RE = re.compile(r"\$([A-Z]{2,10})")

STABLE_BASES = {
    "USDC","FDUSD","TUSD","USDP","DAI","BUSD","USD1","RLUSD","USDD",
    "USTC","FRAX","PYUSD","USDE","USDJ",
}
STABLE_SYMBOLS = {b + "USDT" for b in STABLE_BASES}

MAJOR_CASHTAGS = {"BTC","ETH","BNB","SOL","XRP","DOGE","ADA","TON","TRX"}
MIDCAP_BOOST   = 1.35
MAJOR_PENALTY  = 0.55

USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0 Safari/537.36")

STATE_FILE   = "bot_state.json"
ANALYTICS_DB = "analytics.db"
REQUEST_TIMEOUT = 20

# ============================================================================
# SECTION 2 — AI PROVIDER LAYER
# ============================================================================

def _load_api_keys(env_names, filename, inline_defaults=None):
    seen, keys = set(), []
    def _add(k):
        k = (k or "").strip()
        if k and k not in seen:
            seen.add(k); keys.append(k)
    for k in (inline_defaults or []): _add(k)
    for name in env_names:
        raw = os.getenv(name, "") or ""
        for part in re.split(r"[,\n;]", raw): _add(part)
    if filename and os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"): continue
                    _add(line)
        except Exception as e:
            print(f"[KEYS] {filename}: {e}")
    return keys

GROQ_API_KEYS = _load_api_keys(("GROQ_API_KEY", "GROQ_API_KEYS"), "groq_keys.txt")
CEREBRAS_API_KEYS = _load_api_keys(("CEREBRAS_API_KEY", "CEREBRAS_API_KEYS"), "cerebras_keys.txt")
OPENROUTER_API_KEYS = _load_api_keys(("OPENROUTER_API_KEY", "OPENROUTER_API_KEYS"), "openrouter_keys.txt")

PROVIDERS = [
    {"name": "groq",        "base_url": "https://api.groq.com/openai/v1",
     "model": "openai/gpt-oss-120b", "keys": GROQ_API_KEYS},
    {"name": "groq-backup", "base_url": "https://api.groq.com/openai/v1",
     "model": "openai/gpt-oss-20b",  "keys": GROQ_API_KEYS},
    {"name": "cerebras",    "base_url": "https://api.cerebras.ai/v1",
     "model": "llama3.3-70b",        "keys": CEREBRAS_API_KEYS},
    {"name": "openrouter",  "base_url": "https://openrouter.ai/api/v1",
     "model": "openrouter/free",     "keys": OPENROUTER_API_KEYS},
]

_key_state = {}
for _p in PROVIDERS:
    for _k in _p["keys"]:
        _key_state[f"{_p['name']}:{_k}"] = {"blocked_until_ts": 0}

AI_AVAILABLE = any(p["keys"] for p in PROVIDERS)

def now_ts(): return int(time.time())
def utc_date(): return datetime.now(timezone.utc).strftime("%Y-%m-%d")
def utc_hour(): return datetime.now(timezone.utc).hour
def utc_weekday(): return datetime.now(timezone.utc).weekday()
def current_week_key():
    iso = datetime.now(timezone.utc).isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"

def _get_live_keys(provider):
    now = now_ts()
    return [k for k in provider["keys"]
            if _key_state.get(f"{provider['name']}:{k}", {}).get("blocked_until_ts", 0) < now]

def _block_key(provider_name, key):
    tomorrow = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
                + timedelta(days=1))
    _key_state.setdefault(f"{provider_name}:{key}", {})["blocked_until_ts"] = int(tomorrow.timestamp())
    masked = key[:14] + "..." if len(key) > 14 else key
    print(f"[KEY] {provider_name}:{masked} exhausted")

def ai_chat(messages, temperature=0.9, max_tokens=1500, prefer_json=False):
    if not AI_AVAILABLE: return None
    for provider in PROVIDERS:
        if not provider["keys"]: continue
        live = _get_live_keys(provider)
        if not live: continue
        is_reasoning = "gpt-oss" in provider["model"].lower()
        for key in live:
            try:
                c = OpenAI(base_url=provider["base_url"], api_key=key)
                eff_max = max(max_tokens, 2000) if is_reasoning else max_tokens
                kwargs = {"model": provider["model"], "messages": messages,
                          "temperature": temperature, "max_tokens": eff_max}
                if is_reasoning: kwargs["extra_body"] = {"reasoning_effort": "low"}
                if prefer_json:  kwargs["response_format"] = {"type": "json_object"}
                r = c.chat.completions.create(**kwargs)
                result = r.choices[0].message.content
                if result and result.strip():
                    print(f"[AI] OK {provider['name']}")
                    return result.strip()
            except Exception as e:
                err = str(e)
                if "429" in err or "rate limit" in err.lower() or "quota" in err.lower():
                    _block_key(provider["name"], key); continue
                if "404" in err: break
                continue
    print("[AI] All providers exhausted.")
    return None

def ai_json(messages, temperature=0.7, max_tokens=800):
    raw = ai_chat(messages, temperature=temperature, max_tokens=max_tokens, prefer_json=True)
    if not raw: return None
    raw = re.sub(r"^```(?:json)?", "", raw, flags=re.I)
    raw = re.sub(r"```$", "", raw).strip()
    try: return json.loads(raw)
    except Exception:
        m = re.search(r"\{.*\}", raw, re.S)
        if m:
            try: return json.loads(m.group(0))
            except Exception: pass
    return None

# ============================================================================
# SECTION 3 — HTTP + UTIL
# ============================================================================

session = requests.Session()
session.headers.update({
    "User-Agent": USER_AGENT,
    "Accept": "application/json",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
})
_adapter = HTTPAdapter(
    pool_connections=32, pool_maxsize=32,
    max_retries=Retry(total=3, backoff_factor=0.4,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=frozenset(["GET", "POST"])),
    pool_block=False)
session.mount("https://", _adapter)
session.mount("http://", _adapter)
_IO_POOL = ThreadPoolExecutor(max_workers=8, thread_name_prefix="sc30-io")

def safe_float(v, d=0.0):
    try: return float(v)
    except Exception: return d

def clamp(v, lo, hi): return max(lo, min(hi, v))
def mean(xs): return sum(xs) / len(xs) if xs else 0.0
def pct_change(old, new):
    if old == 0: return 0.0
    return (new - old) / old * 100

def format_price(p):
    if p >= 1000:   return f"{p:,.2f}"
    if p >= 100:    return f"{p:.3f}"
    if p >= 1:      return f"{p:.4f}"
    if p >= 0.01:   return f"{p:.5f}"
    if p >= 0.0001: return f"{p:.7f}"
    return f"{p:.10f}".rstrip("0").rstrip(".")

def format_target(p):
    if p >= 1000:   return f"{p:,.2f}"
    if p >= 100:    return f"{p:.2f}"
    if p >= 1:      return f"{p:.2f}".rstrip("0").rstrip(".")
    if p >= 0.01:   return f"{p:.4f}".rstrip("0").rstrip(".")
    if p >= 0.0001: return f"{p:.6f}".rstrip("0").rstrip(".")
    return f"{p:.8f}".rstrip("0").rstrip(".")

def _strip_markdown(text):
    text = re.sub(r"\*\*(.+?)\*\*", r"\1", text)
    text = re.sub(r"__(.+?)__", r"\1", text)
    text = re.sub(r"(?<!\w)\*(.+?)\*(?!\w)", r"\1", text)
    text = re.sub(r"(?<!\w)_(.+?)_(?!\w)", r"\1", text)
    text = re.sub(r"^#+\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"^>\s*", "", text, flags=re.MULTILINE)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()

def _truncate_for(text, limit):
    text = text.strip()
    if len(text) <= limit: return text
    cut = text.rfind(". ", 0, limit - 3)
    if cut < limit // 2: cut = text.rfind(" ", 0, limit - 3)
    if cut < limit // 2: cut = limit - 3
    return text[:cut].rstrip() + "..."

def truncate_for_square(t):  return _truncate_for(t, 2000)
def truncate_for_discord(t): return _truncate_for(t, 1990)

def parse_compact_number(s):
    if not s: return 0
    s = str(s).strip().replace(",", "")
    mult = 1
    if s.endswith("K"): mult = 1000; s = s[:-1]
    elif s.endswith("M"): mult = 1_000_000; s = s[:-1]
    elif s.endswith("B"): mult = 1_000_000_000; s = s[:-1]
    try: return int(float(s) * mult)
    except Exception: return 0

# ============================================================================
# SECTION 4 — ANALYTICS DB
# ============================================================================

_db_local = threading.local()

def _db():
    conn = getattr(_db_local, "conn", None)
    if conn is not None:
        try: conn.execute("SELECT 1"); return conn
        except sqlite3.Error: pass
    conn = sqlite3.connect(ANALYTICS_DB, timeout=15, isolation_level=None)
    for pragma in ("PRAGMA journal_mode=WAL","PRAGMA synchronous=NORMAL",
                   "PRAGMA temp_store=MEMORY","PRAGMA mmap_size=134217728",
                   "PRAGMA busy_timeout=8000","PRAGMA cache_size=-20000"):
        conn.execute(pragma)
    conn.row_factory = sqlite3.Row
    _db_local.conn = conn
    return conn

def init_analytics_db():
    c = _db()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS posts (
            post_id TEXT PRIMARY KEY, real_post_id TEXT,
            link_status TEXT DEFAULT 'pending', posted_at INTEGER,
            posted_hour_utc INTEGER, posted_weekday INTEGER DEFAULT 0,
            coin TEXT, content_type TEXT, persona TEXT DEFAULT '',
            ab_group TEXT DEFAULT '', hook TEXT, style_angle TEXT,
            emoji_count INTEGER DEFAULT 0, word_count INTEGER DEFAULT 0,
            quality_score INTEGER DEFAULT 0, critic_score INTEGER DEFAULT 0,
            tier TEXT DEFAULT '', entry_price REAL DEFAULT 0,
            views INTEGER DEFAULT 0, likes INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0, shares INTEGER DEFAULT 0,
            saves INTEGER DEFAULT 0, predicted_final_views INTEGER DEFAULT 0,
            view_velocity_15m REAL DEFAULT 0, predicted_earnings REAL DEFAULT 0,
            engagement_score REAL DEFAULT 0, sentiment_bucket TEXT DEFAULT '',
            last_scraped_at INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_posted_at ON posts(posted_at);
        CREATE INDEX IF NOT EXISTS idx_coin ON posts(coin);
        CREATE INDEX IF NOT EXISTS idx_type ON posts(content_type);
        CREATE INDEX IF NOT EXISTS idx_real_id ON posts(real_post_id);
        CREATE INDEX IF NOT EXISTS idx_views ON posts(views DESC);

        CREATE TABLE IF NOT EXISTS learned_weights (key TEXT PRIMARY KEY, value TEXT, updated_at INTEGER);
        CREATE TABLE IF NOT EXISTS post_snapshots (post_id TEXT, scraped_at INTEGER,
            views INTEGER, likes INTEGER, comments INTEGER, PRIMARY KEY (post_id, scraped_at));
        CREATE TABLE IF NOT EXISTS hook_patterns (pattern_hash TEXT PRIMARY KEY, pattern_text TEXT,
            uses INTEGER DEFAULT 0, total_views INTEGER DEFAULT 0, avg_views REAL DEFAULT 0,
            last_used INTEGER, source TEXT DEFAULT 'generated');
        CREATE TABLE IF NOT EXISTS square_feed_posts (post_id TEXT PRIMARY KEY, creator TEXT,
            creator_url TEXT, coin TEXT, text_sample TEXT, hook_sample TEXT,
            views INTEGER, likes INTEGER, comments INTEGER, shares INTEGER,
            age_min INTEGER, scraped_at INTEGER);
        CREATE TABLE IF NOT EXISTS creators (handle TEXT PRIMARY KEY, profile_url TEXT,
            posts_seen INTEGER DEFAULT 0, avg_views REAL DEFAULT 0, peak_views INTEGER DEFAULT 0,
            last_seen INTEGER, tracked INTEGER DEFAULT 0, notes TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS hook_observations (obs_id TEXT PRIMARY KEY, source TEXT,
            creator TEXT, hook_text TEXT, hook_norm TEXT, views INTEGER, seen_at INTEGER);
        CREATE TABLE IF NOT EXISTS news_events (event_id TEXT PRIMARY KEY, source TEXT,
            title TEXT, coins TEXT, category TEXT, seen_ts INTEGER, posted INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS whale_events (event_id TEXT PRIMARY KEY, symbol TEXT,
            side TEXT, notional_usd REAL, price REAL, qty REAL, ts INTEGER, posted INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS sentiment_history (ts INTEGER PRIMARY KEY, fear_greed INTEGER,
            funding_rate REAL, long_short_ratio REAL, news_sentiment REAL, composite REAL, bucket TEXT);
        CREATE TABLE IF NOT EXISTS view_tier_weights (tier INTEGER, feature TEXT, n INTEGER,
            hit_rate REAL, avg_views REAL, weight REAL, updated_at INTEGER,
            PRIMARY KEY (tier, feature));
        CREATE TABLE IF NOT EXISTS ab_tests (test_id TEXT PRIMARY KEY, created_at INTEGER,
            content_type TEXT, coin TEXT, variant_a TEXT, variant_b TEXT,
            persona_a TEXT, persona_b TEXT, winner TEXT, decided_at INTEGER,
            eng_a REAL DEFAULT 0, eng_b REAL DEFAULT 0,
            samples_a INTEGER DEFAULT 0, samples_b INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS health_events (ts INTEGER PRIMARY KEY, kind TEXT, detail TEXT);
        CREATE TABLE IF NOT EXISTS narratives (narrative_id TEXT PRIMARY KEY, label TEXT,
            coins TEXT, keywords TEXT, velocity REAL DEFAULT 0, platform_weight REAL DEFAULT 0,
            cross_source_confirmation REAL DEFAULT 0, freshness REAL DEFAULT 0,
            composite_score REAL DEFAULT 0, source_breakdown TEXT,
            first_seen INTEGER, last_seen INTEGER, posted INTEGER DEFAULT 0);
        CREATE INDEX IF NOT EXISTS idx_narr_score ON narratives(composite_score DESC);
        CREATE TABLE IF NOT EXISTS archetype_log (post_id TEXT PRIMARY KEY, archetype TEXT,
            secondary_archetype TEXT, reasoning TEXT, logged_at INTEGER);
        CREATE TABLE IF NOT EXISTS trust_scores (post_id TEXT PRIMARY KEY,
            loss_ack_rate REAL DEFAULT 0, referral_density_inv REAL DEFAULT 1,
            win_claim_honesty REAL DEFAULT 1, consistency REAL DEFAULT 1,
            age_factor REAL DEFAULT 1, peer_sentiment REAL DEFAULT 0,
            trust_score REAL DEFAULT 0, divergence_score REAL DEFAULT 0,
            discriminator_p_human REAL DEFAULT 0, computed_at INTEGER);
        CREATE INDEX IF NOT EXISTS idx_trust_score ON trust_scores(trust_score DESC);
        CREATE TABLE IF NOT EXISTS early_engagement (post_id TEXT, minute_mark INTEGER,
            views INTEGER, likes INTEGER, comments INTEGER, shares INTEGER,
            PRIMARY KEY (post_id, minute_mark));
        CREATE TABLE IF NOT EXISTS comment_replies (reply_id TEXT PRIMARY KEY, post_id TEXT,
            comment_author TEXT, comment_text TEXT, reply_text TEXT,
            replied_at INTEGER, sentiment TEXT);
        CREATE TABLE IF NOT EXISTS language_variants (variant_id TEXT PRIMARY KEY,
            source_post_id TEXT, target_lang TEXT, adapted_text TEXT,
            scheduled_at INTEGER, posted_at INTEGER, real_post_id TEXT);
        CREATE TABLE IF NOT EXISTS narrative_engagement (narrative_id TEXT, post_id TEXT,
            views INTEGER DEFAULT 0, likes INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0, recorded_at INTEGER,
            PRIMARY KEY (narrative_id, post_id));
        CREATE TABLE IF NOT EXISTS reply_targets (post_id TEXT PRIMARY KEY, creator TEXT,
            coin TEXT, text_sample TEXT, views INTEGER, reply_drafted TEXT,
            replied INTEGER DEFAULT 0, discovered_at INTEGER);
        CREATE TABLE IF NOT EXISTS humanizer_log (action_id TEXT PRIMARY KEY, kind TEXT,
            detail TEXT, ts INTEGER, duration_ms INTEGER);
        CREATE TABLE IF NOT EXISTS text_embeddings (post_id TEXT PRIMARY KEY, text_hash TEXT,
            embedding TEXT, created_at INTEGER);
        CREATE INDEX IF NOT EXISTS idx_emb_hash ON text_embeddings(text_hash);
        CREATE TABLE IF NOT EXISTS svg_generations (gen_id TEXT PRIMARY KEY, kind TEXT,
            json_payload TEXT, svg_path TEXT, created_at INTEGER, posted INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS paper_trades (trade_id TEXT PRIMARY KEY, signal_id TEXT,
            symbol TEXT, side TEXT, qty REAL, entry_price REAL, exit_price REAL,
            status TEXT DEFAULT 'OPEN', pnl REAL DEFAULT 0, opened_at INTEGER, closed_at INTEGER,
            screenshot_path TEXT, posted INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS memes (meme_id TEXT PRIMARY KEY, lang TEXT, template TEXT,
            caption TEXT, image_path TEXT, source_event TEXT, created_at INTEGER, posted INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS bayes_trials (trial_id TEXT PRIMARY KEY, params TEXT,
            metric REAL, created_at INTEGER);
    """)
    existing = {row[1] for row in c.execute("PRAGMA table_info(posts)")}
    for add_col, ddl in [
        ("archetype",     "ALTER TABLE posts ADD COLUMN archetype TEXT DEFAULT ''"),
        ("narrative_id",  "ALTER TABLE posts ADD COLUMN narrative_id TEXT DEFAULT ''"),
        ("trust_score",   "ALTER TABLE posts ADD COLUMN trust_score REAL DEFAULT 0"),
        ("lang_origin",   "ALTER TABLE posts ADD COLUMN lang_origin TEXT DEFAULT 'en'"),
        ("swarm_node",    "ALTER TABLE posts ADD COLUMN swarm_node TEXT DEFAULT ''"),
    ]:
        if add_col not in existing:
            try: c.execute(ddl)
            except sqlite3.OperationalError: pass

def _row_to_dict(r): return {k: r[k] for k in r.keys()} if r is not None else None

# ============================================================================
# SECTION 5 — STATE
# ============================================================================

DEFAULT_STATE = {
    "posted_coins": {}, "active_signals": {},
    "recent_posts": [], "recent_openings": [], "recent_hooks": [],
    "recent_personas": [],
    "square_posts_today": 0, "square_posts_date": "", "last_post_ts": 0,
    "square_replies_today": 0, "last_reply_ts": 0,
    "archetype_counter": {},
    "consecutive_long": 0, "consecutive_short": 0,
    "cashtag_stats": {}, "stats_week_start": "",
    "result_posts": {}, "results_posted_today": 0,
    "gainers_coins": {}, "last_gainers_ts": 0,
    "recent_calls": [], "last_tp_result_ts": 0,
    "last_market_analysis_ts": 0, "last_coin_analysis_ts": 0,
    "coin_analysis_coins": {},
    "last_educational_ts": 0, "educational_topics": {},
    "last_recap_week": "", "coin_performance": {},
    "news_posts_today": 0, "last_news_post_ts": 0,
    "last_onchain_post_ts": 0, "onchain_prev": {}, "last_quiz_ts": 0,
    "last_whale_ts": 0, "last_whale_scan": 0,
    "last_analytics_scrape": 0, "last_learn_ts": 0, "last_tune_ts": 0,
    "last_discover_scan": 0, "discover_cache": {}, "learned_weights": {},
    "last_sentiment_refresh": 0,
    "last_competitor_scan": 0, "last_hijack_ts": 0,
    "last_feed_scan": 0, "last_creator_discovery": 0,
    "last_narrative_refresh": 0,
    "pending_lang_variants": [], "pending_engagement_watches": {},
    "daily_archetype_date": "", "daily_content_counter": {},
    "failed_publish_cooldown": {},
    "elastic_mult_today": {},
    "last_discriminator_train": 0,
    "last_bayes_ts": 0,
    "last_svg_ts": 0,
    "last_meme_ts": 0,
    "last_paper_trade_check": 0,
    "auto_tune": {
        "signal_threshold": SIGNAL_THRESHOLD,
        "min_quality": MIN_POST_QUALITY,
        "min_trust": MIN_TRUST_SCORE,
        "emoji_bias": 1, "view_gate_on": True, "predicted_floor_mult": 0.75,
    },
    "metrics": {
        "signals_posted": 0, "signals_a_plus": 0, "signals_a": 0, "signals_b": 0,
        "news_posted": 0, "market_posted": 0, "coin_analysis_posted": 0,
        "educational_posted": 0, "gainers_posted": 0, "results_posted": 0,
        "whale_alerts": 0, "onchain_posted": 0, "quiz_posted": 0,
        "diary_posted": 0, "curator_posted": 0, "trade_plan_posted": 0,
        "narratives_detected": 0, "narratives_acted_on": 0,
        "trust_rejections": 0, "archetype_cap_hits": 0,
        "replies_sent": 0, "lang_variants_posted": 0,
        "quality_sum": 0.0, "quality_count": 0, "started_at": 0,
        "feed_scans": 0, "creators_discovered": 0, "hooks_mined": 0,
        "browser_posts": 0, "api_posts": 0, "browser_failures": 0,
        "x_firehose_hits": 0, "whale_events": 0, "swarm_replies_sent": 0,
        "paper_trades_opened": 0, "paper_trades_hit": 0,
        "svg_generated": 0, "memes_generated": 0,
        "ab_tests_created": 0, "ab_tests_decided": 0,
        "bayes_trials": 0,
        "ttp_samples": [], "ttp_avg_sec": 0.0,
    },
}

def load_state():
    if not os.path.exists(STATE_FILE):
        return json.loads(json.dumps(DEFAULT_STATE))
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f: s = json.load(f)
        for k, v in DEFAULT_STATE.items():
            if k not in s: s[k] = json.loads(json.dumps(v))
        if isinstance(s.get("metrics"), dict):
            for k, v in DEFAULT_STATE["metrics"].items(): s["metrics"].setdefault(k, v)
        if isinstance(s.get("auto_tune"), dict):
            for k, v in DEFAULT_STATE["auto_tune"].items(): s["auto_tune"].setdefault(k, v)
        return s
    except Exception as e:
        print(f"[STATE] Load failed: {e}")
        return json.loads(json.dumps(DEFAULT_STATE))

state = load_state()
if not state["metrics"].get("started_at"):
    state["metrics"]["started_at"] = int(time.time())

def save_state():
    try:
        temp = STATE_FILE + ".tmp"
        with open(temp, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(temp, STATE_FILE)
    except Exception as e:
        print(f"[STATE] Save failed: {e}")

# ============================================================================
# SECTION 6 — L1: NARRATIVE ENGINE + X FIREHOSE + ON-CHAIN WHALES
# ============================================================================

class XFirehose:
    """Real-time X/Twitter ingestion via RSS bridges. KPI: Time-to-Post < 4 min."""
    def __init__(self):
        self._lock = threading.Lock()
        self._seen = set()
        self._recent = []
        self._last_poll = 0

    def _parse_rss(self, xml_bytes):
        out = []
        try: root = ET.fromstring(xml_bytes)
        except Exception: return out
        for item in root.iter("item"):
            title = (item.findtext("title", "") or "").strip()
            desc  = (item.findtext("description", "") or "").strip()
            link  = (item.findtext("link", "") or "").strip()
            pub   = (item.findtext("pubDate", "") or "").strip()
            if not title: continue
            hid = hashlib.sha1((title + link).encode()).hexdigest()
            if hid in self._seen: continue
            self._seen.add(hid)
            text = f"{title} {desc}"
            coins = list({c.upper() for c in re.findall(r"\$([A-Za-z]{2,8})", text)})
            coins += [t for t in ("BTC", "ETH", "SOL", "BNB", "XRP")
                      if t.lower() in text.lower()]
            coins = list(dict.fromkeys(coins))[:5]
            try:
                from email.utils import parsedate_to_datetime
                age_s = max(0, int(now_ts() - parsedate_to_datetime(pub).timestamp()))
            except Exception:
                age_s = 9999
            out.append({"id": hid, "title": title[:280], "text": text[:600],
                        "link": link, "coins": coins, "age_s": age_s,
                        "source": "x_firehose", "seen_ts": now_ts()})
        return out

    def poll(self):
        if not X_FIREHOSE_ENABLED: return self._recent
        now = now_ts()
        with self._lock:
            if now - self._last_poll < X_FIREHOSE_POLL_SEC: return self._recent
            self._last_poll = now
        fresh = []
        for url in X_FIREHOSE_SOURCES:
            try:
                r = session.get(url, timeout=8, headers={"User-Agent": USER_AGENT})
                if r.status_code == 200:
                    fresh.extend(self._parse_rss(r.content))
            except Exception: continue
        fresh = [f for f in fresh if f["age_s"] <= X_FIREHOSE_MAX_AGE_SEC]
        with self._lock:
            self._recent = (fresh + self._recent)[:200]
        if fresh:
            state["metrics"]["x_firehose_hits"] = \
                state["metrics"].get("x_firehose_hits", 0) + len(fresh)
        return self._recent

x_firehose = XFirehose()

def _get_eth_price_usd():
    try:
        return safe_float(binance_get("/api/v3/ticker/price",
                                      {"symbol": "ETHUSDT"}).get("price"))
    except Exception:
        return 0.0

class OnChainWhaleTracker:
    """Tracks large transfers for curated wallets via JSON-RPC."""
    def __init__(self):
        self._lock = threading.Lock()
        self._last_poll = 0
        self._block_cursor = None
        self._events = []

    def _rpc(self, method, params):
        try:
            r = session.post(ONCHAIN_RPC_URL,
                             json={"jsonrpc": "2.0", "id": 1,
                                   "method": method, "params": params},
                             timeout=8)
            return r.json().get("result")
        except Exception:
            return None

    def _scan_block(self, blk_hex):
        blk = self._rpc("eth_getBlockByNumber", [blk_hex, True])
        if not blk or "transactions" not in blk: return []
        out = []
        for tx in blk["transactions"]:
            frm = (tx.get("from") or "").lower()
            to  = (tx.get("to")   or "").lower()
            if frm not in WHALE_WALLETS and to not in WHALE_WALLETS: continue
            try: val_eth = int(tx["value"], 16) / 1e18
            except Exception: continue
            if val_eth < 50: continue
            out.append({"hash": tx["hash"], "from": frm, "to": to,
                        "eth": val_eth, "block": blk_hex})
        return out

    def poll(self):
        if not ONCHAIN_RPC_ENABLED: return self._events
        now = now_ts()
        with self._lock:
            if now - self._last_poll < WHALE_POLL_SEC: return self._events
            self._last_poll = now
        latest_hex = self._rpc("eth_blockNumber", [])
        if not latest_hex: return self._events
        try: latest = int(latest_hex, 16)
        except Exception: return self._events
        start = latest - 3 if self._block_cursor is None else self._block_cursor + 1
        eth_price = _get_eth_price_usd()
        for n in range(start, latest + 1):
            for ev in self._scan_block(hex(n)):
                ev["notional_usd"] = ev["eth"] * eth_price
                if ev["notional_usd"] >= WHALE_MIN_USD:
                    ev["event_id"] = hashlib.sha1(ev["hash"].encode()).hexdigest()[:16]
                    ev["ts"] = now_ts()
                    try:
                        _db().execute("""INSERT OR IGNORE INTO whale_events
                            (event_id, symbol, side, notional_usd, price, qty, ts)
                            VALUES (?,?,?,?,?,?,?)""",
                            (ev["event_id"], "ETH",
                             "OUT" if ev["from"] in WHALE_WALLETS else "IN",
                             ev["notional_usd"], eth_price, ev["eth"], ev["ts"]))
                    except Exception: pass
                    self._events.append(ev)
                    state["metrics"]["whale_events"] = \
                        state["metrics"].get("whale_events", 0) + 1
        self._block_cursor = latest
        self._events = self._events[-200:]
        return self._events

onchain_whales = OnChainWhaleTracker()

class NarrativeEngine:
    def __init__(self):
        self._lock = threading.Lock()
        self._cache = {"ts": 0, "narratives": []}

    def _narrative_id(self, coins, keywords):
        key = "|".join(sorted(coins)[:4]) + "::" + "|".join(sorted(keywords)[:4])
        return hashlib.sha1(key.encode()).hexdigest()[:16]

    def _classify_narrative(self, coins, headlines):
        if not AI_AVAILABLE or not headlines: return None
        prompt = f"""Classify a crypto narrative. Return JSON:
{{"label":"3-6 words","keywords":["4-6 lowercase"],"register":"bullish|bearish|neutral|fearful|euphoric","platform_fit":0-10,"urgency":0-10}}

COINS: {coins}
HEADLINES:
{chr(10).join('- ' + h for h in headlines[:8])}"""
        data = ai_json([{"role": "user", "content": prompt}], temperature=0.4, max_tokens=400)
        if not data: return None
        return {
            "label": str(data.get("label", "unnamed"))[:80],
            "keywords": [str(k).lower()[:24] for k in (data.get("keywords") or [])][:8],
            "register": str(data.get("register", "neutral")).lower()[:16],
            "platform_fit": clamp(float(data.get("platform_fit", 5)), 0, 10),
            "urgency": clamp(float(data.get("urgency", 5)), 0, 10),
        }

    def _score(self, *, velocity, platform_weight, cross_source, freshness):
        return (velocity * 0.30 + platform_weight * 0.25
                + cross_source * 0.25 + freshness * 0.20)

    def refresh(self, force=False):
        now = now_ts()
        with self._lock:
            if not force and now - self._cache["ts"] < NARRATIVE_REFRESH_SECONDS:
                return self._cache
        narratives = []
        news_rows = []
        try:
            news_rows = _db().execute("""
                SELECT event_id, title, coins, category, seen_ts
                FROM news_events WHERE seen_ts > ?
                ORDER BY seen_ts DESC LIMIT 60
            """, (now - 6 * 3600,)).fetchall()
        except Exception: pass
        # V30: X firehose + whale events
        x_hits = x_firehose.poll()
        whale_events = onchain_whales.poll()
        x_coin_counts = Counter()
        for f in x_hits[:80]:
            for c in f["coins"]: x_coin_counts[c] += 1
        whale_coin_counts = Counter()
        for w in whale_events[-40:]: whale_coin_counts["ETH"] += 1

        trending_coins = []
        try: trending_coins = list(trending_hijacker.top(20))
        except Exception: pass
        feed_coins = []
        try: feed_coins = list(feed_learner._cache.get("coins", {}).keys())[:20]
        except Exception: pass
        discover_coins = []
        try: discover_coins = list(state.get("discover_cache", {}).get("coins", []))[:20]
        except Exception: pass

        coin_headlines = defaultdict(list)
        coin_seen = defaultdict(list)
        for r in news_rows:
            for c in (r["coins"] or "").split(","):
                c = c.strip()
                if not c: continue
                coin_headlines[c].append(r["title"])
                coin_seen[c].append(r["seen_ts"])

        all_coins = set()
        all_coins.update(coin_headlines.keys()); all_coins.update(trending_coins)
        all_coins.update(feed_coins); all_coins.update(discover_coins)
        all_coins.update(x_coin_counts.keys()); all_coins.update(whale_coin_counts.keys())

        for coin in all_coins:
            headlines = coin_headlines.get(coin, [])
            sources = sum([bool(headlines), coin in trending_coins,
                           coin in feed_coins, coin in discover_coins])
            x_velocity = clamp(x_coin_counts.get(coin, 0) / 6.0, 0, 1)
            whale_velocity = clamp(whale_coin_counts.get(coin, 0) / 4.0, 0, 1)
            velocity = clamp((sources / 4.0) * 0.5
                             + x_velocity * 0.30
                             + whale_velocity * 0.20, 0, 1)
            platform_weight = 0.0
            if coin in trending_coins:
                try:
                    idx = trending_coins.index(coin)
                    platform_weight = max(platform_weight, 1.0 - idx / 25.0)
                except ValueError: pass
            if coin in feed_coins:
                try:
                    idx = feed_coins.index(coin)
                    platform_weight = max(platform_weight, 1.0 - idx / 25.0)
                except ValueError: pass
            cross_source = clamp(len(set([bool(headlines), coin in trending_coins,
                                          coin in feed_coins, coin in discover_coins,
                                          bool(x_coin_counts.get(coin))])) / 5.0, 0, 1)
            ages = [(now - t) / 3600 for t in coin_seen.get(coin, [])]
            freshness = clamp(math.exp(-min(ages) / 6.0), 0, 1) if ages else 0.4
            composite = self._score(velocity=velocity, platform_weight=platform_weight,
                                    cross_source=cross_source, freshness=freshness)
            if composite < NARRATIVE_MIN_SCORE: continue
            classification = None
            if AI_AVAILABLE and headlines:
                try: classification = self._classify_narrative([coin], headlines)
                except Exception: classification = None
            narr_id = self._narrative_id([coin], (classification or {}).get("keywords", []))
            narratives.append({
                "narrative_id": narr_id,
                "label": (classification or {}).get("label", f"${coin} momentum"),
                "coins": [coin],
                "keywords": (classification or {}).get("keywords", []),
                "register": (classification or {}).get("register", "neutral"),
                "velocity": round(velocity, 3),
                "platform_weight": round(platform_weight, 3),
                "cross_source_confirmation": round(cross_source, 3),
                "freshness": round(freshness, 3),
                "composite_score": round(composite, 3),
                "headlines": headlines[:4],
            })
        narratives.sort(key=lambda n: -n["composite_score"])
        narratives = narratives[:NARRATIVE_TOP_N]
        try:
            c = _db()
            for n in narratives:
                c.execute("""
                    INSERT INTO narratives (narrative_id, label, coins, keywords,
                        velocity, platform_weight, cross_source_confirmation, freshness,
                        composite_score, first_seen, last_seen)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(narrative_id) DO UPDATE SET
                        composite_score=excluded.composite_score, last_seen=excluded.last_seen
                """, (n["narrative_id"], n["label"], ",".join(n["coins"]),
                      ",".join(n["keywords"]), n["velocity"], n["platform_weight"],
                      n["cross_source_confirmation"], n["freshness"],
                      n["composite_score"], now, now))
        except Exception: pass
        state["metrics"]["narratives_detected"] = len(narratives)
        with self._lock:
            self._cache = {"ts": now, "narratives": narratives}
        return self._cache

    def top(self, n=8): return self._cache["narratives"][:n]
    def by_coin(self, coin):
        for n in self._cache["narratives"]:
            if coin in n["coins"]: return n
        return None
    def best_for_content_type(self, content_type):
        ns = self._cache["narratives"]
        if not ns: return None
        bias = {
            "news": lambda n: n["freshness"],
            "signal": lambda n: n["velocity"],
            "trade_plan": lambda n: n["platform_weight"],
            "curator": lambda n: n["cross_source_confirmation"],
        }.get(content_type, lambda n: n["composite_score"])
        return max(ns, key=bias)

narrative_engine = NarrativeEngine()

# ============================================================================
# SECTION 7 — L2: ARCHETYPE ENGINE (SWARM NODE BINDING)
# ============================================================================

ARCHETYPES = {
    "A1": {"name": "Structured Signal Caller"},
    "A2": {"name": "Aggressive HFT Caller"},
    "A3": {"name": "Disciplined Trade Planner"},
    "A4": {"name": "Emotional Retail Diarist"},
    "A5": {"name": "Community Curator"},
    "A6": {"name": "Utility Educator"},
}

class SwarmNodeBinding:
    """V30 — one archetype permanently bound to this Swarm Node."""
    def __init__(self):
        self._lock = threading.Lock()
        self.node_id = SWARM_NODE_ID
        self.binding = self._load_or_assign()

    def _load_or_assign(self):
        if os.path.exists(ARCHETYPE_BINDING_FILE):
            try:
                with open(ARCHETYPE_BINDING_FILE, "r") as f:
                    b = json.load(f)
                if b.get("node_id") == self.node_id and b.get("archetype"):
                    return b
            except Exception: pass
        archetypes = list(ARCHETYPES.keys())
        idx = int(hashlib.sha1(self.node_id.encode()).hexdigest(), 16) % len(archetypes)
        binding = {
            "node_id": self.node_id,
            "archetype": archetypes[idx],
            "secondary_archetype": archetypes[(idx + 1) % len(archetypes)],
            "bound_at": now_ts(),
            "retention_samples": [],
            "weekly_growth_rate": 0.0,
        }
        self._save(binding)
        return binding

    def _save(self, b):
        try:
            with open(ARCHETYPE_BINDING_FILE, "w") as f:
                json.dump(b, f, indent=2)
        except Exception as e:
            print(f"[L2] binding save failed: {e}")

    def record_follower_snapshot(self, followers):
        s = self.binding.setdefault("retention_samples", [])
        s.append({"ts": now_ts(), "followers": int(followers)})
        s = s[-60:]
        self.binding["retention_samples"] = s
        wk = now_ts() - 7 * 86400
        prior = [x for x in s if x["ts"] <= wk]
        if prior:
            base = prior[-1]["followers"] or 1
            self.binding["weekly_growth_rate"] = round(
                (followers - base) / base * 100, 3)
        self._save(self.binding)

swarm_binding = SwarmNodeBinding()

class ArchetypeEngine:
    """V30 — archetype bound permanently to this Swarm Node."""
    def _reset_daily_if_needed(self):
        today = utc_date()
        if state.get("daily_archetype_date") != today:
            state["daily_archetype_date"] = today
            state["archetype_counter"] = {}
            state["square_replies_today"] = 0

    def assign(self, content_type, persona, narrative=None):
        self._reset_daily_if_needed()
        arch = swarm_binding.binding["archetype"]
        sec  = swarm_binding.binding["secondary_archetype"]
        counter = state.setdefault("archetype_counter", {})
        total_a1a2 = counter.get("A1", 0) + counter.get("A2", 0)
        total = sum(counter.values()) + 1
        if arch in ("A1", "A2") and total > 0 and (total_a1a2 + 1) / total > ARCHETYPE_A1A2_CAP:
            arch = "A6"
        counter[arch] = counter.get(arch, 0) + 1
        return arch, sec, f"bound:{swarm_binding.node_id}"

archetype_engine = ArchetypeEngine()

# ============================================================================
# SECTION 8 — L3: TRUST ENGINE + LOCAL DISCRIMINATOR
# ============================================================================

class LocalDiscriminator:
    """Human-vs-AI classifier trained on platform data. Target: >98% bypass."""
    FEATURES = ["word_count","avg_sent_len","sent_len_var","upper_ratio",
                "punct_per_word","emoji_count","has_question","has_cashtag",
                "num_count","digit_ratio","unique_word_ratio","typo_ratio"]
    def __init__(self):
        self._lock = threading.Lock()
        self.model = None
        self.scaler = None
        self._load()

    def _features(self, text):
        words = text.split()
        sents = [s for s in re.split(r"[.!?]+", text) if s.strip()]
        wc  = max(len(words), 1)
        sl  = [len(s.split()) for s in sents] or [0]
        avg = mean(sl)
        var = mean([(x - avg) ** 2 for x in sl]) if len(sl) > 1 else 0.0
        upper = sum(1 for c in text if c.isupper()) / max(len(text), 1)
        punct = sum(1 for c in text if c in ".,!?;:") / wc
        emoji = len(_EMOJI_RE.findall(text))
        return [wc, avg, var, upper, punct, emoji,
                1 if "?" in text else 0,
                1 if _CASHTAG_RE.search(text) else 0,
                len(re.findall(r"\d+\.?\d*", text)),
                sum(c.isdigit() for c in text) / max(len(text), 1),
                len(set(w.lower() for w in words)) / wc, 0.0]

    def _build_training_set(self):
        try:
            rows = _db().execute("""
                SELECT post_id, hook, engagement_score, views
                FROM posts WHERE views > 0
                ORDER BY posted_at DESC LIMIT 500
            """).fetchall()
        except Exception: return [], []
        X, y = [], []
        for r in rows:
            text = r["hook"] or ""
            if len(text) < 20: continue
            feats = self._features(text)
            eng_per_v = (r["engagement_score"] or 0) / max(r["views"], 1)
            y.append(1 if eng_per_v >= 0.05 else 0)
            X.append(feats)
        return X, y

    def train(self):
        try:
            from sklearn.linear_model import LogisticRegression
            from sklearn.preprocessing import StandardScaler
            import pickle
        except ImportError:
            print("[L3] sklearn missing — discriminator disabled.")
            return False
        X, y = self._build_training_set()
        if len(X) < DISCRIMINATOR_MIN_SAMPLES or sum(y) < 5:
            print(f"[L3] not enough data ({len(X)} samples, {sum(y)} positives).")
            return False
        with self._lock:
            self.scaler = StandardScaler().fit(X)
            Xs = self.scaler.transform(X)
            self.model = LogisticRegression(max_iter=400, class_weight="balanced")
            self.model.fit(Xs, y)
            try:
                with open(DISCRIMINATOR_MODEL_PATH, "wb") as f:
                    pickle.dump({"model": self.model, "scaler": self.scaler,
                                 "trained_at": now_ts(), "n": len(X)}, f)
            except Exception as e: print(f"[L3] save failed: {e}")
        print(f"[L3] discriminator trained on {len(X)} samples.")
        return True

    def _load(self):
        if not os.path.exists(DISCRIMINATOR_MODEL_PATH): return
        try:
            import pickle
            with open(DISCRIMINATOR_MODEL_PATH, "rb") as f:
                d = pickle.load(f)
            self.model, self.scaler = d["model"], d["scaler"]
            print(f"[L3] discriminator loaded (n={d.get('n')}).")
        except Exception: pass

    def p_human(self, text):
        if self.model is None or self.scaler is None: return 0.75
        try:
            Xs = self.scaler.transform([self._features(text)])
            return float(self.model.predict_proba(Xs)[0][1])
        except Exception:
            return 0.75

local_discriminator = LocalDiscriminator()

class TrustEngine:
    REFERRAL_PATTERNS = [
        r"binance\.com/(?:en/)?register", r"binance\.com/(?:en/)?activity",
        r"referral", r"ref\s*code", r"refer", r"\bref\b.*\bcode\b", r"pump.?refund",
    ]
    WIN_CLAIM_PATTERNS = [
        r"tp\d?\s*(?:hit|reached|✅)", r"target\s+hit",
        r"called\s+it", r"told\s+you", r"we\s+win\s+again",
    ]
    LOSS_ACK_PATTERNS = [
        r"\bi\s+(?:was|got|am)\s+wrong\b", r"\bstopped\s+out\b",
        r"\bsl\s+hit\b", r"\bsorry\b", r"\bloss(?:es|ing)?\b",
        r"\bdown\s+[-\d]", r"\bpnl\s*[:=]\s*-", r"-[0-9,.]+\s*usdt",
    ]

    def _referral_density(self, text):
        low = text.lower()
        hits = sum(1 for p in self.REFERRAL_PATTERNS if re.search(p, low))
        return clamp(hits / 2.0, 0, 1)

    def _win_honesty(self, text):
        wins = sum(1 for p in self.WIN_CLAIM_PATTERNS if re.search(p, text, re.I))
        losses = sum(1 for p in self.LOSS_ACK_PATTERNS if re.search(p, text, re.I))
        if wins == 0 and losses == 0: return 1.0
        if wins == 0 and losses > 0:  return 1.0
        return clamp(losses / max(wins + losses, 1), 0, 1)

    def _age_factor(self, days_old=30): return clamp(days_old / 90.0, 0.4, 1.0)
    def _peer_sentiment(self): return 0.0

    def compute(self, text, *, recent_loss_ack=0.5, days_active=30):
        ref_inv = 1.0 - self._referral_density(text)
        honesty = self._win_honesty(text)
        age = self._age_factor(days_active)
        peer = self._peer_sentiment()
        consistency = 0.9
        base = (ref_inv * 0.30 + honesty * 0.25 + recent_loss_ack * 0.20
                + consistency * 0.15 + age * 0.05 + ((peer + 1) / 2 * 0.05))
        base = 0.5 * base + 0.5 * 0.7
        p_human = local_discriminator.p_human(text)
        blended = base * (0.55 + 0.45 * p_human)
        return round(clamp(blended, 0, 1), 3)

trust_engine = TrustEngine()

def discriminator_ok(text):
    p = local_discriminator.p_human(text)
    if p < DISCRIMINATOR_THRESHOLD:
        print(f"[L3] rejected (p_human={p:.2f})")
        return False
    return True

# ============================================================================
# SECTION 9 — L4: ENGAGEMENT OPTIMIZER + SWARM CROSS-ENGAGEMENT
# ============================================================================

class EngagementOptimizer:
    def __init__(self): self._lock = threading.Lock()

    def register_watch(self, post_id, real_post_id, posted_at, persona):
        with self._lock:
            state.setdefault("pending_engagement_watches", {})[post_id] = {
                "real_post_id": real_post_id, "posted_at": posted_at,
                "persona": persona, "last_poll": 0, "replies_sent": 0,
                "deadline": posted_at + ENGAGEMENT_REPLY_DEADLINE,
            }
            save_state()

    def tick(self):
        watches = state.get("pending_engagement_watches", {}) or {}
        if not watches: return
        now = now_ts(); expired = []
        for post_id, w in list(watches.items()):
            if now - w["last_poll"] < 60: continue
            w["last_poll"] = now
            try:
                row = _db().execute("""
                    SELECT views, likes, comments, shares FROM posts
                    WHERE post_id=? OR real_post_id=?
                """, (post_id, w.get("real_post_id") or post_id)).fetchone()
                if row:
                    minute = int((now - w["posted_at"]) / 60)
                    if minute <= 60:
                        _db().execute("""
                            INSERT OR REPLACE INTO early_engagement
                            (post_id, minute_mark, views, likes, comments, shares)
                            VALUES (?,?,?,?,?,?)
                        """, (post_id, minute, row["views"] or 0, row["likes"] or 0,
                              row["comments"] or 0, row["shares"] or 0))
            except Exception: pass
            if now >= w["deadline"]: expired.append(post_id)
        for pid in expired: state["pending_engagement_watches"].pop(pid, None)
        if expired: save_state()

    def seed_from_reply_queue(self):
        try:
            rows = _db().execute("""
                SELECT post_id, creator, coin, text_sample, views
                FROM square_feed_posts WHERE views >= 3000
                  AND post_id NOT IN (SELECT post_id FROM reply_targets WHERE replied=1)
                ORDER BY views DESC LIMIT 20
            """).fetchall()
        except Exception: return False
        candidates = [_row_to_dict(r) for r in rows]
        if not candidates: return False
        for c in candidates:
            try:
                if _db().execute("SELECT 1 FROM reply_targets WHERE post_id=?",
                                 (c["post_id"],)).fetchone(): continue
                if not AI_AVAILABLE: return False
                persona = pick_persona("signal")
                prompt = f"""{persona_block(persona)}
Reply to this public Binance Square post by @{c['creator']}:
\"\"\"{c['text_sample'][:500]}\"\"\"

Write ONE short reply (1-2 sentences). Add value — a level, a nuance, a counter-question.
No links. No shilling. Return ONLY the reply text."""
                reply = ai_chat([{"role": "user", "content": prompt}], temperature=0.9, max_tokens=200)
                if not reply: continue
                reply = _strip_markdown(reply).strip().strip('"').strip("'")
                if not reply or len(reply) < 10 or len(reply) > 350: continue
                _db().execute("""
                    INSERT INTO reply_targets
                    (post_id, creator, coin, text_sample, views, reply_drafted, replied, discovered_at)
                    VALUES (?,?,?,?,?,?,0,?)
                    ON CONFLICT(post_id) DO UPDATE SET reply_drafted=excluded.reply_drafted
                """, (c["post_id"], c["creator"], c["coin"], c["text_sample"][:400],
                      c["views"], reply, now_ts()))
                print(f"[REPLY-QUEUE] drafted reply to @{c['creator']} ({c['views']}v)")
                return True
            except Exception as e:
                print(f"[REPLY-QUEUE] {e}")
                continue
        return False

engagement_optimizer = EngagementOptimizer()

def _post_reply_via_browser(target_post_id, reply_text):
    if not _port_open(DEBUG_PORT): return False
    pw = sync_playwright().start()
    try:
        browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{DEBUG_PORT}")
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        page = ctx.new_page()
        page.set_default_timeout(6000)
        url = f"https://www.binance.com/en/square/post/{target_post_id}"
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            humanizer.pause(1.5, 3.0)
        except Exception: pass
        for sel in ['button:has-text("Reply")', 'button:has-text("Comment")',
                    '[data-testid*="reply"]', '[class*="Comment"] button']:
            try:
                loc = page.locator(sel)
                if loc.count() and loc.first.is_visible():
                    humanizer.click(page, loc.first, "reply-open")
                    humanizer.pause(0.8, 1.8)
                    break
            except Exception: continue
        editor = _find_editor(page)
        if editor is None: return False
        humanizer.click(page, editor, "reply-editor")
        humanizer.type_text(page, reply_text)
        humanizer.pause(0.6, 1.4)
        for sel in ['button:has-text("Reply")', 'button:has-text("Send")',
                    'button:has-text("Post")']:
            try:
                loc = page.locator(sel)
                for i in range(loc.count() - 1, -1, -1):
                    el = loc.nth(i)
                    if el.is_visible() and el.is_enabled():
                        humanizer.click(page, el, "reply-submit")
                        humanizer.pause(1.2, 2.4)
                        return True
            except Exception: continue
        return False
    except Exception as e:
        print(f"[L4] reply browser error: {e}")
        return False
    finally:
        try: pw.stop()
        except Exception: pass

class SwarmCrossEngagement:
    """Peers reply to each other's posts with staggered delays."""
    def __init__(self):
        self._lock = threading.Lock()
        self._queue = []
        self._load_queue()

    def _load_queue(self):
        try:
            row = _db().execute("SELECT value FROM learned_weights WHERE key=?",
                                (f"swarm_queue_{SWARM_NODE_ID}",)).fetchone()
            if row:
                self._queue = json.loads(row["value"])
        except Exception: pass

    def _save_queue(self):
        try:
            _db().execute("""INSERT OR REPLACE INTO learned_weights
                (key, value, updated_at) VALUES (?,?,?)""",
                (f"swarm_queue_{SWARM_NODE_ID}",
                 json.dumps(self._queue), now_ts()))
        except Exception: pass

    def register_post(self, post_id, real_post_id, coin, text):
        if not SWARM_ENABLED or not SWARM_PEER_NODES: return
        for peer in SWARM_PEER_NODES:
            delay = random.randint(SWARM_REPLY_MIN_DELAY, SWARM_REPLY_MAX_DELAY)
            self._queue.append({
                "due_ts": now_ts() + delay, "post_id": post_id,
                "real_post_id": real_post_id or post_id, "peer": peer,
                "coin": coin, "source_text": text[:500],
            })
        self._queue = self._queue[-500:]
        self._save_queue()

    def _send_peer_reply(self, entry):
        if not AI_AVAILABLE: return False
        prompt = f"""{persona_block(pick_persona('signal'))}
Peer posted: \"\"\"{entry['source_text'][:400]}\"\"\"
Coin: ${entry['coin']}

Write ONE short reply (1-2 sentences) that:
- Adds a specific technical nuance.
- Asks an open question OR challenges the invalidation.
- Never says "great post", "nice call", "agreed".
Return ONLY the reply text."""
        reply = ai_chat([{"role": "user", "content": prompt}],
                        temperature=0.9, max_tokens=220)
        if not reply: return False
        reply = _strip_markdown(reply).strip().strip('"').strip("'")
        if len(reply) < 12 or len(reply) > 380: return False
        print(f"========== SWARM REPLY -> {entry['peer']} ==========")
        print(reply); print("=" * 55)
        return _post_reply_via_browser(entry["real_post_id"], reply)

    def tick(self):
        if not SWARM_ENABLED or not SWARM_PEER_NODES: return False
        now = now_ts()
        due = [e for e in self._queue if e["due_ts"] <= now]
        if not due: return False
        entry = due[0]
        self._queue.remove(entry)
        try:
            if self._send_peer_reply(entry):
                state["metrics"]["swarm_replies_sent"] = \
                    state["metrics"].get("swarm_replies_sent", 0) + 1
        except Exception as e:
            print(f"[L4] cross reply failed: {e}")
        self._save_queue()
        return True

swarm_cross = SwarmCrossEngagement()

# ============================================================================
# SECTION 10 — L5: LANGUAGE ARBITRAGE (TIMEZONE PEAK)
# ============================================================================

LANG_NAMES = {"vi": "Vietnamese", "ar": "Arabic", "zh": "Simplified Chinese"}
LANG_CULTURE = {
    "vi": "Vietnamese retail traders. Direct, practical, no-hype. Local slang sparingly.",
    "ar": "Arabic-speaking traders. Respectful, slightly formal. Avoid slang.",
    "zh": "Chinese traders. Brevity, precision, clear risk framing.",
}

class LanguageArbitrage:
    @staticmethod
    def _next_peak_ts(lang):
        cfg = LANG_PEAK_TZ.get(lang)
        if not cfg:
            return now_ts() + LANG_DELAY_MINUTES.get(lang, 15) * 60
        try:
            from zoneinfo import ZoneInfo
        except ImportError:
            return now_ts() + LANG_DELAY_MINUTES.get(lang, 15) * 60
        tz = ZoneInfo(cfg["tz"])
        local = datetime.now(tz)
        target_h = None
        for h in cfg["peak_hours"]:
            if h > local.hour:
                target_h = h; break
        if target_h is None:
            target_h = cfg["peak_hours"][0]
            base = (local + timedelta(days=1)).replace(
                hour=target_h, minute=random.randint(0, 55),
                second=random.randint(0, 59), microsecond=0)
        else:
            base = local.replace(hour=target_h,
                                 minute=random.randint(0, 55),
                                 second=random.randint(0, 59),
                                 microsecond=0)
        base += timedelta(minutes=random.randint(-12, 12))
        return int(base.timestamp())

    def schedule_variants(self, source_post_id, source_text, quality):
        if not LANG_ARBITRAGE_ENABLED or quality < 70 or not AI_AVAILABLE: return
        for lang in LANG_TARGETS:
            sched = self._next_peak_ts(lang)
            variant_id = hashlib.sha1(
                f"{source_post_id}:{lang}:{sched}".encode()).hexdigest()[:16]
            try:
                _db().execute("""
                    INSERT OR IGNORE INTO language_variants
                    (variant_id, source_post_id, target_lang, adapted_text, scheduled_at)
                    VALUES (?,?,?,?,?)
                """, (variant_id, source_post_id, lang, "", sched))
            except Exception as e: print(f"[L5] schedule: {e}")

    def _adapt(self, source_text, lang):
        if not AI_AVAILABLE: return None
        culture = LANG_CULTURE.get(lang, "")
        prompt = f"""Rewrite this Binance Square post into {LANG_NAMES[lang]}.
CULTURAL CONTEXT: {culture}
ORIGINAL (English):
\"\"\"{source_text[:1500]}\"\"\"
HARD RULES: Keep every number, cashtag ($XXX) and level exactly. Under 180 words.
Natural human-sounding {LANG_NAMES[lang]}. Return ONLY the translated text."""
        result = ai_chat([{"role": "user", "content": prompt}], temperature=0.85, max_tokens=1000)
        if not result: return None
        return _strip_markdown(result).strip()

    def tick(self):
        if not LANG_ARBITRAGE_ENABLED: return False
        now = now_ts()
        try:
            due = _db().execute("""
                SELECT variant_id, source_post_id, target_lang, adapted_text
                FROM language_variants WHERE posted_at IS NULL AND scheduled_at <= ?
                ORDER BY scheduled_at ASC LIMIT 3
            """, (now,)).fetchall()
        except Exception: return False
        for row in due:
            text = row["adapted_text"] or ""
            if not text:
                text = state["recent_posts"][-1] if state["recent_posts"] else ""
            if not text: continue
            adapted = self._adapt(text, row["target_lang"])
            if not adapted: continue
            q, _ = score_post_quality(adapted)
            if q < 55: continue
            print(f"========== LANG[{row['target_lang']}] q={q} ==========")
            print(adapted[:400]); print("=" * 50)
            ok = send_square(adapted)
            if ok:
                try:
                    _db().execute("""UPDATE language_variants SET adapted_text=?, posted_at=?
                        WHERE variant_id=?""", (adapted, now_ts(), row["variant_id"]))
                except Exception: pass
                state["metrics"]["lang_variants_posted"] = \
                    state["metrics"].get("lang_variants_posted", 0) + 1
                track_post_publish(adapted, "lang_variant", "BTC", q, persona="curator")
                save_state()
                return True
        return False

language_arbitrage = LanguageArbitrage()

# ============================================================================
# SECTION 11 — L6: CONTENT SCHEDULER + ELASTIC LIMITS
# ============================================================================

class ElasticLimits:
    """V30 — modulates caps by VIX + Fear & Greed."""
    def _vix(self):
        try:
            r = session.get("https://query1.finance.yahoo.com/v8/finance/chart/^VIX",
                            timeout=6, headers={"User-Agent": USER_AGENT})
            data = r.json()
            return safe_float(
                data["chart"]["result"][0]["meta"]["regularMarketPrice"])
        except Exception:
            return 20.0

    def multiplier(self):
        if not DYNAMIC_ELASTIC_ENABLED: return 1.0
        vix = self._vix()
        fg  = _sentiment_cache.get("fear_greed", 50)
        vix_factor = clamp(vix / 20.0, 0.55, 1.45)
        fg_extreme  = abs(fg - 50) / 50.0
        fg_factor   = 0.85 + fg_extreme * 0.55
        return round(clamp(vix_factor * fg_factor,
                           ELASTIC_MIN_MULT, ELASTIC_MAX_MULT), 3)

elastic_limits = ElasticLimits()

class ContentScheduler:
    def _today_counters(self):
        today = utc_date()
        if state.get("daily_archetype_date") != today:
            state["daily_archetype_date"] = today
            state["archetype_counter"] = {}
            state["daily_content_counter"] = {}
            state["square_replies_today"] = 0
            save_state()
        return state.setdefault("daily_content_counter", {})

    def _elastic_mult(self):
        key = "elastic_mult_today"
        cached = state.get(key)
        if cached and cached.get("date") == utc_date():
            return cached.get("mult", 1.0)
        m = elastic_limits.multiplier()
        state[key] = {"date": utc_date(), "mult": m}
        save_state()
        return m

    def _within_bounds(self, content_type):
        c = self._today_counters()
        n = c.get(content_type, 0)
        lo, hi = DIVERSIFICATION.get(content_type, (0, 5))
        hi = int(math.ceil(hi * self._elastic_mult()))
        return n < hi

    def plan(self):
        self._today_counters()
        learned = load_learned_weights()
        type_weights = learned.get("types", {}) or {}
        plan = []
        for ct, (lo, hi) in DIVERSIFICATION.items():
            if not self._within_bounds(ct): continue
            base = float(type_weights.get(ct, 1.0))
            nar  = narrative_engine.best_for_content_type(ct)
            nar_boost = 1.0 + (nar["composite_score"] * 0.6) if nar else 1.0
            trust_bias = 1.25 if ct in ("trade_plan","diary","curator","educational") \
                         else 0.80 if ct == "signal" else 1.0
            plan.append((ct, base * nar_boost * trust_bias, nar))
        failed = state.get("failed_publish_cooldown", {}) or {}
        plan = [(ct, w, n) for (ct, w, n) in plan
                if now_ts() - failed.get(ct, 0) > 300]
        plan.sort(key=lambda t: -t[1])
        return plan

content_scheduler = ContentScheduler()

# ============================================================================
# SECTION 12 — HUMANIZER MODULE
# ============================================================================

def _log_human_action(action_id, kind, detail, duration_ms):
    try:
        _db().execute("""INSERT OR REPLACE INTO humanizer_log
            (action_id, kind, detail, ts, duration_ms) VALUES (?,?,?,?,?)""",
            (action_id, kind, detail[:300], now_ts(), int(duration_ms)))
    except Exception: pass

class Humanizer:
    @staticmethod
    def _bezier(x1, y1, x2, y2, n):
        cx = (x1 + x2) / 2 + random.randint(-180, 180)
        cy = (y1 + y2) / 2 + random.randint(-180, 180)
        pts = []
        for i in range(n + 1):
            t = i / n
            x = (1-t)**2 * x1 + 2*(1-t)*t * cx + t**2 * x2
            y = (1-t)**2 * y1 + 2*(1-t)*t * cy + t**2 * y2
            pts.append((x, y))
        return pts

    @staticmethod
    def move_mouse(page, tx, ty, steps=None):
        try:
            cur = page.evaluate("() => ({x: window._hx||200, y: window._hy||200})")
            x1, y1 = cur.get("x", 200), cur.get("y", 200)
        except Exception:
            x1, y1 = random.randint(100, 500), random.randint(100, 400)
        n = steps or random.randint(HUMAN_MOUSE_STEPS_LO, HUMAN_MOUSE_STEPS_HI)
        for x, y in Humanizer._bezier(x1, y1, tx, ty, n):
            try: page.mouse.move(x, y)
            except Exception: pass
            time.sleep(random.uniform(0.006, 0.028))
        try:
            page.evaluate(f"() => {{window._hx={tx}; window._hy={ty};}}")
        except Exception: pass

    @staticmethod
    def click(page, element, label="click"):
        t0 = time.time()
        try:
            box = element.bounding_box()
            if not box:
                element.click(timeout=5000); return True
            tx = box["x"] + box["width"]  * random.uniform(0.28, 0.72)
            ty = box["y"] + box["height"] * random.uniform(0.28, 0.72)
            Humanizer.move_mouse(page, tx, ty)
            time.sleep(random.uniform(*HUMAN_PRE_CLICK_PAUSE))
            try: page.mouse.move(tx + random.uniform(-3, 3), ty + random.uniform(-3, 3))
            except Exception: pass
            time.sleep(random.uniform(0.03, 0.11))
            page.mouse.down()
            time.sleep(random.uniform(0.035, 0.13))
            page.mouse.up()
            _log_human_action(hashlib.sha1(f"{label}:{time.time()}".encode()).hexdigest()[:16],
                              "click", label, (time.time() - t0) * 1000)
            return True
        except Exception as e:
            print(f"[HUMAN] click fallback ({label}): {e}")
            try:
                element.click(timeout=5000); return True
            except Exception: return False

    @staticmethod
    def type_text(page, text):
        t0 = time.time()
        wpm = random.uniform(HUMAN_WPM_LO, HUMAN_WPM_HI)
        base_delay = 60.0 / wpm
        i = 0
        while i < len(text):
            ch = text[i]
            if ch == "\n":
                try: page.keyboard.press("Enter")
                except Exception: pass
                time.sleep(random.uniform(0.05, 0.20))
                i += 1
                continue
            if random.random() < HUMAN_BURST_PAUSE_RATE:
                time.sleep(random.uniform(0.35, 1.30))
            if (random.random() < HUMAN_TYPO_RATE and ch.isalpha()
                    and i < len(text) - 4 and i > 3):
                wrong = random.choice("abcdefghijklmnopqrstuvwxyz")
                try: page.keyboard.type(wrong)
                except Exception: pass
                time.sleep(random.uniform(0.14, 0.42))
                try: page.keyboard.press("Backspace")
                except Exception: pass
                time.sleep(random.uniform(0.09, 0.24))
            try: page.keyboard.type(ch)
            except Exception:
                try: page.keyboard.insert_text(ch)
                except Exception: pass
            d = base_delay * random.uniform(0.55, 1.75)
            if ch in ".,!?;:": d *= 1.4
            if ch == " ":       d *= 0.9
            time.sleep(d)
            i += 1
        _log_human_action(hashlib.sha1(f"type:{t0}".encode()).hexdigest()[:16],
                          "type", f"{len(text)} chars", (time.time() - t0) * 1000)

    @staticmethod
    def pause(lo=0.5, hi=2.0): time.sleep(random.uniform(lo, hi))

    @staticmethod
    def reading_pause(text_len):
        base = clamp(text_len / 260.0, 1.4, 7.5)
        time.sleep(base * random.uniform(0.75, 1.35))

    @staticmethod
    def idle_jitter(page, seconds=2.0):
        end = time.time() + seconds
        try:
            while time.time() < end:
                page.mouse.move(
                    random.randint(200, 1200) + random.uniform(-5, 5),
                    random.randint(150, 700) + random.uniform(-5, 5))
                time.sleep(random.uniform(0.14, 0.48))
        except Exception: pass

    @staticmethod
    def scroll(page, direction="down", amount=None):
        try:
            amt = amount or random.randint(180, 820)
            page.mouse.wheel(0, amt if direction == "down" else -amt)
        except Exception: pass
        time.sleep(random.uniform(0.25, 1.05))

humanizer = Humanizer()

# ============================================================================
# SECTION 13 — BROWSER LAUNCHER
# ============================================================================

SCRIPT_DIR      = Path(__file__).resolve().parent
BROWSER_PROFILE = SCRIPT_DIR / ".superclaw-browser-profile"

BROWSER_CANDIDATES = [
    r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
    r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Microsoft\Edge\Application\msedge.exe"),
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Google\Chrome\Application\chrome.exe"),
    r"C:\Program Files\Chromium\Application\chrome.exe",
    "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "/usr/bin/microsoft-edge", "/usr/bin/microsoft-edge-stable",
    "/usr/bin/google-chrome", "/usr/bin/chromium",
]

def _port_open(port, host="127.0.0.1", timeout=0.6):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(timeout)
    try: s.connect((host, port)); return True
    except Exception: return False
    finally:
        try: s.close()
        except Exception: pass

def _find_browser():
    for p in BROWSER_CANDIDATES:
        if p and Path(p).exists(): return p
    return ""

def _browser_name(path):
    low = path.lower()
    if "edge" in low: return "Edge"
    if "chrome" in low: return "Chrome"
    if "chromium" in low: return "Chromium"
    return "Browser"

def launch_browser_if_needed():
    if _port_open(DEBUG_PORT): return
    browser_path = _find_browser()
    if not browser_path:
        print("[BROWSER] Edge or Chrome not found.")
        raise SystemExit(3)
    name = _browser_name(browser_path)
    is_edge = (name == "Edge")
    BROWSER_PROFILE.mkdir(parents=True, exist_ok=True)
    args = [
        browser_path,
        f"--remote-debugging-port={DEBUG_PORT}",
        f"--user-data-dir={BROWSER_PROFILE}",
        "--no-first-run", "--no-default-browser-check",
        "--disable-background-networking", "--disable-component-update",
        "--disable-default-apps", "--disable-sync", "--no-service-autorun",
        "--password-store=basic", "--use-mock-keychain",
        "--disable-blink-features=AutomationControlled",
        "--exclude-switches=enable-automation",
    ]
    if is_edge:
        args += ["--disable-features=msEdgeSidebarV2,msEdgeShoppingAssistant,"
                 "msEdgeIdentityFeature,msEdgeCollections,msEdgeReadAloud,"
                 "msEdgeWorkspaces,msEdgeBingChat",
                 "--hide-crash-restore-bubble"]
    if RUN_IN_BACKGROUND:
        args += ["--headless=new", f"--window-size={BROWSER_WINDOW_SIZE}"]
        mode = "headless"
    else:
        args += [f"--window-position={BROWSER_WINDOW_POS}",
                 f"--window-size={BROWSER_WINDOW_SIZE}"]
        mode = "visible (offscreen)"
    args.append(SQUARE_URL)
    print(f"[BROWSER] Launching {name} ({mode}): {browser_path}")
    popen_kwargs = {}
    import sys
    if sys.platform == "win32":
        DETACHED_PROCESS        = 0x00000008
        CREATE_NEW_PROCESS_GROUP = 0x00000200
        CREATE_NO_WINDOW        = 0x08000000
        flags = DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP
        if RUN_IN_BACKGROUND: flags |= CREATE_NO_WINDOW
        popen_kwargs["creationflags"] = flags
        si = subprocess.STARTUPINFO()
        si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        si.wShowWindow = 0 if RUN_IN_BACKGROUND else 1
        popen_kwargs["startupinfo"] = si
    else:
        popen_kwargs["start_new_session"] = True
        popen_kwargs["stdin"]  = subprocess.DEVNULL
        popen_kwargs["stdout"] = subprocess.DEVNULL
        popen_kwargs["stderr"] = subprocess.DEVNULL
    try:
        subprocess.Popen(args, **popen_kwargs)
    except Exception as e:
        print(f"[BROWSER] Launch failed: {e}"); return
    deadline = time.time() + 25
    while time.time() < deadline:
        if _port_open(DEBUG_PORT):
            print(f"[BROWSER] {name} DevTools open."); return
        time.sleep(0.4)
    print("[BROWSER] Port never opened.")

EDITOR_SELECTORS = [
    'div.tiptap.ProseMirror[contenteditable="true"]',
    'div.ProseMirror[contenteditable="true"]',
    'div[contenteditable="true"][role="textbox"]',
    'div[contenteditable="true"][data-placeholder]',
    'textarea[placeholder]',
    '[data-testid*="editor"][contenteditable]',
    '[class*="Composer"] [contenteditable]',
    'div[contenteditable="true"]',
]
COMPOSER_OPEN_SELECTORS = [
    'button:has-text("Post")', 'button:has-text("Create")',
    'div[role="button"]:has-text("Post")', 'div[role="button"]:has-text("Create")',
    '[data-testid*="compose"]',
]
PUBLISH_SELECTORS = [
    'button:has-text("Post")', 'button:has-text("Publish")',
    'button:has-text("Share")', 'button:has-text("发布")', 'button:has-text("发帖")',
    '[data-testid*="publish"]', 'button[type="submit"]:has-text("Post")',
]
MODAL_SELECTORS = [
    '[role="dialog"]', '[class*="Modal"]', '[class*="modal"]',
    '[class*="Dialog"]', '[class*="dialog"]',
    '[class*="composer"]', '[class*="Composer"]',
]
PUBLISH_TEXTS = ("post", "publish", "share", "发布", "发帖")
BLOCK_PHRASES = [
    "you are posting too fast", "posting too frequently", "daily post limit",
    "daily limit reached", "rate limit exceeded", "too many requests",
    "please slow down", "complete the captcha", "verify you are human",
    "security verification", "suspicious activity", "account temporarily locked",
    "access denied", "community guidelines violation",
]
CAPTCHA_SELECTORS = [
    'iframe[src*="captcha"]', 'iframe[src*="hcaptcha"]', 'iframe[src*="recaptcha"]',
    'iframe[src*="geetest"]', '[class*="captcha"]', '[id*="captcha"]', '[class*="geetest"]',
]
LOGIN_INDICATORS = [
    'button:has-text("Log In")', 'button:has-text("Sign Up")',
    'a:has-text("Log In")', 'a:has-text("Sign Up")',
]

def _page_text(page):
    try: return page.inner_text("body", timeout=2500).lower()
    except Exception: return ""

def _scan_phrases(text):
    if not text: return None
    for p in BLOCK_PHRASES:
        if p in text: return p
    return None

def _scan_captcha(page):
    for sel in CAPTCHA_SELECTORS:
        try:
            loc = page.locator(sel); n = loc.count()
            for i in range(min(n, 4)):
                if loc.nth(i).is_visible(): return sel
        except Exception: continue
    return None

def _record_health(kind, detail):
    try:
        _db().execute("INSERT OR REPLACE INTO health_events (ts, kind, detail) VALUES (?,?,?)",
                      (now_ts(), kind, detail[:400]))
    except Exception: pass
    m = state.setdefault("metrics", {})
    if kind == "captcha": m["health_captchas"] = m.get("health_captchas", 0) + 1
    if kind == "block":   m["health_blocks"] = m.get("health_blocks", 0) + 1

def detect_blocking(page):
    cap = _scan_captcha(page)
    if cap:
        _record_health("captcha", cap); return f"CAPTCHA ({cap})"
    hit = _scan_phrases(_page_text(page))
    if hit:
        _record_health("block", hit); return f"Blocked: '{hit}'"
    return None

def is_logged_out(page):
    for sel in LOGIN_INDICATORS:
        try:
            loc = page.locator(sel)
            if loc.count() > 0 and loc.first.is_visible(): return True
        except Exception: continue
    return False

def _stdin_tty():
    try:
        import sys; return sys.stdin.isatty()
    except Exception: return False

def pause_and_notify(reason):
    print(f"\n{'='*72}\n[BLOCK] {reason}\n{'='*72}")
    print("Paused. Fix in browser, then press Enter. 'q' to quit.")
    if not _stdin_tty(): raise SystemExit(5)
    while True:
        try: ans = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt): raise SystemExit(0)
        if ans in ("q", "quit", "exit"): raise SystemExit(0)
        return

def _find_editor(page):
    for sel in EDITOR_SELECTORS:
        try:
            loc = page.locator(sel); n = loc.count()
        except Exception: continue
        for i in range(min(n, 5)):
            el = loc.nth(i)
            try:
                if el.is_visible(): return el
            except Exception: continue
    return None

def _try_open_composer(page):
    for sel in COMPOSER_OPEN_SELECTORS:
        try:
            loc = page.locator(sel); n = loc.count()
        except Exception: continue
        for i in range(min(n, 3)):
            el = loc.nth(i)
            try:
                if not el.is_visible(): continue
            except Exception: continue
            if humanizer.click(page, el, "open-composer"):
                time.sleep(random.uniform(1.0, 2.2))
                return True
    return False

def _find_publish_button(page):
    def _ok(el):
        try:
            if not el.is_visible() or not el.is_enabled(): return False
            txt = (el.inner_text() or "").strip().lower()
            if txt and txt not in PUBLISH_TEXTS:
                if not any(txt.startswith(v) for v in PUBLISH_TEXTS): return False
            return True
        except Exception: return False
    for modal_sel in MODAL_SELECTORS:
        try:
            modal = page.locator(modal_sel)
            if modal.count() == 0: continue
            for btn_sel in PUBLISH_SELECTORS:
                try:
                    btn = modal.locator(btn_sel); n = btn.count()
                except Exception: continue
                for i in range(n - 1, -1, -1):
                    if _ok(btn.nth(i)): return btn.nth(i)
        except Exception: continue
    for sel in PUBLISH_SELECTORS:
        try:
            loc = page.locator(sel); n = loc.count()
        except Exception: continue
        for i in range(n - 1, -1, -1):
            if _ok(loc.nth(i)): return loc.nth(i)
    return None

def _open_square_tab(ctx, url):
    for p in ctx.pages:
        try:
            if "binance.com" in (p.url or "") and "/square" in (p.url or ""): return p
        except Exception: continue
    page = ctx.new_page()
    try: page.goto(url, wait_until="domcontentloaded", timeout=30000)
    except Exception: pass
    return page

def _fill_composer(page, text):
    editor = _find_editor(page)
    if editor is None:
        if _try_open_composer(page):
            humanizer.pause(1.5, 3.0)
            editor = _find_editor(page)
    if editor is None: raise RuntimeError("Composer editor not found.")
    humanizer.click(page, editor, "focus-editor")
    humanizer.pause(0.25, 0.75)
    try:
        page.keyboard.press("Control+A")
        humanizer.pause(0.05, 0.18)
        page.keyboard.press("Delete")
    except Exception: pass
    humanizer.pause(0.20, 0.55)
    try: editor.evaluate("el => el.focus()")
    except Exception: pass
    humanizer.type_text(page, text)
    humanizer.pause(0.35, 1.10)
    try:
        editor.evaluate("""el => {
            el.dispatchEvent(new InputEvent('input', {bubbles:true, cancelable:true, inputType:'insertText'}));
            el.dispatchEvent(new Event('change', {bubbles:true}));
        }""")
    except Exception: pass

def _click_publish(page, draft_text=""):
    btn = _find_publish_button(page)
    if btn is None: raise RuntimeError("Publish button not found.")
    if HUMAN_PRE_PUBLISH_READ:
        humanizer.reading_pause(len(draft_text) or 500)
    if HUMAN_SCROLL_AROUND:
        humanizer.scroll(page, "down", random.randint(120, 260))
        humanizer.pause(0.35, 0.95)
        humanizer.scroll(page, "up", random.randint(80, 180))
        humanizer.pause(0.25, 0.70)
    if HUMAN_IDLE_JITTER:
        humanizer.idle_jitter(page, random.uniform(0.6, 1.8))
    btn = _find_publish_button(page)
    if btn is None: raise RuntimeError("Publish button lost after scroll.")
    if not humanizer.click(page, btn, "publish"):
        try: btn.evaluate("el => el.click()")
        except Exception as e:
            raise RuntimeError(f"All publish click strategies failed: {e}")
    time.sleep(random.uniform(2.5, 4.5))

def publish_via_browser(text):
    if not _port_open(DEBUG_PORT):
        print("[BROWSER] Not on debug port — attempting launch.")
        launch_browser_if_needed()
        if not _port_open(DEBUG_PORT):
            print("[BROWSER] Unavailable.")
            return False
    safe = truncate_for_square(text)
    if len(text) > len(safe):
        print(f"[BROWSER] Truncated {len(text)} → {len(safe)}")
    pw = sync_playwright().start()
    try:
        browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{DEBUG_PORT}")
        ctx = browser.contexts[0] if browser.contexts else browser.new_context()
        page = _open_square_tab(ctx, SQUARE_URL)
        try: page.bring_to_front()
        except Exception: pass
        humanizer.pause(0.8, 2.0)
        try:
            humanizer.scroll(page, "down", random.randint(180, 420))
            humanizer.pause(0.4, 1.1)
            humanizer.scroll(page, "up", random.randint(120, 300))
        except Exception: pass
        reason = detect_blocking(page)
        while reason:
            pause_and_notify(reason + " (session start)")
            reason = detect_blocking(page)
        if is_logged_out(page):
            pause_and_notify("Not logged in.")
            page.reload(wait_until="domcontentloaded", timeout=30000)
            humanizer.pause(1.5, 3.0)
        _fill_composer(page, safe)
        humanizer.pause(*HUMAN_POST_TYPING_PAUSE)
        _click_publish(page, draft_text=safe)
        try:
            reason = detect_blocking(page)
            if reason: print(f"[BROWSER] Post-publish warning: {reason}")
        except Exception: pass
        humanizer.pause(1.2, 2.6)
        print("[BROWSER] Posted.")
        state["metrics"]["browser_posts"] = state["metrics"].get("browser_posts", 0) + 1
        return True
    except SystemExit: raise
    except RuntimeError as e:
        print(f"[BROWSER] Failed: {e}")
        state["metrics"]["browser_failures"] = state["metrics"].get("browser_failures", 0) + 1
        return False
    except Exception as e:
        print(f"[BROWSER] Error: {e}")
        state["metrics"]["browser_failures"] = state["metrics"].get("browser_failures", 0) + 1
        return False
    finally:
        try: pw.stop()
        except Exception: pass

# ============================================================================
# SECTION 14 — BINANCE API + INDICATORS
# ============================================================================

_cache_exchange_info = {"ts": 0, "data": None}
_cache_tickers = {"ts": 0, "data": None}
_cache_klines = {}
_klines_lock = threading.Lock()

def binance_get(endpoint, params=None):
    r = session.get(BINANCE_BASE + endpoint, params=params, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

def get_exchange_info():
    if _cache_exchange_info["data"] and time.time() - _cache_exchange_info["ts"] < 1800:
        return _cache_exchange_info["data"]
    _cache_exchange_info["data"] = binance_get("/api/v3/exchangeInfo")
    _cache_exchange_info["ts"] = time.time()
    return _cache_exchange_info["data"]

def get_tickers_24h():
    if _cache_tickers["data"] and time.time() - _cache_tickers["ts"] < 45:
        return _cache_tickers["data"]
    _cache_tickers["data"] = binance_get("/api/v3/ticker/24hr")
    _cache_tickers["ts"] = time.time()
    return _cache_tickers["data"]

def get_price(symbol):
    return safe_float(binance_get("/api/v3/ticker/price", {"symbol": symbol}).get("price"))

def get_klines(symbol, interval, limit=100):
    key = f"{symbol}:{interval}:{limit}"
    now = time.time()
    with _klines_lock:
        hit = _cache_klines.get(key)
        if hit and (now - hit[0]) < 90: return hit[1]
    data = binance_get("/api/v3/klines",
                       {"symbol": symbol, "interval": interval, "limit": limit})
    with _klines_lock:
        _cache_klines[key] = (now, data)
        if len(_cache_klines) > 400:
            for k, _ in sorted(_cache_klines.items(), key=lambda kv: kv[1][0])[:150]:
                _cache_klines.pop(k, None)
    return data

def get_klines_parallel(symbol, frames=(("15m",100),("1h",100),("4h",100))):
    futs = {f: _IO_POOL.submit(get_klines, symbol, f, lim) for f, lim in frames}
    return {f: fut.result() for f, fut in futs.items()}

def ema(values, period):
    if len(values) < period: return []
    m = 2 / (period + 1)
    out = [sum(values[:period]) / period]
    for p in values[period:]: out.append((p - out[-1]) * m + out[-1])
    return out

def calculate_rsi(closes, period=14):
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        ch = closes[i] - closes[i-1]
        gains.append(max(ch, 0)); losses.append(max(-ch, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = ((ag * (period-1)) + gains[i]) / period
        al = ((al * (period-1)) + losses[i]) / period
    if al == 0: return 100.0
    return 100 - (100 / (1 + ag/al))

def calculate_atr(klines, period=14):
    if len(klines) < period + 1: return 0.0
    trs = []
    for i in range(1, len(klines)):
        h = safe_float(klines[i][2]); l = safe_float(klines[i][3])
        pc = safe_float(klines[i-1][4])
        trs.append(max(h-l, abs(h-pc), abs(l-pc)))
    if len(trs) < period: return 0.0
    return sum(trs[-period:]) / period

def analyze_timeframe(klines):
    closes  = [safe_float(x[4]) for x in klines]
    highs   = [safe_float(x[2]) for x in klines]
    lows    = [safe_float(x[3]) for x in klines]
    volumes = [safe_float(x[5]) for x in klines]
    if len(closes) < 55: raise ValueError("Not enough candles")
    e20v = ema(closes, 20); e50v = ema(closes, 50)
    e20 = e20v[-1] if e20v else closes[-1]
    e50 = e50v[-1] if e50v else closes[-1]
    cc = closes[-1]
    rsi = calculate_rsi(closes)
    atr = calculate_atr(klines)
    lb = min(10, len(closes)-1)
    momentum = pct_change(closes[-1-lb], closes[-1])
    win = min(20, len(volumes)-1)
    avg_v = sum(volumes[-1-win:-1]) / win if win > 0 else volumes[-1]
    vol_ratio = volumes[-1] / avg_v if avg_v > 0 else 1.0
    recent_high = max(highs[-20:-1]); recent_low = min(lows[-20:-1])
    breakout = cc > recent_high; breakdown = cc < recent_low
    if cc > e20 > e50:   trend = "bullish"
    elif cc < e20 < e50: trend = "bearish"
    else:                trend = "neutral"
    return {"close": cc, "ema20": e20, "ema50": e50, "rsi": rsi, "atr": atr,
            "momentum": momentum, "volume_ratio": vol_ratio,
            "recent_high": recent_high, "recent_low": recent_low,
            "breakout": breakout, "breakdown": breakdown, "trend": trend}

# ============================================================================
# SECTION 15 — SENTIMENT
# ============================================================================

_sentiment_cache = {"ts": 0, "composite": 0.0, "fear_greed": 50,
                    "funding_rate": 0.0, "long_short_ratio": 1.0, "bucket": "NEUTRAL"}

def sentiment_bucket(x):
    if x >=  40: return "EUPHORIA"
    if x >=  15: return "BULLISH"
    if x <= -40: return "CAPITULATION"
    if x <= -15: return "BEARISH"
    return "NEUTRAL"

def _fetch_fear_greed():
    try:
        r = session.get("https://api.alternative.me/fng/?limit=1", timeout=10)
        if r.status_code == 200: return int(r.json()["data"][0]["value"])
    except Exception: pass
    return 50

def _fetch_funding_rate():
    try:
        r = session.get(f"{BINANCE_FUTURES}/fapi/v1/premiumIndex",
                        params={"symbol": "BTCUSDT"}, timeout=10)
        if r.status_code == 200: return safe_float(r.json().get("lastFundingRate")) * 100
    except Exception: pass
    return 0.0

def _fetch_long_short_ratio():
    try:
        r = session.get(f"{BINANCE_FUTURES}/futures/data/globalLongShortAccountRatio",
                        params={"symbol": "BTCUSDT", "period": "1h", "limit": 1}, timeout=10)
        if r.status_code == 200:
            data = r.json()
            if data: return safe_float(data[0].get("longShortRatio", 1.0))
    except Exception: pass
    return 1.0

def refresh_sentiment():
    now = now_ts()
    if now - _sentiment_cache["ts"] < 600: return _sentiment_cache
    fg = _fetch_fear_greed(); fr = _fetch_funding_rate(); lsr = _fetch_long_short_ratio()
    fg_score   = (fg - 50) * 1.2
    funding_sc = clamp(-fr * 200, -40, 40)
    lsr_sc     = clamp((lsr - 1.0) * 60, -30, 30)
    composite  = clamp(fg_score * 0.45 + funding_sc * 0.30 + lsr_sc * 0.25, -100, 100)
    _sentiment_cache.update({"ts": now, "composite": round(composite, 2),
                             "fear_greed": fg, "funding_rate": round(fr, 4),
                             "long_short_ratio": round(lsr, 3),
                             "bucket": sentiment_bucket(composite)})
    try:
        _db().execute("""INSERT OR REPLACE INTO sentiment_history
            (ts, fear_greed, funding_rate, long_short_ratio, news_sentiment, composite, bucket)
            VALUES (?,?,?,?,?,?,?)""",
            (now, fg, fr, lsr, 0, composite, sentiment_bucket(composite)))
    except Exception: pass
    return _sentiment_cache

# ============================================================================
# SECTION 16 — TRENDING + FEED LEARNER
# ============================================================================

def _looks_like_coin(s):
    return (isinstance(s, str) and 2 <= len(s) <= 12 and s.isalnum()
            and s.upper() == s and not s.isdigit() and s.upper() not in STABLE_BASES)

def _normalize_to_coin(s):
    if not isinstance(s, str): return None
    s = s.strip().upper()
    if s.endswith("USDT"): s = s[:-4]
    if s in STABLE_BASES: return None
    return s if _looks_like_coin(s) else None

def _extract_coins_from_json(payload):
    found = set()
    def _walk(node):
        if isinstance(node, dict):
            for k in ("symbol","tokenSymbol","token","asset","code","ticker","baseAsset","coin"):
                v = node.get(k)
                if isinstance(v, str):
                    c = _normalize_to_coin(v)
                    if c: found.add(c)
            for v in node.values(): _walk(v)
        elif isinstance(node, list):
            for i in node: _walk(i)
    _walk(payload)
    return found

TRENDING_URLS = ["https://www.binance.com/bapi/composite/v1/public/pgc/trending/token"]

class TrendingHijacker:
    def __init__(self):
        self._lock = threading.Lock()
        self._cache = {"ts": 0, "coins": [], "scores": {}}

    def refresh(self, force=False):
        now = now_ts()
        with self._lock:
            if not force and now - self._cache["ts"] < 300: return self._cache
        coins = {}
        try:
            for url in TRENDING_URLS:
                r = session.get(url, timeout=8, headers={"clienttype": "web"})
                if r.status_code != 200: continue
                for c in _extract_coins_from_json(r.json()): coins[c] = coins.get(c, 0) + 1.0
        except Exception: pass
        try:
            for t in get_tickers_24h():
                sym = t.get("symbol", "")
                if not sym.endswith("USDT"): continue
                c = sym[:-4]
                if c in STABLE_BASES: continue
                qv = safe_float(t.get("quoteVolume")); chg = abs(safe_float(t.get("priceChangePercent")))
                if qv < 3_000_000: continue
                coins[c] = coins.get(c, 0) + (qv / 1e8) * 0.4 + (chg / 20) * 0.6
        except Exception: pass
        try:
            for c, n in (feed_learner._cache.get("coins") or {}).items():
                coins[c] = coins.get(c, 0) + n * 0.6
        except Exception: pass
        ranked = sorted(coins.items(), key=lambda kv: -kv[1])[:40]
        with self._lock:
            self._cache = {"ts": now, "coins": [c for c, _ in ranked], "scores": dict(ranked)}
        return self._cache

    def top(self, n=10): return self._cache["coins"][:n]
    def is_hot(self, coin): return coin in self._cache["coins"][:15]

trending_hijacker = TrendingHijacker()

def _parse_post_card(txt):
    def _find(pats):
        for p in pats:
            m = re.search(p, txt, re.I)
            if m: return parse_compact_number(m.group(1))
        return 0
    views    = _find([r"([\d,.KMB]+)\s*(?:views?|impressions?)"])
    likes    = _find([r"([\d,.KMB]+)\s*(?:likes?)"])
    comments = _find([r"([\d,.KMB]+)\s*(?:comments?|replies)"])
    shares   = _find([r"([\d,.KMB]+)\s*(?:shares?|reposts?|retweets?)"])
    if not views:
        for line in reversed([l.strip() for l in txt.split("\n") if l.strip()]):
            if line in ("View More", "View Less"): continue
            if re.fullmatch(r"[-+]\d+(?:\.\d+)?%", line): continue
            m = re.fullmatch(r"([\d,.]+[KMB]?)", line)
            if m: views = parse_compact_number(m.group(1)); break
    return {"views": views, "likes": likes, "comments": comments, "shares": shares}

class SquareFeedLearner:
    def __init__(self):
        self._lock = threading.Lock()
        self._cache = {"ts": 0, "posts": [], "creators": {}, "coins": {}, "hooks": []}

    def _extract_post(self, card):
        try: txt = card.inner_text(timeout=700)
        except Exception: return None
        if not txt or len(txt) < 30: return None
        post_id = None
        try:
            loc = card.locator("[data-id]")
            if loc.count(): post_id = loc.first.get_attribute("data-id", timeout=300)
        except Exception: pass
        if not post_id:
            try:
                href = card.locator("a[href*='/square/post/']").first.get_attribute("href", timeout=300)
                if href:
                    m = re.search(r"/square/post/(\d+)", href)
                    if m: post_id = m.group(1)
            except Exception: pass
        if not post_id: post_id = hashlib.sha1(txt[:200].encode()).hexdigest()[:16]
        creator = ""; creator_url = ""
        try:
            cloc = card.locator("a[href*='/square/profile/']").first
            if cloc.count():
                creator = (cloc.inner_text(timeout=300) or "").strip()
                creator_url = cloc.get_attribute("href", timeout=300) or ""
        except Exception: pass
        m = _CASHTAG_RE.search(txt)
        coin = m.group(1) if m else ""
        stats = _parse_post_card(txt)
        lines = [l.strip() for l in txt.split("\n") if l.strip()]
        hook = ""
        for line in lines[:6]:
            if len(line) < 12: continue
            if re.fullmatch(r"[\d,.]+[KMB]?%?", line): continue
            if line in ("View More", "View Less"): continue
            if line.startswith("@") and len(line) < 30: continue
            hook = line; break
        return {"post_id": post_id, "creator": creator or "unknown",
                "creator_url": creator_url, "coin": coin or "UNKNOWN",
                "text_sample": txt[:900], "hook_sample": hook[:200],
                "views": stats["views"], "likes": stats["likes"],
                "comments": stats["comments"], "shares": stats["shares"], "age_min": 99999}

    def scan(self, scrolls=6):
        if not _port_open(DEBUG_PORT): return self._cache
        now = now_ts()
        with self._lock:
            if now - self._cache["ts"] < 900: return self._cache
        pw = sync_playwright().start()
        try:
            browser = pw.chromium.connect_over_cdp(f"http://127.0.0.1:{DEBUG_PORT}")
            ctx = browser.contexts[0] if browser.contexts else browser.new_context()
            page = ctx.new_page()
            page.set_default_timeout(2500)
            try:
                page.goto("https://www.binance.com/en/square", wait_until="domcontentloaded", timeout=30000)
                page.wait_for_timeout(4500)
                for _ in range(scrolls):
                    humanizer.scroll(page, "down", random.randint(2400, 3800))
                    time.sleep(random.uniform(0.7, 1.6))
                cards = page.locator("[data-feed-card-index]").all()
                posts = []; c = _db()
                for card in cards[:80]:
                    p = self._extract_post(card)
                    if not p or p["views"] < 500: continue
                    posts.append(p)
                    try:
                        c.execute("""INSERT OR REPLACE INTO square_feed_posts
                            (post_id, creator, creator_url, coin, text_sample, hook_sample,
                             views, likes, comments, shares, age_min, scraped_at)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                            (p["post_id"], p["creator"], p["creator_url"], p["coin"],
                             p["text_sample"], p["hook_sample"], p["views"],
                             p["likes"], p["comments"], p["shares"], p["age_min"], now))
                    except Exception: continue
                coins = Counter()
                for p in posts:
                    if p["coin"] and p["coin"] != "UNKNOWN": coins[p["coin"]] += 1
                top_hooks = [p for p in posts if p["views"] >= 2000]
                top_hooks.sort(key=lambda x: -x["views"])
                with self._lock:
                    self._cache = {"ts": now, "posts": posts, "creators": {},
                                   "coins": dict(coins.most_common(50)), "hooks": top_hooks[:40]}
                state["metrics"]["feed_scans"] = state["metrics"].get("feed_scans", 0) + 1
                print(f"[FEED] {len(posts)} posts")
            finally: page.close()
        except Exception as e:
            print(f"[FEED] scan failed: {e}")
        finally:
            try: pw.stop()
            except Exception: pass
        return self._cache

    def top_hooks(self, limit=20): return [p["hook_sample"] for p in self._cache["hooks"][:limit]]
    def top_coins(self, limit=15): return list(self._cache["coins"].keys())[:limit]

feed_learner = SquareFeedLearner()

# ============================================================================
# SECTION 17 — PERSONAS + QUALITY
# ============================================================================

def pick_persona(content_type, avoid_recent=True):
    pool = PERSONA_ROUTING.get(content_type, ["trader"])
    if avoid_recent:
        recent = state.get("recent_personas", [])[-3:]
        filtered = [p for p in pool if p not in recent]
        pool = filtered or pool
    weights = load_learned_weights().get("personas", {})
    scored = [(p, clamp(float(weights.get(p, 1.0)), 0.5, 2.0)) for p in pool]
    total = sum(w for _, w in scored) or 1.0
    r = random.random() * total; acc = 0.0
    for p, w in scored:
        acc += w
        if r <= acc: return p
    return pool[0]

def persona_block(persona_key):
    p = PERSONAS.get(persona_key) or PERSONAS["trader"]
    return f"PERSONA: {p['name']}\nVOICE: {p['voice']}\n"

def remember_persona(p):
    lst = state.setdefault("recent_personas", [])
    lst.append(p); state["recent_personas"] = lst[-20:]

PERSONA_RULES = """VOICE RULES:
- Use "I" statements. Reference specific observations.
- Short sentences. Vary rhythm.
- Never start with "Watching", "Looking at", "Here's what I see".
- No corporate speak. No "delve", "leverage", "harness".
- If the setup is bad, say so plainly.
- End with a question a real trader would ask.
"""

def score_post_quality(text, signal=None):
    score = 0; low = text.lower(); words = text.split(); wc = len(words)
    if IDEAL_POST_LENGTH_MIN <= wc <= IDEAL_POST_LENGTH_MAX: score += 15
    cashtags = _CASHTAG_RE.findall(text)
    if cashtags:
        score += 12
        if len(cashtags) >= 2: score += 5
    real_prices = [p for p in re.findall(r"\b\d{1,6}(?:\.\d+)?\b", text)
                   if len(p) >= 3 and "." in p]
    if len(real_prices) >= 2: score += 15
    elif real_prices: score += 6
    if re.search(r"\bTP[123]\b", text) and re.search(r"\bSL\b|\bStop\b", text, re.I): score += 18
    if "?" in text:
        score += 15
        if re.search(r"\b(you|your|you're)\b", text, re.I): score += 5
    if re.search(r"\b(long|short|bullish|bearish|breaking|reclaiming|failing)\b", low): score += 10
    tech = ("support","resistance","breakout","breakdown","reclaim","rejected",
            "wick","volume","candle","structure","range","invalidation","trigger")
    if sum(1 for w in tech if w in low) >= 2: score += 10
    for t in BANNED_AI_TELLS:
        if t in low: score -= 15
    for p in FORBIDDEN_GENERIC_PHRASES:
        if p in low: score -= 12
    if len(_EMOJI_RE.findall(text)) > 4: score -= 8
    if sum(1 for c in text if c.isupper()) / max(len(text), 1) > 0.4: score -= 10
    if re.search(r"https?://", text): score -= 20
    return clamp(int(score), 0, 100), []

def normalize_text(text):
    text = text.lower()
    text = re.sub(r"https?://\S+", "", text)
    text = re.sub(r"\$[a-z]{2,10}", "$coin", text)
    text = re.sub(r"\b\d+(?:\.\d+)?%?", "#", text)
    text = re.sub(r"[^a-z0-9$#\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def originality_signature(text):
    n = normalize_text(text)
    skip = {"the","a","an","this","that","and","or","is","are","to","for",
            "of","on","from","with","in","as","at"}
    words = [w for w in n.split()
             if w not in TEMPLATE_WORDS and w not in skip and w not in {"$coin","#"}]
    return " ".join(words) if len(words) >= 3 else n

def content_hash(text):
    return hashlib.sha256(originality_signature(text).encode()).hexdigest()

def opening_text(text):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    return " ".join(lines[:2]) if lines else ""

def passes_originality(text, verbose=True):
    low = text.lower()
    for p in FORBIDDEN_GENERIC_PHRASES:
        if p in low:
            if verbose: print(f"[ORIG] Forbidden: {p}")
            return False
    for t in BANNED_AI_TELLS:
        if t in low:
            if verbose: print(f"[ORIG] AI tell: {t}")
            return False
    cur = content_hash(text)
    for old in state["recent_posts"]:
        if content_hash(old) == cur:
            if verbose: print("[ORIG] Exact dup.")
            return False
    cur_op = opening_text(text)
    for old in state["recent_posts"]:
        old_op = opening_text(old)
        if not old_op: continue
        if SequenceMatcher(None, cur_op, old_op).ratio() >= 0.86:
            if verbose: print("[ORIG] Similar opening.")
            return False
    return True

def remember_post(text, hook=None):
    state["recent_posts"].append(text)
    state["recent_posts"] = state["recent_posts"][-100:]
    state["recent_openings"].append(opening_text(text))
    state["recent_openings"] = state["recent_openings"][-100:]
    if hook:
        state["recent_hooks"].append(hook)
        state["recent_hooks"] = state["recent_hooks"][-80:]

def _ensure_cashtag(text, cashtag, coin):
    if cashtag in text: return text
    if re.search(rf"\b{re.escape(coin)}\b", text):
        return re.sub(rf"\b{re.escape(coin)}\b", cashtag, text)
    titled = coin.capitalize()
    if re.search(rf"\b{re.escape(titled)}\b", text):
        return re.sub(rf"\b{re.escape(titled)}\b", cashtag, text)
    return f"{cashtag} — {text.lstrip()}"

def _fix_levels_formatting(text, signal):
    el, eh = format_price(signal["entry_low"]), format_price(signal["entry_high"])
    tp1, tp2, tp3 = (format_price(signal["tp1"]), format_price(signal["tp2"]),
                     format_price(signal["tp3"]))
    sl = format_price(signal["sl"])
    if re.search(r"^\s*TP1\s*:", text, flags=re.MULTILINE): return text
    lines = text.split("\n"); out = []
    for line in lines:
        markers = sum((bool(re.search(r"\bEP\b|\bEntry\b", line, re.I)),
                       bool(re.search(r"\bTP1\b|\bTarget 1\b", line, re.I)),
                       bool(re.search(r"\bTP2\b|\bTarget 2\b", line, re.I)),
                       bool(re.search(r"\bTP3\b|\bTarget 3\b", line, re.I)),
                       bool(re.search(r"\bSL\b|\bStop\b", line, re.I))))
        if markers >= 3:
            out.extend((f"EP: {el} – {eh}", f"TP1: {tp1}", f"TP2: {tp2}",
                        f"TP3: {tp3}", f"SL: {sl}"))
        else: out.append(line)
    return "\n".join(out)

def _fix_article_formatting(text):
    text = text.strip()
    if not text: return text
    blocks = [b.strip() for b in re.split(r"\n\s*\n", text) if b.strip()]
    if len(blocks) >= 3: return "\n\n".join(blocks)
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+(?=[A-Z$])", text) if s.strip()]
    if len(sentences) < 4: return text
    n = len(sentences); target = 4
    base = n // target; extra = n % target
    paragraphs = []; idx = 0
    for i in range(target):
        size = base + (1 if i < extra else 0)
        paragraphs.append(" ".join(sentences[idx:idx+size]).strip())
        idx += size
    if idx < n: paragraphs[-1] += " " + " ".join(sentences[idx:])
    return "\n\n".join(p for p in paragraphs if p)

# ============================================================================
# SECTION 18 — LEARNED WEIGHTS
# ============================================================================

def _persist_weights(d):
    try:
        c = _db()
        c.executemany("""INSERT INTO learned_weights (key, value, updated_at) VALUES (?,?,?)
                         ON CONFLICT(key) DO UPDATE SET value=excluded.value,
                                                        updated_at=excluded.updated_at""",
                      [(k, json.dumps(v) if not isinstance(v, (int, float)) else str(v),
                        now_ts()) for k, v in d.items()])
    except Exception as e: print(f"[LEARN] persist: {e}")

def load_learned_weights():
    cached = state.get("learned_weights")
    if cached and now_ts() - cached.get("learned_at", 0) < 3600: return cached
    try:
        rows = _db().execute("SELECT key, value FROM learned_weights").fetchall()
        out = {}
        for r in rows:
            try: out[r["key"]] = json.loads(r["value"])
            except Exception: out[r["key"]] = r["value"]
        return out
    except Exception: return {}

def type_weight(t): return float(load_learned_weights().get("types", {}).get(t, 1.0))

def _bucketize(rows, key_fn):
    s, c = defaultdict(float), defaultdict(int)
    for r in rows:
        k = key_fn(r)
        if k is None: continue
        s[k] += (r["engagement_score"] or 0); c[k] += 1
    return s, c

def _weights(s, c):
    avg = {k: s[k] / max(c[k], 1) for k in s}
    base = (sum(avg.values()) / len(avg)) if avg else 1.0
    return {k: round(v / base, 3) for k, v in avg.items()}

def learn_from_analytics(lookback_days=14):
    cutoff = now_ts() - lookback_days * 86400
    try:
        rows = _db().execute("""
            SELECT content_type, coin, posted_hour_utc, hook, persona,
                   engagement_score, views, quality_score, archetype
            FROM posts WHERE posted_at > ? AND views > 0
        """, (cutoff,)).fetchall()
    except Exception: rows = []
    eng_rows = [r for r in rows if (r["engagement_score"] or 0) > 0]
    if len(eng_rows) >= 15:
        ts_, tc_ = _bucketize(eng_rows, lambda r: r["content_type"] or "unknown")
        cs_, cc_ = _bucketize(eng_rows, lambda r: r["coin"] or "UNKNOWN")
        hs_, hc_ = _bucketize(eng_rows, lambda r: r["posted_hour_utc"])
        ps_, pc_ = _bucketize(eng_rows, lambda r: r["persona"] or None)
        ars_, arc_ = _bucketize(eng_rows, lambda r: r["archetype"] or None)
        learned = {"types": _weights(ts_, tc_), "coins": _weights(cs_, cc_),
                   "hours": _weights(hs_, hc_), "personas": _weights(ps_, pc_),
                   "archetypes": _weights(ars_, arc_),
                   "sample_size": len(eng_rows), "learned_at": now_ts()}
    else:
        learned = {"types": {}, "coins": {}, "hours": {}, "personas": {},
                   "archetypes": {}, "sample_size": len(eng_rows),
                   "learned_at": now_ts()}
    _persist_weights(learned)
    print(f"[LEARN] n={len(rows)} eng_n={len(eng_rows)}")
    return learned

# ============================================================================
# SECTION 19 — RAG FEW-SHOT INJECTION + EMBEDDING DEDUP
# ============================================================================

def build_rag_fewshot_block(content_type=None, coin=None, n=RAG_TOP_N):
    cutoff = now_ts() - RAG_LOOKBACK_HOURS * 3600
    try:
        if content_type and coin:
            rows = _db().execute("""
                SELECT coin, content_type, hook, persona, archetype,
                       engagement_score, views, likes FROM posts
                WHERE posted_at > ? AND content_type = ? AND coin = ?
                ORDER BY engagement_score DESC LIMIT ?""",
                (cutoff, content_type, coin, n)).fetchall()
        elif content_type:
            rows = _db().execute("""
                SELECT coin, content_type, hook, persona, archetype,
                       engagement_score, views, likes FROM posts
                WHERE posted_at > ? AND content_type = ?
                ORDER BY engagement_score DESC LIMIT ?""",
                (cutoff, content_type, n)).fetchall()
        else:
            rows = _db().execute("""
                SELECT coin, content_type, hook, persona, archetype,
                       engagement_score, views, likes FROM posts
                WHERE posted_at > ? ORDER BY engagement_score DESC LIMIT ?""",
                (cutoff, n)).fetchall()
    except Exception:
        return ""
    if not rows: return ""
    lines = ["\n═══ TOP-PERFORMING REFERENCE POSTS (RAG, last 48h) ═══"]
    for r in rows:
        lines.append(
            f"— [{r['content_type']} ${r['coin']} | "
            f"eng={r['engagement_score']:.0f} views={r['views']} "
            f"likes={r['likes']} persona={r['persona']} archetype={r['archetype']}]")
        if r["hook"]: lines.append(f"    \"{r['hook'][:260]}\"")
    lines.append("═══ Emulate STRUCTURE and RHYTHM, never copy verbatim. ═══\n")
    return "\n".join(lines)

class EmbeddingDeduplicator:
    """V30 — Contextual Deduplication via embeddings (cosine ≥ 0.85)."""
    THRESHOLD = 0.85
    LOOKBACK_DAYS = 14

    def __init__(self):
        self._lock = threading.Lock()
        self._model = None
        self._model_loaded = False

    def _get_model(self):
        if self._model_loaded: return self._model
        self._model_loaded = True
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer("all-MiniLM-L6-v2")
            print("[EMBED] all-MiniLM-L6-v2 loaded.")
        except Exception as e:
            print(f"[EMBED] model unavailable ({e}); falling back to hash.")
            self._model = None
        return self._model

    def _fallback_embedding(self, text):
        # Cheap hashed bag-of-words vector as a fallback
        import math
        vec = [0.0] * 128
        words = re.findall(r"[a-z]{2,}", text.lower())
        for w in words:
            h = int(hashlib.md5(w.encode()).hexdigest(), 16) % 128
            vec[h] += 1.0
        norm = math.sqrt(sum(v*v for v in vec)) or 1.0
        return [v / norm for v in vec]

    def _embed(self, text):
        m = self._get_model()
        if m is None:
            return self._fallback_embedding(text)
        try:
            return m.encode(text, normalize_embeddings=True).tolist()
        except Exception:
            return self._fallback_embedding(text)

    @staticmethod
    def _cos(a, b):
        if not a or not b: return 0.0
        dot = sum(x*y for x, y in zip(a, b))
        na = math.sqrt(sum(x*x for x in a)) or 1.0
        nb = math.sqrt(sum(x*x for x in b)) or 1.0
        return dot / (na * nb)

    def check(self, text, verbose=True):
        """Returns True if text is unique (safe to post), False if too similar."""
        cur_emb = self._embed(text)
        cutoff = now_ts() - self.LOOKBACK_DAYS * 86400
        try:
            rows = _db().execute("""
                SELECT embedding FROM text_embeddings WHERE created_at > ?
            """, (cutoff,)).fetchall()
        except Exception:
            rows = []
        for r in rows:
            try: prev = json.loads(r["embedding"])
            except Exception: continue
            sim = self._cos(cur_emb, prev)
            if sim >= self.THRESHOLD:
                if verbose: print(f"[EMBED] Rejected sim={sim:.3f}")
                return False
        return True

    def store(self, post_id, text):
        try:
            emb = self._embed(text)
            h = hashlib.sha1(text.encode()).hexdigest()[:16]
            _db().execute("""INSERT OR REPLACE INTO text_embeddings
                (post_id, text_hash, embedding, created_at) VALUES (?,?,?,?)""",
                (post_id, h, json.dumps(emb), now_ts()))
        except Exception as e:
            print(f"[EMBED] store failed: {e}")

embedding_dedup = EmbeddingDeduplicator()

# ============================================================================
# SECTION 20 — A/B TESTING
# ============================================================================

class ABTester:
    HOOK_VARIANTS = ["question", "stat", "bold_claim", "contrarian", "curiosity"]
    CTA_VARIANTS  = ["question_back", "poll_prompt", "level_challenge", "call_to_watch"]
    FMT_VARIANTS  = ["short_paragraphs", "bullet_stack", "single_flow"]

    def __init__(self):
        self._lock = threading.Lock()

    @staticmethod
    def _engagement(views, likes, comments, shares):
        return (views    * AB_ENGAGEMENT_VIEWS_W
              + likes    * AB_ENGAGEMENT_LIKES_W
              + comments * AB_ENGAGEMENT_COMMENTS_W
              + shares   * AB_ENGAGEMENT_SHARES_W)

    def choose_variant(self, content_type, coin):
        """Return a bucket label. Simple alternating assignment until we
        have enough samples."""
        try:
            row = _db().execute("""
                SELECT COUNT(*) AS n FROM posts WHERE content_type=? AND coin=?
            """, (content_type, coin)).fetchone()
            n = (row["n"] if row else 0) or 0
        except Exception:
            n = 0
        buckets = self.HOOK_VARIANTS
        return buckets[n % len(buckets)]

    def record(self, post_id, content_type, coin, bucket):
        try:
            test_id = f"{content_type}:{coin}:{bucket}"
            _db().execute("""INSERT OR IGNORE INTO ab_tests
                (test_id, created_at, content_type, coin, variant_a, variant_b)
                VALUES (?,?,?,?,?,?)""",
                (test_id, now_ts(), content_type, coin, bucket, "control"))
        except Exception: pass

    def decide_winners(self):
        """Compare A/B groups via engagement score."""
        try:
            rows = _db().execute("""
                SELECT content_type, ab_group,
                       AVG(engagement_score) AS avg_eng,
                       COUNT(*) AS n
                FROM posts
                WHERE engagement_score > 0 AND ab_group != ''
                GROUP BY content_type, ab_group
            """).fetchall()
        except Exception:
            return
        by_ct = defaultdict(list)
        for r in rows:
            by_ct[r["content_type"]].append(_row_to_dict(r))
        learned = load_learned_weights()
        hook_w = learned.setdefault("ab_hooks", {})
        for ct, groups in by_ct.items():
            if len(groups) < 2: continue
            groups.sort(key=lambda g: -(g["avg_eng"] or 0))
            winner, loser = groups[0], groups[-1]
            if (winner["n"] or 0) >= 5 and (loser["n"] or 0) >= 5:
                hook_w[winner["ab_group"]] = \
                    round((winner["avg_eng"] or 1) / max(loser["avg_eng"] or 1, 1), 3)
                state["metrics"]["ab_tests_decided"] = \
                    state["metrics"].get("ab_tests_decided", 0) + 1
                print(f"[AB] {ct}: {winner['ab_group']} beats {loser['ab_group']}")
        _persist_weights(learned)

ab_tester = ABTester()

# ============================================================================
# SECTION 21 — BAYESIAN HYPERPARAMETER TUNING
# ============================================================================

class BayesianTuner:
    """Simple 1-D Bayesian-style tuner using a Beta posterior over a discrete
    grid of candidate values. Good enough for threshold selection."""
    def __init__(self):
        self._lock = threading.Lock()

    def tune_signal_threshold(self):
        try:
            from scipy.stats import beta as _beta
        except ImportError:
            _beta = None
        candidates = list(range(50, 90, 2))
        # Alpha/Beta counts from prior posts bucketed by signal_threshold
        counts = {c: {"a": 1.0, "b": 1.0} for c in candidates}
        try:
            rows = _db().execute("""
                SELECT quality_score, engagement_score FROM posts
                WHERE posted_at > ? AND views > 0 AND content_type='signal'
            """, (now_ts() - 30 * 86400,)).fetchall()
        except Exception:
            rows = []
        for r in rows:
            q = (r["quality_score"] or 0)
            # Nearest candidate threshold
            best_c = min(candidates, key=lambda c: abs(c - q))
            if (r["engagement_score"] or 0) >= 200:
                counts[best_c]["a"] += 1
            else:
                counts[best_c]["b"] += 1
        best_c, best_mean = None, -1
        for c in candidates:
            a = counts[c]["a"]; b = counts[c]["b"]
            m = a / (a + b)
            if m > best_mean:
                best_c, best_mean = c, m
        tune = state.setdefault("auto_tune", {})
        old = tune.get("signal_threshold", SIGNAL_THRESHOLD)
        tune["signal_threshold"] = best_c
        print(f"[BAYES] signal_threshold {old} → {best_c} (posterior mean={best_mean:.3f})")
        return best_c

bayes_tuner = BayesianTuner()

# ============================================================================
# SECTION 22 — AI GENERATION
# ============================================================================

def _best_posts_block(content_type=None, limit=3):
    try:
        rows = _db().execute("""
            SELECT coin, content_type, views, hook FROM posts
            WHERE views >= ? AND posted_at > ?
            ORDER BY views DESC LIMIT ?
        """, (TARGET_VIEWS, now_ts() - 21 * 86400, limit)).fetchall()
    except Exception: return ""
    if not rows: return ""
    lines = ["\nHIGH-VIEW EXAMPLES:"]
    for b in rows:
        lines.append(f"  · [${b['coin']} {b['content_type']} {b['views']}v] {str(b['hook'] or '')[:80]}")
    return "\n".join(lines) + "\n"

def generate_signal_post(signal, persona, archetype):
    if not AI_AVAILABLE: return _fallback_signal(signal, archetype)
    tag = f"${signal['coin']}"
    reasons = "\n".join(f"- {r}" for r in signal["reasons"]) or "- (none)"
    prompt = f"""{persona_block(persona)}{PERSONA_RULES}
Write ONE Binance Square post. ARCHETYPE = {archetype}.
Coin: {tag} | Direction: {signal['direction']}
Entry: {format_price(signal['entry_low'])} – {format_price(signal['entry_high'])}
TP1: {format_price(signal['tp1'])} | TP2: {format_price(signal['tp2'])} | TP3: {format_price(signal['tp3'])}
SL: {format_price(signal['sl'])}
WHY: {reasons}
STRUCTURE: 4 short paragraphs. P1 hooks with {tag}. P2 structure. P3 levels. P4 question.
HARD: Plain text, no markdown, {tag} in P1+P2, 90-160 words. Return ONLY the post.
{_best_posts_block("signal", 2)}
{build_rag_fewshot_block(content_type="signal", coin=signal['coin'])}"""
    result = ai_chat([{"role": "system", "content":
        f"Real crypto trader ({PERSONAS[persona]['name']}). No chatbot voice. No markdown."},
        {"role": "user", "content": prompt}], temperature=0.95, max_tokens=1500)
    if not result: return _fallback_signal(signal, archetype)
    result = re.sub(r"^```(?:text)?", "", result, flags=re.I)
    result = re.sub(r"```$", "", result).strip()
    result = _strip_markdown(result)
    result = _fix_levels_formatting(result, signal)
    result = _ensure_cashtag(result, tag, signal["coin"])
    return result

def _fallback_signal(signal, archetype):
    tag = f"${signal['coin']}"
    em = (signal["entry_low"] + signal["entry_high"]) / 2
    return (f"{tag} {signal['direction']} setup forming.\n\n"
            f"{tag} at {format_price(em)}.\n\n"
            f"EP: {format_price(signal['entry_low'])} – {format_price(signal['entry_high'])}\n"
            f"TP1: {format_price(signal['tp1'])}\nTP2: {format_price(signal['tp2'])}\n"
            f"TP3: {format_price(signal['tp3'])}\nSL: {format_price(signal['sl'])}\n\n"
            f"If {format_price(signal['sl'])} fails, I'm out. Are you taking this?")

def generate_news_post(item, persona, narrative=None):
    if not AI_AVAILABLE: return None
    tag = f"${item['coins'][0]}" if item.get("coins") else "$BTC"
    prompt = f"""{persona_block(persona)}{PERSONA_RULES}
BREAKING — react in 60-110 words.
HEADLINE: {item['title']}
COIN: {tag} | CATEGORY: {item['category']}
P1 — Fact with {tag}. P2 — Immediate 4H read. P3 — ONE question.
HARD: 60-110 words, {tag} twice, plain text. Return ONLY the post.
{build_rag_fewshot_block(content_type="news", coin=item.get('coins', ['BTC'])[0])}"""
    post = ai_chat([{"role": "system", "content": "Fast crypto news reaction. No hype."},
                    {"role": "user", "content": prompt}], temperature=0.85, max_tokens=800)
    if not post: return None
    post = _strip_markdown(re.sub(r"^```(?:text)?|```$", "", post).strip())
    return _ensure_cashtag(post, tag, item["coins"][0] if item.get("coins") else "BTC")

def generate_diary_post(recent_pnl=-5478.65, coin="ETH"):
    if not AI_AVAILABLE: return None
    tag = f"${coin}"
    prompt = f"""{persona_block('diarist')}{PERSONA_RULES}
Write ONE post as a retail trader DOWN on a position.
Position: {tag} short | PNL: {recent_pnl:,.2f} USDT
P1 — State PNL with {tag}. P2 — Your plan. P3 — ONE question.
HARD: 70-130 words, {tag} twice, no advice, no targets. Return ONLY the post."""
    post = ai_chat([{"role": "system", "content": "Honest retail trading diaries. Vulnerable, no hype."},
                    {"role": "user", "content": prompt}], temperature=0.95, max_tokens=900)
    if not post: return None
    return _strip_markdown(re.sub(r"^```(?:text)?|```$", "", post).strip())

def generate_curator_post(coin_list, context_label):
    if not AI_AVAILABLE: return None
    bullets = "\n".join(f"${c}" for c in coin_list[:8])
    prompt = f"""{persona_block('curator')}{PERSONA_RULES}
Curated-list post. LIST: {context_label}
COINS: {bullets}
1. Lead with utility. 2. Each $COIN on its own line with a short reason. 3. ONE closing question.
HARD: 90-140 words, plain text, every coin $CASHTAG. Return ONLY the post."""
    post = ai_chat([{"role": "system", "content": "Curate useful crypto lists."},
                    {"role": "user", "content": prompt}], temperature=0.85, max_tokens=1000)
    if not post: return None
    for c in coin_list[:8]:
        post = _ensure_cashtag(post, f"${c}", c)
    return _strip_markdown(re.sub(r"^```(?:text)?|```$", "", post).strip())

def generate_educational_post(topic_title, topic_brief, persona):
    if not AI_AVAILABLE: return None
    prompt = f"""{persona_block(persona)}{PERSONA_RULES}
EDUCATIONAL post. TOPIC: {topic_title}
BRIEF: {topic_brief}
P1 hook. P2 teach with concrete example. P3 what most get wrong. P4 ONE question.
HARD: 130-180 words. Reference $BTC or $ETH once if useful. Plain text. Return ONLY the post."""
    post = ai_chat([{"role": "system", "content":
        f"You are a {PERSONAS[persona]['name']} writing educational posts. Teach, don't signal."},
        {"role": "user", "content": prompt}], temperature=0.9, max_tokens=1200)
    if not post: return None
    return _fix_article_formatting(_strip_markdown(re.sub(r"^```(?:text)?|```$", "", post).strip()))

# ============================================================================
# SECTION 23 — PAPER TRADING BRIDGE
# ============================================================================

class PaperTradingBridge:
    """Initiative Alpha — executes signals on Binance testnet, screenshots PNL."""
    def __init__(self):
        self._lock = threading.Lock()
        self.enabled = PAPER_TRADE_ENABLED and bool(BINANCE_TESTNET_KEY)

    def _signed(self, params):
        try:
            import hmac
            qs = "&".join(f"{k}={v}" for k, v in params.items())
            sig = hmac.new(BINANCE_TESTNET_SECRET.encode(),
                           qs.encode(), hashlib.sha256).hexdigest()
            return qs + f"&signature={sig}"
        except Exception:
            return None

    def _request(self, method, path, params=None):
        if not self.enabled: return None
        params = params or {}
        params["timestamp"] = int(time.time() * 1000)
        qs = self._signed(params)
        if not qs: return None
        headers = {"X-MBX-APIKEY": BINANCE_TESTNET_KEY}
        url = f"{BINANCE_TESTNET_BASE}{path}?{qs}"
        try:
            r = session.request(method, url, headers=headers, timeout=10)
            return r.json()
        except Exception as e:
            print(f"[PAPER] {path}: {e}")
            return None

    def open_trade(self, signal):
        if not self.enabled: return None
        try:
            side = "BUY" if signal["direction"] == "LONG" else "SELL"
            qty = 0.001 if signal["coin"] == "BTC" else 0.01
            resp = self._request("POST", "/api/v3/order", {
                "symbol": signal["symbol"], "side": side,
                "type": "MARKET", "quantity": qty,
            })
            if not resp or "orderId" not in resp:
                print(f"[PAPER] order failed: {resp}")
                return None
            trade_id = f"paper-{resp['orderId']}"
            entry = safe_float(resp.get("fills", [{}])[0].get("price", 0)) \
                    or signal["entry_low"]
            try:
                _db().execute("""INSERT OR REPLACE INTO paper_trades
                    (trade_id, signal_id, symbol, side, qty, entry_price, status, opened_at)
                    VALUES (?,?,?,?,?,?,?,?)""",
                    (trade_id, signal["id"], signal["symbol"], side, qty, entry,
                     "OPEN", now_ts()))
            except Exception: pass
            state["metrics"]["paper_trades_opened"] = \
                state["metrics"].get("paper_trades_opened", 0) + 1
            print(f"[PAPER] opened {side} {signal['symbol']} @ {entry}")
            return trade_id
        except Exception as e:
            print(f"[PAPER] open_trade: {e}")
            return None

    def check_tp_sl(self):
        try:
            rows = _db().execute("""SELECT * FROM paper_trades WHERE status='OPEN'""").fetchall()
        except Exception:
            return
        for r in rows:
            try:
                price = get_price(r["symbol"])
            except Exception:
                continue
            # Find signal
            try:
                sigs = _db().execute("""SELECT * FROM posts WHERE real_post_id=?""",
                                     (r["signal_id"],)).fetchone()
            except Exception:
                sigs = None
            # Close if profitable by ≥ 2% or loss by ≥ 1%
            entry = r["entry_price"] or 0
            if entry == 0: continue
            change = pct_change(entry, price)
            side = r["side"]
            pnl_pct = change if side == "BUY" else -change
            if pnl_pct >= 2.0 or pnl_pct <= -1.0:
                try:
                    _db().execute("""UPDATE paper_trades
                        SET status='CLOSED', exit_price=?, pnl=?, closed_at=?
                        WHERE trade_id=?""",
                        (price, pnl_pct, now_ts(), r["trade_id"]))
                    state["metrics"]["paper_trades_hit"] = \
                        state["metrics"].get("paper_trades_hit", 0) + 1
                    print(f"[PAPER] closed {r['symbol']} pnl={pnl_pct:+.2f}%")
                except Exception: pass

paper_bridge = PaperTradingBridge()

# ============================================================================
# SECTION 24 — SVG GENERATION
# ============================================================================

class SVGRenderer:
    """Initiative Beta — turns JSON price data into branded SVG infographics."""
    def __init__(self):
        self.enabled = SVG_RENDER_ENABLED
        try: SVG_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        except Exception: pass

    def render_gainers_heatmap(self, data, title="Top 5 Gainers Today"):
        if not self.enabled: return None
        try:
            rows = data[:8]
            h = 60 + len(rows) * 44
            w = 640
            parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
                     f'viewBox="0 0 {w} {h}">',
                     f'<rect width="{w}" height="{h}" fill="{SVG_BG_COLOR}"/>',
                     f'<text x="24" y="38" fill="{SVG_BRAND_COLOR}" '
                     f'font-family="monospace" font-size="22" font-weight="700">{title}</text>']
            y = 78
            for r in rows:
                sym = r.get("symbol", "?")
                chg = float(r.get("change", 0))
                color = "#00e29a" if chg >= 0 else "#ff5c7c"
                parts.append(f'<text x="24" y="{y}" fill="#d5dde6" '
                             f'font-family="monospace" font-size="16">${sym}</text>')
                bar_w = min(340, max(20, int(abs(chg) * 12)))
                parts.append(f'<rect x="140" y="{y-14}" width="{bar_w}" height="16" '
                             f'fill="{color}" rx="3"/>')
                parts.append(f'<text x="{140+bar_w+12}" y="{y}" fill="{color}" '
                             f'font-family="monospace" font-size="15">{chg:+.2f}%</text>')
                y += 44
            parts.append("</svg>")
            svg = "\n".join(parts)
            gen_id = hashlib.sha1(svg.encode()).hexdigest()[:16]
            path = SVG_OUTPUT_DIR / f"{gen_id}.svg"
            path.write_text(svg)
            try:
                _db().execute("""INSERT OR REPLACE INTO svg_generations
                    (gen_id, kind, json_payload, svg_path, created_at)
                    VALUES (?,?,?,?,?)""",
                    (gen_id, "gainers", json.dumps(data), str(path), now_ts()))
            except Exception: pass
            state["metrics"]["svg_generated"] = \
                state["metrics"].get("svg_generated", 0) + 1
            print(f"[SVG] wrote {path}")
            return str(path)
        except Exception as e:
            print(f"[SVG] render failed: {e}")
            return None

    def render_liquidation_chart(self, data):
        if not self.enabled: return None
        try:
            # data = [{"label": str, "value": float, "color": str}]
            w = 640; h = 300
            max_v = max((abs(d["value"]) for d in data), default=1) or 1
            bar_w = (w - 80) / max(len(data), 1)
            parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
                     f'viewBox="0 0 {w} {h}">',
                     f'<rect width="{w}" height="{h}" fill="{SVG_BG_COLOR}"/>',
                     f'<text x="24" y="34" fill="{SVG_BRAND_COLOR}" '
                     f'font-family="monospace" font-size="22" font-weight="700">'
                     f'Liquidations (24h)</text>']
            x = 40
            for d in data:
                bh = int((abs(d["value"]) / max_v) * 200)
                y = 260 - bh
                color = d.get("color", "#5b8def")
                parts.append(f'<rect x="{x}" y="{y}" width="{bar_w-10}" height="{bh}" '
                             f'fill="{color}" rx="3"/>')
                parts.append(f'<text x="{x + (bar_w-10)//2}" y="286" fill="#7d8896" '
                             f'font-family="monospace" font-size="12" text-anchor="middle">'
                             f'{d["label"]}</text>')
                x += bar_w
            parts.append("</svg>")
            svg = "\n".join(parts)
            gen_id = hashlib.sha1(svg.encode()).hexdigest()[:16]
            path = SVG_OUTPUT_DIR / f"{gen_id}.svg"
            path.write_text(svg)
            state["metrics"]["svg_generated"] = \
                state["metrics"].get("svg_generated", 0) + 1
            return str(path)
        except Exception as e:
            print(f"[SVG] liquidation render failed: {e}")
            return None

svg_renderer = SVGRenderer()

# ============================================================================
# SECTION 25 — MEME ENGINEERING
# ============================================================================

class MemeEngineer:
    """Initiative Gamma — localises crypto memes via VLM for vi/tr markets."""
    def __init__(self):
        self.enabled = MEME_ENABLED
        self.templates = [
            "drake", "distracted_boyfriend", "this_is_fine",
            "two_buttons", "expanding_brain", "change_my_mind",
        ]

    def _compose_caption(self, event_text, lang):
        culture = LANG_CULTURE.get(lang, "")
        prompt = f"""Create a meme caption in {LANG_NAMES.get(lang, 'English')}.
CULTURAL CONTEXT: {culture}
CRYPTO EVENT: \"\"\"{event_text[:400]}\"\"\"
Pick ONE meme template from: {', '.join(self.templates)}
Return JSON: {{"template": "...", "top_text": "...", "bottom_text": "..."}}
HARD: Short, punchy, culturally appropriate. No hate. No slurs. Under 80 chars each."""
        data = ai_json([{"role": "user", "content": prompt}],
                       temperature=0.95, max_tokens=400)
        return data

    def generate(self, event_text, lang):
        if not self.enabled: return None
        data = self._compose_caption(event_text, lang)
        if not data: return None
        template = data.get("template", "drake")
        top = data.get("top_text", "")[:120]
        bot = data.get("bottom_text", "")[:120]
        # Compose a simple SVG meme card as a local asset
        try:
            w, h = 640, 480
            svg = (f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" '
                   f'viewBox="0 0 {w} {h}">'
                   f'<rect width="{w}" height="{h}" fill="{SVG_BG_COLOR}"/>'
                   f'<text x="{w//2}" y="90" fill="#d5dde6" font-family="monospace" '
                   f'font-size="28" font-weight="700" text-anchor="middle">{top}</text>'
                   f'<rect x="80" y="140" width="{w-160}" height="200" fill="#1a2230" '
                   f'stroke="{SVG_BRAND_COLOR}" stroke-width="2"/>'
                   f'<text x="{w//2}" y="240" fill="{SVG_BRAND_COLOR}" '
                   f'font-family="monospace" font-size="20" text-anchor="middle">'
                   f'[{template}]</text>'
                   f'<text x="{w//2}" y="410" fill="#d5dde6" font-family="monospace" '
                   f'font-size="26" text-anchor="middle">{bot}</text>'
                   f'</svg>')
            gen_id = hashlib.sha1(svg.encode()).hexdigest()[:16]
            path = SVG_OUTPUT_DIR / f"meme_{gen_id}.svg"
            path.write_text(svg)
            try:
                _db().execute("""INSERT OR REPLACE INTO memes
                    (meme_id, lang, template, caption, image_path, source_event,
                     created_at, posted)
                    VALUES (?,?,?,?,?,?,?,0)""",
                    (gen_id, lang, template, f"{top}\n{bot}", str(path),
                     event_text[:200], now_ts()))
            except Exception: pass
            state["metrics"]["memes_generated"] = \
                state["metrics"].get("memes_generated", 0) + 1
            print(f"[MEME] generated {gen_id} ({lang}/{template})")
            return {"meme_id": gen_id, "path": str(path),
                    "caption": f"{top}\n\n{bot}", "lang": lang}
        except Exception as e:
            print(f"[MEME] render failed: {e}")
            return None

meme_engineer = MemeEngineer()

# ============================================================================
# SECTION 26 — POSTING PIPELINE
# ============================================================================

class PostPipeline:
    def _publish_and_track(self, text, content_type, coin, persona,
                           archetype, narrative, quality, trust, entry_price=0.0):
        # V30: embedding dedup
        if not embedding_dedup.check(text, verbose=True):
            print("[PUBLISH] Embedding duplicate — skipped.")
            return False
        # V30: L3 discriminator
        if not discriminator_ok(text):
            state["metrics"]["trust_rejections"] = \
                state["metrics"].get("trust_rejections", 0) + 1
            return False

        ok_discord = send_discord(text)
        ok_square = send_square(text)
        if not (ok_discord or ok_square):
            print(f"[PUBLISH-FAIL] {content_type} ${coin} — no channel accepted.")
            state.setdefault("failed_publish_cooldown", {})[content_type] = now_ts()
            save_state()
            return False
        now = now_ts()
        state["last_post_ts"] = now
        track_cashtag_post(coin)
        remember_post(text)
        remember_persona(persona)
        ab_group = ab_tester.choose_variant(content_type, coin)
        local_id = track_post_publish(text, content_type, coin, quality,
                                      archetype=archetype, persona=persona,
                                      trust_score=trust, entry_price=entry_price)
        # Store embedding
        if local_id:
            embedding_dedup.store(local_id, text)
            try:
                _db().execute("UPDATE posts SET ab_group=?, swarm_node=? WHERE post_id=?",
                              (ab_group, SWARM_NODE_ID, local_id))
            except Exception: pass
            ab_tester.record(local_id, content_type, coin, ab_group)
        try:
            _db().execute("""INSERT OR REPLACE INTO archetype_log
                (post_id, archetype, secondary_archetype, reasoning, logged_at)
                VALUES (?,?,?,?,?)""",
                (local_id or f"local-{now}", archetype, "", f"ct={content_type}", now))
        except Exception: pass
        try:
            _db().execute("""INSERT OR REPLACE INTO trust_scores
                (post_id, trust_score, discriminator_p_human, computed_at)
                VALUES (?,?,?,?)""",
                (local_id or f"local-{now}", trust,
                 local_discriminator.p_human(text), now))
        except Exception: pass
        if narrative:
            try:
                _db().execute("""INSERT OR IGNORE INTO narrative_engagement
                    (narrative_id, post_id, recorded_at) VALUES (?,?,?)""",
                    (narrative["narrative_id"], local_id or f"local-{now}", now))
                _db().execute("UPDATE narratives SET posted=1 WHERE narrative_id=?",
                              (narrative["narrative_id"],))
            except Exception: pass
        engagement_optimizer.register_watch(
            local_id or f"local-{now}", local_id or "", now, persona)
        try:
            q, _ = score_post_quality(text)
            if q >= 70 and content_type in ("signal", "trade_plan", "news", "curator"):
                language_arbitrage.schedule_variants(local_id or f"local-{now}", text, q)
        except Exception: pass
        # V30: register with swarm for cross-engagement
        try:
            swarm_cross.register_post(local_id or f"local-{now}", local_id or "",
                                      coin, text)
        except Exception: pass
        c = state.setdefault("daily_content_counter", {})
        c[content_type] = c.get(content_type, 0) + 1
        save_state()
        return True

    def publish_signal(self, signal):
        persona = pick_persona("signal")
        arch, sec, _ = archetype_engine.assign("signal", persona)
        text = generate_signal_post(signal, persona, arch)
        if not text: return False
        q, _ = score_post_quality(text, signal)
        if q < _active_min_quality(): return False
        if not passes_originality(text, verbose=True): return False
        trust = trust_engine.compute(text)
        if trust < state["auto_tune"].get("min_trust", MIN_TRUST_SCORE) / 100.0:
            state["metrics"]["trust_rejections"] += 1
            print(f"[TRUST] Rejected trust={trust}")
            return False
        print(f"========== SIGNAL[{arch}] q={q} trust={trust:.2f} ==========")
        print(text); print("=" * 55)
        ok = self._publish_and_track(text, "signal", signal["coin"], persona, arch,
                                     narrative_engine.by_coin(signal["coin"]),
                                     q, trust, entry_price=signal.get("entry_high", 0))
        if ok:
            state["posted_coins"][signal["symbol"]] = now_ts()
            state["active_signals"][signal["id"]] = signal
            _record_recent_call(signal)
            if signal["direction"] == "LONG":
                state["consecutive_long"] = state.get("consecutive_long", 0) + 1
                state["consecutive_short"] = 0
            else:
                state["consecutive_short"] = state.get("consecutive_short", 0) + 1
                state["consecutive_long"] = 0
            state["metrics"]["signals_posted"] += 1
            state["metrics"]["quality_sum"] += q
            state["metrics"]["quality_count"] += 1
            # V30: fire off paper trade
            try: paper_bridge.open_trade(signal)
            except Exception as e: print(f"[PAPER] {e}")
            save_state()
        return ok

    def publish_news(self, item):
        persona = pick_persona("news")
        arch, _, _ = archetype_engine.assign("news", persona)
        nar = narrative_engine.by_coin(item["coins"][0]) if item.get("coins") else None
        text = generate_news_post(item, persona, nar)
        if not text: return False
        q, _ = score_post_quality(text)
        if q < _active_min_quality(): return False
        if not passes_originality(text, verbose=False): return False
        trust = trust_engine.compute(text)
        if trust < 0.5: state["metrics"]["trust_rejections"] += 1; return False
        print(f"========== NEWS[{arch}] q={q} trust={trust:.2f} ==========")
        print(text); print("=" * 55)
        ok = self._publish_and_track(text, "news",
                                     item["coins"][0] if item.get("coins") else "BTC",
                                     persona, arch, nar, q, trust)
        if ok:
            state["news_posts_today"] = state.get("news_posts_today", 0) + 1
            state["metrics"]["news_posted"] += 1
            try: _db().execute("UPDATE news_events SET posted=1 WHERE event_id=?", (item["id"],))
            except Exception: pass
            save_state()
        return ok

    def publish_diary(self):
        text = generate_diary_post()
        if not text: return False
        q, _ = score_post_quality(text)
        if q < 45: return False
        if not passes_originality(text, verbose=False): return False
        trust = trust_engine.compute(text, recent_loss_ack=0.9)
        print(f"========== DIARY[A4] q={q} trust={trust:.2f} ==========")
        print(text); print("=" * 55)
        ok = self._publish_and_track(text, "diary", "ETH", "diarist", "A4", None, q, trust)
        if ok: state["metrics"]["diary_posted"] += 1; save_state()
        return ok

    def publish_curator(self):
        coins = list(dict.fromkeys(feed_learner.top_coins(6) + trending_hijacker.top(6)))[:8]
        if len(coins) < 3: return False
        text = generate_curator_post(coins, "Trending now on Binance Square")
        if not text: return False
        q, _ = score_post_quality(text)
        if q < _active_min_quality(): return False
        if not passes_originality(text, verbose=False): return False
        trust = trust_engine.compute(text)
        print(f"========== CURATOR[A5] q={q} trust={trust:.2f} ==========")
        print(text); print("=" * 55)
        ok = self._publish_and_track(text, "curator", coins[0], "curator", "A5", None, q, trust)
        if ok: state["metrics"]["curator_posted"] += 1; save_state()
        return ok

    def publish_educational(self):
        topics = [
            ("rsi_divergence", "How to read RSI divergence without over-trading",
             "Explain RSI divergence and when to ignore it."),
            ("liquidity_sweep", "What a liquidity sweep actually looks like",
             "Explain liquidity sweeps."),
            ("risk_management", "Position sizing matters more than entry",
             "Explain R-multiples and risk sizing."),
            ("invalidation", "Every trade should have an invalidation level",
             "Explain invalidation."),
        ]
        topic = random.choice(topics)
        persona = pick_persona("educational")
        arch, _, _ = archetype_engine.assign("educational", persona)
        text = generate_educational_post(topic[1], topic[2], persona)
        if not text: return False
        q, _ = score_post_quality(text)
        if q < _active_min_quality(): return False
        if not passes_originality(text, verbose=False): return False
        trust = trust_engine.compute(text)
        print(f"========== EDU[{arch}] q={q} trust={trust:.2f} ==========")
        print(text); print("=" * 55)
        ok = self._publish_and_track(text, "educational", "BTC", persona, arch, None, q, trust)
        if ok: state["metrics"]["educational_posted"] += 1; save_state()
        return ok

pipeline = PostPipeline()

# ============================================================================
# SECTION 27 — SQUARE POSTING
# ============================================================================

def post_via_api(text):
    if not BINANCE_SQUARE_OPENAPI_KEY: return False
    headers = {
        "X-Square-OpenAPI-Key": BINANCE_SQUARE_OPENAPI_KEY,
        "Content-Type": "application/json",
        "clienttype": "binanceSkill",
    }
    try:
        r = session.post(BINANCE_SQUARE_API_URL, json={"bodyTextOnly": text},
                         headers=headers, timeout=REQUEST_TIMEOUT)
        data = r.json()
        if data.get("code") == "000000":
            print(f"[SQUARE-API] Posted. ID={(data.get('data') or {}).get('id')}")
            return True
        print(f"[SQUARE-API] Failed code={data.get('code')} msg={data.get('message')}")
        if data.get("code") == "220009":
            state["square_posts_today"] = SQUARE_DAILY_LIMIT; save_state()
        return False
    except Exception as e:
        print(f"[SQUARE-API] Error: {e}")
        return False

def reset_square_counter():
    today = utc_date()
    if state.get("square_posts_date", "") != today:
        state["square_posts_date"] = today
        state["square_posts_today"] = 0
        state["square_replies_today"] = 0
        state["news_posts_today"] = 0
        state["archetype_counter"] = {}
        state["daily_content_counter"] = {}
        save_state()

def square_available():
    reset_square_counter()
    return state.get("square_posts_today", 0) < SQUARE_DAILY_LIMIT

def send_square(text):
    if not square_available():
        print("[SQUARE] Daily cap reached."); return False
    ok = False
    try: ok = publish_via_browser(text)
    except Exception as e:
        print(f"[SQUARE] Browser exception: {e}")
        ok = False
    if ok:
        state["square_posts_today"] += 1
        save_state()
        print(f"[SQUARE] Posted via browser ({state['square_posts_today']}/{SQUARE_DAILY_LIMIT})")
        return True
    if USE_SQUARE_OPENAPI:
        print("[SQUARE] Browser failed → trying API fallback...")
        ok = post_via_api(text)
        if ok:
            state["square_posts_today"] += 1
            state["metrics"]["api_posts"] = state["metrics"].get("api_posts", 0) + 1
            save_state()
            print(f"[SQUARE] Posted via API ({state['square_posts_today']}/{SQUARE_DAILY_LIMIT})")
            return True
    print("[SQUARE] All publish paths failed.")
    return False

def send_discord(text):
    if not DISCORD_WEBHOOK_URL:
        print("[DISCORD] Not configured.")
        return False
    try:
        r = session.post(DISCORD_WEBHOOK_URL,
                         json={"content": truncate_for_discord(text)},
                         timeout=REQUEST_TIMEOUT)
        if r.status_code in (200, 204):
            print("[DISCORD] OK."); return True
        print(f"[DISCORD] Failed {r.status_code}")
        return False
    except Exception as e:
        print(f"[DISCORD] Error: {e}")
        return False

# ============================================================================
# SECTION 28 — POST TRACKING
# ============================================================================

def _engagement(views, likes, comments, shares):
    return views * 1.0 + likes * 50.0 + comments * 200.0 + shares * 300.0

def track_post_publish(post_text, content_type, coin, quality, archetype="",
                       persona="", trust_score=0, entry_price=0.0):
    ts = now_ts()
    local_id = f"local-{content_type}-{coin}-{ts}"
    emoji_count = len(_EMOJI_RE.findall(post_text))
    bucket = sentiment_bucket(_sentiment_cache.get("composite", 0.0))
    try:
        _db().execute("""INSERT OR IGNORE INTO posts
            (post_id, link_status, posted_at, posted_hour_utc, posted_weekday,
             coin, content_type, persona, archetype, hook, emoji_count, word_count,
             quality_score, entry_price, sentiment_bucket, trust_score, swarm_node)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (local_id, "pending", ts, utc_hour(), utc_weekday(),
             coin, content_type, persona, archetype, "", emoji_count,
             len(post_text.split()), int(quality), float(entry_price),
             bucket, float(trust_score), SWARM_NODE_ID))
        return local_id
    except Exception as e:
        print(f"[TRACK] {e}")
        return None

def track_cashtag_post(coin):
    week = current_week_key()
    if state.get("stats_week_start", "") != week:
        state["stats_week_start"] = week
        state["cashtag_stats"] = {}
    s = state.setdefault("cashtag_stats", {})
    e = s.setdefault(coin, {"posts": 0, "first_ts": now_ts(), "last_ts": 0})
    e["posts"] += 1; e["last_ts"] = now_ts()

def _record_recent_call(signal):
    calls = state.setdefault("recent_calls", [])
    em = (signal["entry_low"] + signal["entry_high"]) / 2
    calls.append({"id": signal["id"], "symbol": signal["symbol"], "coin": signal["coin"],
                  "direction": signal["direction"], "entry_mid": em,
                  "tp1": signal["tp1"], "tp2": signal["tp2"], "tp3": signal["tp3"],
                  "sl": signal["sl"], "posted_at": now_ts(),
                  "outcome": None, "outcome_price": None})
    state["recent_calls"] = calls[-60:]

# ============================================================================
# SECTION 29 — SIGNAL BUILDING
# ============================================================================

def build_candidates():
    allowed = set()
    try:
        info = get_exchange_info()
        for it in info.get("symbols", []):
            if it.get("status") != "TRADING": continue
            if it.get("quoteAsset") != "USDT": continue
            allowed.add(it.get("symbol"))
    except Exception: pass
    tickers = get_tickers_24h()
    candidates = []
    for t in tickers:
        sym = t.get("symbol", "")
        if sym not in allowed or not sym.endswith("USDT"): continue
        if sym in STABLE_SYMBOLS: continue
        qv = safe_float(t.get("quoteVolume"))
        if qv < MIN_QUOTE_VOLUME: continue
        coin = sym[:-4]
        if coin in STABLE_BASES: continue
        candidates.append({"symbol": sym, "coin": coin, "quote_volume": qv,
                           "price": safe_float(t.get("lastPrice")),
                           "change_percent": safe_float(t.get("priceChangePercent")),
                           "trending": coin in trending_hijacker.top(20)})
    candidates.sort(key=lambda x: (x["trending"], x["quote_volume"],
                                    abs(x["change_percent"])), reverse=True)
    return candidates[:TOP_CANDIDATES]

def build_signal(candidate, relaxed=False):
    symbol = candidate["symbol"]; coin = symbol.replace("USDT", "")
    try:
        k = get_klines_parallel(symbol, frames=(("15m",100),("1h",100),("4h",100)))
        a15 = analyze_timeframe(k["15m"]); a1h = analyze_timeframe(k["1h"]); a4h = analyze_timeframe(k["4h"])
    except Exception: return None
    ls = ss = 0.0; rl, rs = [], []
    if a4h["trend"] == "bullish": ls += 20; rl.append("4H trend bullish")
    elif a4h["trend"] == "bearish": ss += 20; rs.append("4H trend bearish")
    if a1h["trend"] == "bullish": ls += 16; rl.append("1H trend bullish")
    elif a1h["trend"] == "bearish": ss += 16; rs.append("1H trend bearish")
    if a1h["momentum"] > 1.2: ls += 10; rl.append("1H momentum positive")
    elif a1h["momentum"] < -1.2: ss += 10; rs.append("1H momentum negative")
    if 54 <= a1h["rsi"] <= 66: ls += 8
    elif 34 <= a1h["rsi"] <= 46: ss += 8
    if a15["breakout"]: ls += 14; rl.append("15M breakout")
    if a15["breakdown"]: ss += 14; rs.append("15M breakdown")
    if a1h["volume_ratio"] >= 2.0:
        if a1h["trend"] == "bullish": ls += 15
        elif a1h["trend"] == "bearish": ss += 15
    if candidate.get("trending"): ls += 8; ss += 8
    direction = "LONG" if ls >= ss else "SHORT"
    score = clamp(ls if direction == "LONG" else ss, 0, 100)
    reasons = rl if direction == "LONG" else rs
    threshold = state["auto_tune"].get("signal_threshold", SIGNAL_THRESHOLD)
    if relaxed:
        if score < SIGNAL_THRESHOLD_RELAXED: return None
        tier = "B"
    else:
        if score >= SIGNAL_THRESHOLD_STRONG: tier = "A+"
        elif score >= threshold: tier = "A"
        else: return None
    price = candidate["price"]; atr = a1h["atr"]
    if atr <= 0: return None
    if direction == "LONG":
        el, eh = price * 0.998, price * 1.002
        sl = price - atr * 1.4
        rk = max(price - sl, price * 0.005)
        tp1, tp2, tp3 = price + rk, price + rk*1.8, price + rk*2.6
    else:
        el, eh = price * 0.998, price * 1.002
        sl = price + atr * 1.4
        rk = max(sl - price, price * 0.005)
        tp1, tp2, tp3 = price - rk, price - rk*1.8, price - rk*2.6
    return {"id": f"{symbol}-{direction}-{now_ts()}",
            "symbol": symbol, "coin": coin, "direction": direction,
            "score": round(score, 2), "tier": tier,
            "entry_low": el, "entry_high": eh,
            "tp1": tp1, "tp2": tp2, "tp3": tp3, "sl": sl,
            "reasons": reasons[:4], "trending": candidate.get("trending", False),
            "created_at": now_ts(), "status": "ACTIVE",
            "tp1_hit": False, "tp2_hit": False, "tp3_hit": False, "sl_hit": False}

# ============================================================================
# SECTION 30 — NEWS LAYER
# ============================================================================

NEWS_SOURCES = [
    {"name": "coindesk_rss", "url": "https://www.coindesk.com/arc/outboundfeeds/rss/"},
    {"name": "cointelegraph", "url": "https://cointelegraph.com/rss"},
]
NEWS_KEYWORDS = {
    "listing": ["will list", "listing", "lists"],
    "delisting": ["will delist", "delisting"],
    "etf": ["etf", "spot etf", "approved"],
    "sec": ["sec", "sec charges", "lawsuit"],
    "hack": ["hack", "exploit", "breach"],
    "partnership": ["partnership", "integrates"],
}
NEWS_MAX_AGE_SECONDS = 900
NEWS_MAX_PER_HOUR = 8

def extract_coins_from_title(title):
    tags = set(re.findall(r"\$([A-Z]{2,8})", title.upper()))
    name_map = {"bitcoin": "BTC", "ethereum": "ETH", "solana": "SOL", "ripple": "XRP"}
    for n, t in name_map.items():
        if n in title.lower(): tags.add(t)
    return list(tags)

def _record_ttp(trigger_ts):
    ttp = now_ts() - trigger_ts
    state["metrics"]["ttp_samples"].append(ttp)
    state["metrics"]["ttp_samples"] = state["metrics"]["ttp_samples"][-100:]
    state["metrics"]["ttp_avg_sec"] = mean(state["metrics"]["ttp_samples"])

class NewsSpeedLayer:
    def __init__(self):
        self._seen = set(); self._last_scan = 0; self._hour_hits = []
        self._lock = threading.Lock()

    def _clean_hour(self):
        self._hour_hits = [t for t in self._hour_hits if t > now_ts() - 3600]

    def _can_post(self):
        self._clean_hour(); return len(self._hour_hits) < NEWS_MAX_PER_HOUR

    def _mark_post(self): self._hour_hits.append(now_ts())

    @staticmethod
    def _parse_age(pub):
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(pub)
            return max(0, int(now_ts() - dt.timestamp()))
        except Exception: return None

    def scan(self):
        now = now_ts()
        with self._lock:
            if now - self._last_scan < 60: return []
            self._last_scan = now
        hits = []
        for src in NEWS_SOURCES:
            try:
                r = session.get(src["url"], timeout=8)
                if r.status_code != 200: continue
                root = ET.fromstring(r.content)
                for item in list(root.iter("item"))[:25]:
                    title = (item.findtext("title", "") or "").strip()
                    pub = item.findtext("pubDate", "") or ""
                    if not title: continue
                    hid = hashlib.sha1(title.encode()).hexdigest()
                    if hid in self._seen: continue
                    self._seen.add(hid)
                    age_s = self._parse_age(pub)
                    if age_s is None or age_s > NEWS_MAX_AGE_SECONDS: continue
                    cat = next((c for c, kws in NEWS_KEYWORDS.items()
                                if any(k in title.lower() for k in kws)), None)
                    if not cat: continue
                    coins = extract_coins_from_title(title)
                    if not coins: continue
                    try:
                        _db().execute("""INSERT OR IGNORE INTO news_events
                            (event_id, source, title, coins, category, seen_ts)
                            VALUES (?,?,?,?,?,?)""",
                            (hid, src["name"], title, ",".join(coins), cat, now))
                    except Exception: pass
                    hits.append({"id": hid, "title": title, "source": src["name"],
                                 "category": cat, "coins": coins, "age_s": age_s,
                                 "trigger_ts": now})
            except Exception: continue
        return hits

    def publish_if_worthy(self):
        if not self._can_post(): return False
        hits = self.scan()
        if not hits: return False
        def score(h):
            cat_w = {"listing": 1.4, "etf": 1.5, "sec": 1.3, "hack": 1.5,
                     "partnership": 1.1, "delisting": 1.2}.get(h["category"], 1.0)
            coin_w = 1.4 if any(c in ("BTC","ETH","SOL","BNB") for c in h["coins"]) else 1.0
            return cat_w * coin_w / max(1, h["age_s"] / 300.0)
        hits.sort(key=score, reverse=True)
        for h in hits[:3]:
            if pipeline.publish_news(h):
                self._mark_post()
                _record_ttp(h["trigger_ts"])
                return True
        return False

news_layer = NewsSpeedLayer()

# ============================================================================
# SECTION 31 — SIGNAL TRACKING + RESULT PUBLISHING
# ============================================================================

def update_active_signals():
    if not state["active_signals"]: return
    to_remove = []
    for sid, sig in list(state["active_signals"].items()):
        try: price = get_price(sig["symbol"])
        except Exception: continue
        long_side = sig["direction"] == "LONG"
        if long_side:
            if price <= sig["sl"]:
                sig["sl_hit"] = True; sig["status"] = "SL HIT"
                to_remove.append(sid); continue
            if not sig["tp1_hit"] and price >= sig["tp1"]:
                sig["tp1_hit"] = True; _publish_result(sig, "TP1", price)
            if not sig["tp2_hit"] and price >= sig["tp2"]:
                sig["tp2_hit"] = True; _publish_result(sig, "TP2", price)
            if not sig["tp3_hit"] and price >= sig["tp3"]:
                sig["tp3_hit"] = True; sig["status"] = "TP3 HIT"
                _publish_result(sig, "TP3", price); to_remove.append(sid)
        else:
            if price >= sig["sl"]:
                sig["sl_hit"] = True; sig["status"] = "SL HIT"
                to_remove.append(sid); continue
            if not sig["tp1_hit"] and price <= sig["tp1"]:
                sig["tp1_hit"] = True; _publish_result(sig, "TP1", price)
            if not sig["tp2_hit"] and price <= sig["tp2"]:
                sig["tp2_hit"] = True; _publish_result(sig, "TP2", price)
            if not sig["tp3_hit"] and price <= sig["tp3"]:
                sig["tp3_hit"] = True; sig["status"] = "TP3 HIT"
                _publish_result(sig, "TP3", price); to_remove.append(sid)
    for sid in to_remove: state["active_signals"].pop(sid, None)
    save_state()

def _publish_result(sig, rt, price):
    tag = f"${sig['coin']}"
    em = (sig["entry_low"] + sig["entry_high"]) / 2
    pnl = pct_change(em, price)
    emoji = {"TP1": "✅", "TP2": "🎯", "TP3": "🏆"}.get(rt, "📌")
    text = (f"{emoji} {tag} {sig['direction']} — {rt} hit at {format_price(price)}.\n\n"
            f"From {format_price(em)} → {format_price(price)} ({pnl:+.2f}%).\n\n"
            f"Still holding for more, or taking profit here?")
    q, _ = score_post_quality(text)
    trust = trust_engine.compute(text)
    ok = pipeline._publish_and_track(text, "result", sig["coin"], pick_persona("result"),
                                     "A3", narrative_engine.by_coin(sig["coin"]), q, trust)
    if ok: state["metrics"]["results_posted"] += 1; save_state()
    return ok

# ============================================================================
# SECTION 32 — DASHBOARD
# ============================================================================

dashboard = Flask("superclaw-30")

DASHBOARD_HTML = """
<!doctype html><html><head><title>SuperClaw 30.0</title>
<meta http-equiv="refresh" content="30">
<style>
:root{--bg:#0b0f14;--card:#131920;--brd:#232b36;--fg:#d5dde6;--muted:#7d8896;
--good:#00e29a;--warn:#ffb547;--bad:#ff5c7c;--purple:#a371f7;--blue:#5b8def;--gold:#f0b90b;}
*{box-sizing:border-box}
body{background:var(--bg);color:var(--fg);font-family:ui-monospace,monospace;padding:20px;margin:0;font-size:13px}
h1{font-size:20px;color:var(--gold);margin:0 0 8px}
h3{font-size:11px;color:var(--muted);text-transform:uppercase;letter-spacing:1.2px;margin:0 0 12px}
.card{background:var(--card);border:1px solid var(--brd);border-radius:10px;padding:16px 18px;margin-bottom:14px}
.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:14px}
.metric-val{font-size:26px;font-weight:700;color:var(--warn);line-height:1.1}
.metric-lbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px;margin-top:6px}
table{width:100%;border-collapse:collapse}
th,td{padding:6px 8px;text-align:left;border-bottom:1px solid var(--brd);font-size:12px}
th{color:var(--muted);font-weight:600;text-transform:uppercase;font-size:10px}
.good{color:var(--good)}.bad{color:var(--bad)}.warn{color:var(--warn)}
.purple{color:var(--purple)}.blue{color:var(--blue)}.gold{color:var(--gold)}.muted{color:var(--muted)}
.pool{display:flex;flex-wrap:wrap;gap:6px}
.pool span{background:#1a2230;padding:4px 10px;border-radius:12px;font-size:12px;border:1px solid #3a4451}
.actions{margin-bottom:16px;display:flex;gap:8px;flex-wrap:wrap}
.actions a{background:var(--card);color:var(--fg);border:1px solid var(--brd);padding:7px 14px;border-radius:8px;text-decoration:none;font-size:12px}
</style></head><body>
<h1>SuperClaw 30.0 <span class="gold">·</span> Flywheel Edition</h1>
<div class="muted" style="font-size:11px;margin-bottom:16px">
Node <b>{{ swarm_node }}</b> · archetype <b>{{ swarm_arch }}</b> ·
elastic ×{{ elastic_mult }} · ttp avg {{ ttp_avg }}s ·
updated {{ generated_at|datetime }} UTC
</div>
<div class="actions">
<a href="/api/stats">Stats</a><a href="/api/analytics">Analytics</a>
<a href="/api/narratives">Narratives</a><a href="/api/humanizer">Humanizer</a>
<a href="/api/feed-now">Feed</a><a href="/api/learn">Learn</a><a href="/api/tune">Tune</a>
<a href="/api/bayes">Bayes</a><a href="/api/discriminator">Train L3</a>
<a href="/api/ab">A/B Decide</a>
</div>

<div class="grid4">
<div class="card"><div class="metric-val good">{{ t.views or 0 }}</div><div class="metric-lbl">views 7d</div></div>
<div class="card"><div class="metric-val">{{ t.total or 0 }}</div><div class="metric-lbl">posts 7d</div></div>
<div class="card"><div class="metric-val purple">{{ metrics.browser_posts or 0 }}</div><div class="metric-lbl">browser posts</div></div>
<div class="card"><div class="metric-val gold">{{ metrics.x_firehose_hits or 0 }}</div><div class="metric-lbl">X firehose hits</div></div>
</div>
<div class="grid4">
<div class="card"><div class="metric-val blue">{{ metrics.whale_events or 0 }}</div><div class="metric-lbl">whale events</div></div>
<div class="card"><div class="metric-val">{{ metrics.swarm_replies_sent or 0 }}</div><div class="metric-lbl">swarm replies</div></div>
<div class="card"><div class="metric-val">{{ metrics.svg_generated or 0 }}</div><div class="metric-lbl">SVGs</div></div>
<div class="card"><div class="metric-val">{{ metrics.memes_generated or 0 }}</div><div class="metric-lbl">memes</div></div>
</div>

<div class="card">
<h3>🔥 Active Narratives</h3>
<div class="pool">
{% for n in narratives %}<span>${{ n.coins[0] }} · {{ n.composite_score }}</span>{% endfor %}
</div>
</div>

<div class="card">
<h3>📊 Recent Posts</h3>
<table>
<tr><th>Coin</th><th>Type</th><th>Archetype</th><th>Persona</th><th>Views</th><th>Trust</th></tr>
{% for r in recent %}
<tr><td>${{ r.coin }}</td><td class="muted">{{ r.content_type }}</td>
<td class="purple">{{ r.archetype or '—' }}</td><td class="blue">{{ r.persona or '—' }}</td>
<td class="good">{{ r.views or 0 }}</td><td class="warn">{{ '%.2f'|format(r.trust_score or 0) }}</td></tr>
{% else %}<tr><td colspan="6" class="muted">No posts yet</td></tr>{% endfor %}
</table>
</div>

<div class="card">
<h3>🤖 Humanizer Actions</h3>
<table>
<tr><th>Kind</th><th>Detail</th><th>Duration</th></tr>
{% for h in humanizer_log %}
<tr><td class="purple">{{ h.kind }}</td><td class="muted">{{ h.detail[:80] }}</td>
<td class="good">{{ h.duration_ms }}ms</td></tr>
{% else %}<tr><td colspan="3" class="muted">No actions yet</td></tr>{% endfor %}
</table>
</div>
</body></html>
"""

def _fmt_ts(v):
    try: return datetime.fromtimestamp(int(v), tz=timezone.utc).strftime("%m-%d %H:%M")
    except Exception: return "—"

dashboard.jinja_env.filters["datetime"] = _fmt_ts

@dashboard.route("/")
def index():
    s = get_analytics_summary()
    try:
        hl = [_row_to_dict(r) for r in _db().execute(
            "SELECT kind, detail, duration_ms FROM humanizer_log ORDER BY ts DESC LIMIT 20").fetchall()]
    except Exception: hl = []
    return render_template_string(
        DASHBOARD_HTML, generated_at=s["generated_at"], t=s["totals"],
        metrics=state.get("metrics", {}), narratives=s["narratives"],
        recent=s["recent"], humanizer_log=hl,
        swarm_node=SWARM_NODE_ID,
        swarm_arch=swarm_binding.binding.get("archetype", "?"),
        elastic_mult=content_scheduler._elastic_mult(),
        ttp_avg=state["metrics"].get("ttp_avg_sec", 0.0))

@dashboard.route("/api/stats")
def api_stats(): return jsonify(state.get("metrics", {}))

@dashboard.route("/api/analytics")
def api_analytics(): return jsonify(get_analytics_summary())

@dashboard.route("/api/narratives")
def api_narratives(): return jsonify(narrative_engine.top(20))

@dashboard.route("/api/humanizer")
def api_humanizer():
    try:
        rows = _db().execute("""SELECT kind, detail, duration_ms, ts FROM humanizer_log
                                ORDER BY ts DESC LIMIT 50""").fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception: return jsonify([])

@dashboard.route("/api/feed-now")
def api_feed_now():
    threading.Thread(target=feed_learner.scan, daemon=True).start()
    return jsonify({"status": "started"})

@dashboard.route("/api/learn")
def api_learn():
    learned = learn_from_analytics()
    state["learned_weights"] = learned
    state["last_learn_ts"] = now_ts(); save_state()
    return jsonify(learned)

@dashboard.route("/api/tune")
def api_tune():
    auto_tune(); save_state()
    return jsonify(state.get("auto_tune", {}))

@dashboard.route("/api/bayes")
def api_bayes():
    c = bayes_tuner.tune_signal_threshold()
    save_state()
    return jsonify({"signal_threshold": c})

@dashboard.route("/api/discriminator")
def api_discriminator():
    ok = local_discriminator.train()
    return jsonify({"trained": ok})

@dashboard.route("/api/ab")
def api_ab():
    ab_tester.decide_winners()
    return jsonify({"status": "ok"})

def start_dashboard_thread():
    t = threading.Thread(
        target=lambda: dashboard.run(host="127.0.0.1", port=8080,
                                     debug=False, use_reloader=False),
        daemon=True)
    t.start()
    print("[DASHBOARD] http://localhost:8080")

# ============================================================================
# SECTION 33 — ANALYTICS SUMMARY
# ============================================================================

def get_analytics_summary():
    cutoff = now_ts() - 7 * 86400
    try:
        row = _db().execute("""SELECT COUNT(*) AS total, SUM(views) AS views,
            SUM(likes) AS likes, SUM(comments) AS comments, SUM(shares) AS shares
            FROM posts WHERE posted_at > ?""", (cutoff,)).fetchone()
        totals = _row_to_dict(row) or {}
    except Exception: totals = {}
    try:
        recent = [_row_to_dict(r) for r in _db().execute("""
            SELECT coin, content_type, archetype, persona, quality_score,
                   trust_score, views, posted_at, link_status
            FROM posts ORDER BY posted_at DESC LIMIT 15""").fetchall()]
    except Exception: recent = []
    return {"totals": totals, "recent": recent,
            "narratives": narrative_engine.top(12),
            "trending_top": trending_hijacker.top(15),
            "generated_at": now_ts()}

# ============================================================================
# SECTION 34 — ADAPTIVE TUNER
# ============================================================================

def auto_tune():
    cutoff = now_ts() - 7 * 86400
    try:
        row = _db().execute("""SELECT COUNT(*) AS n, AVG(engagement_score) AS avg_eng,
            AVG(quality_score) AS avg_q FROM posts WHERE posted_at > ? AND views > 0""",
            (cutoff,)).fetchone()
    except Exception: return
    n = row["n"] or 0
    if n < 25: return
    avg_eng = row["avg_eng"] or 0; avg_q = row["avg_q"] or 0
    tune = state.setdefault("auto_tune", {})
    tune.setdefault("signal_threshold", SIGNAL_THRESHOLD)
    tune.setdefault("min_quality", MIN_POST_QUALITY)
    tune.setdefault("min_trust", MIN_TRUST_SCORE)
    if avg_eng < 200 and tune["signal_threshold"] < 80:
        tune["signal_threshold"] = min(85, tune["signal_threshold"] + 2)
    elif avg_eng > 800 and tune["signal_threshold"] > 58:
        tune["signal_threshold"] = max(58, tune["signal_threshold"] - 1)
    if avg_q < 62 and tune["min_quality"] < 74:
        tune["min_quality"] = min(74, tune["min_quality"] + 1)
    elif avg_q > 80 and tune["min_quality"] > 55:
        tune["min_quality"] = max(55, tune["min_quality"] - 1)
    try:
        low_trust = _db().execute("""SELECT COUNT(*) AS n FROM trust_scores
            WHERE trust_score < 0.55 AND computed_at > ?""", (cutoff,)).fetchone()["n"] or 0
        if low_trust > 5 and tune["min_trust"] < 70:
            tune["min_trust"] = min(70, tune["min_trust"] + 2)
    except Exception: pass
    print(f"[TUNE] thresh={tune['signal_threshold']} min_q={tune['min_quality']} "
          f"min_trust={tune['min_trust']}")

def _active_min_quality():
    return state.get("auto_tune", {}).get("min_quality", MIN_POST_QUALITY)

# ============================================================================
# SECTION 35 — V30 FLYWHEEL: MEME / SVG PERIODIC JOBS
# ============================================================================

def _flywheel_jobs():
    # SVG gainers heatmap every 30 min
    if now_ts() - state.get("last_svg_ts", 0) > 1800:
        try:
            tickers = get_tickers_24h()
            top = sorted(
                [{"symbol": t["symbol"].replace("USDT",""),
                  "change": safe_float(t.get("priceChangePercent"))}
                 for t in tickers if t.get("symbol","").endswith("USDT")
                 and safe_float(t.get("quoteVolume")) > 5_000_000],
                key=lambda x: -x["change"])[:5]
            if top:
                svg_renderer.render_gainers_heatmap(top, "Top 5 Gainers Today")
            state["last_svg_ts"] = now_ts(); save_state()
        except Exception as e: print(f"[SVG] periodic: {e}")

    # Meme generation every 60 min if a hot event exists
    if now_ts() - state.get("last_meme_ts", 0) > 3600:
        try:
            events = x_firehose.poll()[:5]
            if events:
                ev = random.choice(events)
                lang = random.choice(MEME_LANGS)
                meme = meme_engineer.generate(ev["text"], lang)
                if meme:
                    print(f"[MEME] ready: {meme['caption'][:120]}")
            state["last_meme_ts"] = now_ts(); save_state()
        except Exception as e: print(f"[MEME] periodic: {e}")

    # Paper trade exit checks every 5 min
    if now_ts() - state.get("last_paper_trade_check", 0) > 300:
        try:
            paper_bridge.check_tp_sl()
            state["last_paper_trade_check"] = now_ts()
        except Exception as e: print(f"[PAPER] periodic: {e}")

# ============================================================================
# SECTION 36 — BACKGROUND WORK + MAIN CYCLE
# ============================================================================

def _bg_work():
    try: refresh_sentiment()
    except Exception: pass
    try: update_active_signals()
    except Exception: pass
    try: trending_hijacker.refresh()
    except Exception: pass
    try: engagement_optimizer.tick()
    except Exception: pass
    try: language_arbitrage.tick()
    except Exception as e: print(f"[L5] {e}")
    try: swarm_cross.tick()
    except Exception as e: print(f"[L4] {e}")
    try: _flywheel_jobs()
    except Exception as e: print(f"[FLYWHEEL] {e}")

    if now_ts() - state.get("last_narrative_refresh", 0) > NARRATIVE_REFRESH_SECONDS:
        try:
            _IO_POOL.submit(narrative_engine.refresh)
            state["last_narrative_refresh"] = now_ts()
        except Exception: pass
    if now_ts() - state.get("last_feed_scan", 0) > 900:
        try:
            _IO_POOL.submit(feed_learner.scan)
            state["last_feed_scan"] = now_ts()
        except Exception: pass
    if now_ts() - state.get("last_learn_ts", 0) > 1800:
        try:
            state["learned_weights"] = learn_from_analytics()
            state["last_learn_ts"] = now_ts(); save_state()
        except Exception: pass
    if now_ts() - state.get("last_tune_ts", 0) > 3600:
        try:
            auto_tune(); state["last_tune_ts"] = now_ts(); save_state()
        except Exception: pass
    if now_ts() - state.get("last_discriminator_train", 0) > 6 * 3600:
        try:
            local_discriminator.train()
            state["last_discriminator_train"] = now_ts(); save_state()
        except Exception as e: print(f"[L3] train: {e}")
    if BAYES_TUNE_ENABLED and now_ts() - state.get("last_bayes_ts", 0) > 3600 * 4:
        try:
            bayes_tuner.tune_signal_threshold()
            state["last_bayes_ts"] = now_ts(); save_state()
        except Exception as e: print(f"[BAYES] {e}")
    # A/B winners every 6h
    if now_ts() - state.get("last_ab_decide", 0) > 6 * 3600:
        try:
            ab_tester.decide_winners()
            state["last_ab_decide"] = now_ts(); save_state()
        except Exception as e: print(f"[AB] {e}")

def _interval_ok(last_key, interval, jitter=0):
    last = state.get(last_key, 0)
    if not last: return True
    j = random.uniform(-jitter, jitter) if jitter else 0
    return now_ts() - last >= (interval + j)

def run_cycle():
    print("\n" + "=" * 60)
    print(f"[CYCLE] {datetime.now(timezone.utc).isoformat()}")
    print("=" * 60)
    _bg_work()

    # Fast lane: X firehose or News
    try:
        if news_layer.publish_if_worthy():
            print("[CASCADE] picked=news (fast lane)"); return
    except Exception as e: print(f"[NEWS] {e}")

    reset_square_counter()
    daily_posts = state.get("daily_content_counter", {}) or {}
    total_posts_today = sum(daily_posts.values())
    elastic_target = int(round(DAILY_POST_TARGET * content_scheduler._elastic_mult()))
    if total_posts_today >= elastic_target:
        print(f"[BUDGET] Daily target reached ({total_posts_today}/{elastic_target}). Reply-only.")
        if _interval_ok("last_reply_ts", MIN_REPLY_INTERVAL_SECONDS):
            if engagement_optimizer.seed_from_reply_queue():
                state["last_reply_ts"] = now_ts()
                state["metrics"]["replies_sent"] = \
                    state["metrics"].get("replies_sent", 0) + 1
                save_state()
        return

    if not _interval_ok("last_post_ts", MIN_POST_INTERVAL_SECONDS, POST_INTERVAL_JITTER_SECONDS):
        wait = MIN_POST_INTERVAL_SECONDS - (now_ts() - state.get("last_post_ts", 0))
        print(f"[SLOT] Post slot closed — {wait:.0f}s to next."); return

    plan = content_scheduler.plan()
    if not plan:
        print("[PLAN] No content types due."); return
    print(f"[PLAN] top choices: {[p[0] for p in plan[:5]]}")

    for content_type, weight, narrative in plan:
        try:
            if content_type == "signal":
                if not pipeline._try_signal(): continue
                print(f"[CASCADE] picked=signal w={weight:.2f}"); return
            elif content_type == "news":
                if not news_layer.publish_if_worthy(): continue
                print(f"[CASCADE] picked=news w={weight:.2f}"); return
            elif content_type == "diary":
                if not pipeline.publish_diary(): continue
                print(f"[CASCADE] picked=diary w={weight:.2f}"); return
            elif content_type == "curator":
                if not pipeline.publish_curator(): continue
                print(f"[CASCADE] picked=curator w={weight:.2f}"); return
            elif content_type == "educational":
                if not pipeline.publish_educational(): continue
                print(f"[CASCADE] picked=educational w={weight:.2f}"); return
            elif content_type == "trade_plan":
                if not pipeline._try_signal(): continue
                print(f"[CASCADE] picked=trade_plan w={weight:.2f}"); return
            elif content_type in ("market","coin_analysis","gainers","whale",
                                  "onchain","quiz","recap","result"):
                continue
        except Exception as e:
            print(f"[CASCADE] {content_type}: {e}")
            continue

    print("[SCHED] Nothing due this cycle.")
    save_state()

def _try_signal():
    try: candidates = build_candidates()
    except Exception as e: print(f"[SIG] {e}"); return False
    ls_streak = state.get("consecutive_long", 0)
    ss_streak = state.get("consecutive_short", 0)
    for c in candidates[:60]:
        if c["symbol"] in state["posted_coins"]:
            last = state["posted_coins"][c["symbol"]]
            if now_ts() - int(last) < 3 * 3600: continue
        try:
            sig = build_signal(c, relaxed=False)
            if not sig: continue
            if sig["direction"] == "LONG" and ls_streak >= 2: continue
            if sig["direction"] == "SHORT" and ss_streak >= 2: continue
            if pipeline.publish_signal(sig): return True
        except Exception as e:
            print(f"[SIG] {c['symbol']}: {e}")
    return False
pipeline._try_signal = _try_signal

# ============================================================================
# SECTION 37 — STARTUP + MAIN
# ============================================================================

def startup_check():
    print("=" * 60)
    print("SuperClaw 30.0 — APEX + HUMANIZER + FLYWHEEL EDITION")
    print("=" * 60)
    print(f"[CONFIG] Node: {SWARM_NODE_ID} | archetype: {swarm_binding.binding['archetype']}")
    print(f"[CONFIG] Publish path: BROWSER (humanized) with API fallback")
    print(f"[CONFIG] Square API key: {'set' if BINANCE_SQUARE_OPENAPI_KEY else 'NOT SET'}")
    print(f"[CONFIG] AI providers: {sum(1 for p in PROVIDERS if p['keys'])}")
    print(f"[CONFIG] Daily post target: {DAILY_POST_TARGET} (elastic)")
    print(f"[CONFIG] A1/A2 cap: {ARCHETYPE_A1A2_CAP:.0%}")
    print(f"[CONFIG] Min trust: {MIN_TRUST_SCORE}/100")
    print(f"[CONFIG] L5 language arbitrage: "
          f"{'ON' if LANG_ARBITRAGE_ENABLED else 'OFF'} ({','.join(LANG_TARGETS)})")
    print(f"[CONFIG] L6 elastic limits: {'ON' if DYNAMIC_ELASTIC_ENABLED else 'OFF'}")
    print(f"[CONFIG] X firehose: {'ON' if X_FIREHOSE_ENABLED else 'OFF'}")
    print(f"[CONFIG] On-chain whales: {'ON' if ONCHAIN_RPC_ENABLED else 'OFF'} "
          f"({len(WHALE_WALLETS)} wallets)")
    print(f"[CONFIG] Swarm peers: {len(SWARM_PEER_NODES)}")
    print(f"[CONFIG] Paper trades: {'ON' if paper_bridge.enabled else 'OFF'}")
    print(f"[CONFIG] SVG: {'ON' if SVG_RENDER_ENABLED else 'OFF'}")
    print(f"[CONFIG] Memes: {'ON' if MEME_ENABLED else 'OFF'} ({','.join(MEME_LANGS)})")
    print(f"[CONFIG] Humanizer: WPM {HUMAN_WPM_LO}-{HUMAN_WPM_HI}, "
          f"typo_rate={HUMAN_TYPO_RATE}")
    print(f"[CONFIG] Browser: {'headless' if RUN_IN_BACKGROUND else 'offscreen'}, "
          f"debug port {DEBUG_PORT}")
    print("=" * 60)
    if not AI_AVAILABLE:
        print("[FATAL] No AI providers configured."); raise SystemExit(9)
    if not DISCORD_WEBHOOK_URL:
        print("[WARN] DISCORD_WEBHOOK_URL not set — secondary channel off.")

def boot():
    print("\n[BOOT] Initial intelligence pass…")
    try: trending_hijacker.refresh(force=True)
    except Exception: pass
    try: refresh_sentiment()
    except Exception: pass
    try: x_firehose.poll()
    except Exception: pass
    try: onchain_whales.poll()
    except Exception: pass
    try: narrative_engine.refresh(force=True)
    except Exception: pass
    try: feed_learner.scan()
    except Exception: pass

def main():
    startup_check()
    init_analytics_db()
    print("\n[BOOT] Launching browser…")
    launch_browser_if_needed()
    start_dashboard_thread()
    boot()

    while True:
        started = time.time()
        try: run_cycle()
        except KeyboardInterrupt:
            print("\n[EXIT] Stopped by user."); break
        except SystemExit: raise
        except Exception as e: print(f"[MAIN] Unhandled: {e}")
        elapsed = time.time() - started
        sleep_time = max(5, SCAN_INTERVAL_SECONDS - elapsed)
        try: time.sleep(sleep_time)
        except KeyboardInterrupt: print("\n[EXIT] Stopped."); break

if __name__ == "__main__":
    main()
