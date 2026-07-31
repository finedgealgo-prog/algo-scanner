"""
algotest_option_chain_sync.py
──────────────────────────────
Backfills NIFTY/BANKNIFTY/... option-chain history into MongoDB by replaying
prices.algotest.in's own `/option-chain` endpoint minute-by-minute over a
date range, instead of computing/storing Greeks ourselves like
algo.trade/option_chain_backfill.py (Kite path) does.

Why this exists
────────────────
AlgoTest's endpoint already returns close, open-interest and implied vol per
strike — Greeks (delta/gamma/theta/vega/rho) come back too, but we don't
store them: they're cheap to recompute on read (Black-Scholes from
close+strike+spot+expiry+iv) and dropping them keeps ~40% of columns out of
every one of the ~1,600 strike/expiry/type rows a single minute produces, ×
375 minutes/day × N days. Re-derive via delta_selector.py's Greeks helpers
(or a fresh Black-Scholes call) at read time if a caller ever needs them.

What it writes
──────────────
1. option_chain_index_spot     — one row per (underlying, timestamp).
     NIFTY → token "NSE_01" (same mapping option_chain_backfill.py uses).
     India VIX is written too, as underlying="INDIAVIX", token "NSE_00".
2. option_chain_index_futures  — one row per (underlying, expiry, timestamp).
     `close` = the real traded future price (only present for the near
     expiries AlgoTest lists under "futures"); `implied_close` = the
     put-call-parity forward price AlgoTest derives for every expiry
     ("implied_futures"). Either can be null if the source didn't have it.
3. option_chain_historical_data — one row per
     (underlying, expiry, strike, type, timestamp).
     fields: close, oi, iv (percent, e.g. 20.25 not 0.2025), token.
     No open/high/low/volume — AlgoTest's endpoint is a point-in-time
     snapshot (no OHLC), and it does not return traded volume at all, only
     open interest. `volume` is still included as null so the document
     shape matches what other collections in this DB expect.
     `token` is a synthetic, deterministic string
     "AT_<underlying>_<expiry>_<strike>_<type>" (prefixed "AT_" so it can
     never collide with an actual numeric Kite instrument token already
     sitting in this same collection from option_chain_backfill.py) — same
     token for the same (underlying, expiry, strike, type) on every call,
     which is what makes it usable as a stable series key.

Auth
────
prices.algotest.in is a logged-in-session endpoint (cookie + CSRF header),
not a public API — there's no key to rotate, just a browser session that
expires (~72h, see access_token_cookie's JWT `exp`). Credentials live in
shared/.env as ALGOTEST_COOKIE / ALGOTEST_CSRF_TOKEN (gitignored). When this
script starts failing with "auth expired", refresh both from a fresh
DevTools → Network → copy-as-cURL on algotest.in.

Rate limiting & resumability
─────────────────────────────
- One request every 0.6s minimum (~100/min); on 429 or a rate-limit-looking
  error body, backs off (5s, 10s, 15s, ...) and retries the SAME minute
  rather than skipping it.
- Resumability is two-tier, one query per day either way:
    1. Fully-synced day → 0 API calls. One indexed range query against
       option_chain_index_spot returns every minute already written for
       that date; if that covers all ~375 trading minutes, the day is
       skipped entirely.
    2. Partially-synced day (e.g. a run that died halfway through) → only
       the minutes NOT in that set get fetched; minutes already present
       are neither re-fetched nor re-written.
  Pass --force to ignore existing rows and re-fetch every minute in range.
- Within a day, fetched minutes are held in memory and written with ONE
  insert_many per collection at the end of the day (not an upsert per
  minute) — far fewer DB round trips, which is where most of the wall-clock
  time was going. Each collection has a unique index on its natural key
  (underlying+expiry+strike+type+timestamp for the chain, underlying+
  timestamp for spot, underlying+expiry+timestamp for futures); the insert
  swallows duplicate-key (11000) errors only, so re-running after a crash
  can't double-insert a minute that already made it in. Non-duplicate write
  errors still raise.
- Weekends are always skipped. Trading holidays are skipped too, using
  whatever's in the market_holidays collection (mongo_data.py) — if that
  collection is empty, only weekends are skipped.

Usage
─────
    cd /media/ashok-innoppl/7CD60970D6092C48/algo-backend/algo.scanner
    python algotest_option_chain_sync.py NIFTY 2026-06-01 2026-06-30
    python algotest_option_chain_sync.py NIFTY 2026-06-01 2026-06-30 --force
"""
from __future__ import annotations

import argparse
import logging
import pathlib
import sys
import time
from datetime import datetime, timedelta
from threading import Lock
from typing import Any

import requests
from dotenv import load_dotenv

load_dotenv(pathlib.Path(__file__).resolve().parent / ".env")

import os  # noqa: E402

log = logging.getLogger("algotest_sync")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ── config ─────────────────────────────────────────────────────────────────
BASE_URL = "https://prices.algotest.in/option-chain"
MIN_REQUEST_INTERVAL_S = 0.6           # ~100 req/min cap
MARKET_OPEN  = "09:15"
MARKET_CLOSE = "15:29"                 # last 1-min bar of the trading day
MAX_RATE_LIMIT_RETRIES = 8
MAX_TRANSIENT_RETRIES  = 4

# Same underlying → spot-token mapping used by algo.trade/option_chain_backfill.py,
# duplicated here since algo.scanner does not import algo.trade modules.
NSE_TOKEN = {
    "NIFTY":      "NSE_01",
    "INDIAVIX":   "NSE_00",
    "SENSEX":     "NSE_02",
    "BANKNIFTY":  "NSE_03",
    "BANKEX":     "NSE_04",
    "FINNIFTY":   "NSE_05",
    "MIDCPNIFTY": "NSE_06",
}


# ── rate limiter ─────────────────────────────────────────────────────────────
class _RateLimiter:
    def __init__(self, min_interval_s: float):
        self._interval = min_interval_s
        self._lock = Lock()
        self._next_allowed = 0.0

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            wait = self._next_allowed - now
            if wait > 0:
                time.sleep(wait)
            self._next_allowed = time.monotonic() + self._interval


_rate_limiter = _RateLimiter(MIN_REQUEST_INTERVAL_S)


# ── HTTP session ───────────────────────────────────────────────────────────
def _build_session() -> requests.Session:
    cookie = os.getenv("ALGOTEST_COOKIE", "").strip()
    csrf   = os.getenv("ALGOTEST_CSRF_TOKEN", "").strip()
    if not cookie or not csrf:
        raise RuntimeError(
            "ALGOTEST_COOKIE / ALGOTEST_CSRF_TOKEN missing from shared/.env — "
            "log into algotest.in, copy the option-chain request as cURL from "
            "DevTools → Network, and set both values."
        )
    session = requests.Session()
    session.headers.update({
        "accept": "application/json, text/plain, */*",
        "origin": "https://algotest.in",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "x-csrf-token-access": csrf,
        "cookie": cookie,
    })
    return session


def _is_rate_limited(resp: requests.Response) -> bool:
    if resp.status_code == 429:
        return True
    body = resp.text.lower()
    return "rate limit" in body or "too many requests" in body


def _is_auth_error(resp: requests.Response) -> bool:
    return resp.status_code in (401, 403)


def _fetch_option_chain(session: requests.Session, underlying: str, candle_ts: str) -> dict[str, Any] | None:
    """
    GET one minute's snapshot. Retries on rate-limit (same minute, backing off)
    and on transient errors (few retries then gives up on this minute only).
    Raises RuntimeError immediately on auth failure — no point retrying that.
    """
    url = f"{BASE_URL}?underlying={underlying}&candle={candle_ts}"
    rate_limit_attempt = 0
    transient_attempt = 0

    while True:
        _rate_limiter.acquire()
        try:
            resp = session.get(url, timeout=15)
        except requests.RequestException as exc:
            transient_attempt += 1
            if transient_attempt > MAX_TRANSIENT_RETRIES:
                log.warning("[%s %s] network error after %d attempts: %s", underlying, candle_ts, transient_attempt, exc)
                return None
            time.sleep(1.0 * transient_attempt)
            continue

        if _is_auth_error(resp):
            raise RuntimeError(
                f"AlgoTest session rejected the request ({resp.status_code}). "
                "Cookie/CSRF token has expired — refresh ALGOTEST_COOKIE / "
                "ALGOTEST_CSRF_TOKEN in shared/.env and re-run (it will resume "
                "from this minute, not from the start)."
            )

        if _is_rate_limited(resp):
            rate_limit_attempt += 1
            if rate_limit_attempt > MAX_RATE_LIMIT_RETRIES:
                raise RuntimeError(
                    f"AlgoTest kept rate-limiting after {rate_limit_attempt} backed-off "
                    f"retries at {candle_ts}. Stopping — re-run later to resume from here."
                )
            sleep_s = 5.0 * rate_limit_attempt
            log.warning("[%s %s] rate-limited, sleeping %.0fs (attempt %d)", underlying, candle_ts, sleep_s, rate_limit_attempt)
            time.sleep(sleep_s)
            continue

        if resp.status_code != 200:
            transient_attempt += 1
            if transient_attempt > MAX_TRANSIENT_RETRIES:
                log.warning("[%s %s] HTTP %d after %d attempts, skipping minute", underlying, candle_ts, resp.status_code, transient_attempt)
                return None
            time.sleep(1.0 * transient_attempt)
            continue

        try:
            return resp.json()
        except ValueError:
            log.warning("[%s %s] non-JSON response, skipping minute", underlying, candle_ts)
            return None


# ── token + parsing ──────────────────────────────────────────────────────────
def _fmt_strike(strike: float) -> str:
    return str(int(strike)) if float(strike).is_integer() else str(strike)


def _make_token(underlying: str, expiry: str, strike: float, opt_type: str) -> str:
    return f"AT_{underlying}_{expiry}_{_fmt_strike(strike)}_{opt_type}"


def _parse_response(raw: dict[str, Any], underlying: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Returns (spot_docs, futures_docs, chain_docs)."""
    ts = str(raw.get("candle") or "")[:19]
    if not ts:
        return [], [], []

    spot_docs: list[dict] = []
    cash = raw.get("cash") or {}
    if cash.get("close") is not None:
        spot_docs.append({
            "underlying": underlying, "timestamp": ts,
            "token": NSE_TOKEN.get(underlying, f"SPOT_{underlying}"),
            "close": float(cash["close"]), "spot_price": float(cash["close"]),
            "source": "algotest_api",
        })
    vix = raw.get("vix") or {}
    if vix.get("close") is not None:
        spot_docs.append({
            "underlying": "INDIAVIX", "timestamp": ts,
            "token": NSE_TOKEN["INDIAVIX"],
            "close": float(vix["close"]), "spot_price": float(vix["close"]),
            "source": "algotest_api",
        })

    futures_docs: list[dict] = []
    real_futures    = raw.get("futures") or {}
    implied_futures = raw.get("implied_futures") or {}
    for expiry in set(real_futures) | set(implied_futures):
        real    = real_futures.get(expiry) or {}
        implied = implied_futures.get(expiry)
        real_close = real.get("close")
        if real_close is None and implied is None:
            continue
        futures_docs.append({
            "underlying": underlying, "expiry": expiry[:10], "timestamp": ts,
            "close": float(real_close) if real_close is not None else None,
            "implied_close": float(implied) if implied is not None else None,
            "source": "algotest_api",
        })

    chain_docs: list[dict] = []
    options = raw.get("options") or {}
    for expiry, cols in options.items():
        strikes = cols.get("strike") or []
        for i, strike in enumerate(strikes):
            for side, close_key, oi_key, iv_key in (
                ("CE", "call_close", "call_open_interest", "call_implied_vol"),
                ("PE", "put_close",  "put_open_interest",  "put_implied_vol"),
            ):
                close = (cols.get(close_key) or [None] * len(strikes))[i]
                if close is None:
                    continue
                oi_list = cols.get(oi_key) or []
                iv_list = cols.get(iv_key) or []
                oi = oi_list[i] if i < len(oi_list) else None
                iv_raw = iv_list[i] if i < len(iv_list) else None
                chain_docs.append({
                    "underlying": underlying, "expiry": expiry[:10],
                    "strike": float(strike), "type": side, "timestamp": ts,
                    "token": _make_token(underlying, expiry[:10], float(strike), side),
                    "close": float(close),
                    "oi": int(oi) if oi is not None else None,
                    "iv": round(float(iv_raw) * 100, 4) if iv_raw else None,
                    "volume": None,
                    "source": "algotest_api",
                })

    return spot_docs, futures_docs, chain_docs


# ── DB writes ──────────────────────────────────────────────────────────────
def _ensure_indexes(db) -> None:
    from pymongo import ASCENDING
    db._db["option_chain_historical_data"].create_index(
        [("underlying", ASCENDING), ("expiry", ASCENDING), ("strike", ASCENDING),
         ("type", ASCENDING), ("timestamp", ASCENDING)],
        unique=True, background=True, name="chain_natural_key_uq",
    )
    db._db["option_chain_index_spot"].create_index(
        [("underlying", ASCENDING), ("timestamp", ASCENDING)],
        unique=True, background=True, name="spot_underlying_timestamp_uq",
    )
    db._db["option_chain_index_futures"].create_index(
        [("underlying", ASCENDING), ("expiry", ASCENDING), ("timestamp", ASCENDING)],
        unique=True, background=True, name="futures_underlying_expiry_timestamp_uq",
    )


def _existing_minutes_for_day(db, underlying: str, date_str: str) -> set[str]:
    """
    One indexed range query per day → set of "YYYY-MM-DDTHH:MM" minutes already
    written to option_chain_index_spot for this underlying/date. A day where this
    covers every trading minute is fully synced and skipped with zero API calls;
    a partially-synced day (e.g. a run that died halfway through) only re-fetches
    the minutes still missing, instead of either re-fetching the whole day or
    wrongly treating it as done.
    """
    docs = db._db["option_chain_index_spot"].find(
        {"underlying": underlying,
         "timestamp": {"$gte": f"{date_str}T00:00:00", "$lte": f"{date_str}T23:59:59"}},
        {"_id": 0, "timestamp": 1},
    )
    return {d["timestamp"][:16] for d in docs}


def _bulk_insert_ignore_dupes(collection, docs: list[dict]) -> int:
    """insert_many(ordered=False), swallowing duplicate-key (11000) errors only.
    Returns the number of docs actually inserted (best-effort)."""
    if not docs:
        return 0
    from pymongo.errors import BulkWriteError
    try:
        collection.insert_many(docs, ordered=False)
        return len(docs)
    except BulkWriteError as exc:
        write_errors = exc.details.get("writeErrors", [])
        non_dupe = [e for e in write_errors if e.get("code") != 11000]
        if non_dupe:
            raise
        return len(docs) - len(write_errors)


# ── trading calendar ─────────────────────────────────────────────────────────
def _trading_days(start_date: str, end_date: str, holidays: set[str]):
    day = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    while day <= end:
        date_str = day.strftime("%Y-%m-%d")
        if day.weekday() < 5 and date_str not in holidays:
            yield date_str
        day += timedelta(days=1)


def _minutes_for_day(date_str: str):
    open_h, open_m   = (int(x) for x in MARKET_OPEN.split(":"))
    close_h, close_m = (int(x) for x in MARKET_CLOSE.split(":"))
    base = datetime.strptime(date_str, "%Y-%m-%d")
    cur  = base.replace(hour=open_h, minute=open_m, second=0, microsecond=0)
    last = base.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    while cur <= last:
        yield cur.strftime("%Y-%m-%dT%H:%M")
        cur += timedelta(minutes=1)


# ── main driver ─────────────────────────────────────────────────────────────
def sync_range(underlying: str, start_date: str, end_date: str, *, force: bool = False) -> dict:
    from features.mongo_data import MongoData

    underlying = underlying.upper()
    db = MongoData()
    session = _build_session()
    _ensure_indexes(db)
    holidays = db.get_holidays()

    days = list(_trading_days(start_date, end_date, holidays))
    log.info("[%s] syncing %s → %s: %d trading days (force=%s)", underlying, start_date, end_date, len(days), force)

    days_synced = days_skipped = 0
    total_spot = total_futures = total_chain = 0
    failed_minutes: list[str] = []

    aborted = False
    for day_str in days:
        if aborted:
            break

        minutes = list(_minutes_for_day(day_str))
        existing = _existing_minutes_for_day(db, underlying, day_str) if not force else set()
        if existing and len(existing) >= len(minutes):
            days_skipped += 1
            log.info("[%s] %s already fully synced (%d/%d minutes), skipping day",
                      underlying, day_str, len(existing), len(minutes))
            continue
        if existing:
            log.info("[%s] %s partially synced (%d/%d minutes) — resuming remaining minutes",
                      underlying, day_str, len(existing), len(minutes))

        day_spot: list[dict] = []
        day_futures: list[dict] = []
        day_chain: list[dict] = []
        day_failed = 0

        for minute_ts in minutes:
            if minute_ts in existing:
                continue
            try:
                raw = _fetch_option_chain(session, underlying, minute_ts)
            except RuntimeError as exc:
                log.error("[%s] stopping (persistent auth/rate-limit failure): %s", underlying, exc)
                aborted = True
                break

            if raw is None:
                day_failed += 1
                failed_minutes.append(minute_ts)
                log.warning("[%s] %s no data returned", underlying, minute_ts)
                continue

            spot_docs, futures_docs, chain_docs = _parse_response(raw, underlying)
            if spot_docs or futures_docs or chain_docs:
                day_spot.extend(spot_docs)
                day_futures.extend(futures_docs)
                day_chain.extend(chain_docs)
                log.info("[%s] %s fetched  (spot=%d futures=%d options=%d)",
                          underlying, minute_ts, len(spot_docs), len(futures_docs), len(chain_docs))
            else:
                day_failed += 1
                failed_minutes.append(minute_ts)
                log.warning("[%s] %s empty response", underlying, minute_ts)

        # Flush whatever was fetched for this day even if aborted partway through —
        # real fetched data shouldn't be thrown away just because the run is stopping.
        n_chain   = _bulk_insert_ignore_dupes(db._db["option_chain_historical_data"], day_chain)
        n_futures = _bulk_insert_ignore_dupes(db._db["option_chain_index_futures"], day_futures)
        n_spot    = _bulk_insert_ignore_dupes(db._db["option_chain_index_spot"], day_spot)

        total_spot += n_spot
        total_futures += n_futures
        total_chain += n_chain
        if not aborted:
            days_synced += 1
        log.info("[%s] %s done — inserted spot=%d futures=%d options=%d (failed minutes=%d)%s",
                  underlying, day_str, n_spot, n_futures, n_chain, day_failed,
                  " [ABORTED — re-run to resume]" if aborted else "")

    summary = {
        "underlying": underlying, "start": start_date, "end": end_date,
        "days_synced": days_synced, "days_skipped": days_skipped,
        "rows_inserted": {"spot": total_spot, "futures": total_futures, "chain": total_chain},
        "failed_minutes": failed_minutes,
    }
    log.info("[%s] ALL DONE: %s", underlying, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("underlying", help="e.g. NIFTY, BANKNIFTY, FINNIFTY, SENSEX")
    parser.add_argument("start_date", help="YYYY-MM-DD")
    parser.add_argument("end_date", help="YYYY-MM-DD")
    parser.add_argument("--force", action="store_true",
                         help="Re-fetch minutes even if already present in option_chain_index_spot")
    args = parser.parse_args()

    try:
        sync_range(args.underlying, args.start_date, args.end_date, force=args.force)
    except KeyboardInterrupt:
        log.warning("Interrupted — already-written minutes are safe; re-run the same command to resume.")
        sys.exit(1)


if __name__ == "__main__":
    main()
