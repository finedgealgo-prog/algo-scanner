"""stock_historical/service.py
────────────────────────────────
Given one stock symbol, fetch straight from Dhan's live REST APIs — nothing
is written to Mongo here, every call below hits Dhan and the result is
handed back to the caller as-is:

  equity          — daily OHLCV for the requested N years, via
                    /v2/charts/historical, chunked into <=365-day windows
                    (Dhan's per-call limit for that endpoint).
  future          — daily OHLCV for every currently-listed FUTSTK contract
                    of this stock, for that contract's own lifetime.
  option_history  — ATM call + put daily OHLCV across the requested N years
                    (capped at 5 — Dhan's stated limit), via Dhan's "Expired
                    Options Data" endpoint (/v2/charts/rollingoption). This
                    is the one Dhan endpoint that actually returns
                    multi-year option data — it's keyed off the
                    *underlying's* security id + a relative ATM strike, not
                    the long-expired contract's own (no longer listed)
                    security id, which is why it can reach back years where
                    the option-chain/scrip-master approach can't.
  option_chain    — the CURRENT live OPTSTK chain (every strike, both
                    CE/PE, greeks, security ids) for the nearest expiry, via
                    /v2/optionchain — a live snapshot, not history.

Why futures can't go back years
────────────────────────────────
Dhan's scrip master CSV only lists currently-tradeable contracts (a stock's
FUTSTK typically has 1-3 live months at any time), and unlike options there
is no "expired futures data" endpoint on Dhan — /v2/charts/rollingoption is
options-only. Once a futures contract expires, its security id drops out of
the scrip master and its historical data becomes unreachable. So `future`
below is honestly scoped to "every contract currently listed," not N years.

Rate limiting
─────────────
Every POST in this module — whether it's common.historical_data's own
post_dhan_chart_request (equity/future, via fetch_dhan_daily_candles) or this
module's own _dhan_post_with_retry (rollingoption/optionchain/expirylist,
which additionally need a client-id header that the /v2/charts/* endpoints
don't) — funnels onto features.broker_gateway.wait_for_dhan_slot(), the same
shared, account-wide ~1 req/sec clock every other Dhan REST caller in this
process already uses (live quotes, tickers, the scanner's own historical
sync). Both retry with backoff on 429 / DH-904 / DH-905 instead of failing
the request outright. That's what keeps this module inside Dhan's rate limit
even when it ends up firing off dozens of contract requests for one symbol.
"""

from __future__ import annotations

import csv
import io
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any

import requests

from common.historical_data import (
    DHAN_HISTORICAL_CHUNK_DAYS,
    aggregate_intraday_candles,
    fetch_dhan_daily_candles,
)
from features.broker_gateway import wait_for_dhan_slot
from features.mongo_data import MongoData

DHAN_SCRIP_MASTER_URL = "https://images.dhan.co/api-data/api-scrip-master.csv"
DHAN_ROLLINGOPTION_URL = "https://api.dhan.co/v2/charts/rollingoption"
DHAN_OPTIONCHAIN_URL = "https://api.dhan.co/v2/optionchain"
DHAN_OPTIONCHAIN_EXPIRYLIST_URL = "https://api.dhan.co/v2/optionchain/expirylist"

# Dhan's own docs say "up to 30 days per call", but live testing showed OPTSTK
# rollingoption requests get slow enough over a full 30-day/60-min-interval
# window to hit Dhan's own ~30s gateway timeout (empty response body, not even
# a proper error). 15 days stayed well under 10s per call in testing, so that's
# the real safe chunk size for this endpoint — not the documented 30.
ROLLINGOPTION_CHUNK_DAYS = 15
ROLLINGOPTION_MAX_YEARS = 5.0       # Dhan states "last 5 years" is the cap for this endpoint
ROLLINGOPTION_INTERVAL = "60"       # coarsest native interval — aggregated to daily below, keeps payload/requests small
ROLLINGOPTION_TIMEOUT = 45.0        # generous margin over the ~30s cliff observed above
MAX_STRIKE_OFFSET = 3               # Dhan's documented ATM+-3 cap for non-index (stock) contracts

# NSE stock futures are listed for (at most) 3 serial months, so a far-month
# contract's own first trading day is never more than ~95 days before its
# expiry. Requesting /v2/charts/historical from further back than that (e.g.
# the far-month contract using the same N-years-back from_date as equity)
# gets rejected outright with DH-905 "bad values for parameters" — confirmed
# by direct testing — since the security id simply didn't exist yet on that
# date. Clamping each contract's own from_date to its expiry minus this many
# days avoids ever asking for data from before the contract existed.
FUTSTK_MAX_LISTING_LEAD_DAYS = 95

# /v2/optionchain (live snapshot) and /v2/charts/rollingoption both 400 with
# "ClientId is invalid" unless client-id is sent alongside access-token —
# unlike /v2/charts/historical, which only ever needed access-token. Confirmed
# by direct testing against a real Dhan account.
_RATE_LIMIT_CODES = {"DH-904", "DH-905"}


def _dhan_post_with_retry(
    url: str,
    body: dict[str, Any],
    access_token: str,
    client_id: str,
    *,
    timeout: float = 30.0,
    _retry: int = 6,
) -> dict[str, Any]:
    """POST to a Dhan v2 endpoint that needs both access-token and client-id
    (unlike common.historical_data.post_dhan_chart_request's /v2/charts/*
    calls, which only need access-token) — retrying on rate-limit responses
    and on timeouts (the rollingoption endpoint is slow enough under load to
    occasionally hit its own gateway timeout, confirmed by direct testing).
    Funnels onto the same shared wait_for_dhan_slot() clock as every other
    Dhan REST caller in this process so this can't blow past Dhan's
    account-wide rate budget no matter how many contracts get fetched.
    """
    headers = {"access-token": access_token, "client-id": str(client_id), "Content-Type": "application/json"}
    extra_backoff = 0.0
    last_err_json: dict[str, Any] = {}
    for attempt in range(1, _retry + 1):
        wait_for_dhan_slot()
        if extra_backoff:
            time.sleep(extra_backoff)
        try:
            resp = requests.post(url, json=body, headers=headers, timeout=timeout)
        except requests.exceptions.Timeout:
            extra_backoff = min(8.0, extra_backoff * 2 or 2.0)
            continue
        if resp.status_code == 429:
            extra_backoff = min(8.0, extra_backoff * 2 or 2.0)
            continue
        if resp.status_code == 400:
            try:
                err_json = resp.json()
            except Exception:
                err_json = {}
            if err_json.get("errorCode", "") in _RATE_LIMIT_CODES:
                last_err_json = err_json
                extra_backoff = min(8.0, extra_backoff * 2 or 2.0)
                continue
            raise Exception(f"400 — {err_json}")
        if not resp.ok:
            try:
                err_detail = resp.json()
            except Exception:
                err_detail = resp.text[:200]
            raise Exception(f"{resp.status_code} {resp.reason} — {err_detail}")
        try:
            return resp.json()
        except Exception:
            # Empty/non-JSON body — the gateway-timeout failure mode seen in
            # testing when a rollingoption window ran too long. Treat like a
            # rate limit: back off and retry rather than surfacing a confusing
            # JSON-parse error.
            extra_backoff = min(8.0, extra_backoff * 2 or 2.0)
            continue
    raise Exception(f"Dhan request failed after {_retry} retries for url={url} — last error: {last_err_json}")


_SCRIP_MASTER_CACHE: dict[str, Any] = {}   # {"rows": [...], "date": "YYYY-MM-DD"}


def _get_dhan_scrip_master_rows() -> list[dict]:
    """Raw Dhan scrip master CSV rows, cached once per calendar day — same
    caching shape every other Dhan contract lookup in this codebase already
    uses (dhan_token_sync.py, algo.trade/api.py). Duplicated here rather than
    imported so this module has no import-time dependency on api.py/other
    services — same tradeoff features/candle_fetch.py already makes for its
    verbatim copy of common/historical_data.py.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _SCRIP_MASTER_CACHE.get("rows") and _SCRIP_MASTER_CACHE.get("date") == today_str:
        return _SCRIP_MASTER_CACHE["rows"]
    resp = requests.get(DHAN_SCRIP_MASTER_URL, timeout=30)
    resp.raise_for_status()
    rows = list(csv.DictReader(io.StringIO(resp.text)))
    _SCRIP_MASTER_CACHE["rows"] = rows
    _SCRIP_MASTER_CACHE["date"] = today_str
    return rows


def _load_dhan_credentials() -> tuple[str, str]:
    """(client_id, access_token) for Dhan, regardless of which broker the app
    currently has marked "active" — this module is Dhan-only by design, so it
    reads the Dhan row directly instead of gating on kite_market_config.enabled."""
    db = MongoData()._db
    cfg = db["kite_market_config"].find_one({"broker": "dhan"}) or {}
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    access_token = str(cfg.get("access_token") or "").strip()
    return client_id, access_token


def _resolve_symbol_contracts(symbol: str) -> dict[str, Any]:
    """One pass over the (day-cached) scrip master for everything this module
    needs about one stock: its NSE equity security id, plus every currently
    live FUTSTK/OPTSTK contract. Already-expired rows are skipped — Dhan
    won't serve historical data off their security id anyway (see module
    docstring), so there's no point carrying them forward."""
    today_str = datetime.now().strftime("%Y-%m-%d")
    deriv_types = {"OPTSTK", "OPTIDX", "FUTSTK", "FUTIDX", "FUTCUR", "OPTCUR", "FUTCOM", "OPTFUT"}
    equity_sec_id = ""
    futures: list[dict[str, Any]] = []
    options: list[dict[str, Any]] = []

    for row in _get_dhan_scrip_master_rows():
        if row.get("SEM_EXM_EXCH_ID", "").strip() != "NSE":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        row_symbol = (ts.split("-")[0].strip().upper() if "-" in ts else ts.strip().upper())
        if row_symbol != symbol:
            continue

        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        sec_id = row.get("SEM_SMST_SECURITY_ID", "").strip()

        if inst not in deriv_types:
            if sec_id and not equity_sec_id:
                equity_sec_id = sec_id
            continue

        if inst not in ("FUTSTK", "OPTSTK") or not sec_id:
            continue

        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry or expiry < today_str:
            continue

        if inst == "FUTSTK":
            futures.append({
                "sec_id": sec_id,
                "trading_symbol": ts,
                "expiry": expiry,
                "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
            })
        else:
            options.append({
                "sec_id": sec_id,
                "trading_symbol": ts,
                "expiry": expiry,
                "strike": float(row.get("SEM_STRIKE_PRICE") or 0),
                "option_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
                "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
            })

    futures.sort(key=lambda c: c["expiry"])
    options.sort(key=lambda c: (c["expiry"], c["strike"], c["option_type"]))
    return {"equity_sec_id": equity_sec_id, "futures": futures, "options": options}


def _fetch_dhan_daily_history_chunked(
    access_token: str,
    sec_id: str,
    exchange_segment: str,
    instrument: str,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    """Daily OHLCV across an arbitrarily long date range, stitched from
    /v2/charts/historical's <=365-day-per-call windows. Two boundary bugs
    confirmed by direct testing against a real Dhan account, both fixed here:

    1. DHAN_HISTORICAL_CHUNK_DAYS is 365, so the upper bound must be
       current_from + 364 days to keep the inclusive span at 365 days, not
       366 — a request spanning current_from..current_from+365d (366 days
       inclusive, e.g. years=1.0 landing on "today-365d" to "today" in one
       chunk) gets rejected outright with DH-905 "bad values for parameters."

    2. Dhan also rejects fromDate == toDate (a zero-width single-day
       request) with the same DH-905 code, different message ("no data
       present"). That exact case shows up as the *trailing* chunk whenever
       the total span happens to be a clean multiple of the chunk width
       (years=1, 2, 3, ... all hit this) — the loop below ends with
       current_from having walked up to exactly to_date. Stopping the loop
       at `current_from < to_date` (not `<=`) skips that degenerate final
       request instead of sending it; the cost is not fetching a candle for
       `to_date` itself when the math lines up that way, which in practice
       is always "today" — a day whose session usually isn't even closed
       yet, so there'd be no EOD candle to get regardless.

    Both mistake the resulting 400 for a transient rate limit and burn all 6
    retries in post_dhan_chart_request before giving up, which is what made
    this so slow to notice — every affected request looked like it was just
    stuck retrying, not permanently failing on a bad date range.
    """
    candles: list[dict[str, Any]] = []
    current_from = from_date
    while current_from < to_date:
        current_to = min(current_from + timedelta(days=DHAN_HISTORICAL_CHUNK_DAYS - 1), to_date)
        candles.extend(fetch_dhan_daily_candles(access_token, sec_id, exchange_segment, instrument, current_from, current_to))
        current_from = current_to + timedelta(days=1)
    return candles


def _strike_label(offset: int) -> str:
    if offset == 0:
        return "ATM"
    sign = "+" if offset > 0 else ""
    return f"ATM{sign}{offset}"


def _rolling_option_chunk(
    access_token: str,
    client_id: str,
    underlying_sec_id: str,
    strike_label: str,
    option_type: str,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    """One <=15-day window of minute-level ATM(+-N) option history from
    Dhan's Expired Options Data endpoint. expiryCode=0 ("near/current
    expiry") 400s on this endpoint with "expiryCode is required" — confirmed
    by direct testing, seemingly treated as falsy server-side — so 1 ("next
    expiry") is used instead; per Dhan's own docs this is resolved relative
    to the from/to date window itself (that's the whole point of an
    "expired options" endpoint), so walking expiryCode=1 across consecutive
    windows still rolls correctly through each month's contract in turn
    rather than pinning one fixed calendar expiry.
    """
    body = {
        "exchangeSegment": "NSE_FNO",
        "interval": ROLLINGOPTION_INTERVAL,
        "securityId": int(underlying_sec_id),
        "instrument": "OPTSTK",
        "expiryFlag": "MONTH",
        "expiryCode": 1,
        "strike": strike_label,
        "drvOptionType": "CALL" if option_type == "CE" else "PUT",
        "requiredData": ["open", "high", "low", "close", "volume"],
        "fromDate": from_date.strftime("%Y-%m-%d"),
        "toDate": to_date.strftime("%Y-%m-%d"),
    }
    data = _dhan_post_with_retry(DHAN_ROLLINGOPTION_URL, body, access_token, client_id, timeout=ROLLINGOPTION_TIMEOUT)
    side = "ce" if option_type == "CE" else "pe"
    payload = ((data or {}).get("data") or {}).get(side) or {}
    timestamps = payload.get("timestamp") or []
    opens = payload.get("open") or []
    highs = payload.get("high") or []
    lows = payload.get("low") or []
    closes = payload.get("close") or []
    volumes = payload.get("volume") or []
    return [
        {
            "date": datetime.utcfromtimestamp(timestamps[i]),
            "open": opens[i] if i < len(opens) else 0.0,
            "high": highs[i] if i < len(highs) else 0.0,
            "low": lows[i] if i < len(lows) else 0.0,
            "close": closes[i] if i < len(closes) else 0.0,
            "volume": volumes[i] if i < len(volumes) else 0,
        }
        for i in range(len(timestamps))
    ]


def _fetch_rolling_option_years(
    access_token: str,
    client_id: str,
    underlying_sec_id: str,
    strike_label: str,
    option_type: str,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    """Daily OHLCV for one (strike, CE/PE) series across the full requested
    range — stitched from Dhan's rollingoption windows (see
    ROLLINGOPTION_CHUNK_DAYS) and aggregated from 60-min bars down to daily
    (same aggregate-then-bucket approach fetch_dhan_daily_index_candles_cached
    already uses for Dhan index daily bars). Loop bound is `< to_date`, not
    `<=`, for the same reason as _fetch_dhan_daily_history_chunked above —
    Dhan 400s on a zero-width fromDate==toDate request, which a `<=` bound
    produces whenever the span is an exact multiple of the chunk width."""
    minute_candles: list[dict[str, Any]] = []
    current_from = from_date
    while current_from < to_date:
        current_to = min(current_from + timedelta(days=ROLLINGOPTION_CHUNK_DAYS - 1), to_date)
        minute_candles.extend(_rolling_option_chunk(
            access_token, client_id, underlying_sec_id, strike_label, option_type, current_from, current_to,
        ))
        current_from = current_to + timedelta(days=1)
    minute_candles.sort(key=lambda c: c["date"])
    return aggregate_intraday_candles(minute_candles, factor=1000)


def _get_expiry_list(access_token: str, client_id: str, underlying_sec_id: str) -> list[str]:
    data = _dhan_post_with_retry(
        DHAN_OPTIONCHAIN_EXPIRYLIST_URL,
        {"UnderlyingScrip": int(underlying_sec_id), "UnderlyingSeg": "NSE_EQ"},
        access_token, client_id,
    )
    return list((data or {}).get("data") or [])


def _get_option_chain_snapshot(access_token: str, client_id: str, underlying_sec_id: str, expiry: str) -> dict[str, Any]:
    data = _dhan_post_with_retry(
        DHAN_OPTIONCHAIN_URL,
        {"UnderlyingScrip": int(underlying_sec_id), "UnderlyingSeg": "NSE_EQ", "Expiry": expiry},
        access_token, client_id,
    )
    payload = (data or {}).get("data") or {}
    oc = payload.get("oc") or {}
    strikes = [
        {"strike": float(strike_str), "CE": sides.get("ce"), "PE": sides.get("pe")}
        for strike_str, sides in sorted(oc.items(), key=lambda kv: float(kv[0]))
    ]
    return {"expiry": expiry, "spot_price": payload.get("last_price"), "strikes": strikes}


def _fetch_live_option_chain(access_token: str, client_id: str, underlying_sec_id: str) -> dict[str, Any]:
    expiries = _get_expiry_list(access_token, client_id, underlying_sec_id)
    if not expiries:
        return {"expiry": "", "spot_price": None, "strikes": [], "expiries": []}
    snapshot = _get_option_chain_snapshot(access_token, client_id, underlying_sec_id, expiries[0])
    snapshot["expiries"] = expiries
    return snapshot


def _serialize_candles(candles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_day: dict[str, dict[str, Any]] = {}
    for c in candles:
        d = c.get("date")
        if not isinstance(d, datetime):
            continue
        by_day[d.strftime("%Y-%m-%d")] = c
    return [
        {
            "date": day,
            "open": round(float(c.get("open") or 0), 2),
            "high": round(float(c.get("high") or 0), 2),
            "low": round(float(c.get("low") or 0), 2),
            "close": round(float(c.get("close") or 0), 2),
            "volume": int(c.get("volume") or 0),
        }
        for day, c in sorted(by_day.items())
    ]


def get_stock_option_future_historical_data(
    symbol: str,
    years: float,
    strike_offsets: list[int] | None = None,
    include_option_chain: bool = True,
) -> dict[str, Any]:
    """Given a stock symbol, returns years of equity history, every live
    FUTSTK contract's own history, N-year (capped at 5) ATM option history,
    and (optionally) the live option chain snapshot — all fetched directly
    from Dhan, nothing persisted. See module docstring for exactly what each
    section can and can't cover."""
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        raise ValueError("symbol is required.")
    try:
        years = float(years)
    except (TypeError, ValueError):
        raise ValueError("years must be a number.")
    if years <= 0:
        raise ValueError("years must be greater than 0.")
    years = min(years, 25.0)

    offsets = sorted({int(o) for o in (strike_offsets or [0])})
    for offset in offsets:
        if abs(offset) > MAX_STRIKE_OFFSET:
            raise ValueError(f"strike_offsets must be within +-{MAX_STRIKE_OFFSET} of ATM for stock options.")

    client_id, access_token = _load_dhan_credentials()
    if not access_token:
        raise ValueError("Dhan access token not configured (kite_market_config, broker=dhan).")
    if not client_id:
        raise ValueError("Dhan client id not configured (kite_market_config, broker=dhan).")

    contracts = _resolve_symbol_contracts(normalized)
    equity_sec_id = contracts["equity_sec_id"]
    if not equity_sec_id:
        raise ValueError(f"Unknown symbol '{normalized}' — not found in Dhan's NSE scrip master.")

    to_date = datetime.now()
    from_date = to_date - timedelta(days=int(years * 365))
    option_years = min(years, ROLLINGOPTION_MAX_YEARS)
    option_from_date = to_date - timedelta(days=int(option_years * 365))

    errors: list[dict[str, str]] = []
    jobs: dict[str, Any] = {}

    with ThreadPoolExecutor(max_workers=8) as pool:
        jobs["equity"] = pool.submit(
            _fetch_dhan_daily_history_chunked, access_token, equity_sec_id, "NSE_EQ", "EQUITY", from_date, to_date,
        )
        def _future_from_date(expiry: str) -> datetime:
            expiry_dt = datetime.strptime(expiry, "%Y-%m-%d")
            earliest_possible = expiry_dt - timedelta(days=FUTSTK_MAX_LISTING_LEAD_DAYS)
            return max(from_date, earliest_possible)

        future_jobs = {
            c["expiry"]: pool.submit(
                _fetch_dhan_daily_history_chunked, access_token, c["sec_id"], "NSE_FNO", "FUTSTK",
                _future_from_date(c["expiry"]), to_date,
            )
            for c in contracts["futures"]
        }
        option_jobs = {
            (offset, side): pool.submit(
                _fetch_rolling_option_years, access_token, client_id, equity_sec_id, _strike_label(offset), side,
                option_from_date, to_date,
            )
            for offset in offsets
            for side in ("CE", "PE")
        }
        chain_job = (
            pool.submit(_fetch_live_option_chain, access_token, client_id, equity_sec_id)
            if include_option_chain else None
        )

        try:
            equity_candles = _serialize_candles(jobs["equity"].result())
        except Exception as exc:
            equity_candles = []
            errors.append({"contract": "EQUITY", "error": str(exc)})

        future_results = []
        for c in contracts["futures"]:
            try:
                candles = _serialize_candles(future_jobs[c["expiry"]].result())
            except Exception as exc:
                candles = []
                # A far-month contract can be freshly listed with literally zero
                # trades yet — Dhan returns the same DH-905 "bad values for
                # parameters" 400 for that as it does for other input errors
                # (confirmed by direct testing: even a single recent day 400s
                # for such a contract), not an empty candle array. Framed as a
                # data-availability note rather than a scary failure, since
                # nothing is actually wrong with the request.
                note = (
                    " (likely just means this contract has no trading history yet — "
                    "common for a freshly-listed far-month future)"
                    if "DH-905" in str(exc) else ""
                )
                errors.append({"contract": c["trading_symbol"], "error": f"{exc}{note}"})
            future_results.append({
                "trading_symbol": c["trading_symbol"],
                "security_id": c["sec_id"],
                "expiry": c["expiry"],
                "lot_size": c["lot_size"],
                "candles": candles,
            })

        option_history: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for offset in offsets:
            label = _strike_label(offset)
            option_history[label] = {}
            for side in ("CE", "PE"):
                try:
                    option_history[label][side] = _serialize_candles(option_jobs[(offset, side)].result())
                except Exception as exc:
                    option_history[label][side] = []
                    errors.append({"contract": f"{label}-{side}", "error": str(exc)})

        option_chain: dict[str, Any] | None = None
        if chain_job is not None:
            try:
                option_chain = chain_job.result()
            except Exception as exc:
                errors.append({"contract": "OPTION_CHAIN_SNAPSHOT", "error": str(exc)})

    return {
        "status": "success",
        "symbol": normalized,
        "equity_security_id": equity_sec_id,
        "years_requested": years,
        "from_date": from_date.strftime("%Y-%m-%d"),
        "to_date": to_date.strftime("%Y-%m-%d"),
        "option_years_covered": option_years,
        "equity": {"candles": equity_candles},
        "future": future_results,
        "option_history": option_history,
        "option_chain": option_chain,
        "errors": errors,
    }
