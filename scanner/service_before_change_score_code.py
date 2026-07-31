from __future__ import annotations

import calendar
import re
import threading
import time
from datetime import datetime, timedelta
from typing import Any
from xml.etree import ElementTree as ET

import numpy as np
import pandas as pd
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import UpdateOne

from features.mongo_data import MongoData
from features.kite_broker import get_kite_instance
from features.spot_atm_utils import KITE_INDEX_TOKENS

DEFAULT_FORMULA = "((70% * 6 Month Volatility) + (20% * 3 Month Performance) + (10% * 1 Year Performance)) / 3 Month Volatility"
LOOKBACK_DAYS = 500
KITE_MARKET_CONFIG_ID = "69e18416c3d234dc8c90e6ca"
TRADING_PERIODS = {
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "9M": 189,
    "1Y": 252,
}
FORMULA_TOKEN_MAP = {
    "1 Month Performance": "perf_1M",
    "3 Month Performance": "perf_3M",
    "6 Month Performance": "perf_6M",
    "9 Month Performance": "perf_9M",
    "1 Year Performance": "perf_1Y",
    "1 Month Volatility": "vol_1M",
    "3 Month Volatility": "vol_3M",
    "6 Month Volatility": "vol_6M",
    "9 Month Volatility": "vol_9M",
    "1 Year Volatility": "vol_1Y",
}
PORTFOLIO_COLLECTION = "scanner_portfolio_settings"
INVESTMENT_COLLECTION = "scanner_investment_portfolio"
STOCKS_COLLECTION = "scanner_stocks_list"
HISTORY_COLLECTION = "scanner_stock_historical_data"
INDEX_HISTORY_COLLECTION = "scanner_index_historical_data"
INDEX_STOCKS_COLLECTION = "scanner_index_stocks"
MARKET_HOLIDAYS_COLLECTION = "market_holidays"
PORTFOLIO_SUMMARY_CACHE_TTL_SECONDS = 10.0
HISTORICAL_INTERVAL = "day"
HISTORICAL_CHUNK_DAYS = 2000   # Kite allows up to 2000 days per call for day interval
HISTORICAL_LOOKBACK_BUFFER_DAYS = 10

SCANNER_INDEX_ALIASES: dict[str, tuple[str, str]] = {
    "GOLDBEES": ("GOLDBEES", "gold_bees"),
    "GOLD_BEES": ("GOLDBEES", "gold_bees"),
    "NIFTY": ("NIFTY", "nifty_50"),
    "GOLDBEES-EQ": ("GOLDBEES", "gold_bees"),
    "NIFTY50": ("NIFTY", "nifty_50"),
    "NIFTY 50": ("NIFTY", "nifty_50"),
    "NIFTY_50": ("NIFTY", "nifty_50"),
    "NIFTY500": ("NIFTY500", "nifty_500"),
    "NIFTY 500": ("NIFTY500", "nifty_500"),
    "NIFTY_500": ("NIFTY500", "nifty_500"),
    "NIFTYMIDCAP100": ("NIFTYMIDCAP100", "nifty_midcap_100"),
    "NIFTY MIDCAP100": ("NIFTYMIDCAP100", "nifty_midcap_100"),
    "NIFTY_MIDCAP_100": ("NIFTYMIDCAP100", "nifty_midcap_100"),
    "NIFTY_MIDCAP_50": ("NIFTYMIDCAP50", "nifty_midcap_50"),
    "NIFTYMIDCAP50": ("NIFTYMIDCAP50", "nifty_midcap_50"),
    "NIFTY MIDCAP50": ("NIFTYMIDCAP50", "nifty_midcap_50"),
    "NIFTY100": ("NIFTY100", "nifty_100"),
    "NIFTY 100": ("NIFTY100", "nifty_100"),
    "NIFTY_100": ("NIFTY100", "nifty_100"),
    "NIFTYNXT50": ("NIFTYNXT50", "nifty_next_50"),
    "NIFTY NXT50": ("NIFTYNXT50", "nifty_next_50"),
    "NIFTY_NEXT_50": ("NIFTYNXT50", "nifty_next_50"),
    "NIFTYSMLCAP100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY SMLCAP100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY_SMALLCAP_50": ("NIFTYSMLCAP100", "nifty_smallcap_50"),
    "NIFTY_SMALLCAP_100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY_SMLCAP_100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTYSMALLCAP100": ("NIFTYSMLCAP100", "nifty_smallcap_100"),
    "NIFTY200": ("NIFTY200", "nifty_200"),
    "NIFTY 200": ("NIFTY200", "nifty_200"),
    "NIFTY_200": ("NIFTY200", "nifty_200"),
    "BANKNIFTY": ("BANKNIFTY", "nifty_bank"),
    "NIFTY BANK": ("BANKNIFTY", "nifty_bank"),
    "NIFTY_BANK": ("BANKNIFTY", "nifty_bank"),
    "FINNIFTY": ("FINNIFTY", "nifty_fin_service"),
    "NIFTY FIN SERVICE": ("FINNIFTY", "nifty_fin_service"),
    "NIFTY_FIN_SERVICE": ("FINNIFTY", "nifty_fin_service"),
    "MIDCPNIFTY": ("MIDCPNIFTY", "nifty_mid_select"),
    "NIFTY MID SELECT": ("MIDCPNIFTY", "nifty_mid_select"),
    "NIFTY_MID_SELECT": ("MIDCPNIFTY", "nifty_mid_select"),
    "SENSEX": ("SENSEX", "sensex"),
    "INDIA VIX": ("INDIA_VIX", "india_vix"),
    "INDIA_VIX": ("INDIA_VIX", "india_vix"),
    "INDIAVIX": ("INDIA_VIX", "india_vix"),
}

DEFAULT_SCANNER_INDEXES: list[dict[str, Any]] = [
    {"lookup_symbol": "GOLDBEES", "raw_symbol": "GOLDBEES-EQ", "normalized_symbol": "gold_bees", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY", "raw_symbol": "NIFTY50", "normalized_symbol": "nifty_50", "fetch_mode": "token"},
    {"lookup_symbol": "NIFTY500", "raw_symbol": "NIFTY 500", "normalized_symbol": "nifty_500", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYMIDCAP100", "raw_symbol": "NIFTY MIDCAP 100", "normalized_symbol": "nifty_midcap_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYMIDCAP50", "raw_symbol": "NIFTY MIDCAP 50", "normalized_symbol": "nifty_midcap_50", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY100", "raw_symbol": "NIFTY 100", "normalized_symbol": "nifty_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYNXT50", "raw_symbol": "NIFTY NEXT 50", "normalized_symbol": "nifty_next_50", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTYSMLCAP100", "raw_symbol": "NIFTY SMLCAP 100", "normalized_symbol": "nifty_smallcap_100", "fetch_mode": "nse"},
    {"lookup_symbol": "NIFTY200", "raw_symbol": "NIFTY 200", "normalized_symbol": "nifty_200", "fetch_mode": "nse"},
    {"lookup_symbol": "BANKNIFTY", "raw_symbol": "BANKNIFTY", "normalized_symbol": "nifty_bank", "fetch_mode": "token"},
    {"lookup_symbol": "FINNIFTY", "raw_symbol": "FINNIFTY", "normalized_symbol": "nifty_fin_service", "fetch_mode": "token"},
    {"lookup_symbol": "MIDCPNIFTY", "raw_symbol": "MIDCPNIFTY", "normalized_symbol": "nifty_mid_select", "fetch_mode": "token"},
    {"lookup_symbol": "SENSEX", "raw_symbol": "SENSEX", "normalized_symbol": "sensex", "fetch_mode": "token"},
    {"lookup_symbol": "INDIA_VIX", "raw_symbol": "INDIA VIX", "normalized_symbol": "india_vix", "fetch_mode": "token"},
]

NSE_MARKET_HOLIDAYS: dict[int, list[dict[str, str]]] = {
    2026: [
        {"date": "2026-01-26", "description": "Republic Day"},
        {"date": "2026-03-03", "description": "Holi"},
        {"date": "2026-03-26", "description": "Shri Ram Navami"},
        {"date": "2026-03-31", "description": "Shri Mahavir Jayanti"},
        {"date": "2026-04-03", "description": "Good Friday"},
        {"date": "2026-04-14", "description": "Dr. Baba Saheb Ambedkar Jayanti"},
        {"date": "2026-05-01", "description": "Maharashtra Day"},
        {"date": "2026-05-28", "description": "Bakri Id"},
        {"date": "2026-06-26", "description": "Muharram"},
        {"date": "2026-09-14", "description": "Ganesh Chaturthi"},
        {"date": "2026-10-02", "description": "Mahatma Gandhi Jayanti"},
        {"date": "2026-10-20", "description": "Dussehra"},
        {"date": "2026-11-10", "description": "Diwali-Balipratipada"},
        {"date": "2026-11-24", "description": "Prakash Gurpurb Sri Guru Nanak Dev"},
        {"date": "2026-12-25", "description": "Christmas"},
    ],
}

_portfolio_summary_cache_value: list[dict[str, Any]] | None = None
_portfolio_summary_cache_expires_at = 0.0
_historical_sync_stop_requested = False
_historical_sync_running = False
_historical_sync_thread: threading.Thread | None = None
_historical_sync_last_result: dict[str, Any] | None = None
_historical_sync_lock = threading.Lock()


def _coerce_score_date(raw_value: Any) -> datetime:
    if isinstance(raw_value, datetime):
        return raw_value
    if not raw_value:
        return datetime.now()
    try:
        return datetime.fromisoformat(str(raw_value))
    except Exception:
        return pd.to_datetime(raw_value, errors="coerce").to_pydatetime()


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, str):
            value = value.strip().replace(",", "").replace("%", "")
        numeric = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(numeric) or np.isinf(numeric):
        return default
    return numeric


def _serialize_value(value: Any) -> Any:
    if isinstance(value, ObjectId):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, list):
        return [_serialize_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _serialize_value(item) for key, item in value.items()}
    return value


def _serialize_doc(document: dict[str, Any]) -> dict[str, Any]:
    return {key: _serialize_value(value) for key, value in document.items()}


def _as_object_id(raw_value: str) -> ObjectId:
    try:
        return ObjectId(str(raw_value).strip())
    except (InvalidId, TypeError) as exc:
        raise ValueError("Invalid portfolio id.") from exc


def _normalize_list_payload(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    return [str(value).strip()]


def _normalize_formula(formula: str | None) -> str:
    normalized = str(formula or DEFAULT_FORMULA).strip() or DEFAULT_FORMULA
    normalized = re.sub(r"(\d+(?:\.\d+)?)\s*%", lambda match: str(float(match.group(1)) / 100.0), normalized)
    for label, column in sorted(FORMULA_TOKEN_MAP.items(), key=lambda item: len(item[0]), reverse=True):
        normalized = re.sub(re.escape(label), column, normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"[^A-Za-z0-9_+\-*/(). <>=!&|]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def request_stop_scanner_historical_sync() -> dict[str, Any]:
    global _historical_sync_stop_requested
    _historical_sync_stop_requested = True
    return {
        "status": "success",
        "message": "Stop requested. Historical sync will stop after the current symbol completes.",
    }


def get_scanner_historical_sync_status() -> dict[str, Any]:
    return {
        "status": "success",
        "running": bool(_historical_sync_running),
        "stop_requested": bool(_historical_sync_stop_requested),
        "last_result": _historical_sync_last_result,
    }


def _scanner_historical_sync_worker(start_date: str, end_date: str, use_universe_list: bool = False) -> None:
    global _historical_sync_last_result, _historical_sync_thread
    try:
        result = sync_scanner_historical_data(start_date, end_date, use_universe_list=use_universe_list)
        _historical_sync_last_result = result
    except Exception as exc:
        _historical_sync_last_result = {
            "status": "error",
            "message": str(exc),
            "start_date": start_date,
            "end_date": end_date,
        }
    finally:
        _historical_sync_thread = None


def start_scanner_historical_data_sync(
    start_date: str,
    end_date: str,
    use_universe_list: bool = False,
) -> dict[str, Any]:
    global _historical_sync_thread, _historical_sync_last_result
    with _historical_sync_lock:
        if _historical_sync_running or (_historical_sync_thread and _historical_sync_thread.is_alive()):
            raise ValueError("Historical sync is already running.")

        source_label = "universe_list" if use_universe_list else "stocks_list"
        _historical_sync_last_result = {
            "status": "started",
            "message": f"Historical sync started in background (source={source_label}).",
            "start_date": start_date,
            "end_date": end_date,
            "source": source_label,
            "started_at": datetime.utcnow().isoformat(),
        }
        _historical_sync_thread = threading.Thread(
            target=_scanner_historical_sync_worker,
            args=(start_date, end_date, use_universe_list),
            daemon=True,
            name="scanner-historical-sync",
        )
        _historical_sync_thread.start()

    return {
        "status": "success",
        "message": f"Historical sync started in background (source={source_label}).",
        "start_date": start_date,
        "end_date": end_date,
        "source": source_label,
        "running": True,
    }


def _load_company_map(symbols: list[str]) -> dict[str, str]:
    if not symbols:
        return {}
    db = MongoData()._db
    rows = list(
        db[STOCKS_COLLECTION].find(
            {"symbol": {"$in": symbols}},
            {"_id": 0, "symbol": 1, "company_name": 1},
        )
    )
    company_map: dict[str, str] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            company_map[symbol] = str(row.get("company_name") or symbol).strip()
    return company_map


def _load_stock_meta_map(symbols: list[str]) -> dict[str, dict[str, Any]]:
    if not symbols:
        return {}
    db = MongoData()._db
    normalized_symbols = [str(symbol or "").strip() for symbol in symbols if str(symbol or "").strip()]
    rows = list(
        db[STOCKS_COLLECTION].find(
            {"symbol": {"$in": normalized_symbols}},
            {
                "_id": 0,
                "symbol": 1,
                "company_name": 1,
                "token": 1,
                "tokens": 1,
                "instrument_token": 1,
                "exchange_token": 1,
                "code": 1,
            },
        )
    )
    stock_meta_map: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        if symbol:
            stock_meta_map[symbol] = row

    return stock_meta_map


def _load_latest_closes(symbols: list[str]) -> dict[str, float]:
    if not symbols:
        return {}
    db = MongoData()._db
    rows = list(
        db[HISTORY_COLLECTION].find(
            {"h_symbol": {"$in": symbols}},
            {"_id": 0, "h_symbol": 1, "ch_timestamp": 1, "ch_closing_price": 1},
        ).sort([("h_symbol", 1), ("ch_timestamp", -1)])
    )
    close_map: dict[str, float] = {}
    for row in rows:
        symbol = str(row.get("h_symbol") or "").strip()
        if not symbol or symbol in close_map:
            continue
        close_map[symbol] = round(_safe_float(row.get("ch_closing_price")), 2)
    return close_map


def _load_kite_credentials() -> tuple[str, str]:
    db = MongoData()._db
    doc = db["kite_market_config"].find_one(
        {"_id": _as_object_id(KITE_MARKET_CONFIG_ID)},
        {"api_key": 1, "access_token": 1},
    ) or {}
    api_key = str(doc.get("api_key") or "").strip()
    access_token = str(doc.get("access_token") or "").strip()
    return api_key, access_token


def _build_kite_client_from_config():
    from kiteconnect import KiteConnect  # type: ignore

    api_key, access_token = _load_kite_credentials()
    if not access_token:
        raise ValueError("Kite access token not configured.")
    if not api_key:
        raise ValueError("Kite api key not configured.")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite


def _parse_ymd_date(value: Any, *, field_name: str) -> datetime:
    raw = str(value or "").strip()
    try:
        return datetime.strptime(raw, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid {field_name}. Expected YYYY-MM-DD.") from exc


def _load_market_holiday_dates() -> set[str]:
    db = MongoData()._db
    rows = list(db[MARKET_HOLIDAYS_COLLECTION].find({}, {"_id": 0, "date": 1}))
    return {
        str(row.get("date") or "").strip()
        for row in rows
        if str(row.get("date") or "").strip()
    }


def _resolve_stock_kite_token(stock: dict[str, Any]) -> tuple[str, str]:
    symbol = str(stock.get("symbol") or stock.get("tradingsymbol") or "").strip().upper()
    raw_token = str(
        stock.get("kite_token")
        or stock.get("token")
        or stock.get("tokens")
        or stock.get("instrument_token")
        or stock.get("exchange_token")
        or stock.get("code")
        or ""
    ).strip()
    try:
        token = str(int(float(raw_token))) if raw_token else ""
    except (ValueError, TypeError):
        token = raw_token
    return symbol, token


def _resolve_index_identity(stock: dict[str, Any]) -> tuple[str, str] | None:
    candidates = [
        str(stock.get("symbol") or "").strip(),
        str(stock.get("tradingsymbol") or "").strip(),
        str(stock.get("name") or "").strip(),
    ]
    for candidate in candidates:
        alias = SCANNER_INDEX_ALIASES.get(candidate.upper())
        if alias:
            return alias

    series = str(stock.get("series") or "").strip().lower()
    if series == "index":
        fallback = str(stock.get("symbol") or stock.get("tradingsymbol") or "").strip().upper()
        alias = SCANNER_INDEX_ALIASES.get(fallback)
        if alias:
            return alias
    return None


def _fetch_kite_daily_candles(
    kite,
    instrument_token: int,
    from_date: datetime,
    to_date: datetime,
) -> list[dict[str, Any]]:
    candles = kite.historical_data(
        instrument_token=instrument_token,
        from_date=from_date,
        to_date=to_date,
        interval=HISTORICAL_INTERVAL,
    )
    return candles if isinstance(candles, list) else []


def _chunkify_strings(items: list[str], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _load_kite_cash_instrument_maps(kite) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_exchange_symbol: dict[str, dict[str, Any]] = {}
    by_token: dict[str, dict[str, Any]] = {}

    for exchange in ("NSE", "BSE"):
        try:
            rows = kite.instruments(exchange) or []
        except Exception:
            rows = []
        for row in rows:
            tradingsymbol = str(row.get("tradingsymbol") or "").strip().upper()
            instrument_token = str(row.get("instrument_token") or "").strip()
            if tradingsymbol:
                by_exchange_symbol[f"{exchange}:{tradingsymbol}"] = row
            if instrument_token:
                by_token[instrument_token] = row

    return by_exchange_symbol, by_token


def _load_kite_nse_instrument_map(kite) -> dict[str, dict[str, Any]]:
    instrument_map: dict[str, dict[str, Any]] = {}
    try:
        for inst in kite.instruments("NSE") or []:
            tradingsymbol = str(inst.get("tradingsymbol") or "").strip().upper()
            if tradingsymbol:
                instrument_map[tradingsymbol] = inst
    except Exception:
        return {}
    return instrument_map


def _load_scanner_index_master(db, nse_instrument_map: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index_rows: dict[str, dict[str, Any]] = {}

    rows = list(
        db[INDEX_STOCKS_COLLECTION].find(
            {},
            {"_id": 0, "filter_symbol": 1, "name": 1, "symbol": 1, "tradingsymbol": 1},
        )
    )

    for row in rows:
        candidates = [
            str(row.get("filter_symbol") or "").strip(),
            str(row.get("symbol") or "").strip(),
            str(row.get("tradingsymbol") or "").strip(),
            str(row.get("name") or "").strip(),
        ]
        index_meta = None
        for candidate in candidates:
            if not candidate:
                continue
            index_meta = _resolve_index_identity({"symbol": candidate, "name": candidate, "tradingsymbol": candidate})
            if index_meta:
                break
        if not index_meta:
            continue

        index_key, normalized_symbol = index_meta
        raw_symbol = (
            str(row.get("symbol") or "").strip()
            or str(row.get("tradingsymbol") or "").strip()
            or str(row.get("filter_symbol") or "").strip()
            or str(row.get("name") or "").strip()
            or index_key
        )
        fetch_mode = "token" if index_key in KITE_INDEX_TOKENS else "nse"
        if fetch_mode == "nse":
            inst = _resolve_nse_index_instrument(nse_instrument_map, index_key, raw_symbol, normalized_symbol)
            if inst:
                raw_symbol = str(inst.get("tradingsymbol") or raw_symbol).strip()

        index_rows[index_key] = {
            "kite_key": index_key,
            "normalized_symbol": normalized_symbol,
            "raw_symbol": raw_symbol,
            "fetch_mode": fetch_mode,
        }

    for default_index in DEFAULT_SCANNER_INDEXES:
        lookup_symbol = str(default_index["lookup_symbol"]).strip().upper()
        if lookup_symbol in index_rows:
            continue
        index_rows[lookup_symbol] = {
            "kite_key": lookup_symbol,
            "normalized_symbol": str(default_index["normalized_symbol"]),
            "raw_symbol": str(default_index["raw_symbol"]),
            "fetch_mode": str(default_index["fetch_mode"]),
        }

    return index_rows


def _resolve_nse_index_instrument(
    nse_instrument_map: dict[str, dict[str, Any]],
    index_key: str,
    raw_symbol: str,
    normalized_symbol: str,
) -> dict[str, Any]:
    candidates = [
        str(raw_symbol or "").strip().upper(),
        str(index_key or "").strip().upper(),
        str(normalized_symbol or "").strip().upper().replace("_", " "),
        str(index_key or "").strip().upper().replace("MIDCAP", " MIDCAP ").replace("SMLCAP", " SMLCAP "),
    ]

    default_index = next(
        (item for item in DEFAULT_SCANNER_INDEXES if str(item.get("lookup_symbol") or "").strip().upper() == str(index_key or "").strip().upper()),
        None,
    )
    if default_index:
        candidates.append(str(default_index.get("raw_symbol") or "").strip().upper())

    normalized_candidates: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        cleaned = " ".join(str(candidate or "").split())
        if cleaned and cleaned not in seen:
            normalized_candidates.append(cleaned)
            seen.add(cleaned)

    for candidate in normalized_candidates:
        inst = nse_instrument_map.get(candidate)
        if inst:
            return inst
    return {}


def backfill_index_stocks_kite_tokens() -> dict[str, Any]:
    db = MongoData()._db
    kite = _build_kite_client_from_config()
    nse_instrument_map = _load_kite_nse_instrument_map(kite)
    now = datetime.utcnow()

    rows = list(
        db[INDEX_STOCKS_COLLECTION].find(
            {},
            {"_id": 1, "filter_symbol": 1, "name": 1, "symbol": 1, "tradingsymbol": 1},
        )
    )

    updated = 0
    skipped: list[dict[str, str]] = []

    for row in rows:
        candidates = [
            str(row.get("filter_symbol") or "").strip(),
            str(row.get("symbol") or "").strip(),
            str(row.get("tradingsymbol") or "").strip(),
            str(row.get("name") or "").strip(),
        ]
        index_meta = None
        for candidate in candidates:
            if not candidate:
                continue
            index_meta = _resolve_index_identity({"symbol": candidate, "name": candidate, "tradingsymbol": candidate})
            if index_meta:
                break

        if not index_meta:
            skipped.append(
                {
                    "filter_symbol": str(row.get("filter_symbol") or "").strip() or "-",
                    "reason": "unable_to_resolve_index_symbol",
                }
            )
            continue

        index_key, normalized_symbol = index_meta
        default_index = next(
            (item for item in DEFAULT_SCANNER_INDEXES if str(item.get("lookup_symbol") or "").strip().upper() == index_key),
            None,
        )
        raw_symbol = str(
            row.get("symbol")
            or row.get("tradingsymbol")
            or (default_index or {}).get("raw_symbol")
            or index_key
        ).strip()

        fetch_mode = "token" if index_key in KITE_INDEX_TOKENS else "nse"
        kite_token = ""
        exchange_token = ""
        tradingsymbol = raw_symbol

        if fetch_mode == "token":
            kite_token = str(int(KITE_INDEX_TOKENS.get(index_key) or 0) or "").strip()
        else:
            inst = _resolve_nse_index_instrument(nse_instrument_map, index_key, raw_symbol, normalized_symbol)
            kite_token = str(inst.get("instrument_token") or "").strip()
            exchange_token = str(inst.get("exchange_token") or "").strip()
            tradingsymbol = str(inst.get("tradingsymbol") or raw_symbol).strip()

        db[INDEX_STOCKS_COLLECTION].update_one(
            {"_id": row["_id"]},
            {
                "$set": {
                    "symbol": index_key,
                    "tradingsymbol": tradingsymbol,
                    "normalized_symbol": normalized_symbol,
                    "kite_token": kite_token,
                    "instrument_token": kite_token,
                    "exchange_token": exchange_token,
                    "fetch_mode": fetch_mode,
                    "updated_at": now,
                }
            },
        )
        updated += 1

    return {
        "status": "success",
        "total": len(rows),
        "updated": updated,
        "skipped": skipped,
        "skipped_count": len(skipped),
    }


def _build_scanner_daily_stock_instruments(
    db,
    cash_by_exchange_symbol: dict[str, dict[str, Any]],
    cash_by_token: dict[str, dict[str, Any]],
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    rows = list(
        db[STOCKS_COLLECTION].find(
            {},
            {
                "_id": 0,
                "exchange": 1,
                "symbol": 1,
                "tradingsymbol": 1,
                "kite_token": 1,
                "token": 1,
                "instrument_token": 1,
                "exchange_token": 1,
                "series": 1,
            },
        )
    )
    instruments: list[str] = []
    meta_map: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    skipped_no_symbol = 0
    skipped_index_series = 0
    duplicate_count = 0
    resolved_from_token = 0
    resolved_from_symbol = 0

    for row in rows:
        exchange = str(row.get("exchange") or "NSE").strip().upper() or "NSE"
        if exchange not in {"NSE", "BSE"}:
            exchange = "NSE"
        if str(row.get("series") or "").strip().lower() == "index":
            skipped_index_series += 1
            continue
        token = str(
            row.get("kite_token")
            or row.get("token")
            or row.get("instrument_token")
            or row.get("exchange_token")
            or ""
        ).strip()
        inst = cash_by_token.get(token) if token else None
        if inst:
            tradingsymbol = str(inst.get("tradingsymbol") or "").strip().upper()
            exchange = str(inst.get("exchange") or exchange).strip().upper() or exchange
            resolved_from_token += 1
        else:
            input_tradingsymbol = str(row.get("tradingsymbol") or row.get("symbol") or "").strip().upper()
            inst = cash_by_exchange_symbol.get(f"{exchange}:{input_tradingsymbol}") if input_tradingsymbol else None
            tradingsymbol = str((inst or {}).get("tradingsymbol") or input_tradingsymbol).strip().upper()
            if inst:
                exchange = str(inst.get("exchange") or exchange).strip().upper() or exchange
                resolved_from_symbol += 1
        if not tradingsymbol:
            skipped_no_symbol += 1
            continue
        instrument = f"{exchange}:{tradingsymbol}"
        if instrument in seen:
            duplicate_count += 1
            continue
        seen.add(instrument)
        instruments.append(instrument)
        meta_map[instrument] = row

    print(
        f"📘 Stock loader: total_rows={len(rows)} loaded={len(instruments)} "
        f"skipped_index_series={skipped_index_series} skipped_no_symbol={skipped_no_symbol} duplicates={duplicate_count} "
        f"resolved_from_token={resolved_from_token} resolved_from_symbol={resolved_from_symbol}",
        flush=True,
    )
    return instruments, meta_map


def _build_scanner_daily_index_instruments(
    db,
    cash_by_exchange_symbol: dict[str, dict[str, Any]],
    cash_by_token: dict[str, dict[str, Any]],
) -> tuple[list[str], dict[str, dict[str, Any]]]:
    instruments: list[str] = []
    meta_map: dict[str, dict[str, Any]] = {}
    seen: set[str] = set()
    skipped_unresolved = 0
    skipped_no_tradingsymbol = 0
    duplicate_count = 0
    resolved_from_token = 0
    resolved_from_symbol = 0

    rows = list(
        db[INDEX_STOCKS_COLLECTION].find(
            {},
            {"_id": 0, "filter_symbol": 1, "name": 1, "symbol": 1, "tradingsymbol": 1, "kite_token": 1, "exchange": 1},
        )
    )

    for row in rows:
        filter_symbol = str(row.get("filter_symbol") or "").strip().lower()
        index_meta = None
        for candidate in (
            str(row.get("filter_symbol") or "").strip(),
            str(row.get("name") or "").strip(),
            str(row.get("symbol") or "").strip(),
            str(row.get("tradingsymbol") or "").strip(),
        ):
            if not candidate:
                continue
            index_meta = _resolve_index_identity({"symbol": candidate, "name": candidate, "tradingsymbol": candidate})
            if index_meta:
                break
        normalized_symbol = str(index_meta[1]).strip().lower() if index_meta else filter_symbol
        default_index = next(
            (
                item for item in DEFAULT_SCANNER_INDEXES
                if str(item.get("normalized_symbol") or "").strip().lower() == normalized_symbol
            ),
            None,
        )
        if not default_index:
            skipped_unresolved += 1
            continue
        exchange = str(row.get("exchange") or ("BSE" if default_index["lookup_symbol"] == "SENSEX" else "NSE")).strip().upper()
        token = str(row.get("kite_token") or row.get("instrument_token") or row.get("exchange_token") or "").strip()
        inst = cash_by_token.get(token) if token else None
        if inst:
            tradingsymbol = str(inst.get("tradingsymbol") or "").strip()
            exchange = str(inst.get("exchange") or exchange).strip().upper() or exchange
            resolved_from_token += 1
        else:
            base_symbol = str(row.get("tradingsymbol") or default_index.get("raw_symbol") or "").strip()
            inst = cash_by_exchange_symbol.get(f"{exchange}:{base_symbol.upper()}") if base_symbol else None
            tradingsymbol = str((inst or {}).get("tradingsymbol") or base_symbol).strip()
            if inst:
                exchange = str(inst.get("exchange") or exchange).strip().upper() or exchange
                resolved_from_symbol += 1
        if not tradingsymbol:
            skipped_no_tradingsymbol += 1
            continue
        instrument = f"{exchange}:{tradingsymbol}"
        if instrument in seen:
            duplicate_count += 1
            continue
        seen.add(instrument)
        instruments.append(instrument)
        meta_map[instrument] = {
            **row,
            "symbol": row.get("symbol") or default_index.get("lookup_symbol"),
            "tradingsymbol": tradingsymbol,
            "exchange": exchange,
            "normalized_symbol": normalized_symbol,
        }

    print(
        f"📗 Index loader: total_rows={len(rows)} loaded={len(instruments)} "
        f"skipped_unresolved={skipped_unresolved} skipped_no_tradingsymbol={skipped_no_tradingsymbol} duplicates={duplicate_count} "
        f"resolved_from_token={resolved_from_token} resolved_from_symbol={resolved_from_symbol}",
        flush=True,
    )
    return instruments, meta_map


def _fetch_kite_quotes_in_batches(kite, instruments: list[str], *, batch_size: int = 500) -> tuple[dict[str, Any], int, list[dict[str, Any]]]:
    if not instruments:
        return {}, 0, []

    quotes: dict[str, Any] = {}
    total_batches = 0
    missing_info: list[dict[str, Any]] = []
    for idx, batch in enumerate(_chunkify_strings(instruments, batch_size), start=1):
        total_batches = idx
        print(f"📦 Quote batch {idx} ({len(batch)} instruments)", flush=True)
        batch_quotes = kite.quote(batch) or {}
        quotes.update(batch_quotes)
        missing = [instrument for instrument in batch if instrument not in batch_quotes]
        print(f"✅ Quote batch {idx} completed fetched={len(batch_quotes)} missing={len(missing)}", flush=True)
        if missing:
            print(f"⚠️ Missing quotes in batch {idx}: {missing[:20]}", flush=True)
            retry_success = 0
            still_missing: list[str] = []
            for instrument in missing:
                try:
                    single_quote = kite.quote([instrument]) or {}
                except Exception as exc:
                    still_missing.append(instrument)
                    missing_info.append({"instrument": instrument, "reason": f"retry_error:{exc}"})
                    continue
                if instrument in single_quote:
                    quotes[instrument] = single_quote[instrument]
                    retry_success += 1
                else:
                    still_missing.append(instrument)
                    missing_info.append({"instrument": instrument, "reason": "not_returned_by_quote_api"})
            print(
                f"🔁 Quote batch {idx} retry completed retry_success={retry_success} still_missing={len(still_missing)}",
                flush=True,
            )
            if still_missing:
                print(f"❌ Still missing after retry: {still_missing[:20]}", flush=True)
    return quotes, total_batches, missing_info


def _scanner_daily_quote_timestamp(payload: dict[str, Any]) -> datetime:
    dt = payload.get("timestamp") or payload.get("last_trade_time") or datetime.now(timezone.utc)
    return dt if isinstance(dt, datetime) else datetime.now(timezone.utc)


def _transform_scanner_daily_stock_quote(instrument: str, payload: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    dt = _scanner_daily_quote_timestamp(payload)
    ohlc = payload.get("ohlc") or {}
    symbol = str(meta.get("symbol") or instrument.split(":", 1)[-1]).strip().upper()
    token = str(
        meta.get("kite_token")
        or meta.get("token")
        or meta.get("instrument_token")
        or payload.get("instrument_token")
        or ""
    ).strip()
    return {
        "h_symbol": symbol,
        "ch_timestamp": dt.strftime("%Y-%m-%d"),
        "ch_series": str(meta.get("series") or "EQ").strip() or "EQ",
        "ch_opening_price": ohlc.get("open"),
        "ch_high_price": ohlc.get("high"),
        "ch_low_price": ohlc.get("low"),
        "ch_closing_price": payload.get("last_price"),
        "ch_previous_cls_price": ohlc.get("close"),
        "ch_last_traded_price": payload.get("last_price"),
        "ch_tot_traded_val": payload.get("volume"),
        "ch_52week_high_price": ohlc.get("high"),
        "ch_52week_low_price": ohlc.get("low"),
        "ch_unix_timestamp": calendar.timegm(dt.utctimetuple()),
        "ch_utc_timestamp": dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "h_symbol_token": token,
    }


def _transform_scanner_daily_index_quote(payload: dict[str, Any], meta: dict[str, Any]) -> dict[str, Any]:
    dt = _scanner_daily_quote_timestamp(payload)
    ohlc = payload.get("ohlc") or {}
    last_price = _safe_float(payload.get("last_price"))
    open_price = _safe_float(ohlc.get("open"), last_price)
    high_price = _safe_float(ohlc.get("high"), last_price)
    low_price = _safe_float(ohlc.get("low"), last_price)
    previous_close = _safe_float(ohlc.get("close"), last_price)
    net_change_pct = _safe_float(payload.get("net_change"))
    if previous_close > 0 and last_price > 0:
        net_change_pct = round(((last_price - previous_close) / previous_close) * 100.0, 2)
    else:
        net_change_pct = 0.0
    return {
        "i_symbol": str(meta.get("normalized_symbol") or "").strip(),
        "ih_timestamp": dt.strftime("%Y-%m-%d"),
        "i_open_price": open_price,
        "i_high_price": high_price,
        "i_low_price": low_price,
        "i_last_traded_price": last_price,
        "i_close_price": last_price,
        "i_previous_close": previous_close,
        "i_volume": payload.get("volume") or 0,
        "i_ch": net_change_pct,
        "i_unix_timestamp": calendar.timegm(dt.utctimetuple()),
    }


def _bulk_upsert_scanner_daily_quotes(
    target_col,
    docs: list[dict[str, Any]],
    key_fields: tuple[str, str],
    batch_size: int = 1000,
    fallback_symbol_field: str | None = None,
) -> int:
    if not docs:
        return 0

    total = 0
    skipped_missing_key = 0
    fallback_key_used = 0
    for start in range(0, len(docs), batch_size):
        batch = docs[start:start + batch_size]
        ops: list[UpdateOne] = []
        for doc in batch:
            key_value = doc.get(key_fields[0])
            ts_value = doc.get(key_fields[1])
            if not ts_value:
                skipped_missing_key += 1
                continue
            if key_value:
                query = {key_fields[0]: key_value, key_fields[1]: ts_value}
            elif fallback_symbol_field and doc.get(fallback_symbol_field):
                fallback_key_used += 1
                query = {fallback_symbol_field: doc.get(fallback_symbol_field), key_fields[1]: ts_value}
            else:
                skipped_missing_key += 1
                continue
            ops.append(UpdateOne(query, {"$set": doc}, upsert=True))
        if not ops:
            continue
        result = target_col.bulk_write(ops, ordered=False)
        total += int(result.upserted_count or 0) + int(result.modified_count or 0)
    print(
        f"💾 {target_col.name}: upserts={total} fallback_key_used={fallback_key_used} skipped_missing_key={skipped_missing_key}",
        flush=True,
    )
    return total


def sync_scanner_daily_market_snapshot() -> dict[str, Any]:
    started_at = datetime.now()
    print(f"\n🕒 Starting scanner daily market snapshot — {started_at.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)

    db = MongoData()._db
    kite = _build_kite_client_from_config()
    print("✅ Kite client ready.", flush=True)
    cash_by_exchange_symbol, cash_by_token = _load_kite_cash_instrument_maps(kite)
    print(
        f"✅ Loaded Kite cash master exchange_symbols={len(cash_by_exchange_symbol)} token_map={len(cash_by_token)}",
        flush=True,
    )

    stock_instruments, stock_meta_map = _build_scanner_daily_stock_instruments(db, cash_by_exchange_symbol, cash_by_token)
    index_instruments, index_meta_map = _build_scanner_daily_index_instruments(db, cash_by_exchange_symbol, cash_by_token)
    print(f"✅ Loaded {len(stock_instruments)} stock instruments from {STOCKS_COLLECTION}", flush=True)
    print(f"✅ Loaded {len(index_instruments)} index instruments from {INDEX_STOCKS_COLLECTION}", flush=True)

    stock_quotes, stock_batches, stock_missing = _fetch_kite_quotes_in_batches(kite, stock_instruments, batch_size=500)
    index_quotes, index_batches, index_missing = _fetch_kite_quotes_in_batches(kite, index_instruments, batch_size=500)

    stock_docs = [
        _transform_scanner_daily_stock_quote(instrument, payload, stock_meta_map[instrument])
        for instrument, payload in stock_quotes.items()
        if instrument in stock_meta_map
    ]
    index_docs = [
        _transform_scanner_daily_index_quote(payload, index_meta_map[instrument])
        for instrument, payload in index_quotes.items()
        if instrument in index_meta_map
    ]

    stock_done = _bulk_upsert_scanner_daily_quotes(
        db[HISTORY_COLLECTION],
        stock_docs,
        ("h_symbol_token", "ch_timestamp"),
        batch_size=1000,
        fallback_symbol_field="h_symbol",
    )
    index_done = _bulk_upsert_scanner_daily_quotes(
        db[INDEX_HISTORY_COLLECTION],
        index_docs,
        ("i_symbol", "ih_timestamp"),
        batch_size=1000,
    )

    elapsed = round((datetime.now() - started_at).total_seconds(), 2)
    print(
        f"🎉 Snapshot completed. stock_upserts={stock_done} index_upserts={index_done} elapsed={elapsed}s",
        flush=True,
    )
    if stock_missing:
        print(f"⚠️ Stock missing quote count={len(stock_missing)} details={stock_missing[:20]}", flush=True)
    if index_missing:
        print(f"⚠️ Index missing quote count={len(index_missing)} details={index_missing[:20]}", flush=True)

    return {
        "status": "success",
        "message": "Scanner daily market snapshot synced.",
        "stock_instruments": len(stock_instruments),
        "index_instruments": len(index_instruments),
        "stock_batches": stock_batches,
        "index_batches": index_batches,
        "stock_quotes": len(stock_quotes),
        "index_quotes": len(index_quotes),
        "stock_missing_quotes": stock_missing,
        "index_missing_quotes": index_missing,
        "stock_upserts": stock_done,
        "index_upserts": index_done,
        "elapsed_seconds": elapsed,
        "date": started_at.strftime("%Y-%m-%d"),
    }


def _resolve_kite_access_token_error(exc: Exception) -> str | None:
    message = str(exc or "").strip().lower()
    if not message:
        return None
    if "expired" in message:
        return "Expired Kite access token."
    if any(
        token in message
        for token in (
            "tokenexception",
            "invalid api_key or access_token",
            "invalid access token",
            "incorrect `api_key` or `access_token`",
            "access_token",
            "token is invalid",
        )
    ):
        return "Invalid Kite access token."
    return None


def _to_ymd(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    try:
        return pd.to_datetime(value, errors="coerce").strftime("%Y-%m-%d")
    except Exception:
        return str(value or "")[:10]


def _transform_scanner_stock_candles(
    symbol: str,
    token: str,
    candles: list[dict[str, Any]],
    valid_dates: set[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    previous_close: float | None = None

    for candle in candles:
        candle_date = _to_ymd(candle.get("date"))
        close_price = _safe_float(candle.get("close"), default=0.0)
        if candle_date not in valid_dates:
            if close_price > 0:
                previous_close = close_price
            continue

        out.append(
            {
                "h_symbol": symbol,
                "token": token,
                "ch_timestamp": candle_date,
                "ch_series": "EQ",
                "ch_opening_price": _safe_float(candle.get("open")),
                "ch_high_price": _safe_float(candle.get("high")),
                "ch_low_price": _safe_float(candle.get("low")),
                "ch_closing_price": close_price,
                "ch_volume": int(_safe_float(candle.get("volume"))),
                "ch_previous_cls_price": previous_close,
            }
        )
        if close_price > 0:
            previous_close = close_price

    return out


def _transform_scanner_index_candles(
    normalized_symbol: str,
    raw_symbol: str,
    candles: list[dict[str, Any]],
    valid_dates: set[str],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    previous_close: float | None = None

    for candle in candles:
        candle_date = _to_ymd(candle.get("date"))
        close_price = _safe_float(candle.get("close"), default=0.0)
        if candle_date not in valid_dates:
            if close_price > 0:
                previous_close = close_price
            continue

        change_pct = 0.0
        if previous_close and previous_close > 0:
            change_pct = round(((close_price - previous_close) / previous_close) * 100.0, 2)

        out.append(
            {
                "i_symbol": normalized_symbol,
                "index_symbol": raw_symbol,
                "ih_timestamp": candle_date,
                "i_open_price": _safe_float(candle.get("open")),
                "i_high_price": _safe_float(candle.get("high")),
                "i_low_price": _safe_float(candle.get("low")),
                "i_close_price": close_price,
                "i_volume": int(_safe_float(candle.get("volume"))),
                "i_previous_close": previous_close if previous_close is not None else close_price,
                "i_ch": change_pct,
            }
        )
        if close_price > 0:
            previous_close = close_price

    return out


def sync_market_holidays(year: int | None = None) -> dict[str, Any]:
    requested_year = int(year or datetime.now().year)
    holidays = NSE_MARKET_HOLIDAYS.get(requested_year)
    if not holidays:
        raise ValueError(f"Holiday data not configured for year {requested_year}.")

    db = MongoData()._db
    now = datetime.utcnow().isoformat()
    inserted = 0
    updated = 0

    for holiday in holidays:
        payload = {
            "date": holiday["date"],
            "description": holiday["description"],
            "year": requested_year,
            "updated_at": now,
            "source": "NSE Trading Holidays 2026",
        }
        result = db[MARKET_HOLIDAYS_COLLECTION].update_one(
            {"date": holiday["date"]},
            {"$set": payload, "$setOnInsert": {"created_at": now}},
            upsert=True,
        )
        if result.upserted_id is not None:
            inserted += 1
        elif result.modified_count:
            updated += 1

    return {
        "status": "success",
        "year": requested_year,
        "holiday_count": len(holidays),
        "inserted": inserted,
        "updated": updated,
        "dates": [holiday["date"] for holiday in holidays],
    }


def sync_scanner_historical_data(
    start_date: str,
    end_date: str,
    use_universe_list: bool = False,
) -> dict[str, Any]:
    def progress_print(message: str = "") -> None:
        print(message, flush=True)
    global _historical_sync_running, _historical_sync_stop_requested

    from_dt = _parse_ymd_date(start_date, field_name="start_date")
    to_dt = _parse_ymd_date(end_date, field_name="end_date")
    if from_dt > to_dt:
        raise ValueError("start_date cannot be after end_date.")

    if _historical_sync_running:
        raise ValueError("Historical sync is already running.")

    _historical_sync_running = True
    _historical_sync_stop_requested = False

    try:
        progress_print("🔐 Loading Kite market config credentials...")
        db = MongoData()._db
        kite = _build_kite_client_from_config()
        progress_print("✅ Kite client ready.")
        auth_validated = False

        holiday_dates = _load_market_holiday_dates()
        target_dates: set[str] = set()
        current = from_dt
        while current <= to_dt:
            date_str = current.strftime("%Y-%m-%d")
            if current.weekday() < 5 and date_str not in holiday_dates:
                target_dates.add(date_str)
            current += timedelta(days=1)

        progress_print(
            f"\n📅 Fetching scanner historical data from {from_dt.strftime('%Y-%m-%d')} → {to_dt.strftime('%Y-%m-%d')}"
        )
        progress_print(f"📊 Trading days in range: {len(target_dates)}")

        if not target_dates:
            progress_print("⏭️ Selected range contains only market holidays.")
            return {
                "status": "success",
                "message": "Selected range contains only market holidays.",
                "start_date": from_dt.strftime("%Y-%m-%d"),
                "end_date": to_dt.strftime("%Y-%m-%d"),
                "stocks_processed": 0,
                "indexes_processed": 0,
            }

        stock_fields = {
            "_id": 0, "symbol": 1, "tradingsymbol": 1, "name": 1, "series": 1,
            "kite_token": 1, "token": 1, "tokens": 1,
            "instrument_token": 1, "exchange_token": 1, "code": 1,
        }

        if use_universe_list:
            # Collect unique symbols from all active universe records
            universe_records = list(db[UNIVERSE_STOCK_LIST_COLLECTION].find(
                {"universe_period_end_date": ""},
                {"_id": 0, "filter_symbol": 1, "stocks": 1},
            ))
            universe_symbols: set[str] = set()
            for rec in universe_records:
                for sym in (rec.get("stocks") or []):
                    sym = str(sym or "").strip().upper()
                    if sym:
                        universe_symbols.add(sym)
            progress_print(
                f"📂 Source: scanner_universe_stock_list — "
                f"{len(universe_records)} active records, {len(universe_symbols)} unique symbols"
            )
            rows = list(db[STOCKS_COLLECTION].find(
                {"symbol": {"$in": list(universe_symbols)}},
                stock_fields,
            ))
            # Report symbols in universe but missing from stocks_list
            found_symbols = {str(r.get("symbol") or "").strip().upper() for r in rows}
            missing_from_stocks = sorted(universe_symbols - found_symbols)
            if missing_from_stocks:
                progress_print(
                    f"⚠️ {len(missing_from_stocks)} universe symbols not in scanner_stocks_list "
                    f"(no token): {missing_from_stocks[:10]}"
                )
        else:
            progress_print("📂 Source: scanner_stocks_list")
            rows = list(db[STOCKS_COLLECTION].find({}, stock_fields))
        nse_instrument_map = _load_kite_nse_instrument_map(kite)
        index_rows = _load_scanner_index_master(db, nse_instrument_map)

        stock_rows: list[dict[str, Any]] = []
        for row in rows:
            stock_rows.append(row)

        total_items = len(stock_rows) + len(index_rows)
        progress_print(f"📦 Total symbols to process: {total_items} ({len(stock_rows)} stocks, {len(index_rows)} indexes)")

        stock_collection = db[HISTORY_COLLECTION]
        index_collection = db[INDEX_HISTORY_COLLECTION]
        delete_from = from_dt.strftime("%Y-%m-%d")
        delete_to = to_dt.strftime("%Y-%m-%d")

        stocks_processed = 0
        indexes_processed = 0
        stock_records_inserted = 0
        index_records_inserted = 0
        stocks_skipped_existing = 0
        indexes_skipped_existing = 0
        skipped: list[dict[str, str]] = []
        processed_items = 0
        stop_reason = ""

        # Pre-load symbols that already have data in this date range (avoid Kite API calls for them)
        existing_stock_symbols: set[str] = set(
            stock_collection.distinct(
                "h_symbol",
                {"ch_timestamp": {"$gte": delete_from, "$lte": delete_to}},
            )
        )
        existing_index_symbols: set[str] = set(
            index_collection.distinct(
                "i_symbol",
                {"ih_timestamp": {"$gte": delete_from, "$lte": delete_to}},
            )
        )
        progress_print(
            f"📋 Already have data: {len(existing_stock_symbols)} stocks, {len(existing_index_symbols)} indexes — will skip these"
        )

        # Pre-load company names: symbol → company_name
        all_symbols = [str(r.get("symbol") or "").strip().upper() for r in stock_rows if r.get("symbol")]
        company_name_map: dict[str, str] = {}
        for doc in db[STOCKS_COLLECTION].find(
            {"symbol": {"$in": all_symbols}},
            {"_id": 0, "symbol": 1, "company_name": 1, "name": 1},
        ):
            sym = str(doc.get("symbol") or "").strip().upper()
            company_name_map[sym] = str(doc.get("company_name") or doc.get("name") or "").strip()

        # Pre-load universe membership: symbol → [filter_symbol, ...]
        universe_membership: dict[str, list[str]] = {}
        for rec in db[UNIVERSE_STOCK_LIST_COLLECTION].find(
            {"universe_period_end_date": ""},
            {"_id": 0, "filter_symbol": 1, "stocks": 1},
        ):
            filter_sym = str(rec.get("filter_symbol") or "").strip()
            for sym in (rec.get("stocks") or []):
                sym = str(sym or "").strip().upper()
                if sym:
                    universe_membership.setdefault(sym, []).append(filter_sym)

        # Fetch a short lookback so previous close remains correct for new inserts.
        fetch_start_base = from_dt - timedelta(days=HISTORICAL_LOOKBACK_BUFFER_DAYS)

        for _, index_row in sorted(index_rows.items()):
            if _historical_sync_stop_requested:
                stop_reason = "Stopped by user request before stock processing."
                progress_print("\n🛑 Stop requested. Ending sync before remaining symbols.")
                break
            kite_key = str(index_row["kite_key"]).strip().upper()
            normalized_symbol = str(index_row["normalized_symbol"]).strip()
            processed_items += 1
            progress_print(f"\n[{processed_items}/{total_items}] 📉 {kite_key or '-'} (index)")

            if normalized_symbol in existing_index_symbols:
                indexes_skipped_existing += 1
                progress_print(f"   ⏭️ Skipped: data already exists for this date range")
                continue

            fetch_mode = str(index_row.get("fetch_mode") or "").strip().lower() or (
                "token" if kite_key in KITE_INDEX_TOKENS else "nse"
            )
            if fetch_mode == "nse":
                inst = _resolve_nse_index_instrument(
                    nse_instrument_map,
                    kite_key,
                    str(index_row.get("raw_symbol") or ""),
                    str(index_row.get("normalized_symbol") or ""),
                )
                index_token = int(str((inst or {}).get("instrument_token") or "0").strip() or 0)
            else:
                index_token = int(KITE_INDEX_TOKENS.get(kite_key) or 0)
            if not index_token:
                skipped.append({"symbol": kite_key, "reason": "missing_index_token"})
                progress_print(f"   ⚠️ Skipped: missing index token ({fetch_mode})")
                continue

            all_candles = []
            current_from = fetch_start_base
            while current_from <= to_dt:
                current_to = min(current_from + timedelta(days=HISTORICAL_CHUNK_DAYS), to_dt)
                try:
                    all_candles.extend(_fetch_kite_daily_candles(kite, index_token, current_from, current_to))
                    auth_validated = True
                except Exception as exc:
                    token_error = _resolve_kite_access_token_error(exc)
                    if token_error:
                        progress_print(f"   ❌ {token_error}")
                        raise ValueError(token_error) from exc
                    skipped.append({"symbol": kite_key, "reason": f"kite_error:{exc}"})
                    progress_print(f"   ⚠️ Kite fetch error: {exc}")
                    all_candles = []
                    break
                current_from = current_to + timedelta(days=1)

            if not all_candles:
                progress_print("   ⏭️ No candles fetched")
                continue

            docs = _transform_scanner_index_candles(
                normalized_symbol,
                str(index_row["raw_symbol"]),
                all_candles,
                target_dates,
            )
            if not docs:
                progress_print("   ⏭️ No valid trading-day records to insert")
                continue

            index_collection.insert_many(docs, ordered=False)
            indexes_processed += 1
            index_records_inserted += len(docs)
            progress_print(f"   ✅ Inserted {len(docs)} records")
            progress_print(
                f"   📌 Progress: completed {processed_items}/{total_items} | total inserted {stock_records_inserted + index_records_inserted}"
            )

        if not stop_reason:
            for row in stock_rows:
                if _historical_sync_stop_requested:
                    stop_reason = "Stopped by user request during stock processing."
                    progress_print("\n🛑 Stop requested. Ending sync before remaining symbols.")
                    break
                symbol, token = _resolve_stock_kite_token(row)
                processed_items += 1
                progress_print(f"\n[{processed_items}/{total_items}] 📈 {symbol or '-'} (stock)")
                if not symbol or not token.isdigit():
                    company = company_name_map.get(symbol, "")
                    universes = universe_membership.get(symbol, [])
                    universe_str = ", ".join(universes) if universes else "not in any universe"
                    skipped.append({
                        "symbol": symbol or "-",
                        "reason": "missing_kite_token",
                        "company_name": company,
                        "universes": universes,
                    })
                    progress_print(
                        f"   ⚠️ Skipped: missing kite token"
                        f" | company={company or '-'}"
                        f" | universe={universe_str}"
                    )
                    continue

                if symbol in existing_stock_symbols:
                    stocks_skipped_existing += 1
                    progress_print(f"   ⏭️ Skipped: data already exists for this date range")
                    continue

                all_candles: list[dict[str, Any]] = []
                current_from = fetch_start_base
                while current_from <= to_dt:
                    current_to = min(current_from + timedelta(days=HISTORICAL_CHUNK_DAYS), to_dt)
                    try:
                        all_candles.extend(_fetch_kite_daily_candles(kite, int(token), current_from, current_to))
                        auth_validated = True
                    except Exception as exc:
                        token_error = _resolve_kite_access_token_error(exc)
                        if token_error:
                            progress_print(f"   ❌ {token_error}")
                            raise ValueError(token_error) from exc
                        skipped.append({"symbol": symbol, "reason": f"kite_error:{exc}"})
                        progress_print(f"   ⚠️ Kite fetch error: {exc}")
                        all_candles = []
                        break
                    current_from = current_to + timedelta(days=1)

                if not all_candles:
                    progress_print("   ⏭️ No candles fetched")
                    continue

                docs = _transform_scanner_stock_candles(symbol, token, all_candles, target_dates)
                if not docs:
                    progress_print("   ⏭️ No valid trading-day records to insert")
                    continue

                stock_collection.insert_many(docs, ordered=False)
                stocks_processed += 1
                stock_records_inserted += len(docs)
                progress_print(f"   ✅ Inserted {len(docs)} records")
                progress_print(
                    f"   📌 Progress: completed {processed_items}/{total_items} | total inserted {stock_records_inserted + index_records_inserted}"
                )

        if not auth_validated and (stock_rows or index_rows):
            progress_print("   ❌ Invalid Kite access token.")
            raise ValueError("Invalid Kite access token.")

        if stop_reason:
            progress_print(f"\n🛑 Scanner historical data sync stopped.")
            progress_print(f"   Reason       : {stop_reason}")
        else:
            progress_print("\n🎉 Scanner historical data sync completed.")
        progress_print(f"   Trading days : {len(target_dates)}")
        progress_print(f"   Indexes done : {indexes_processed} (skipped existing: {indexes_skipped_existing})")
        progress_print(f"   Stocks done  : {stocks_processed} (skipped existing: {stocks_skipped_existing})")
        progress_print(f"   Total insert : {stock_records_inserted + index_records_inserted}")
        progress_print(f"   Skipped      : {len(skipped)}")

        return {
            "status": "success",
            "message": "Historical sync stopped by user." if stop_reason else "Historical sync completed.",
            "stopped": bool(stop_reason),
            "start_date": delete_from,
            "end_date": delete_to,
            "trading_days": sorted(target_dates),
            "stocks_processed": stocks_processed,
            "stock_records_inserted": stock_records_inserted,
            "stocks_skipped_existing": stocks_skipped_existing,
            "indexes_processed": indexes_processed,
            "index_records_inserted": index_records_inserted,
            "indexes_skipped_existing": indexes_skipped_existing,
            "skipped": skipped,
        }
    finally:
        _historical_sync_running = False
        _historical_sync_stop_requested = False


def _load_kite_quotes(symbols: list[str]) -> tuple[dict[str, float], dict[str, float]]:
    normalized_symbols = [str(symbol or "").strip().upper() for symbol in symbols if str(symbol or "").strip()]
    if not normalized_symbols:
        return {}, {}

    try:
        access_token = _load_kite_access_token()
        if not access_token:
            return {}, {}

        kite = get_kite_instance(access_token)
        instruments = [f"NSE:{symbol}" for symbol in normalized_symbols]
        quotes = kite.quote(instruments) or {}

        ltp_map: dict[str, float] = {}
        close_map: dict[str, float] = {}
        for instrument, payload in quotes.items():
            symbol = str(instrument).split(":")[-1].strip().upper()
            last_price = _safe_float((payload or {}).get("last_price"))
            ohlc = (payload or {}).get("ohlc") or {}
            prev_close = _safe_float(ohlc.get("close"))
            if last_price > 0:
                ltp_map[symbol] = round(last_price, 2)
            if prev_close > 0:
                close_map[symbol] = round(prev_close, 2)
        return ltp_map, close_map
    except Exception:
        return {}, {}


def _clear_portfolio_summary_cache() -> None:
    global _portfolio_summary_cache_value, _portfolio_summary_cache_expires_at
    _portfolio_summary_cache_value = None
    _portfolio_summary_cache_expires_at = 0.0


def _build_live_price_map(
    investments: list[dict[str, Any]],
    *,
    allow_history_fallback: bool = True,
    prefetched_ltp_map: dict[str, float] | None = None,
    prefetched_close_map: dict[str, float] | None = None,
) -> tuple[dict[str, float], dict[str, float]]:
    active_symbols = sorted(
        {
            str(item.get("symbol") or "").strip()
            for item in investments
            if int(item.get("position_status", 1) or 1) == 1 and str(item.get("symbol") or "").strip()
        }
    )
    if prefetched_ltp_map is None or prefetched_close_map is None:
        kite_ltp_map, kite_close_map = _load_kite_quotes(active_symbols)
        close_map = dict(kite_close_map)
        if allow_history_fallback:
            for symbol, close in _load_latest_closes(active_symbols).items():
                close_map.setdefault(symbol.upper(), close)
        ltp_map: dict[str, float] = dict(kite_ltp_map)
    else:
        close_map = {
            symbol.upper(): round(_safe_float(prefetched_close_map.get(symbol)), 2)
            for symbol in active_symbols
            if _safe_float(prefetched_close_map.get(symbol)) > 0
        }
        ltp_map = {
            symbol.upper(): round(_safe_float(prefetched_ltp_map.get(symbol)), 2)
            for symbol in active_symbols
            if _safe_float(prefetched_ltp_map.get(symbol)) > 0
        }

    fallback_close_map: dict[str, float] = {}
    fallback_ltp_map: dict[str, float] = {}

    for item in investments:
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        fallback_close = _safe_float(item.get("yesterday_close"))
        if fallback_close > 0 and symbol not in fallback_close_map:
            fallback_close_map[symbol] = round(fallback_close, 2)
        if symbol in ltp_map:
            continue
        live_candidates = (
            item.get("ltp"),
            item.get("last_price"),
            item.get("live_price"),
            item.get("cmp"),
        )
        for candidate in live_candidates:
            live_value = _safe_float(candidate)
            if live_value > 0:
                fallback_ltp_map[symbol] = round(live_value, 2)
                break

    for symbol, close in fallback_close_map.items():
        close_map.setdefault(symbol, close)

    for symbol, live_value in fallback_ltp_map.items():
        ltp_map.setdefault(symbol, live_value)

    for symbol in active_symbols:
        symbol = symbol.upper()
        fallback_ltp = fallback_close_map.get(symbol, 0.0)
        ltp_map.setdefault(symbol, close_map.get(symbol, fallback_ltp))

    return ltp_map, close_map


def _build_portfolio_snapshot(
    portfolio_doc: dict[str, Any],
    investments: list[dict[str, Any]],
    *,
    include_investments: bool,
    allow_history_fallback: bool = True,
    prefetched_ltp_map: dict[str, float] | None = None,
    prefetched_close_map: dict[str, float] | None = None,
) -> dict[str, Any]:
    symbols = sorted({str(item.get("symbol") or "").strip() for item in investments if str(item.get("symbol") or "").strip()})
    company_map = _load_company_map(symbols) if include_investments else {}
    stock_meta_map = _load_stock_meta_map(symbols) if include_investments else {}
    ltp_map, close_map = _build_live_price_map(
        investments,
        allow_history_fallback=allow_history_fallback,
        prefetched_ltp_map=prefetched_ltp_map,
        prefetched_close_map=prefetched_close_map,
    )

    current_investment = 0.0
    unrealized_returns = 0.0
    realized_returns = 0.0
    day_returns = 0.0
    active_holdings = 0
    enriched_investments: list[dict[str, Any]] = []

    for item in investments:
        symbol = str(item.get("symbol") or "").strip()
        entry_price = _safe_float(item.get("entry_price"))
        quantity = int(_safe_float(item.get("position_qty") or item.get("qty") or item.get("quantity"), 0))
        position_status = int(item.get("position_status", 1) or 1)
        exit_price = _safe_float(item.get("exit_price"), entry_price)

        if position_status == 1:
            ltp = _safe_float(ltp_map.get(symbol), 0.0)
            if ltp <= 0:
                ltp = entry_price
            close = _safe_float(
                item.get("yesterday_close"),
                close_map.get(symbol, ltp if ltp > 0 else entry_price),
            )
            current_investment += entry_price * quantity
            unrealized_returns += (ltp - entry_price) * quantity
            day_returns += (ltp - close) * quantity
            active_holdings += 1
        else:
            ltp = exit_price if exit_price > 0 else entry_price
            close = ltp
            if position_status == 2:
                realized_returns += (ltp - entry_price) * quantity

        if include_investments:
            overall_pnl_amount = (ltp - entry_price) * quantity
            overall_pnl_percent = ((ltp - entry_price) / entry_price * 100.0) if entry_price else 0.0
            today_pnl = (ltp - close) * quantity if position_status == 1 else 0.0
            today_change = ((ltp - close) / close * 100.0) if close and position_status == 1 else 0.0
            investment_row = _serialize_doc(item)
            stock_meta = stock_meta_map.get(symbol, {})
            investment_row.update(
                {
                    "company_name": company_map.get(symbol, symbol or "N/A"),
                    "kite_token": investment_row.get("kite_token") or stock_meta.get("kite_token") or stock_meta.get("token") or "",
                    "token": investment_row.get("token") or stock_meta.get("token") or stock_meta.get("tokens") or stock_meta.get("instrument_token") or stock_meta.get("exchange_token") or stock_meta.get("code") or "",
                    "tokens": investment_row.get("tokens") or stock_meta.get("tokens") or stock_meta.get("token") or "",
                    "instrument_token": investment_row.get("instrument_token") or stock_meta.get("instrument_token") or stock_meta.get("token") or stock_meta.get("tokens") or "",
                    "exchange_token": investment_row.get("exchange_token") or stock_meta.get("exchange_token") or stock_meta.get("code") or "",
                    "investment_amount": round(entry_price * quantity, 2),
                    "ltp": round(ltp, 2),
                    "overall_pnl_amount": round(overall_pnl_amount, 2),
                    "overall_pnl_percent": round(overall_pnl_percent, 2),
                    "today_pnl": round(today_pnl, 2),
                    "today_change": round(today_change, 2),
                    "yesterday_close": round(close, 2),
                }
            )
            enriched_investments.append(investment_row)

    total_returns = realized_returns + unrealized_returns
    current_value = current_investment + total_returns
    current_value = round(current_value, 2)
    total_returns_pct = (total_returns / current_investment * 100.0) if current_investment else 0.0
    day_returns_pct = (day_returns / current_value * 100.0) if current_value else 0.0

    snapshot = {
        "portfolio_id": str(portfolio_doc.get("_id") or ""),
        "portfolio_name": str(portfolio_doc.get("strategy_name") or portfolio_doc.get("name") or "Unnamed").strip() or "Unnamed",
        "description": str(portfolio_doc.get("description") or "").strip(),
        "investment_value": round(current_investment, 2),
        "current_value": current_value,
        "realized_returns": round(realized_returns, 2),
        "unrealized_returns": round(unrealized_returns, 2),
        "returns": round(total_returns, 2),
        "return_pct": round(total_returns_pct, 2),
        "returns_percent": round(total_returns_pct, 2),
        "day_returns": round(day_returns, 2),
        "day_returns_percent": round(day_returns_pct, 2),
        "combained_portfilio": bool(portfolio_doc.get("combained_portfilio")),
        "holdings": active_holdings,
        "created_at": _serialize_value(portfolio_doc.get("created_at")),
    }

    if include_investments:
        serialized_portfolio = _serialize_doc(portfolio_doc)
        snapshot.update(
            {
                "strategy_id": snapshot["portfolio_id"],
                "strategy_name": snapshot["portfolio_name"],
                "total_stocks": len(investments),
                "current_investment": snapshot["investment_value"],
                "current_value": current_value,
                "realized_returns_value": snapshot["realized_returns"],
                "unrealized_returns_value": snapshot["unrealized_returns"],
                "total_returns_value": snapshot["returns"],
                "total_returns_percent": snapshot["returns_percent"],
                "day_returns_value": snapshot["day_returns"],
                "day_returns_percent": snapshot["day_returns_percent"],
                "investments": enriched_investments,
                "formula": serialized_portfolio.get("formula", ""),
                "strategy_index": serialized_portfolio.get("indexes", ""),
                "sector": serialized_portfolio.get("sectors", ""),
                "starting_capital": _safe_float(serialized_portfolio.get("starting_capital")),
                "entry_rank": serialized_portfolio.get("entry_rank", 0),
                "exit_rank": serialized_portfolio.get("exit_rank", 0),
                "rebalance_frequency": serialized_portfolio.get("rebalance_frequency", ""),
                "rebalance_date": serialized_portfolio.get("rebalance_date", ""),
                "alternative_rebalance_days": serialized_portfolio.get("alternative_rebalance_days", []),
                "position_sizing": serialized_portfolio.get("position_sizing", ""),
                "timestamp": datetime.utcnow().isoformat(),
                "uncorrelated_asset_status": bool(serialized_portfolio.get("uncorrelated_asset_status", False)),
                "uncorrelated_asset_allocation": _safe_float(serialized_portfolio.get("uncorrelated_asset_allocation")),
                "uncorrelated_asset_type": serialized_portfolio.get("uncorrelated_asset_type", "gold_bees"),
                "stocks_ltp": {key: round(_safe_float(value), 2) for key, value in ltp_map.items()},
                "portfolio": serialized_portfolio,
            }
        )

    return snapshot


def get_indexes() -> list[dict[str, str]]:
    db = MongoData()._db
    rows = list(
        db["scanner_index_stocks"].find(
            {},
            {"_id": 0, "name": 1, "filter_symbol": 1},
        ).sort("name", 1)
    )
    return [
        {
            "label": str(row.get("name") or row.get("filter_symbol") or "").strip(),
            "value": str(row.get("filter_symbol") or "").strip(),
        }
        for row in rows
        if str(row.get("filter_symbol") or "").strip()
    ]


UNIVERSE_STOCK_LIST_COLLECTION = "scanner_universe_stock_list"
_INITIAL_START_DATE = "01-01-1999"


_NSE_INDEX_CSV_URLS: dict[str, str] = {
    "nifty_50":               "https://nsearchives.nseindia.com/content/indices/ind_nifty50list.csv",
    "nifty_next_50":          "https://nsearchives.nseindia.com/content/indices/ind_niftynext50list.csv",
    "nifty_100":              "https://nsearchives.nseindia.com/content/indices/ind_nifty100list.csv",
    "nifty_200":              "https://nsearchives.nseindia.com/content/indices/ind_nifty200list.csv",
    "nifty_500":              "https://nsearchives.nseindia.com/content/indices/ind_nifty500list.csv",
    "nifty_midcap_50":        "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap50list.csv",
    "nifty_midcap_100":       "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap100list.csv",
    "nifty_midcap_150":       "https://nsearchives.nseindia.com/content/indices/ind_niftymidcap150list.csv",
    "nifty_smallcap_50":      "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap50list.csv",
    "nifty_smallcap_100":     "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap100list.csv",
    "nifty_smallcap_250":     "https://nsearchives.nseindia.com/content/indices/ind_niftysmallcap250list.csv",
    "nifty_large_midcap_250": "https://nsearchives.nseindia.com/content/indices/ind_niftylargemidcap250list.csv",
}


def _fetch_nse_csv(csv_url: str) -> "pd.DataFrame":
    import urllib.request
    import io

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.nseindia.com/",
    }
    req = urllib.request.Request(csv_url, headers=headers)
    with urllib.request.urlopen(req, timeout=15) as resp:
        content = resp.read().decode("utf-8", errors="ignore")
    return pd.read_csv(pd.io.common.StringIO(content))


def _fetch_nse_index_symbols(csv_url: str) -> list[str]:
    df = _fetch_nse_csv(csv_url)
    for col in ("Symbol", "SYMBOL", "symbol", "TradingSymbol", "tradingsymbol"):
        if col in df.columns:
            return [str(s).strip().upper() for s in df[col].dropna() if str(s).strip()]
    return []


def _fetch_nse_index_symbol_industry(csv_url: str) -> dict[str, str]:
    """Returns {SYMBOL: Industry} from NSE CSV."""
    df = _fetch_nse_csv(csv_url)
    sym_col = next((c for c in ("Symbol", "SYMBOL", "symbol") if c in df.columns), None)
    ind_col = next((c for c in ("Industry", "INDUSTRY", "industry") if c in df.columns), None)
    if not sym_col or not ind_col:
        return {}
    result: dict[str, str] = {}
    for _, row in df.iterrows():
        sym = str(row[sym_col] or "").strip().upper()
        ind = str(row[ind_col] or "").strip()
        if sym and ind:
            result[sym] = ind
    return result


def sync_stocks_industry_from_nse() -> dict[str, Any]:
    """Fetch industry for all stocks from NSE CSVs and update scanner_stocks_list."""
    import io

    # Build symbol → industry map from all universe CSVs
    symbol_industry: dict[str, str] = {}
    csv_errors: list[str] = []

    for filter_symbol, csv_url in _NSE_INDEX_CSV_URLS.items():
        try:
            data = _fetch_nse_index_symbol_industry(csv_url)
            symbol_industry.update(data)
            print(f"  {filter_symbol}: {len(data)} symbols with industry", flush=True)
        except Exception as exc:
            csv_errors.append(f"{filter_symbol}: {exc}")
            print(f"  {filter_symbol}: failed — {exc}", flush=True)

    if not symbol_industry:
        raise ValueError("Could not fetch industry data from any NSE CSV.")

    # Update scanner_stocks_list
    db = MongoData()._db
    now = datetime.utcnow()
    updated = 0
    not_found = 0

    from pymongo import UpdateOne
    ops = []
    for symbol, industry in symbol_industry.items():
        ops.append(UpdateOne(
            {"symbol": symbol},
            {"$set": {"industry": industry, "updated_at": now}},
        ))

    if ops:
        result = db[STOCKS_COLLECTION].bulk_write(ops, ordered=False)
        updated = result.modified_count
        not_found = len(ops) - result.matched_count

    return {
        "status": "success",
        "source": "NSE CSV",
        "total_symbols_with_industry": len(symbol_industry),
        "stocks_updated": updated,
        "stocks_not_found_in_db": not_found,
        "csv_errors": csv_errors,
    }


def get_kite_universe_stocks(filter_symbol: str) -> dict[str, Any]:
    filter_symbol = str(filter_symbol or "").strip().lower()
    if not filter_symbol:
        raise ValueError("Universe filter_symbol is required.")

    csv_url = _NSE_INDEX_CSV_URLS.get(filter_symbol)
    if not csv_url:
        supported = sorted(_NSE_INDEX_CSV_URLS.keys())
        raise ValueError(
            f"Universe '{filter_symbol}' not supported. "
            f"Supported: {supported}"
        )

    # Step 1: Fetch constituent symbols from NSE CSV
    try:
        nse_symbols = _fetch_nse_index_symbols(csv_url)
    except Exception as exc:
        raise ValueError(f"Failed to fetch NSE index CSV for '{filter_symbol}': {exc}") from exc

    if not nse_symbols:
        raise ValueError(f"No symbols found in NSE CSV for '{filter_symbol}'.")

    # Step 2: Fetch all NSE EQ instruments from Kite to get tokens + company names
    kite = _build_kite_client_from_config()
    nse_eq_map: dict[str, dict] = {}
    for inst in kite.instruments("NSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            nse_eq_map[sym] = inst

    # Step 3: Build enriched stock list
    stock_list: list[dict[str, Any]] = []
    missing_tokens: list[str] = []

    for symbol in sorted(nse_symbols):
        inst = nse_eq_map.get(symbol)
        if inst:
            stock_list.append({
                "symbol": symbol,
                "company_name": str(inst.get("name") or symbol).strip(),
                "kite_token": str(inst.get("instrument_token") or "").strip(),
                "exchange_token": str(inst.get("exchange_token") or "").strip(),
                "exchange": "NSE",
                "has_token": bool(inst.get("instrument_token")),
            })
        else:
            missing_tokens.append(symbol)
            stock_list.append({
                "symbol": symbol,
                "company_name": symbol,
                "kite_token": "",
                "exchange_token": "",
                "exchange": "NSE",
                "has_token": False,
            })

    return {
        "status": "success",
        "filter_symbol": filter_symbol,
        "source": "NSE CSV + Kite instruments",
        "total": len(stock_list),
        "missing_token_count": len(missing_tokens),
        "missing_tokens": missing_tokens,
        "stocks": stock_list,
    }


def _sync_one_universe(
    filter_symbol: str,
    nse_eq_map: dict[str, dict],
    db: Any,
    now: datetime,
) -> dict[str, Any]:
    """Core logic: fetch NSE CSV → upsert scanner_stocks_list → replace universe stocks array."""
    csv_url = _NSE_INDEX_CSV_URLS.get(filter_symbol)
    if not csv_url:
        return {"filter_symbol": filter_symbol, "status": "error", "reason": "unsupported_universe"}

    try:
        nse_symbols = _fetch_nse_index_symbols(csv_url)
    except Exception as exc:
        return {"filter_symbol": filter_symbol, "status": "error", "reason": f"csv_fetch_failed:{exc}"}

    if not nse_symbols:
        return {"filter_symbol": filter_symbol, "status": "error", "reason": "no_symbols_in_csv"}

    added: list[str] = []
    skipped_existing: list[str] = []
    missing_token: list[str] = []
    final_symbols: list[str] = []

    for symbol in sorted({s.strip().upper() for s in nse_symbols if s.strip()}):
        final_symbols.append(symbol)
        inst = nse_eq_map.get(symbol)
        kite_token = str(inst.get("instrument_token") or "").strip() if inst else ""
        exchange_token = str(inst.get("exchange_token") or "").strip() if inst else ""
        company_name = str(inst.get("name") or "").strip() if inst else ""

        existing = db[STOCKS_COLLECTION].find_one({"symbol": symbol}, {"_id": 1})
        if existing:
            skipped_existing.append(symbol)
        else:
            db[STOCKS_COLLECTION].insert_one({
                "symbol": symbol,
                "tradingsymbol": symbol,
                "company_name": company_name,
                "exchange": "NSE",
                "kite_token": kite_token,
                "instrument_token": kite_token,
                "exchange_token": exchange_token,
                "created_at": now,
                "updated_at": now,
            })
            added.append(symbol)
            if not kite_token:
                missing_token.append(symbol)

    # Replace stocks array in scanner_universe_stock_list
    universe_result = db[UNIVERSE_STOCK_LIST_COLLECTION].update_one(
        {"filter_symbol": filter_symbol, "universe_period_end_date": ""},
        {"$set": {"stocks": final_symbols, "updated_at": now}},
    )
    if universe_result.matched_count == 0:
        db[UNIVERSE_STOCK_LIST_COLLECTION].insert_one({
            "universe_id": str(ObjectId()),
            "filter_symbol": filter_symbol,
            "stocks": final_symbols,
            "universe_period_start_date": _INITIAL_START_DATE,
            "universe_period_end_date": "",
            "created_at": now,
            "updated_at": now,
        })
        universe_action = "created"
    else:
        universe_action = "updated"

    return {
        "filter_symbol": filter_symbol,
        "status": "success",
        "nse_symbols_fetched": len(nse_symbols),
        "stocks_list": {
            "added": len(added),
            "skipped_existing": len(skipped_existing),
            "missing_token": len(missing_token),
            "missing_token_symbols": missing_token,
        },
        "universe_list": {
            "action": universe_action,
            "stocks_count": len(final_symbols),
        },
    }


def sync_kite_universe_to_db(filter_symbol: str) -> dict[str, Any]:
    filter_symbol = str(filter_symbol or "").strip().lower()
    if not filter_symbol:
        raise ValueError("Universe filter_symbol is required.")
    if filter_symbol not in _NSE_INDEX_CSV_URLS:
        raise ValueError(f"Universe '{filter_symbol}' not supported. Supported: {sorted(_NSE_INDEX_CSV_URLS.keys())}")

    kite = _build_kite_client_from_config()
    nse_eq_map: dict[str, dict] = {}
    for inst in kite.instruments("NSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            nse_eq_map[sym] = inst

    db = MongoData()._db
    now = datetime.utcnow()
    result = _sync_one_universe(filter_symbol, nse_eq_map, db, now)
    if result.get("status") == "error":
        raise ValueError(result.get("reason", "unknown error"))
    return {"status": "success", "source": "NSE CSV + Kite instruments", **result}


def sync_all_kite_universes_to_db() -> dict[str, Any]:
    """Sync ALL supported universes — loads Kite instruments only once."""
    kite = _build_kite_client_from_config()
    nse_eq_map: dict[str, dict] = {}
    for inst in kite.instruments("NSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            nse_eq_map[sym] = inst

    db = MongoData()._db
    now = datetime.utcnow()

    results: list[dict[str, Any]] = []
    for filter_symbol in sorted(_NSE_INDEX_CSV_URLS.keys()):
        print(f"  Syncing {filter_symbol}...", flush=True)
        result = _sync_one_universe(filter_symbol, nse_eq_map, db, now)
        results.append(result)

    success_count = sum(1 for r in results if r.get("status") == "success")
    total_added = sum(r.get("stocks_list", {}).get("added", 0) for r in results)
    total_skipped = sum(r.get("stocks_list", {}).get("skipped_existing", 0) for r in results)

    return {
        "status": "success",
        "source": "NSE CSV + Kite instruments",
        "universes_total": len(results),
        "universes_success": success_count,
        "universes_failed": len(results) - success_count,
        "total_stocks_added_to_stocks_list": total_added,
        "total_stocks_skipped_existing": total_skipped,
        "results": results,
    }


def get_previous_universe_stocks(filter_symbol: str) -> dict[str, Any]:
    db = MongoData()._db
    filter_symbol = filter_symbol.strip()

    # Most recent closed record (end_date is not empty), sorted by end_date desc
    record = db[UNIVERSE_STOCK_LIST_COLLECTION].find_one(
        {"filter_symbol": filter_symbol, "universe_period_end_date": {"$ne": ""}},
        sort=[("universe_period_end_date", -1)],
    )

    is_current = False
    if not record:
        # No closed record — fall back to current active record
        record = db[UNIVERSE_STOCK_LIST_COLLECTION].find_one(
            {"filter_symbol": filter_symbol, "universe_period_end_date": ""},
        )
        is_current = True

    if not record:
        return {
            "status": "no_data",
            "message": f"No record found for '{filter_symbol}'.",
        }

    return {
        "status": "success",
        "is_current_active": is_current,
        "filter_symbol": filter_symbol,
        "universe_period_start_date": record.get("universe_period_start_date", ""),
        "universe_period_end_date": record.get("universe_period_end_date", ""),
        "stock_count": len(record.get("stocks") or []),
        "stocks": sorted(record.get("stocks") or []),
    }


def sync_universe_stock_list() -> dict[str, Any]:
    # Deprecated — use kite_sync_universe_stock_list instead.
    # scanner_universe_stock_list is now the single source of truth.
    return kite_sync_universe_stock_list()


def remove_universe_field_from_stocks_list() -> dict[str, Any]:
    db = MongoData()._db
    result = db[STOCKS_COLLECTION].update_many(
        {"universe": {"$exists": True}},
        {"$unset": {"universe": ""}},
    )
    return {
        "status": "success",
        "matched": result.matched_count,
        "modified": result.modified_count,
    }


def backfill_stocks_list_exchange() -> dict[str, Any]:
    db = MongoData()._db
    result = db[STOCKS_COLLECTION].update_many(
        {
            "$or": [
                {"exchange": {"$exists": False}},
                {"exchange": ""},
                {"exchange": None},
            ]
        },
        {"$set": {"exchange": "NSE"}},
    )
    return {
        "status": "success",
        "matched": result.matched_count,
        "modified": result.modified_count,
        "exchange": "NSE",
    }


def kite_sync_universe_stock_list() -> dict[str, Any]:
    db = MongoData()._db
    now = datetime.utcnow()
    today = now.strftime("%d-%m-%Y")
    yesterday = (now - timedelta(days=1)).strftime("%d-%m-%Y")

    # Fetch all current NSE EQ tradingsymbols from Kite
    access_token = _load_kite_access_token()
    if not access_token:
        raise ValueError("Kite access token not configured.")

    kite = get_kite_instance(access_token)
    raw_instruments = kite.instruments("NSE") or []

    kite_eq_symbols: set[str] = set()
    for inst in raw_instruments:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            kite_eq_symbols.add(sym)

    # Source: scanner_universe_stock_list active records (not stocks_list.universe)
    active_records = list(db[UNIVERSE_STOCK_LIST_COLLECTION].find(
        {"universe_period_end_date": ""},
        {"_id": 1, "filter_symbol": 1, "stocks": 1, "universe_id": 1},
    ))

    synced = []
    for active in active_records:
        filter_symbol = str(active.get("filter_symbol") or "").strip()
        universe_id = str(active.get("universe_id") or "").strip()
        if not filter_symbol:
            continue

        old_symbols = sorted(
            str(s or "").strip().upper()
            for s in (active.get("stocks") or [])
            if str(s or "").strip()
        )

        # Validate: keep only stocks still tradeable in Kite NSE EQ (detect delistings)
        new_symbols = sorted(s for s in old_symbols if s in kite_eq_symbols)
        missing_from_kite = [s for s in old_symbols if s not in kite_eq_symbols]

        if old_symbols == new_symbols:
            db[UNIVERSE_STOCK_LIST_COLLECTION].update_one(
                {"_id": active["_id"]},
                {"$set": {"updated_at": now}},
            )
            synced.append({
                "filter_symbol": filter_symbol,
                "action": "no_change",
                "stock_count": len(new_symbols),
                "missing_from_kite": missing_from_kite,
            })
            continue

        # Some stocks delisted — close old record, open new one
        db[UNIVERSE_STOCK_LIST_COLLECTION].update_one(
            {"_id": active["_id"]},
            {"$set": {"universe_period_end_date": yesterday, "updated_at": now}},
        )
        db[UNIVERSE_STOCK_LIST_COLLECTION].insert_one({
            "universe_id": universe_id,
            "filter_symbol": filter_symbol,
            "stocks": new_symbols,
            "universe_period_start_date": today,
            "universe_period_end_date": "",
            "created_at": now,
            "updated_at": now,
        })
        synced.append({
            "filter_symbol": filter_symbol,
            "action": "updated",
            "stock_count": len(new_symbols),
            "missing_from_kite": missing_from_kite,
        })

    return {
        "status": "success",
        "synced": synced,
        "total": len(synced),
    }


def _resolve_kite_instrument(
    symbol: str,
    nse_eq_map: dict[str, dict],
    bse_eq_map: dict[str, dict],
    nse_index_map: dict[str, dict],
) -> tuple[dict[str, Any], str]:
    """Try NSE EQ → BSE EQ → NSE INDEX → KITE_INDEX_TOKENS. Returns (inst, exchange)."""
    inst = nse_eq_map.get(symbol)
    if inst:
        return inst, "NSE"

    inst = bse_eq_map.get(symbol)
    if inst:
        return inst, "BSE"

    # Try NSE INDEX using SCANNER_INDEX_ALIASES to resolve raw symbol
    alias = SCANNER_INDEX_ALIASES.get(symbol)
    if alias:
        kite_key = alias[0]
        # Direct KITE_INDEX_TOKENS match
        index_token = KITE_INDEX_TOKENS.get(kite_key)
        if index_token:
            return {"instrument_token": str(index_token), "exchange_token": "", "tradingsymbol": symbol}, "NSE"
        # Try DEFAULT_SCANNER_INDEXES raw_symbol in nse_index_map
        default_idx = next(
            (d for d in DEFAULT_SCANNER_INDEXES if str(d.get("lookup_symbol") or "").strip().upper() == kite_key),
            None,
        )
        if default_idx:
            raw_sym = str(default_idx.get("raw_symbol") or "").strip().upper()
            inst = nse_index_map.get(raw_sym)
            if inst:
                return inst, "NSE"

    # Try NSE INDEX directly by symbol
    inst = nse_index_map.get(symbol)
    if inst:
        return inst, "NSE"

    return {}, ""


def backfill_stocks_list_kite_tokens() -> dict[str, Any]:
    db = MongoData()._db
    now = datetime.utcnow()

    access_token = _load_kite_access_token()
    if not access_token:
        raise ValueError("Kite access token not configured.")

    kite = get_kite_instance(access_token)

    # Build NSE EQ, NSE INDEX, BSE EQ maps
    nse_eq_map: dict[str, dict] = {}
    nse_index_map: dict[str, dict] = {}
    for inst in kite.instruments("NSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        itype = str(inst.get("instrument_type") or "").strip().upper()
        if not sym:
            continue
        if itype == "EQ":
            nse_eq_map[sym] = inst
        elif itype == "INDEX":
            nse_index_map[sym] = inst

    bse_eq_map: dict[str, dict] = {}
    for inst in kite.instruments("BSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            bse_eq_map[sym] = inst

    # ── Step 1: Update kite tokens for existing stocks_list symbols ──────────
    stocks = list(db[STOCKS_COLLECTION].find({}, {"_id": 1, "symbol": 1}))
    existing_symbols: set[str] = set()

    updated = 0
    not_found_in_kite: list[str] = []

    for row in stocks:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        existing_symbols.add(symbol)

        inst, exchange = _resolve_kite_instrument(symbol, nse_eq_map, bse_eq_map, nse_index_map)
        if not inst:
            not_found_in_kite.append(symbol)
            continue

        db[STOCKS_COLLECTION].update_one(
            {"_id": row["_id"]},
            {"$set": {
                "exchange": exchange,
                "kite_token": str(inst.get("instrument_token") or "").strip(),
                "instrument_token": str(inst.get("instrument_token") or "").strip(),
                "exchange_token": str(inst.get("exchange_token") or "").strip(),
                "tradingsymbol": str(inst.get("tradingsymbol") or symbol).strip(),
            }},
        )
        updated += 1

    # ── Step 2: Check universe stocks — find missing from stocks_list ─────────
    universe_records = list(db[UNIVERSE_STOCK_LIST_COLLECTION].find(
        {"universe_period_end_date": ""},
        {"_id": 0, "filter_symbol": 1, "stocks": 1},
    ))

    missing_from_stocks_list: list[dict[str, Any]] = []
    newly_added: list[str] = []

    for rec in universe_records:
        filter_symbol = str(rec.get("filter_symbol") or "").strip()
        for sym in (rec.get("stocks") or []):
            sym = str(sym or "").strip().upper()
            if not sym or sym in existing_symbols:
                continue

            inst, exchange = _resolve_kite_instrument(sym, nse_eq_map, bse_eq_map, nse_index_map)
            db[STOCKS_COLLECTION].insert_one({
                "symbol": sym,
                "tradingsymbol": sym,
                "exchange": exchange or "NSE",
                "kite_token": str(inst.get("instrument_token") or "").strip(),
                "instrument_token": str(inst.get("instrument_token") or "").strip(),
                "exchange_token": str(inst.get("exchange_token") or "").strip(),
                "created_at": now,
            })
            existing_symbols.add(sym)
            newly_added.append(sym)
            missing_from_stocks_list.append({"symbol": sym, "universe": filter_symbol})

    return {
        "status": "success",
        "stocks_list_total": len(stocks),
        "kite_token_updated": updated,
        "not_found_in_kite": not_found_in_kite,
        "not_found_in_kite_count": len(not_found_in_kite),
        "missing_from_stocks_list_added": missing_from_stocks_list,
        "missing_added_count": len(newly_added),
    }


_MISSING_STOCK_SYMBOLS = [
    "TATAMOTORS", "LTIM", "AKZOINDIA", "DUMMYDBRLT", "GSPL", "RELINFRA",
    "DBREALTY", "CIGNITITEC", "DHANI", "INFIBEAM", "JAIBALAJI", "JCHAC",
    "KRN", "RAJESHEXPO", "SEQUENT", "STLTECH", "SUNDARMHLD",
]

_MISSING_INDEX_SYMBOLS = [
    "NIFTY50", "NIFTY500", "NIFTYMIDCAP100", "NIFTY100",
    "NIFTYNXT50", "NIFTYSMLCAP100", "NIFTY200",
]


def backfill_missing_tokens() -> dict[str, Any]:
    db = MongoData()._db
    now = datetime.utcnow()

    kite = _build_kite_client_from_config()

    # Build instrument maps: NSE EQ, NSE INDEX, BSE EQ
    nse_eq_map: dict[str, dict] = {}
    nse_index_map: dict[str, dict] = {}
    bse_eq_map: dict[str, dict] = {}
    for inst in kite.instruments("NSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        itype = str(inst.get("instrument_type") or "").strip().upper()
        if not sym:
            continue
        if itype == "EQ":
            nse_eq_map[sym] = inst
        elif itype == "INDEX":
            nse_index_map[sym] = inst
    for inst in kite.instruments("BSE") or []:
        sym = str(inst.get("tradingsymbol") or "").strip().upper()
        if sym and str(inst.get("instrument_type") or "").strip().upper() == "EQ":
            bse_eq_map[sym] = inst

    stocks_updated: list[dict] = []
    stocks_already_have_token: list[str] = []
    stocks_not_found: list[dict] = []

    # ── Stocks ─────────────────────────────────────────────────────────────────
    for symbol in _MISSING_STOCK_SYMBOLS:
        row = db[STOCKS_COLLECTION].find_one(
            {"symbol": symbol},
            {"_id": 1, "kite_token": 1, "company_name": 1},
        )
        existing_token = str((row or {}).get("kite_token") or "").strip()
        try:
            token_ok = bool(existing_token and int(float(existing_token)) > 0)
        except (ValueError, TypeError):
            token_ok = False

        if token_ok:
            stocks_already_have_token.append(symbol)
            continue

        inst, exchange = _resolve_kite_instrument(symbol, nse_eq_map, bse_eq_map, nse_index_map)
        raw_token = str(inst.get("instrument_token") or "").strip() if inst else ""
        try:
            resolved_token = str(int(float(raw_token))) if raw_token else ""
        except (ValueError, TypeError):
            resolved_token = ""

        if resolved_token:
            company_name = str(inst.get("name") or "").strip() or symbol
            update_fields = {
                "kite_token": resolved_token,
                "instrument_token": resolved_token,
                "exchange_token": str(inst.get("exchange_token") or "").strip(),
                "exchange": exchange,
                "tradingsymbol": str(inst.get("tradingsymbol") or symbol).strip(),
                "updated_at": now,
            }
            if company_name and company_name != symbol:
                update_fields["company_name"] = company_name
            if row:
                db[STOCKS_COLLECTION].update_one({"_id": row["_id"]}, {"$set": update_fields})
            else:
                db[STOCKS_COLLECTION].insert_one({"symbol": symbol, **update_fields})
            stocks_updated.append({
                "symbol": symbol,
                "token": resolved_token,
                "exchange": exchange,
                "company_name": company_name,
            })
        else:
            db_company = str((row or {}).get("company_name") or "").strip()
            kite_company = str((inst or {}).get("name") or "").strip() if inst else ""
            stocks_not_found.append({
                "symbol": symbol,
                "company_name": db_company or kite_company or symbol,
            })

    # ── Indexes ────────────────────────────────────────────────────────────────
    indexes_updated: list[dict] = []
    indexes_already_have_token: list[str] = []
    indexes_not_found: list[dict] = []

    for symbol in _MISSING_INDEX_SYMBOLS:
        alias = SCANNER_INDEX_ALIASES.get(symbol)
        if not alias:
            indexes_not_found.append({"symbol": symbol, "reason": "no_alias"})
            continue
        kite_key, normalized_symbol = alias

        # Resolve index token
        index_token_int = int(KITE_INDEX_TOKENS.get(kite_key) or 0)
        exchange_token_str = ""
        if not index_token_int:
            default_idx = next(
                (d for d in DEFAULT_SCANNER_INDEXES if str(d.get("lookup_symbol") or "").strip().upper() == kite_key),
                None,
            )
            raw_sym = str((default_idx or {}).get("raw_symbol") or "").strip().upper()
            nse_inst = nse_index_map.get(raw_sym) if raw_sym else None
            if nse_inst:
                index_token_int = int(str(nse_inst.get("instrument_token") or "0") or 0)
                exchange_token_str = str(nse_inst.get("exchange_token") or "").strip()

        if not index_token_int:
            indexes_not_found.append({"symbol": symbol, "kite_key": kite_key, "reason": "token_not_found"})
            continue

        token_str = str(index_token_int)

        # Update scanner_index_stocks (match by normalized_symbol or symbol)
        idx_row = db[INDEX_STOCKS_COLLECTION].find_one(
            {"$or": [{"normalized_symbol": normalized_symbol}, {"symbol": kite_key}, {"filter_symbol": symbol.lower()}]},
            {"_id": 1, "kite_token": 1},
        )
        existing_idx_token = str((idx_row or {}).get("kite_token") or "").strip()
        try:
            idx_token_ok = bool(existing_idx_token and int(float(existing_idx_token)) > 0)
        except (ValueError, TypeError):
            idx_token_ok = False

        if idx_token_ok:
            indexes_already_have_token.append(symbol)
        elif idx_row:
            db[INDEX_STOCKS_COLLECTION].update_one(
                {"_id": idx_row["_id"]},
                {"$set": {
                    "kite_token": token_str,
                    "instrument_token": token_str,
                    "exchange_token": exchange_token_str,
                    "normalized_symbol": normalized_symbol,
                    "updated_at": now,
                }},
            )

        # Also update scanner_stocks_list if this index symbol exists there
        stock_row = db[STOCKS_COLLECTION].find_one({"symbol": symbol}, {"_id": 1})
        if stock_row:
            db[STOCKS_COLLECTION].update_one(
                {"_id": stock_row["_id"]},
                {"$set": {"kite_token": token_str, "instrument_token": token_str, "updated_at": now}},
            )

        indexes_updated.append({
            "symbol": symbol,
            "kite_key": kite_key,
            "normalized_symbol": normalized_symbol,
            "token": token_str,
        })

    # Print company names for still-missing stocks
    if stocks_not_found:
        print("\n❌ Stocks still missing token — search by company name:", flush=True)
        for item in stocks_not_found:
            print(f"   {item['symbol']:20} → company: {item['company_name']}", flush=True)

    return {
        "status": "success",
        "stocks_updated": stocks_updated,
        "stocks_updated_count": len(stocks_updated),
        "stocks_already_have_token": stocks_already_have_token,
        "stocks_not_found": stocks_not_found,
        "stocks_not_found_count": len(stocks_not_found),
        "indexes_updated": indexes_updated,
        "indexes_updated_count": len(indexes_updated),
        "indexes_already_have_token": indexes_already_have_token,
        "indexes_not_found": indexes_not_found,
    }


def get_sectors() -> list[dict[str, str]]:
    db = MongoData()._db
    values = db[STOCKS_COLLECTION].distinct("industry")
    sectors = sorted(str(item).strip() for item in values if str(item).strip())
    return [{"label": sector, "value": sector} for sector in sectors]


def _get_symbols_from_universe_history(index_names: list[str], score_date: datetime) -> list[str]:
    """Return stock symbols from scanner_universe_stock_list valid for score_date."""
    db = MongoData()._db
    score_day = score_date.date()

    def _parse_ddmmyyyy(s: str):
        try:
            return datetime.strptime(s.strip(), "%d-%m-%Y").date()
        except Exception:
            return None

    symbols: list[str] = []
    for index_name in index_names:
        records = list(
            db[UNIVERSE_STOCK_LIST_COLLECTION].find(
                {"filter_symbol": index_name},
                {"_id": 0, "stocks": 1, "universe_period_start_date": 1, "universe_period_end_date": 1},
            )
        )
        for rec in records:
            start = _parse_ddmmyyyy(str(rec.get("universe_period_start_date") or ""))
            end_str = str(rec.get("universe_period_end_date") or "").strip()
            end = _parse_ddmmyyyy(end_str) if end_str else None

            if start is None:
                continue
            if start > score_day:
                continue
            if end is not None and end < score_day:
                continue

            symbols.extend(str(s).strip().upper() for s in (rec.get("stocks") or []) if str(s).strip())
            break  # found the matching period for this index

    return sorted(set(symbols))


def _load_meta(
    index_names: list[str],
    sectors: list[str],
    symbols_override: list[str] | None = None,
) -> pd.DataFrame:
    """
    Always use symbols_override (from scanner_universe_stock_list) as the universe source.
    stocks_list is queried only for metadata (sector/industry) — never by universe field.
    """
    db = MongoData()._db

    if not symbols_override:
        return pd.DataFrame()

    projection = {"_id": 0, "symbol": 1, "industry": 1}
    meta_rows = list(db[STOCKS_COLLECTION].find(
        {"symbol": {"$in": symbols_override}},
        projection,
    ))

    # Symbols from universe that have no entry in stocks_list still need to appear
    meta_symbol_set = {str(r.get("symbol") or "").strip() for r in meta_rows}
    for sym in symbols_override:
        if sym not in meta_symbol_set:
            meta_rows.append({"symbol": sym, "industry": ""})

    meta = pd.DataFrame(meta_rows)
    meta["symbol"] = meta["symbol"].astype(str).str.strip()
    meta["sector"] = meta["industry"].fillna("").astype(str).str.strip()
    meta["universe"] = ",".join(sorted(index_names)) if index_names else ""

    meta = meta[meta["symbol"] != ""]
    if sectors:
        meta = meta[meta["sector"].isin(sectors)]
    meta = meta.drop_duplicates(subset=["symbol"], keep="first").reset_index(drop=True)
    return meta[["symbol", "sector", "universe"]]


def _load_history(symbols: list[str], hist_start: datetime, hist_end: datetime) -> pd.DataFrame:
    if not symbols:
        return pd.DataFrame()
    db = MongoData()._db
    query = {
        "h_symbol": {"$in": symbols},
        "ch_timestamp": {
            "$gte": hist_start.strftime("%Y-%m-%d"),
            "$lte": hist_end.strftime("%Y-%m-%d 23:59:59"),
        },
    }
    projection = {"_id": 0, "h_symbol": 1, "ch_timestamp": 1, "ch_closing_price": 1}
    rows = list(db[HISTORY_COLLECTION].find(query, projection))
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["h_symbol"] = df["h_symbol"].astype(str).str.strip()
    df["ch_timestamp"] = pd.to_datetime(df["ch_timestamp"], errors="coerce")
    df["ch_closing_price"] = pd.to_numeric(df["ch_closing_price"], errors="coerce")
    df = df.dropna(subset=["h_symbol", "ch_timestamp", "ch_closing_price"])
    df = df.sort_values(["h_symbol", "ch_timestamp"]).reset_index(drop=True)
    return df


def _compute_metrics(history: pd.DataFrame) -> pd.DataFrame:
    df = history.copy()
    df["ret"] = df.groupby("h_symbol")["ch_closing_price"].pct_change()

    for suffix, periods in TRADING_PERIODS.items():
        perf_col = f"perf_{suffix}"
        vol_col = f"vol_{suffix}"
        df[perf_col] = df.groupby("h_symbol")["ch_closing_price"].pct_change(periods=periods) * 100.0
        df[vol_col] = df.groupby("h_symbol")["ret"].transform(
            lambda series: series.rolling(periods, min_periods=5).std() * 100.0
        )

    metric_columns = [column for column in df.columns if column.startswith(("perf_", "vol_"))]
    df[metric_columns] = df[metric_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0)
    return df


def _evaluate_scores(df: pd.DataFrame, formula: str) -> pd.Series:
    expression = _normalize_formula(formula)
    try:
        return df.eval(expression, engine="python")
    except Exception as exc:
        raise ValueError(f"Invalid scoring formula: {formula}. Parsed: {expression}") from exc


def _attach_kite_tokens(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        return rows

    stock_meta_map = _load_stock_meta_map(
        [str(row.get("symbol") or "").strip() for row in rows if str(row.get("symbol") or "").strip()]
    )

    enriched_rows: list[dict[str, Any]] = []
    for row in rows:
        symbol = str(row.get("symbol") or "").strip()
        stock_meta = stock_meta_map.get(symbol, {})
        resolved_token = (
            row.get("kite_token")
            or row.get("token")
            or row.get("tokens")
            or row.get("instrument_token")
            or row.get("exchange_token")
            or row.get("code")
            or stock_meta.get("kite_token")
            or stock_meta.get("token")
            or stock_meta.get("tokens")
            or stock_meta.get("instrument_token")
            or stock_meta.get("exchange_token")
            or stock_meta.get("code")
            or ""
        )
        enriched_rows.append(
            {
                **row,
                "kite_token": resolved_token,
                "symbol_token": row.get("symbol_token") or resolved_token,
                "token": row.get("token") or resolved_token,
                "tokens": row.get("tokens") or resolved_token,
                "instrument_token": row.get("instrument_token") or resolved_token,
            }
        )
    return enriched_rows


def _get_portfolio_settings(strategy_id: str) -> dict[str, Any]:
    strategy_oid = _as_object_id(strategy_id)
    db = MongoData()._db
    portfolio_doc = db[PORTFOLIO_COLLECTION].find_one({"_id": strategy_oid})
    if not portfolio_doc:
        raise ValueError("Portfolio not found.")
    return portfolio_doc


def get_portfolio_settings(strategy_id: str) -> dict[str, Any]:
    return _serialize_doc(_get_portfolio_settings(strategy_id))


def _build_investment_payload_from_scores(
    strategy_id: str,
    score_data: list[dict[str, Any]],
    total_capital: float | None = None,
    top_n: int | None = None,
) -> list[dict[str, Any]]:
    if not score_data:
        return []

    portfolio_doc = _get_portfolio_settings(strategy_id)
    resolved_total_capital = _safe_float(
        total_capital,
        _safe_float(portfolio_doc.get("current_value"), _safe_float(portfolio_doc.get("starting_capital"))),
    )
    resolved_top_n = max(1, int(_safe_float(top_n, _safe_float(portfolio_doc.get("entry_rank"), 1))))

    rows_df = pd.DataFrame(score_data)
    if rows_df.empty:
        return []

    for column in ("rank", "last_price", "score", "perf_1M", "perf_3M", "perf_6M", "perf_1Y", "vol_1M", "vol_3M", "vol_6M", "vol_1Y"):
        if column not in rows_df.columns:
            rows_df[column] = 0.0

    rows_df["rank"] = pd.to_numeric(rows_df["rank"], errors="coerce").fillna(0).astype(int)
    rows_df["last_price"] = pd.to_numeric(rows_df["last_price"], errors="coerce").fillna(0.0)
    rows_df["score"] = pd.to_numeric(rows_df["score"], errors="coerce").fillna(0.0)

    investment_rows, _summary = _build_portfolio(rows_df, resolved_total_capital, resolved_top_n)
    return investment_rows


def _build_portfolio(score_rows: pd.DataFrame, total_capital: float, top_n: int) -> tuple[list[dict[str, Any]], dict[str, float]]:
    top_n = max(1, int(top_n or 1))
    equal_investment = float(total_capital) / float(top_n)
    portfolio_rows: list[dict[str, Any]] = []
    used_capital = 0.0

    for row in score_rows.sort_values("rank").head(top_n).to_dict(orient="records"):
        last_price = float(row.get("last_price") or 0.0)
        qty = int(equal_investment // last_price) if last_price > 0 else 0
        amount = round(qty * last_price, 2)
        used_capital += amount
        portfolio_rows.append(
            {
                **row,
                "qty": qty,
                "amount": amount,
                "Investment": round(equal_investment, 2),
            }
        )

    portfolio_rows = _attach_kite_tokens(portfolio_rows)

    summary = {
        "total_capital": round(float(total_capital), 2),
        "used_capital": round(used_capital, 2),
        "remaining_capital": round(float(total_capital) - used_capital, 2),
    }
    return portfolio_rows, summary


def generate_stock_scores(payload: dict[str, Any]) -> dict[str, Any]:
    index_names = _normalize_list_payload(payload.get("index_name") or payload.get("index_names"))
    sectors = _normalize_list_payload(payload.get("sectors"))
    min_price = payload.get("min_price")
    max_price = payload.get("max_price")
    top_n = int(payload.get("top_n", 12) or 12)
    total_capital = float(payload.get("total_capital", 1_000_000) or 1_000_000)
    formula = str(payload.get("formula") or DEFAULT_FORMULA)
    score_date = _coerce_score_date(payload.get("score_date"))
    if pd.isna(score_date):
        score_date = datetime.now()

    if index_names:
        symbols_override = _get_symbols_from_universe_history(index_names, score_date)
        if not symbols_override:
            return {
                "status": "no_data",
                "message": f"No universe stock list found for {index_names} on {score_date.date()}. "
                           "Please run /scanner/kite_sync_universe_stocks first.",
            }
    else:
        symbols_override = []

    meta = _load_meta(index_names, sectors, symbols_override=symbols_override)
    if meta.empty:
        return {
            "status": "no_data",
            "message": "No stocks found for the selected filters.",
        }

    # Load 500 days so compute_sigma_scores_core has enough data for its 400-day filter
    hist_start = score_date - timedelta(days=LOOKBACK_DAYS)
    hist_end = score_date - timedelta(days=1)
    history = _load_history(meta["symbol"].tolist(), hist_start, hist_end)
    if history.empty:
        return {
            "status": "no_data",
            "message": "No historical data found for the selected filters.",
        }

    # Use compute_sigma_scores_core (400/401-day lookback) — exact same logic as sigma-backtest
    from scanner.backtest_engine_before_change_score_code import compute_sigma_scores_core, parse_dynamic_formula
    expr = parse_dynamic_formula(formula)
    score_ts = pd.Timestamp(score_date)
    history_df = (
        history.set_index("ch_timestamp").sort_index()
        if not isinstance(history.index, pd.DatetimeIndex)
        else history
    )
    latest = compute_sigma_scores_core(history_df, score_ts, expr, meta, "sigma")
    if latest is None or latest.empty:
        return {"status": "no_data", "message": "No stocks scored."}

    latest["last_price"] = latest.get("last_price", pd.Series(dtype=float))
    if "last_price" not in latest.columns or latest["last_price"].isna().all():
        latest["last_price"] = latest.get("ch_closing_price", pd.Series(dtype=float)).round(2)

    if min_price not in (None, "", 0, "0"):
        latest = latest[latest["last_price"] >= float(min_price)]
    if max_price not in (None, "", 0, "0"):
        latest = latest[latest["last_price"] <= float(max_price)]

    if latest.empty:
        return {
            "status": "no_data",
            "message": "No stocks matched the selected price range.",
        }

    # Re-rank after price filter (compute_sigma_scores_core already ranked, but filter may change order)
    latest["rank"] = latest["score"].rank(ascending=False, method="first").astype(int)
    latest = latest.sort_values("rank").reset_index(drop=True)

    ordered_columns = [
        "rank",
        "symbol",
        "sector",
        "universe",
        "score",
        "last_price",
        "perf_1M",
        "perf_3M",
        "perf_6M",
        "perf_9M",
        "perf_1Y",
        "vol_1M",
        "vol_3M",
        "vol_6M",
        "vol_9M",
        "vol_1Y",
    ]
    for column in ordered_columns:
        if column not in latest.columns:
            latest[column] = 0.0

    snapshot = latest[ordered_columns].replace([np.inf, -np.inf], np.nan).fillna(0.0).round(6)
    snapshot_rows = _attach_kite_tokens(snapshot.to_dict(orient="records"))
    investment_portfolio, summary = _build_portfolio(pd.DataFrame(snapshot_rows), total_capital, top_n)

    return {
        "status": "success",
        "meta": {
            "index_names": index_names,
            "sectors": sectors,
            "min_price": min_price,
            "max_price": max_price,
            "top_n": top_n,
            "total_capital": total_capital,
            "formula_used": formula,
            "score_date": score_date.strftime("%Y-%m-%d"),
            "lookback_start": hist_start.strftime("%Y-%m-%d"),
            "lookback_end": hist_end.strftime("%Y-%m-%d"),
        },
        "stocks_scored": snapshot_rows,
        "investment_portfolio": investment_portfolio,
        "summary": summary,
    }


def get_portfolio_summary() -> list[dict[str, Any]]:
    global _portfolio_summary_cache_value, _portfolio_summary_cache_expires_at
    now = time.monotonic()
    if _portfolio_summary_cache_value is not None and now < _portfolio_summary_cache_expires_at:
        return _portfolio_summary_cache_value

    db = MongoData()._db
    portfolios = list(
        db[PORTFOLIO_COLLECTION]
        .find(
            {},
            {
                "_id": 1,
                "strategy_name": 1,
                "name": 1,
                "description": 1,
                "combained_portfilio": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
    )
    if not portfolios:
        return []

    portfolio_ids = [item.get("_id") for item in portfolios if item.get("_id") is not None]
    investments = list(
        db[INVESTMENT_COLLECTION].find(
            {"strategy_id": {"$in": portfolio_ids}},
            {
                "_id": 0,
                "strategy_id": 1,
                "symbol": 1,
                "entry_price": 1,
                "position_qty": 1,
                "qty": 1,
                "quantity": 1,
                "position_status": 1,
                "exit_price": 1,
                "yesterday_close": 1,
                "ltp": 1,
                "last_price": 1,
                "live_price": 1,
                "cmp": 1,
            },
        )
    )

    investments_by_strategy: dict[ObjectId, list[dict[str, Any]]] = {}
    all_active_symbols: set[str] = set()
    for item in investments:
        strategy_id = item.get("strategy_id")
        if isinstance(strategy_id, ObjectId):
            investments_by_strategy.setdefault(strategy_id, []).append(item)
        if int(item.get("position_status", 1) or 1) == 1:
            symbol = str(item.get("symbol") or "").strip().upper()
            if symbol:
                all_active_symbols.add(symbol)

    prefetched_ltp_map, prefetched_close_map = _load_kite_quotes(sorted(all_active_symbols))
    for symbol, close in _load_latest_closes(sorted(all_active_symbols)).items():
        prefetched_close_map.setdefault(symbol.upper(), close)

    snapshots: list[dict[str, Any]] = []
    for portfolio_doc in portfolios:
        strategy_id = portfolio_doc.get("_id")
        if not isinstance(strategy_id, ObjectId):
            continue
        strategy_investments = investments_by_strategy.get(strategy_id, [])
        if not strategy_investments:
            continue
        snapshots.append(
            _build_portfolio_snapshot(
                portfolio_doc,
                strategy_investments,
                include_investments=False,
                allow_history_fallback=False,
                prefetched_ltp_map=prefetched_ltp_map,
                prefetched_close_map=prefetched_close_map,
            )
        )
    _portfolio_summary_cache_value = snapshots
    _portfolio_summary_cache_expires_at = now + PORTFOLIO_SUMMARY_CACHE_TTL_SECONDS
    return snapshots


def get_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    object_id = _as_object_id(strategy_id)
    db = MongoData()._db
    portfolio_doc = db[PORTFOLIO_COLLECTION].find_one({"_id": object_id})
    if not portfolio_doc:
        raise ValueError("Portfolio not found.")

    investments = list(db[INVESTMENT_COLLECTION].find({"strategy_id": object_id}))
    if not investments:
        raise ValueError("No investments found for this portfolio.")

    return _build_portfolio_snapshot(
        portfolio_doc,
        investments,
        include_investments=True,
        allow_history_fallback=True,
    )


def get_combained_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    object_id = _as_object_id(strategy_id)
    db = MongoData()._db
    portfolio_doc = db[PORTFOLIO_COLLECTION].find_one({"_id": object_id})
    if not portfolio_doc:
        raise ValueError("Portfolio not found.")

    investments = list(db[INVESTMENT_COLLECTION].find({"strategy_id": object_id}))
    if not investments:
        raise ValueError("No investments found for this portfolio.")

    return _build_combained_portfolio_snapshot(portfolio_doc, investments)


def _build_combained_portfolio_snapshot(portfolio_doc: dict[str, Any], investments: list[dict[str, Any]]) -> dict[str, Any]:
    symbols = sorted({str(item.get("symbol") or "").strip() for item in investments if str(item.get("symbol") or "").strip()})
    company_map = _load_company_map(symbols)
    ltp_map, close_map = _build_live_price_map(investments, allow_history_fallback=True)

    active_investment = 0.0
    realized_returns = 0.0
    unrealized_returns = 0.0
    day_returns = 0.0
    active_holdings = 0
    enriched_investments: list[dict[str, Any]] = []

    for item in investments:
        symbol = str(item.get("symbol") or "").strip()
        entry_price = _safe_float(item.get("entry_price"))
        base_qty = int(_safe_float(item.get("position_qty") or item.get("qty") or item.get("quantity"), 0))
        position_status = int(item.get("position_status", 1) or 1)
        primary_qty = int(_safe_float(item.get("primary_investment_qty"), 0))
        secondary_qty = int(_safe_float(item.get("secondary_investment_qty"), 0))
        if primary_qty <= 0 and secondary_qty <= 0:
          primary_qty = base_qty
        exit_price = _safe_float(item.get("exit_price"), entry_price)

        if position_status == 1:
            ltp = _safe_float(ltp_map.get(symbol), entry_price)
            close = _safe_float(item.get("yesterday_close"), close_map.get(symbol, ltp))
            if not item.get("primary_exit_date"):
                qty = primary_qty or 0
                active_investment += entry_price * qty
                unrealized_returns += (ltp - entry_price) * qty
            elif item.get("primary_exit_price") is not None:
                exit_qty = int(_safe_float(item.get("primary_exit_qty"), primary_qty or base_qty))
                realized_returns += (_safe_float(item.get("primary_exit_price"), ltp) - entry_price) * exit_qty

            if secondary_qty > 0:
                if not item.get("secondary_exit_date"):
                    active_investment += entry_price * secondary_qty
                    unrealized_returns += (ltp - entry_price) * secondary_qty
                elif item.get("secondary_exit_price") is not None:
                    exit_qty = int(_safe_float(item.get("secondary_exit_qty"), secondary_qty))
                    realized_returns += (_safe_float(item.get("secondary_exit_price"), ltp) - entry_price) * exit_qty

            day_returns += (ltp - close) * base_qty
            active_holdings += 1
        else:
            ltp = exit_price if exit_price > 0 else entry_price
            close = ltp
            exit_qty = int(_safe_float(item.get("exit_qty"), base_qty))
            realized_returns += (ltp - entry_price) * exit_qty

        overall_pnl_amount = (ltp - entry_price) * base_qty
        overall_pnl_percent = ((ltp - entry_price) / entry_price * 100.0) if entry_price else 0.0
        today_pnl = (ltp - close) * base_qty if position_status == 1 else 0.0
        today_change = ((ltp - close) / close * 100.0) if close and position_status == 1 else 0.0

        investment_row = _serialize_doc(item)
        investment_row.update(
            {
                "_id": str(item.get("_id") or ""),
                "company_name": company_map.get(symbol, symbol or "N/A"),
                "investment_amount": round(entry_price * base_qty, 2),
                "ltp": round(ltp, 2),
                "overall_pnl_amount": round(overall_pnl_amount, 2),
                "overall_pnl_percent": round(overall_pnl_percent, 2),
                "today_pnl": round(today_pnl, 2),
                "today_change": round(today_change, 2),
                "yesterday_close": round(close, 2),
                "exit_type": investment_row.get("indentification"),
                "entry_type": investment_row.get("indentification"),
            }
        )

        if item.get("secondary_exit_date") and not item.get("primary_exit_date"):
            exit_qty = int(_safe_float(item.get("secondary_exit_qty"), secondary_qty))
            secondary_exit_price = _safe_float(item.get("secondary_exit_price"), ltp)
            investment_row.update(
                {
                    "secondary_exit_date": _serialize_value(item.get("secondary_exit_date")),
                    "secondary_exit_price": round(secondary_exit_price, 2),
                    "secondary_exit_qty": exit_qty,
                    "secondary_exit_rank": item.get("secondary_exit_rank"),
                    "exit_type": "Secondary",
                    "exit_date": _serialize_value(item.get("secondary_exit_date")),
                    "exit_price": round(secondary_exit_price, 2),
                    "exit_qty": exit_qty,
                    "exit_overall_pnl_amount": round((secondary_exit_price - entry_price) * exit_qty, 2),
                    "exit_overall_pnl_percent": round(((secondary_exit_price - entry_price) / entry_price * 100.0) if entry_price else 0.0, 2),
                    "entry_type": "Primary" if position_status == 1 else investment_row.get("entry_type"),
                }
            )

        if item.get("primary_exit_date") and not item.get("secondary_exit_date"):
            exit_qty = int(_safe_float(item.get("primary_exit_qty"), primary_qty or base_qty))
            primary_exit_price = _safe_float(item.get("primary_exit_price"), ltp)
            investment_row.update(
                {
                    "primary_exit_date": _serialize_value(item.get("primary_exit_date")),
                    "primary_exit_price": round(primary_exit_price, 2),
                    "primary_exit_qty": exit_qty,
                    "primary_exit_rank": item.get("primary_exit_rank"),
                    "exit_type": "Primary",
                    "exit_date": _serialize_value(item.get("primary_exit_date")),
                    "exit_price": round(primary_exit_price, 2),
                    "exit_qty": exit_qty,
                    "exit_overall_pnl_amount": round((primary_exit_price - entry_price) * exit_qty, 2),
                    "exit_overall_pnl_percent": round(((primary_exit_price - entry_price) / entry_price * 100.0) if entry_price else 0.0, 2),
                    "entry_type": "Secondary" if position_status == 1 and secondary_qty > 0 else investment_row.get("entry_type"),
                }
            )

        if item.get("primary_exit_date") and item.get("secondary_exit_date"):
            exit_qty = int(_safe_float(item.get("exit_qty"), base_qty))
            investment_row.update(
                {
                    "entry_type": "Primary & Secondary",
                    "exit_overall_pnl_amount": round((ltp - entry_price) * exit_qty, 2),
                    "exit_overall_pnl_percent": round(((ltp - entry_price) / entry_price * 100.0) if entry_price else 0.0, 2),
                }
            )

        enriched_investments.append(investment_row)

    total_returns = realized_returns + unrealized_returns
    current_value = active_investment + total_returns
    total_returns_pct = (total_returns / active_investment * 100.0) if active_investment else 0.0
    day_returns_pct = (day_returns / current_value * 100.0) if current_value else 0.0

    serialized_portfolio = _serialize_doc(portfolio_doc)
    return {
        "portfolio_id": str(portfolio_doc.get("_id") or ""),
        "portfolio_name": str(portfolio_doc.get("strategy_name") or portfolio_doc.get("name") or "Unnamed").strip() or "Unnamed",
        "strategy_id": str(portfolio_doc.get("_id") or ""),
        "strategy_name": str(portfolio_doc.get("strategy_name") or portfolio_doc.get("name") or "Unnamed").strip() or "Unnamed",
        "description": str(portfolio_doc.get("description") or "").strip(),
        "total_stocks": len(investments),
        "holdings": active_holdings,
        "current_investment": round(active_investment, 2),
        "investment_value": round(active_investment, 2),
        "current_value": round(current_value, 2),
        "realized_returns_value": round(realized_returns, 2),
        "unrealized_returns_value": round(unrealized_returns, 2),
        "total_returns_value": round(total_returns, 2),
        "total_returns_percent": round(total_returns_pct, 2),
        "day_returns_value": round(day_returns, 2),
        "day_returns_percent": round(day_returns_pct, 2),
        "investments": enriched_investments,
        "formula": serialized_portfolio.get("formula", ""),
        "strategy_index": serialized_portfolio.get("indexes", ""),
        "sector": serialized_portfolio.get("sectors", ""),
        "starting_capital": _safe_float(serialized_portfolio.get("starting_capital")),
        "entry_rank": serialized_portfolio.get("entry_rank", 0),
        "exit_rank": serialized_portfolio.get("exit_rank", 0),
        "rebalance_frequency": serialized_portfolio.get("rebalance_frequency", ""),
        "rebalance_date": serialized_portfolio.get("rebalance_date", ""),
        "entry_rank_1": serialized_portfolio.get("entry_rank_1", 0),
        "exit_rank_1": serialized_portfolio.get("exit_rank_1", 0),
        "rebalance_frequency_1": serialized_portfolio.get("rebalance_frequency_1", ""),
        "rebalance_date_1": serialized_portfolio.get("rebalance_date_1", ""),
        "position_sizing": serialized_portfolio.get("position_sizing", ""),
        "formula_1": serialized_portfolio.get("formula_1", ""),
        "alternative_rebalance_days": serialized_portfolio.get("alternative_rebalance_days", []),
        "uncorrelated_asset_status": bool(serialized_portfolio.get("uncorrelated_asset_status", False)),
        "uncorrelated_asset_allocation": _safe_float(serialized_portfolio.get("uncorrelated_asset_allocation")),
        "uncorrelated_asset_type": serialized_portfolio.get("uncorrelated_asset_type", "gold_bees"),
        "created_at": _serialize_value(portfolio_doc.get("created_at")),
        "timestamp": datetime.utcnow().isoformat(),
        "stocks_ltp": {key: round(_safe_float(value), 2) for key, value in ltp_map.items()},
        "portfolio": serialized_portfolio,
    }


def generate_combained_stock_scores(payload: dict[str, Any]) -> dict[str, Any]:
    first = generate_stock_scores(payload)
    if first.get("status") != "success":
        return first

    second_payload = {
        "index_name": payload.get("index_name_1") or payload.get("index_names_1"),
        "sectors": payload.get("sectors_1"),
        "min_price": payload.get("min_price_1"),
        "max_price": payload.get("max_price_1"),
        "top_n": payload.get("top_n_1", payload.get("top_n", 12)),
        "total_capital": payload.get("total_capital_1", payload.get("total_capital", 1_000_000)),
        "score_date": payload.get("score_date_1", payload.get("score_date")),
        "formula": payload.get("formula_1"),
    }
    second = generate_stock_scores(second_payload)
    if second.get("status") != "success":
        return {
            **first,
            "stocks_scored_1": [],
        }

    response = dict(first)
    response["stocks_scored_1"] = second.get("stocks_scored", [])
    return response


def save_investment_portfolio(invest_stocks: list[dict[str, Any]], strategy_id: str) -> int:
    strategy_oid = _as_object_id(strategy_id)
    db = MongoData()._db
    stock_meta_map = _load_stock_meta_map(
        [str(stock.get("symbol") or "").strip() for stock in invest_stocks if str(stock.get("symbol") or "").strip()]
    )
    investment_docs = []
    for stock in invest_stocks:
        symbol = str(stock.get("symbol") or "").strip()
        stock_meta = stock_meta_map.get(symbol, {})
        resolved_token = (
            stock.get("kite_token")
            or stock.get("symbol_token")
            or stock.get("token")
            or stock.get("tokens")
            or stock.get("instrument_token")
            or stock.get("exchange_token")
            or stock.get("code")
            or stock_meta.get("kite_token")
            or stock_meta.get("symbol_token")
            or stock_meta.get("token")
            or stock_meta.get("tokens")
            or stock_meta.get("instrument_token")
            or stock_meta.get("exchange_token")
            or stock_meta.get("code")
        )
        investment_doc = {
            "entry_price": _safe_float(stock.get("last_price")),
            "position_qty": int(_safe_float(stock.get("qty"), 0)),
            "rank": int(_safe_float(stock.get("rank"), 0)),
            "universe": stock.get("universe"),
            "sector": stock.get("sector"),
            "score": _safe_float(stock.get("score")),
            "symbol": stock.get("symbol"),
            "investment_amount": _safe_float(stock.get("amount")),
            "perf_1M": _safe_float(stock.get("perf_1M")),
            "perf_3M": _safe_float(stock.get("perf_3M")),
            "perf_6M": _safe_float(stock.get("perf_6M")),
            "perf_1Y": _safe_float(stock.get("perf_1Y")),
            "vol_1M": _safe_float(stock.get("vol_1M")),
            "vol_3M": _safe_float(stock.get("vol_3M")),
            "vol_6M": _safe_float(stock.get("vol_6M")),
            "vol_1Y": _safe_float(stock.get("vol_1Y")),
            "entry_date": datetime.utcnow(),
            "strategy_id": strategy_oid,
            "position_status": 1,
            "exited_qty": 0,
            "symbol_type": "equity",
            "kite_token": resolved_token,
            "symbol_token": resolved_token,
            "token": resolved_token,
            "tokens": resolved_token,
            "instrument_token": resolved_token,
            "exchange_token": stock.get("exchange_token") or stock_meta.get("exchange_token") or stock.get("code") or stock_meta.get("code"),
            "indentification": stock.get("indentification"),
            "primary_investment_amount": stock.get("primary_investment_amount"),
            "primary_investment_investment": stock.get("primary_investment_investment"),
            "primary_investment_qty": stock.get("primary_investment_qty"),
            "primary_investment_rank": stock.get("primary_investment_rank"),
            "secondary_investment_amount": stock.get("secondary_investment_amount"),
            "secondary_investment_allocation": stock.get("secondary_investment_allocation"),
            "secondary_investment_qty": stock.get("secondary_investment_qty"),
            "secondary_investment_rank": stock.get("secondary_investment_rank"),
        }
        investment_docs.append(investment_doc)
    if not investment_docs:
        return 0
    insert_result = db[INVESTMENT_COLLECTION].insert_many(investment_docs)
    inserted_ids = list(insert_result.inserted_ids or [])
    post_insert_updates: list[UpdateOne] = []
    for inserted_id, investment_doc in zip(inserted_ids, investment_docs):
        resolved_token = investment_doc.get("symbol_token") or investment_doc.get("kite_token") or investment_doc.get("token")
        if resolved_token in (None, ""):
            continue
        post_insert_updates.append(
            UpdateOne(
                {"_id": inserted_id},
                {
                    "$set": {
                        "symbol_token": resolved_token,
                        "kite_token": resolved_token,
                        "token": resolved_token,
                        "tokens": resolved_token,
                        "instrument_token": resolved_token,
                    }
                },
            )
        )
    if post_insert_updates:
        db[INVESTMENT_COLLECTION].bulk_write(post_insert_updates, ordered=False)
    _clear_portfolio_summary_cache()
    return len(investment_docs)


def save_portfolio_settings(portfolio_data: dict[str, Any]) -> str:
    if not isinstance(portfolio_data, dict) or not portfolio_data:
        raise ValueError("portfolio_settings is required.")

    db = MongoData()._db
    portfolio_doc = dict(portfolio_data)
    portfolio_doc["created_at"] = datetime.utcnow()
    result = db[PORTFOLIO_COLLECTION].insert_one(portfolio_doc)
    _clear_portfolio_summary_cache()
    return str(result.inserted_id)


def save_portfolio(portfolio_settings: dict[str, Any], invest_stock_data: list[dict[str, Any]]) -> dict[str, Any]:
    if not portfolio_settings:
        raise ValueError("portfolio_settings is required.")
    if not invest_stock_data:
        raise ValueError("invest_stock_data is required.")

    portfolio_strategy_id = save_portfolio_settings(portfolio_settings)
    inserted_count = save_investment_portfolio(invest_stock_data, portfolio_strategy_id)
    return {
        "status": "success",
        "portfolio_strategy_id": portfolio_strategy_id,
        "inserted_investments": inserted_count,
    }


def delete_portfolio(portfolio_id: str) -> dict[str, Any]:
    object_id = _as_object_id(portfolio_id)
    db = MongoData()._db
    result = db[PORTFOLIO_COLLECTION].delete_one({"_id": object_id})
    if result.deleted_count == 0:
        raise ValueError(f"Portfolio not found: {portfolio_id}")
    db[INVESTMENT_COLLECTION].delete_many({"strategy_id": object_id})
    _clear_portfolio_summary_cache()
    return {"status": "success", "deleted_portfolio_id": portfolio_id}


def update_portfolio_investments(
    portfolio_strategy_id: str,
    invest_stock_data: list[dict[str, Any]] | None,
    *,
    score_data: list[dict[str, Any]] | None = None,
    total_capital: float | None = None,
    top_n: int | None = None,
) -> dict[str, Any]:
    payload_rows = list(invest_stock_data or [])
    if not payload_rows and score_data:
        payload_rows = _build_investment_payload_from_scores(
            portfolio_strategy_id,
            score_data,
            total_capital=total_capital,
            top_n=top_n,
        )
    if not payload_rows:
        raise ValueError("invest_stock_data or score_data is required.")

    inserted_count = save_investment_portfolio(payload_rows, portfolio_strategy_id)
    _clear_portfolio_summary_cache()
    return {
        "status": "success",
        "portfolio_strategy_id": portfolio_strategy_id,
        "inserted_investments": inserted_count,
    }


def get_live_portfolio(strategy_id: str) -> dict[str, Any]:
    return get_portfolio_detail(strategy_id)


def get_live_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    return get_combained_portfolio_detail(strategy_id)


def _update_position_as_exited(investment_id: ObjectId, exit_price: float, exit_qty: int, exit_rank: int = 0) -> bool:
    db = MongoData()._db
    result = db[INVESTMENT_COLLECTION].update_one(
        {"_id": investment_id},
        {
            "$set": {
                "exit_price": round(exit_price, 2),
                "exit_date": datetime.utcnow(),
                "exit_qty": exit_qty,
                "exit_rank": exit_rank,
                "position_status": 2,
            }
        },
    )
    if result.modified_count > 0:
        _clear_portfolio_summary_cache()
    return result.modified_count > 0


def rebalance_portfolio(strategy_id: str) -> dict[str, Any]:
    detail = get_portfolio_detail(strategy_id)
    score_request = {
        "index_name": detail.get("strategy_index"),
        "sectors": detail.get("sector"),
        "min_price": None,
        "max_price": None,
        "top_n": detail.get("entry_rank"),
        "total_capital": detail.get("starting_capital"),
        "score_date": datetime.now().strftime("%Y-%m-%d"),
        "formula": detail.get("formula"),
    }
    score_result = generate_stock_scores(score_request)
    stocks_scored = score_result.get("stocks_scored", [])
    score_map = {str(item.get("symbol")): item for item in stocks_scored}
    active_positions = [row for row in detail.get("investments", []) if int(row.get("position_status", 1) or 1) == 1]

    total_investment_amount = sum(_safe_float(stock.get("ltp")) * int(_safe_float(stock.get("position_qty"), 0)) for stock in active_positions)
    remaining_payment = _safe_float(detail.get("starting_capital")) - total_investment_amount

    exited_stocks: list[dict[str, Any]] = []
    update_status: list[dict[str, Any]] = []
    insert_status: list[dict[str, Any]] = []
    exit_stock_investment_amount = 0.0

    exit_rank_threshold = int(_safe_float(detail.get("exit_rank"), 0))
    entry_rank_threshold = int(_safe_float(detail.get("entry_rank"), 0))
    for stock in active_positions:
        symbol = str(stock.get("symbol") or "").strip()
        current_rank = int(_safe_float((score_map.get(symbol) or {}).get("rank"), 999999))
        if symbol and current_rank > exit_rank_threshold:
            ltp = _safe_float(stock.get("ltp"), _safe_float(stock.get("entry_price")))
            qty = int(_safe_float(stock.get("position_qty"), 0))
            exit_value = qty * ltp
            ok = _update_position_as_exited(_as_object_id(stock.get("_id")), ltp, qty, current_rank)
            update_status.append({"symbol": symbol, "status": "Updated" if ok else "Failed"})
            exited_stocks.append(
                {
                    "symbol": symbol,
                    "exit_price": round(ltp, 2),
                    "exit_date": datetime.utcnow().isoformat(),
                    "exit_qty": qty,
                    "exit_value": round(exit_value, 2),
                }
            )
            exit_stock_investment_amount += exit_value

    remaining_payment += exit_stock_investment_amount
    active_symbols = {str(p.get("symbol") or "").strip() for p in active_positions if int(p.get("position_status", 1) or 1) == 1}
    new_stocks = [row for row in stocks_scored if int(_safe_float(row.get("rank"), 999999)) <= entry_rank_threshold and str(row.get("symbol") or "").strip() not in active_symbols][: len(exited_stocks)]
    capital_per_stock = (remaining_payment / len(new_stocks)) if new_stocks else 0.0
    db = MongoData()._db
    for stock in new_stocks:
        ltp = _safe_float(stock.get("last_price"))
        qty = int(capital_per_stock // ltp) if ltp > 0 else 0
        if qty <= 0:
            continue
        investment_amount = qty * ltp
        db[INVESTMENT_COLLECTION].insert_one(
            {
                "symbol": stock.get("symbol"),
                "entry_price": ltp,
                "entry_date": datetime.utcnow(),
                "strategy_id": _as_object_id(strategy_id),
                "position_status": 1,
                "position_qty": qty,
                "exited_qty": 0,
                "symbol_type": "equity",
                "sector": stock.get("sector"),
                "universe": stock.get("universe"),
                "score": stock.get("score"),
                "rank": stock.get("rank"),
                "investment_amount": round(investment_amount, 2),
            }
        )
        insert_status.append({"symbol": stock.get("symbol"), "status": "Inserted"})
        remaining_payment -= investment_amount
        _clear_portfolio_summary_cache()

    return {
        "total_investment_before": round(total_investment_amount, 2),
        "exit_stock_value": round(exit_stock_investment_amount, 2),
        "remaining_capital_after": round(remaining_payment, 2),
        "exited_count": len(exited_stocks),
        "new_buy_count": len(insert_status),
        "exited_stocks": exited_stocks,
        "update_status": update_status,
        "insert_status": insert_status,
    }


def rebalance_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    return rebalance_portfolio(strategy_id)


def invest_goldbees_strategy(strategy_id: str) -> dict[str, Any]:
    detail = get_portfolio_detail(strategy_id)
    active_positions = [row for row in detail.get("investments", []) if int(row.get("position_status", 1) or 1) == 1]
    exited_stocks: list[dict[str, Any]] = []
    update_status: list[dict[str, Any]] = []
    db = MongoData()._db

    total_investment_amount = sum(_safe_float(stock.get("entry_price")) * int(_safe_float(stock.get("position_qty"), 0)) for stock in active_positions)
    current_investment_amount = sum(_safe_float(stock.get("ltp")) * int(_safe_float(stock.get("position_qty"), 0)) for stock in active_positions)
    current_remaining_amount = _safe_float(detail.get("starting_capital")) - total_investment_amount
    remaining_payment = current_remaining_amount + current_investment_amount

    for stock in active_positions:
        symbol = str(stock.get("symbol") or "").strip()
        ltp = _safe_float(stock.get("ltp"), _safe_float(stock.get("entry_price")))
        qty = int(_safe_float(stock.get("position_qty"), 0))
        ok = _update_position_as_exited(_as_object_id(stock.get("_id")), ltp, qty, 0)
        update_status.append({"symbol": symbol, "status": "Updated" if ok else "Failed"})
        exited_stocks.append(
            {
                "symbol": symbol,
                "exit_price": round(ltp, 2),
                "exit_date": datetime.utcnow().isoformat(),
                "exit_qty": qty,
                "exit_value": round(qty * ltp, 2),
            }
        )

    gold_cash = max(0.0, (remaining_payment - 1000.0) * (_safe_float(detail.get("uncorrelated_asset_allocation")) / 100.0))
    gold_ltp = _build_live_price_map([{"symbol": "GOLDBEES", "position_status": 1}], allow_history_fallback=True)[0].get("GOLDBEES")
    if not gold_ltp:
        gold_ltp = _load_latest_closes(["GOLDBEES"]).get("GOLDBEES", 0.0)
    if not gold_ltp:
        gold_ltp = 125.4
    gold_qty = int(gold_cash // gold_ltp) if gold_ltp > 0 else 0
    investment_amount = gold_qty * gold_ltp
    insert_status: list[dict[str, Any]] = []
    if gold_qty > 0:
        db[INVESTMENT_COLLECTION].insert_one(
            {
                "symbol": "GOLDBEES",
                "entry_price": round(gold_ltp, 2),
                "position_qty": gold_qty,
                "rank": 0,
                "universe": "nifty_500",
                "sector": "Gold",
                "score": 0,
                "investment_amount": round(investment_amount, 2),
                "perf_1M": 0,
                "perf_3M": 0,
                "perf_6M": 0,
                "perf_1Y": 0,
                "vol_1M": 0,
                "vol_3M": 0,
                "vol_6M": 0,
                "vol_1Y": 0,
                "entry_date": datetime.utcnow(),
                "strategy_id": _as_object_id(strategy_id),
                "position_status": 1,
                "exited_qty": 0,
                "symbol_type": "equity",
            }
        )
        insert_status.append({"symbol": "GOLDBEES", "status": "Inserted"})
        _clear_portfolio_summary_cache()

    return {
        "total_investment_before": round(total_investment_amount, 2),
        "exit_stock_value": round(current_investment_amount, 2),
        "remaining_capital_after": round(remaining_payment - investment_amount, 2),
        "exited_count": len(exited_stocks),
        "new_buy_count": len(insert_status),
        "exited_stocks": exited_stocks,
        "update_status": update_status,
        "insert_status": insert_status,
    }


def exit_goldbees_strategy(strategy_id: str) -> dict[str, Any]:
    detail = get_portfolio_detail(strategy_id)
    gold_rows = [
        row
        for row in detail.get("investments", [])
        if int(row.get("position_status", 1) or 1) == 1 and str(row.get("symbol") or "").strip().upper() == "GOLDBEES"
    ]
    if not gold_rows:
        raise ValueError("No active GOLDBEES position found.")

    exited_stocks = []
    update_status = []
    for stock in gold_rows:
        symbol = str(stock.get("symbol") or "").strip()
        ltp = _safe_float(stock.get("ltp"), _safe_float(stock.get("entry_price")))
        qty = int(_safe_float(stock.get("position_qty"), 0))
        ok = _update_position_as_exited(_as_object_id(stock.get("_id")), ltp, qty, 0)
        update_status.append({"symbol": symbol, "status": "Updated" if ok else "Failed"})
        exited_stocks.append(
            {
                "symbol": symbol,
                "exit_price": round(ltp, 2),
                "exit_date": datetime.utcnow().isoformat(),
                "exit_qty": qty,
                "exit_value": round(qty * ltp, 2),
            }
        )

    return {
        "status": "success",
        "exited_count": len(exited_stocks),
        "new_buy_count": 0,
        "exited_stocks": exited_stocks,
        "update_status": update_status,
    }


# =====================================================================
# Scanner Backtest
# =====================================================================
def run_scanner_backtest(payload: dict[str, Any]) -> dict[str, Any]:
    from scanner.backtest_engine_before_change_score_code import (
        ultra_backtest_v2_fastest_fixed_v3,
        load_index_daily,
        load_stocks_bulk,
        get_universe_stocks,
    )

    INDICATOR_SETTINGS: dict[str, dict[str, Any]] = {
        "supertrend_1_2_5": {"name": "supertrend", "params": (1, 2.7)},
    }
    DEFAULT_FORMULA = "((70% * 6 Month Volatility) + (20% * 3 Month Performance) + (10% * 1 Year Performance)) / 3 Month Volatility"
    DEFAULT_FILTER_ACTION = "go_cash"
    DEFAULT_UNIVERSES = ["nifty_50", "nifty_500"]
    DEFAULT_GOLD_ALLOC = 0.85

    # Basic portfolio settings
    capital = float(payload.get("starting_capital", 1_000_000))
    indexes = payload.get("indexes", DEFAULT_UNIVERSES)
    entry_rank = int(payload.get("entry_rank", 12))
    exit_rank = int(payload.get("exit_rank", 40))
    rebalance_frequency = str(payload.get("rebalance_frequency", "Monthly"))
    rebalance_day = int(payload.get("rebalance_date", 10))
    min_price = payload.get("min_price")
    max_price = payload.get("max_price")
    formula = str(payload.get("formula", DEFAULT_FORMULA)).strip() or DEFAULT_FORMULA
    strategy_name = str(payload.get("strategy_name", "My Strategy")).strip() or "My Strategy"

    start_date_str = payload.get("start_date")
    end_date_str = payload.get("end_date")
    start_dt = datetime.strptime(str(start_date_str), "%Y-%m-%d")
    end_dt = datetime.strptime(str(end_date_str), "%Y-%m-%d")

    # Regime filter
    exit_regime_filter_status = bool(payload.get("regime_filter_status", True))
    regime_filter = str(payload.get("regime_filter", "supertrend_1_2_5"))
    indicator_cfg = INDICATOR_SETTINGS.get(regime_filter, INDICATOR_SETTINGS["supertrend_1_2_5"])
    indicator_name = indicator_cfg["name"]
    regime_filter_indexes = str(payload.get("regime_filter_indexes", "nifty_500")).strip()
    regime_filter_action = str(payload.get("regime_filter_action", DEFAULT_FILTER_ACTION))

    # Uncorrelated asset
    uncorrelated_asset_status = bool(payload.get("uncorrelated_asset_status", True))
    uncorrelated_asset_type = str(payload.get("uncorrelated_asset_type", "gold_bees"))
    uncorrelated_asset_allocation = int(payload.get("uncorrelated_asset_allocation", 85))
    gold_alloc = uncorrelated_asset_allocation / 100.0 if uncorrelated_asset_type == "gold_bees" else DEFAULT_GOLD_ALLOC

    print(f"\n=== Scanner Backtest Configuration ===")
    print(f"Strategy: {strategy_name} | Indexes: {indexes} | Capital: {capital:,.0f}")
    print(f"Entry: {entry_rank} | Exit: {exit_rank} | {rebalance_frequency} day={rebalance_day}")
    print(f"Regime: {indicator_name} ({regime_filter_indexes}) → {regime_filter_action}")
    print(f"Gold: {uncorrelated_asset_type} alloc={gold_alloc:.0%} | Start: {start_dt.date()} | End: {end_dt.date()}")

    # Load data
    stock_symbols = get_universe_stocks(indexes)
    print(f"Loaded {len(stock_symbols)} symbols.")

    df_index = load_index_daily(regime_filter_indexes, start_dt, end_dt)
    df_gold = load_index_daily(uncorrelated_asset_type, start_dt, end_dt)
    extended_start = pd.Timestamp(start_dt) - pd.Timedelta(days=500)
    df_stocks = load_stocks_bulk(stock_symbols, extended_start, end_dt, min_price, max_price)

    print("Running backtest...")
    (
        metrics,
        eq_df,
        trades_df,
        monthly_json,
        monthly_stats_json,
        equity_curve_json,
        snapshots_df,
        monthly_equity_json,
        yearly_equity_json,
        daily_json,
        weekly_json,
        monthly_final_json,
        yearly_final_json,
        quarterly_json,
        monthly_closed_json,
        closed_trades_df,
        regime_events,
    ) = ultra_backtest_v2_fastest_fixed_v3(
        df_index,
        df_stocks,
        df_gold,
        start_date=start_dt,
        end_date=end_dt,
        top_n=entry_rank,
        initial_capital=capital,
        gold_alloc=gold_alloc,
        FORMULA=formula,
        EXIT_RANK=exit_rank,
        EXIT_REGIME_FILTER_STATUS=exit_regime_filter_status,
        UNCORRELATED_ASSET_STATUS=uncorrelated_asset_status,
        INDICATOR_NAME=indicator_name,
        INDICATOR_PARAMS={"atr_period": 1, "multiplier": 2.7},
        FILTER_ACTION=regime_filter_action,
        REBALANCE_FREQUENCY=rebalance_frequency,
        REBALANCE_DAY_OR_WEEK=rebalance_day,
        UNIVERSE_INDEXES=indexes,
    )

    # Serialize closed_trades_df — convert Timestamps to ISO strings for JSON safety
    closed_trades_json: list[dict[str, Any]] = []
    if closed_trades_df is not None and not closed_trades_df.empty:
        ct = closed_trades_df.copy()
        for col in ["buy_date", "sell_date"]:
            if col in ct.columns:
                ct[col] = ct[col].apply(lambda v: v.strftime("%Y-%m-%d") if pd.notna(v) and hasattr(v, "strftime") else (str(v) if pd.notna(v) else None))
        closed_trades_json = ct.to_dict(orient="records")

    # Build flat regime events rows for Excel
    regime_excel_rows = []
    for ev in (regime_events or []):
        base = {
            "Signal Date": ev.get("signal_date", ""),
            "Signal": ev.get("signal", ""),
            "Trade Date": ev.get("sell_date", ev.get("buy_date", "")),
        }
        if ev.get("signal") == "Sell":
            for s in ev.get("stocks_sold", []):
                regime_excel_rows.append({**base, "Action": s.get("action", "SELL_SIGNAL"), "Symbol": s["symbol"], "Qty": s["qty"], "Price": s["price"], "Rank": s.get("rank", "")})
            for s in ev.get("stocks_skipped", []):
                regime_excel_rows.append({**base, "Action": "SKIPPED_NO_PRICE", "Symbol": s["symbol"], "Qty": "", "Price": "", "Rank": ""})
            g = ev.get("gold_bought")
            if g:
                regime_excel_rows.append({**base, "Action": "BUY_GOLDBEES", "Symbol": "gold_bees", "Qty": g["qty"], "Price": g["price"], "Rank": ""})
        else:
            g = ev.get("gold_sold")
            if g:
                regime_excel_rows.append({**base, "Action": "SELL_GOLDBEES", "Symbol": "gold_bees", "Qty": g["qty"], "Price": g["price"], "Rank": ""})
            for s in ev.get("stocks_bought", []):
                regime_excel_rows.append({**base, "Action": s.get("action", "BUY_REENTER"), "Symbol": s["symbol"], "Qty": s["qty"], "Price": s["price"], "Rank": s.get("rank", "")})

    if regime_excel_rows:
        import random
        df_regime = pd.DataFrame(regime_excel_rows)
        out_path = f"regime_events_{random.randint(1000, 9999)}.xlsx"
        df_regime.to_excel(out_path, index=False)
        print(f"📁 Regime Events Excel → {out_path}")

    return {
        "status": "success",
        "metrics": metrics,
        "monthly_roi": monthly_json,
        "equity_curve_json": equity_curve_json,
        "daily_drawdown_json": metrics.pop("daily_drawdown_json", []),
        "monthly_drawdown_json": metrics.pop("monthly_drawdown_json", []),
        "equity_details": eq_df.to_dict(orient="records"),
        "trade_history": trades_df.to_dict(orient="records"),
        "closed_trades_json": closed_trades_json,
        "monthly_equity_json": monthly_equity_json,
        "yearly_equity_json": yearly_equity_json,
        "daily_json": daily_json,
        "weekly_json": weekly_json,
        "monthly_final_json": monthly_final_json,
        "yearly_final_json": yearly_final_json,
        "quarterly_json": quarterly_json,
        "monthly_closed_json": monthly_closed_json,
        "regime_events": regime_events or [],
    }


def _xml_local_name(tag: str) -> str:
    return str(tag).split("}", 1)[-1]


def _sheet_rows_from_xml(file_bytes: bytes) -> dict[str, list[list[str]]]:
    root = ET.fromstring(file_bytes)
    sheet_map: dict[str, list[list[str]]] = {}

    for worksheet in root.iter():
        if _xml_local_name(worksheet.tag) != "Worksheet":
            continue
        sheet_name = (
            worksheet.attrib.get("{urn:schemas-microsoft-com:office:spreadsheet}Name")
            or worksheet.attrib.get("ss:Name")
            or worksheet.attrib.get("Name")
            or ""
        ).strip()
        if not sheet_name:
            continue

        table = next((child for child in worksheet if _xml_local_name(child.tag) == "Table"), None)
        if table is None:
            sheet_map[sheet_name] = []
            continue

        rows: list[list[str]] = []
        for row in table:
            if _xml_local_name(row.tag) != "Row":
                continue
            values: list[str] = []
            cursor = 1
            for cell in row:
                if _xml_local_name(cell.tag) != "Cell":
                    continue
                index_attr = (
                    cell.attrib.get("{urn:schemas-microsoft-com:office:spreadsheet}Index")
                    or cell.attrib.get("ss:Index")
                    or cell.attrib.get("Index")
                )
                target_index = int(index_attr) if str(index_attr or "").isdigit() else cursor
                while cursor < target_index:
                    values.append("")
                    cursor += 1
                data = next((child for child in cell if _xml_local_name(child.tag) == "Data"), None)
                values.append((data.text if data is not None and data.text is not None else "".join(cell.itertext())).strip())
                cursor += 1
            rows.append(values)
        sheet_map[sheet_name] = rows

    return sheet_map


def parse_single_file_inputs(file_bytes: bytes, file_name: str) -> dict[str, Any]:
    try:
        sheet_map = _sheet_rows_from_xml(file_bytes)
    except Exception as exc:
        raise ValueError(f"{file_name} could not be parsed.") from exc

    inputs_rows = sheet_map.get("Inputs", [])
    if not inputs_rows:
        raise ValueError(f"{file_name} is missing the Inputs sheet.")

    input_map: dict[str, str] = {
        str(row[0]).strip(): str(row[1]).strip() if len(row) > 1 else ""
        for row in inputs_rows
        if row and str(row[0]).strip()
    }

    def _int_input(key: str, default: int) -> int:
        try:
            return int(float(input_map.get(key, str(default)) or str(default)))
        except (ValueError, TypeError):
            return default

    def _float_input(key: str, default: float) -> float:
        try:
            return float(input_map.get(key, str(default)) or str(default))
        except (ValueError, TypeError):
            return default

    # Uses exact same keys/defaults as run_backtest_from_file_inputs → run_scanner_backtest
    return {
        "status": "success",
        "inputs": {
            "strategy_name": input_map.get("Strategy Name", ""),
            "starting_capital": _float_input("Starting Capital", 300000.0),
            "indexes": [p.strip() for p in input_map.get("Universe", "nifty_50").split(",") if p.strip()],
            "entry_rank": _int_input("No Of Stocks in Portfolio", 12),
            "exit_rank": _int_input("Exit Rank", 25),
            "stock_price_min": _float_input("Min Price", 0.0),
            "stock_price_max": _float_input("Max Price", 0.0),
            "rebalance_frequency": input_map.get("Rebalance Frequency", "monthly"),
            "rebalance_date": input_map.get("Rebalance Date", ""),
            "alternative_rebalance_day": input_map.get("Alternate Rebalance Day", "next_day"),
            "position_sizing": input_map.get("Position Sizing", "equal_weight"),
            "start_date": input_map.get("Start Date", ""),
            "end_date": input_map.get("End Date", ""),
            "regime_filter": input_map.get("Regime Filter", "supertrend_1_2_5"),
            "regime_filter_action": input_map.get("Regime Filter Action", "go_cash"),
            "regime_filter_indexes": input_map.get("Regime Filter Index", "nifty_500"),
            "uncorrelated_asset_type": input_map.get("Asset Type", "gold_bees"),
            "uncorrelated_asset_allocation": _int_input("Asset Alloc", 100),
            "formula": input_map.get("Formula", DEFAULT_FORMULA),
        },
    }


def _rows_to_objects(rows: list[list[str]]) -> list[dict[str, str]]:
    if not rows:
        return []
    headers = [str(header or "").strip() for header in rows[0]]
    records: list[dict[str, str]] = []
    for row in rows[1:]:
        if not any(str(cell or "").strip() for cell in row):
            continue
        records.append({
            header: str(row[index] if index < len(row) else "").strip()
            for index, header in enumerate(headers)
            if header
        })
    return records


def _parse_sheet_number(value: str) -> Any:
    text = str(value or "").strip().replace(",", "").replace("%", "")
    if not text:
        return ""
    try:
        number = float(text)
    except ValueError:
        return str(value or "").strip()
    return int(number) if number.is_integer() else round(number, 6)


def _metric_label_to_key(label: str) -> str:
    explicit = {
        "total trade": "total_trades",
        "no.of winners": "no_of_winners",
        "no.of losers": "no_of_losers",
        "win rate(%)": "win_rate_percent",
        "avg. winners roi(%)": "avg_winners_roi_percent",
        "avg. losers roi(%)": "avg_losers_roi_percent",
        "biggest winner roi(%)": "biggest_winner_roi_percent",
        "biggest loser roi(%)": "biggest_loser_roi_percent",
        "risk to reward": "risk_reward",
        "max. dd(%)": "max_drawdown",
        "cagr(%)": "gagr",
        "avg. trade per year": "avg_trades_per_year",
        "kurtosis(monthlyroi)": "kurtosis_monthly_roi",
        "kurtosis(traderoi)": "kurtosis_trade_roi",
        "std": "std",
        "calmar ratio": "calmar_ratio",
        "invested capital": "invested_capital",
        "current capital": "final_capital",
        "total return(%)": "total_return",
    }
    normalized = str(label or "").strip().lower()
    if normalized in explicit:
        return explicit[normalized]
    normalized = re.sub(r"[()%.]", "", normalized)
    normalized = re.sub(r"[^a-z0-9]+", "_", normalized).strip("_")
    return normalized


def parse_multi_strategy_reports(files: list[dict[str, Any]]) -> dict[str, Any]:
    started_at = time.perf_counter()
    reports: list[dict[str, Any]] = []

    for index, item in enumerate(files, start=1):
        file_bytes = item.get("content") or b""
        file_name = str(item.get("file_name") or f"strategy_{index}.xls")
        strategy_name = str(item.get("strategy_name") or f"Strategy {index}").strip() or f"Strategy {index}"
        if not file_bytes:
            raise ValueError(f"Upload file missing for {strategy_name}.")

        try:
            sheet_map = _sheet_rows_from_xml(file_bytes)
        except Exception as exc:
            raise ValueError(f"{file_name} could not be parsed.") from exc

        inputs_rows = sheet_map.get("Inputs", [])
        metrics_rows = sheet_map.get("Performance Metrics", [])
        monthly_breakup_rows = sheet_map.get("Monthly Breakup", [])
        local_daily_rows = sheet_map.get("Local Daily Report", [])
        weekly_rows = sheet_map.get("Weekly Json", [])
        monthly_final_rows = sheet_map.get("Monthly Final Json", [])
        yearly_rows = sheet_map.get("Yearly Final Json", [])
        daily_drawdown_rows = sheet_map.get("Daily Drawdown", [])

        if not inputs_rows or not metrics_rows or not local_daily_rows:
            raise ValueError(f"{file_name} is missing required report sheets.")

        input_map = {str(row[0]).strip(): str(row[1]).strip() if len(row) > 1 else "" for row in inputs_rows if row and str(row[0]).strip()}
        metrics: dict[str, Any] = {}
        for row in metrics_rows[1:]:
            label = str(row[0] if row else "").strip()
            if not label or label in {"Start Date", "End date"}:
                continue
            metrics[_metric_label_to_key(label)] = _parse_sheet_number(row[1] if len(row) > 1 else "")

        monthly_rows_payload: list[dict[str, Any]] = []
        for row in monthly_breakup_rows[1:]:
            if not any(str(cell or "").strip() for cell in row):
                continue
            values = []
            for cell in row[1:13]:
                text = str(cell or "").strip() or "-"
                numeric = _safe_float(str(text).replace("%", ""), default=np.nan)
                values.append({
                    "value": text,
                    "positive": None if text == "-" or np.isnan(numeric) else bool(numeric >= 0),
                })
            total_text = str(row[13] if len(row) > 13 else "-").strip() or "-"
            total_numeric = _safe_float(total_text.replace("%", ""), default=np.nan)
            monthly_rows_payload.append({
                "year": str(row[0] if row else "--").strip() or "--",
                "values": values,
                "total": total_text,
                "totalPositive": None if total_text == "-" or np.isnan(total_numeric) else bool(total_numeric >= 0),
            })

        reports.append({
            "id": f"parsed-{index}",
            "strategyName": strategy_name,
            "fileName": file_name,
            "form": {
                "strategy_name": input_map.get("Strategy Name", strategy_name),
                "starting_capital": input_map.get("Starting Capital", "300000"),
                "indexes": [part.strip() for part in input_map.get("Universe", "").split(",") if part.strip()],
                "entry_rank": input_map.get("No Of Stocks in Portfolio", "12"),
                "exit_rank": input_map.get("Exit Rank", "25"),
                "rebalance_frequency": input_map.get("Rebalance Frequency", "monthly"),
                "alternative_rebalance_day": input_map.get("Alternate Rebalance Day", "next_day"),
                "start_date": input_map.get("Start Date", ""),
                "end_date": input_map.get("End Date", ""),
                "regime_filter": input_map.get("Regime Filter", "supertrend_1_2_5"),
                "regime_filter_action": input_map.get("Regime Filter Action", "go_cash"),
                "regime_filter_indexes": input_map.get("Regime Filter Index", "nifty_500"),
                "uncorrelated_asset_type": input_map.get("Asset Type", "gold_bees"),
                "uncorrelated_asset_allocation": input_map.get("Asset Alloc", "100"),
                "position_sizing": input_map.get("Position Sizing", "equal_weight"),
                "formula": input_map.get("Formula", DEFAULT_FORMULA),
            },
            "metrics": metrics,
            "monthlyBreakupRows": monthly_rows_payload,
            "localDailyRows": [
                {
                    "date": row.get("Date", "--"),
                    "portfolio_value": _safe_float(row.get("Portfolio Value"), 0.0),
                    "daily_pnl": _safe_float(row.get("Daily PnL"), 0.0),
                    "daily_roi": _safe_float(row.get("Daily ROI"), 0.0),
                    "index_value": _safe_float(row.get("Index Value"), 0.0),
                    "index_daily_pnl": _safe_float(row.get("Index Daily PnL"), 0.0),
                }
                for row in _rows_to_objects(local_daily_rows)
            ],
            "weeklyRows": _rows_to_objects(weekly_rows),
            "monthlyFinalRows": _rows_to_objects(monthly_final_rows),
            "yearlyRows": _rows_to_objects(yearly_rows),
            "dailyDrawdownRows": [
                {
                    "date": row.get("date") or row.get("Date") or "",
                    "drawdown_pct": _safe_float(row.get("drawdown_pct"), 0.0),
                    "peak_value": _safe_float(row.get("peak_value"), 0.0),
                    "bottom_value": _safe_float(row.get("bottom_value"), 0.0),
                    "drawdown_rupee": _safe_float(row.get("drawdown_rupee"), 0.0),
                }
                for row in _rows_to_objects(daily_drawdown_rows)
            ],
        })

    combined_report = _build_combined_multi_strategy_report(reports) if len(reports) > 1 else None
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    return {
        "status": "success",
        "strategy_count": len(reports),
        "processing_ms": elapsed_ms,
        "reports": reports,
        "combined_report": combined_report,
    }


def _build_combined_multi_strategy_report(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    if len(reports) < 2:
        return None

    daily_maps: list[dict[str, dict[str, Any]]] = []
    for report in reports:
        rows = report.get("localDailyRows") or []
        daily_maps.append({str(row.get("date") or ""): row for row in rows if str(row.get("date") or "")})

    all_dates = sorted({date for rows in daily_maps for date in rows.keys()})
    if not all_dates:
        return None

    combined_daily: list[dict[str, Any]] = []
    prev_portfolio = 0.0
    prev_index = 0.0
    for index, date in enumerate(all_dates):
        portfolio_value = sum(_safe_float(rows.get(date, {}).get("portfolio_value"), 0.0) for rows in daily_maps)
        index_value = sum(_safe_float(rows.get(date, {}).get("index_value"), 0.0) for rows in daily_maps)
        daily_pnl = portfolio_value - (prev_portfolio if index > 0 else portfolio_value)
        index_daily_pnl = index_value - (prev_index if index > 0 else index_value)
        daily_roi = (daily_pnl / prev_portfolio) * 100 if index > 0 and prev_portfolio else 0.0
        combined_daily.append({
            "date": date,
            "portfolio_value": round(portfolio_value, 2),
            "daily_pnl": round(daily_pnl, 2),
            "daily_roi": round(daily_roi, 4),
            "index_value": round(index_value, 2),
            "index_daily_pnl": round(index_daily_pnl, 2),
        })
        prev_portfolio = portfolio_value
        prev_index = index_value

    combined_drawdown: list[dict[str, Any]] = []
    running_peak = 0.0
    max_drawdown = 0.0
    # track the single worst drawdown event
    _dd_start_date = ""
    _dd_start_portfolio = 0.0
    _dd_bottom_date = ""
    _dd_bottom_portfolio = 0.0
    _dd_recovery_date = ""
    _cur_dd_start_date = ""
    _cur_dd_start_portfolio = 0.0
    _in_drawdown = False
    for row in combined_daily:
        portfolio_value = _safe_float(row.get("portfolio_value"), 0.0)
        date_str = str(row.get("date", ""))
        if portfolio_value > running_peak:
            if _in_drawdown and not _dd_recovery_date:
                _dd_recovery_date = date_str
            running_peak = portfolio_value
            _in_drawdown = False
        drawdown_pct = ((portfolio_value - running_peak) / running_peak) * 100 if running_peak else 0.0
        if drawdown_pct < 0 and not _in_drawdown:
            _in_drawdown = True
            _cur_dd_start_date = date_str
            _cur_dd_start_portfolio = running_peak
        if drawdown_pct < max_drawdown:
            max_drawdown = drawdown_pct
            _dd_start_date = _cur_dd_start_date
            _dd_start_portfolio = _cur_dd_start_portfolio
            _dd_bottom_date = date_str
            _dd_bottom_portfolio = portfolio_value
            _dd_recovery_date = ""
        combined_drawdown.append({
            "date": date_str,
            "drawdown_pct": round(drawdown_pct, 4),
            "peak_value": round(running_peak, 2),
            "bottom_value": round(portfolio_value, 2),
            "drawdown_rupee": round(portfolio_value - running_peak, 2),
        })

    def _combine_period_rows(row_key: str, rows_key: str, start_key: str, end_key: str) -> list[dict[str, Any]]:
        bucket_map: dict[str, dict[str, Any]] = {}
        for report in reports:
            for row in report.get(rows_key) or []:
                bucket = str(row.get(row_key) or "").strip()
                if not bucket:
                    continue
                current = bucket_map.setdefault(bucket, {
                    row_key: bucket,
                    "start_date": row.get("start_date") or "",
                    "end_date": row.get("end_date") or "",
                    "portfolio_start_value": 0.0,
                    "portfolio_end_value": 0.0,
                    "portfolio_return": 0.0,
                    "pnl_rupee": 0.0,
                })
                current["start_date"] = min(
                    [value for value in [str(current.get("start_date") or ""), str(row.get("start_date") or "")] if value]
                ) if current.get("start_date") or row.get("start_date") else ""
                current["end_date"] = max(
                    [value for value in [str(current.get("end_date") or ""), str(row.get("end_date") or "")] if value]
                ) if current.get("end_date") or row.get("end_date") else ""
                current["portfolio_start_value"] += _safe_float(row.get(start_key), 0.0)
                current["portfolio_end_value"] += _safe_float(row.get(end_key), 0.0)
                current["portfolio_return"] += _safe_float(row.get("portfolio_return"), _safe_float(row.get("pnl_rupee"), 0.0))
                current["pnl_rupee"] += _safe_float(row.get("pnl_rupee"), _safe_float(row.get("portfolio_return"), 0.0))

        results: list[dict[str, Any]] = []
        for bucket in sorted(bucket_map.keys()):
            row = bucket_map[bucket]
            start_val = _safe_float(row.get("portfolio_start_value"), 0.0)
            end_val = _safe_float(row.get("portfolio_end_value"), 0.0)
            pnl = end_val - start_val if not row.get("pnl_rupee") else _safe_float(row.get("pnl_rupee"), 0.0)
            roi = (pnl / start_val) * 100 if start_val else 0.0
            results.append({
                row_key: bucket,
                "start_date": row.get("start_date") or "",
                "end_date": row.get("end_date") or "",
                "portfolio_start_value": round(start_val, 2),
                "portfolio_end_value": round(end_val, 2),
                "portfolio_return": round(pnl, 2),
                "pnl_rupee": round(pnl, 2),
                "portfolio_roi": round(roi, 3),
            })
        return results

    weekly_rows = _combine_period_rows("week", "weeklyRows", "portfolio_start_value", "portfolio_end_value")
    monthly_final_rows = _combine_period_rows("month", "monthlyFinalRows", "portfolio_start_value", "portfolio_end_value")
    yearly_rows = _combine_period_rows("year", "yearlyRows", "portfolio_start_value", "portfolio_end_value")

    # Build monthly breakup as simple average of each strategy's displayed monthly ROI %
    # Use monthlyBreakupRows (built from monthly_roi / Monthly_ROI_Pct) — same source as UI display
    monthly_breakup_rows: list[dict[str, Any]] = []
    strategy_monthly_roi_maps: list[dict[str, dict[int, float]]] = []
    for report in reports:
        yr_map: dict[str, dict[int, float]] = {}
        for row in (report.get("monthlyBreakupRows") or []):
            yr = str(row.get("year") or "")
            if not yr:
                continue
            for month_idx, cell in enumerate(row.get("values") or [], start=1):
                val_str = str(cell.get("value") or "").replace("%", "").strip()
                if not val_str or val_str == "-":
                    continue
                try:
                    yr_map.setdefault(yr, {})[month_idx] = float(val_str)
                except ValueError:
                    pass
        strategy_monthly_roi_maps.append(yr_map)

    all_years_set: set[str] = set()
    for yr_map in strategy_monthly_roi_maps:
        all_years_set.update(yr_map.keys())

    strategy_names = [str(r.get("strategyName") or f"Strategy {i + 1}") for i, r in enumerate(reports)]

    for year in sorted(all_years_set):
        values = []
        year_total = 0.0
        for month in range(1, 13):
            breakdown = [
                {"name": strategy_names[i], "value": f"{yr_map[year][month]:.2f}%", "positive": yr_map[year][month] >= 0}
                for i, yr_map in enumerate(strategy_monthly_roi_maps)
                if year in yr_map and month in yr_map[year]
            ]
            if not breakdown:
                values.append({"value": "-", "positive": None})
            else:
                month_rois = [float(b["value"].replace("%", "")) for b in breakdown]
                avg_roi = sum(month_rois) / len(month_rois)
                year_total += avg_roi
                values.append({"value": f"{avg_roi:.2f}%", "positive": avg_roi >= 0, "breakdown": breakdown})
        monthly_breakup_rows.append({
            "year": year,
            "values": values,
            "total": f"{year_total:.2f}%",
            "totalPositive": year_total >= 0,
        })

    invested_capital = sum(_safe_float(report.get("metrics", {}).get("invested_capital"), _safe_float(report.get("form", {}).get("starting_capital"), 0.0)) for report in reports)
    final_capital = _safe_float(combined_daily[-1]["portfolio_value"], 0.0)
    total_return = ((final_capital - invested_capital) / invested_capital) * 100 if invested_capital else 0.0
    no_of_winners = int(sum(_safe_float(report.get("metrics", {}).get("no_of_winners"), 0.0) for report in reports))
    no_of_losers = int(sum(_safe_float(report.get("metrics", {}).get("no_of_losers"), 0.0) for report in reports))
    total_trades = int(sum(_safe_float(report.get("metrics", {}).get("total_trades"), 0.0) for report in reports)) or (no_of_winners + no_of_losers)
    _winner_rois = [_safe_float(r.get("metrics", {}).get("avg_winners_roi_percent"), 0.0) for r in reports]
    avg_winners_roi_percent = sum(_winner_rois) / len(_winner_rois) if _winner_rois else 0.0

    _loser_rois = [_safe_float(r.get("metrics", {}).get("avg_losers_roi_percent"), 0.0) for r in reports]
    avg_losers_roi_percent = sum(_loser_rois) / len(_loser_rois) if _loser_rois else 0.0
    win_rate_percent = (no_of_winners / total_trades) * 100 if total_trades else 0.0
    _rr_values = [_safe_float(r.get("metrics", {}).get("risk_reward"), 0.0) for r in reports]
    risk_reward = sum(_rr_values) / len(_rr_values) if _rr_values else 0.0

    def _metric_avg(key: str) -> float | None:
        vals = [_safe_float(r.get("metrics", {}).get(key), None) for r in reports]
        vals = [v for v in vals if v is not None]
        return round(sum(vals) / len(vals), 4) if vals else None

    kurtosis_monthly_roi = _metric_avg("kurtosis_monthly_roi")
    kurtosis_trade_roi = _metric_avg("kurtosis_trade_roi")
    std = _metric_avg("std")

    _dd_duration_days = (
        (pd.to_datetime(_dd_bottom_date) - pd.to_datetime(_dd_start_date)).days
        if _dd_start_date and _dd_bottom_date else 0
    )

    start_date = pd.to_datetime(all_dates[0])
    end_date = pd.to_datetime(all_dates[-1])
    years_bt = max(1e-9, (end_date - start_date).days / 365)
    cagr = (((final_capital / invested_capital) ** (1 / years_bt)) - 1) * 100 if invested_capital and final_capital > 0 else 0.0
    avg_trades_per_year = total_trades / years_bt if years_bt else 0.0
    calmar_ratio = (cagr / abs(max_drawdown)) if max_drawdown else 0.0

    winner_source = max(reports, key=lambda report: _safe_float(report.get("metrics", {}).get("biggest_winner_roi_percent"), -1e9))
    loser_source = min(reports, key=lambda report: _safe_float(report.get("metrics", {}).get("biggest_loser_roi_percent"), 1e9))

    metrics = {
        "invested_capital": round(invested_capital, 2),
        "final_capital": round(final_capital, 2),
        "total_return": round(total_return, 2),
        "total_trades": total_trades,
        "no_of_winners": no_of_winners,
        "no_of_losers": no_of_losers,
        "win_rate_percent": round(win_rate_percent, 3),
        "avg_winners_roi_percent": round(avg_winners_roi_percent, 3),
        "avg_losers_roi_percent": round(avg_losers_roi_percent, 3),
        "biggest_winner_roi_percent": round(_safe_float(winner_source.get("metrics", {}).get("biggest_winner_roi_percent"), 0.0), 3),
        "biggest_winner_stock": winner_source.get("metrics", {}).get("biggest_winner_stock"),
        "biggest_winner_holding_days": int(_safe_float(winner_source.get("metrics", {}).get("biggest_winner_holding_days"), 0.0)),
        "biggest_winner_holding_start_date": winner_source.get("metrics", {}).get("biggest_winner_holding_start_date"),
        "biggest_winner_holding_end_date": winner_source.get("metrics", {}).get("biggest_winner_holding_end_date"),
        "biggest_winner_holding_buy_price": winner_source.get("metrics", {}).get("biggest_winner_holding_buy_price"),
        "biggest_winner_holding_sell_price": winner_source.get("metrics", {}).get("biggest_winner_holding_sell_price"),
        "biggest_loser_roi_percent": round(_safe_float(loser_source.get("metrics", {}).get("biggest_loser_roi_percent"), 0.0), 3),
        "biggest_loser_stock": loser_source.get("metrics", {}).get("biggest_loser_stock"),
        "biggest_loser_holding_days": int(_safe_float(loser_source.get("metrics", {}).get("biggest_loser_holding_days"), 0.0)),
        "biggest_loser_holding_start_date": loser_source.get("metrics", {}).get("biggest_loser_holding_start_date"),
        "biggest_loser_holding_end_date": loser_source.get("metrics", {}).get("biggest_loser_holding_end_date"),
        "biggest_loser_holding_buy_price": loser_source.get("metrics", {}).get("biggest_loser_holding_buy_price"),
        "biggest_loser_holding_sell_price": loser_source.get("metrics", {}).get("biggest_loser_holding_sell_price"),
        "risk_reward": round(risk_reward, 3),
        "max_drawdown": round(abs(max_drawdown), 3),
        "gagr": round(cagr, 3),
        "avg_trades_per_year": round(avg_trades_per_year, 2),
        "calmar_ratio": round(calmar_ratio, 3),
        "drawdown_start_date": _dd_start_date or None,
        "drawdown_start_portfolio": round(_dd_start_portfolio, 2) if _dd_start_portfolio else None,
        "drawdown_bottom_date": _dd_bottom_date or None,
        "drawdown_bottom_portfolio": round(_dd_bottom_portfolio, 2) if _dd_bottom_portfolio else None,
        "drawdown_drop_percent": round(abs(max_drawdown), 3) if max_drawdown else None,
        "drawdown_drop_amount": round(_dd_start_portfolio - _dd_bottom_portfolio, 2) if _dd_start_portfolio and _dd_bottom_portfolio else None,
        "drawdown_duration_days": _dd_duration_days or None,
        "drawdown_recovery_date": _dd_recovery_date or None,
        **({"kurtosis_monthly_roi": kurtosis_monthly_roi} if kurtosis_monthly_roi is not None else {}),
        **({"kurtosis_trade_roi": kurtosis_trade_roi} if kurtosis_trade_roi is not None else {}),
        **({"std": std} if std is not None else {}),
    }

    combined_form = {
        "strategy_name": "Combined Strategy",
        "starting_capital": str(round(invested_capital, 2)),
        "indexes": [report.get("strategyName") or report.get("form", {}).get("strategy_name") for report in reports],
        "entry_rank": "",
        "exit_rank": "",
        "rebalance_frequency": "combined",
        "alternative_rebalance_day": "",
        "start_date": str(all_dates[0]),
        "end_date": str(all_dates[-1]),
        "regime_filter": "",
        "regime_filter_action": "",
        "regime_filter_indexes": "",
        "uncorrelated_asset_type": "",
        "uncorrelated_asset_allocation": "",
        "position_sizing": "",
        "formula": "",
    }

    # Build combined equity_curve_json by summing portfolio values across strategies.
    # Each strategy's equity_curve_json has {date, portfolio, index} — sum portfolio,
    # use index from first strategy that has a value (index values should be consistent).
    equity_maps: list[dict[str, dict[str, Any]]] = []
    for report in reports:
        curve = report.get("equity_curve_json") or []
        em: dict[str, dict[str, Any]] = {str(pt.get("date") or ""): pt for pt in curve if str(pt.get("date") or "")}
        equity_maps.append(em)

    combined_equity_curve: list[dict[str, Any]] = []
    all_equity_dates = sorted({d for em in equity_maps for d in em.keys()})
    for d in all_equity_dates:
        portfolio_sum = 0.0
        index_val = 0.0
        for em in equity_maps:
            pt = em.get(d)
            if pt:
                portfolio_sum += float(pt.get("portfolio") or 0)
                if not index_val:
                    index_val = float(pt.get("index") or 0)
        combined_equity_curve.append({
            "date": d,
            "portfolio": round(portfolio_sum, 2),
            "index": round(index_val, 2),
        })

    # Build combined monthly_roi using the same month set as individual reports.
    # monthly_roi items have: {month, Monthly_ROI_Pct, MonthPnL, Capital_Base, CumBeforeMonth}
    # We use Capital_Base (start value) and MonthPnL (pnl) directly — NOT monthlyFinalRows —
    # because monthlyFinalRows uses calendar-month periods while monthly_roi uses rebalancing periods.
    roi_maps: list[dict[str, dict[str, Any]]] = []
    for report in reports:
        rm: dict[str, dict[str, Any]] = {}
        for item in (report.get("monthly_roi") or []):
            month_key = str(item.get("month") or "").strip()
            if month_key:
                rm[month_key] = item
        roi_maps.append(rm)

    all_roi_months = sorted({m for rm in roi_maps for m in rm.keys()})
    combined_monthly_roi: list[dict[str, Any]] = []
    for month in all_roi_months:
        total_start_val = 0.0
        total_pnl = 0.0
        present_items: list[dict[str, Any]] = []

        for i in range(len(reports)):
            roi_item = roi_maps[i].get(month)
            if roi_item is None:
                continue
            present_items.append(roi_item)
            capital_base = _safe_float(roi_item.get("Capital_Base"), 0.0)
            month_pnl = _safe_float(roi_item.get("MonthPnL"), 0.0)
            total_start_val += capital_base
            total_pnl += month_pnl

        if not present_items:
            continue

        total_end_val = total_start_val + total_pnl
        combined_roi = (total_pnl / total_start_val * 100) if total_start_val else 0.0
        cum_before = sum(_safe_float(item.get("CumBeforeMonth"), 0.0) for item in present_items)

        entry: dict[str, Any] = {
            "month": month,
            "Monthly_ROI_Pct": round(combined_roi, 4),
            "MonthPnL": round(total_pnl, 2),
            "CumBeforeMonth": round(cum_before, 2),
            "Capital_Base": round(total_start_val, 2),
        }
        if total_start_val:
            entry["Start_Value"] = round(total_start_val, 2)
            entry["End_Value"] = round(total_end_val, 2)
        combined_monthly_roi.append(entry)

    return {
        "id": "combined-report",
        "strategyName": "Combined Strategy",
        "fileName": ", ".join(str(report.get("fileName") or "") for report in reports),
        "form": combined_form,
        "metrics": metrics,
        "monthlyBreakupRows": monthly_breakup_rows,
        "localDailyRows": combined_daily,
        "weeklyRows": weekly_rows,
        "monthlyFinalRows": monthly_final_rows,
        "yearlyRows": yearly_rows,
        "dailyDrawdownRows": combined_drawdown,
        "monthly_roi": [],
        "equity_curve_json": combined_equity_curve,
    }


def _build_monthly_breakup_from_monthly_roi(monthly_roi: list[dict[str, Any]]) -> list[dict[str, Any]]:
    year_months: dict[str, dict[int, float]] = {}
    for item in monthly_roi:
        month_str = str(item.get("month", ""))
        if len(month_str) < 7:
            continue
        year = month_str[:4]
        try:
            month = int(month_str[5:7])
        except (ValueError, IndexError):
            continue
        roi = float(item.get("Monthly_ROI_Pct", 0) or 0)
        year_months.setdefault(year, {})[month] = roi

    rows: list[dict[str, Any]] = []
    for year in sorted(year_months.keys()):
        months = year_months[year]
        values: list[dict[str, Any]] = []
        year_total = 0.0
        has_any = False
        for m in range(1, 13):
            if m in months:
                roi = months[m]
                year_total += roi
                has_any = True
                values.append({"value": f"{roi:.2f}%", "positive": roi >= 0})
            else:
                values.append({"value": "-", "positive": None})
        total_str = f"{year_total:.2f}%" if has_any else "-"
        rows.append({
            "year": year,
            "values": values,
            "total": total_str,
            "totalPositive": None if not has_any else year_total >= 0,
        })
    return rows


def _backtest_result_to_report(
    result: dict[str, Any],
    payload: dict[str, Any],
    strategy_name: str,
    report_id: str,
    file_name: str = "",
) -> dict[str, Any]:
    """Map run_scanner_backtest() result → ParsedStrategyReport dict (shared by rerun and multi-payload paths)."""
    import numpy as np
    import pandas as pd

    backtest_metrics: dict[str, Any] = dict(result.get("metrics") or {})
    daily_json: list[dict[str, Any]] = list(result.get("daily_json") or [])
    weekly_json: list[dict[str, Any]] = list(result.get("weekly_json") or [])
    monthly_final_json: list[dict[str, Any]] = list(result.get("monthly_final_json") or [])
    yearly_final_json: list[dict[str, Any]] = list(result.get("yearly_final_json") or [])
    daily_drawdown_json: list[dict[str, Any]] = list(result.get("daily_drawdown_json") or [])
    monthly_roi: list[dict[str, Any]] = list(result.get("monthly_roi") or [])
    equity_curve_json: list[dict[str, Any]] = list(result.get("equity_curve_json") or [])
    trade_history: list[dict[str, Any]] = list(result.get("trade_history") or [])
    closed_trades_json: list[dict[str, Any]] = list(result.get("closed_trades_json") or [])
    monthly_closed_json: list[dict[str, Any]] = list(result.get("monthly_closed_json") or [])
    monthly_equity_json: list[dict[str, Any]] = list(result.get("monthly_equity_json") or [])

    date_to_index: dict[str, float] = {}
    try:
        from scanner.backtest_engine_before_change_score_code import load_index_daily as _load_idx
        start_dt = datetime.strptime(str(payload["start_date"]), "%Y-%m-%d")
        end_dt = datetime.strptime(str(payload["end_date"]), "%Y-%m-%d")
        df_idx = _load_idx(str(payload.get("regime_filter_indexes", "nifty_500")), start_dt, end_dt)
        if "i_close_price" in df_idx.columns:
            _col: str | None = "i_close_price"
        elif "ch_closing_price" in df_idx.columns:
            _col = "ch_closing_price"
        else:
            _numeric_cols = df_idx.select_dtypes(include=[np.number]).columns
            _col = str(_numeric_cols[0]) if len(_numeric_cols) else None
        if _col and not df_idx.empty:
            date_to_index = {
                str(pd.Timestamp(d).date()): round(float(p), 2)
                for d, p in zip(df_idx.index, df_idx[_col])
            }
    except Exception:
        pass

    local_daily_rows: list[dict[str, Any]] = []
    prev_idx_val = 0.0
    for row in daily_json:
        date_str = str(row.get("date", ""))
        portfolio_value = float(row.get("portfolio_end_value", 0) or 0)
        daily_pnl = float(row.get("pnl_rupee", 0) or 0)
        daily_roi = float(row.get("portfolio_roi", 0) or 0)
        idx_val = date_to_index.get(date_str, prev_idx_val)
        idx_daily_pnl = round(idx_val - prev_idx_val, 2) if prev_idx_val and idx_val else 0.0
        local_daily_rows.append({
            "date": date_str,
            "portfolio_value": portfolio_value,
            "daily_pnl": round(daily_pnl, 2),
            "daily_roi": round(daily_roi, 4),
            "index_value": idx_val,
            "index_daily_pnl": idx_daily_pnl,
        })
        if idx_val:
            prev_idx_val = idx_val

    form: dict[str, Any] = {
        "strategy_name": str(payload.get("strategy_name", strategy_name)),
        "starting_capital": str(int(float(payload.get("starting_capital", 300000)))),
        "indexes": list(payload.get("indexes", [])),
        "entry_rank": str(payload.get("entry_rank", "")),
        "exit_rank": str(payload.get("exit_rank", "")),
        "rebalance_frequency": str(payload.get("rebalance_frequency", "")),
        "rebalance_date": str(payload.get("rebalance_date", "")),
        "alternative_rebalance_day": str(payload.get("alternative_rebalance_day", "")),
        "start_date": str(payload.get("start_date", "")),
        "end_date": str(payload.get("end_date", "")),
        "regime_filter": str(payload.get("regime_filter", "")),
        "regime_filter_action": str(payload.get("regime_filter_action", "")),
        "regime_filter_indexes": str(payload.get("regime_filter_indexes", "")),
        "uncorrelated_asset_type": str(payload.get("uncorrelated_asset_type", "")),
        "uncorrelated_asset_allocation": str(payload.get("uncorrelated_asset_allocation", "")),
        "position_sizing": str(payload.get("position_sizing", "")),
        "formula": str(payload.get("formula", "")),
        "stock_price_min": str(payload.get("stock_price_min", payload.get("min_price", 0) or 0)),
        "stock_price_max": str(payload.get("stock_price_max", payload.get("max_price", 0) or 0)),
    }

    return {
        "id": report_id,
        "strategyName": strategy_name,
        "fileName": file_name,
        "form": form,
        "metrics": backtest_metrics,
        "monthlyBreakupRows": _build_monthly_breakup_from_monthly_roi(monthly_roi),
        "localDailyRows": local_daily_rows,
        "weeklyRows": weekly_json,
        "monthlyFinalRows": monthly_final_json,
        "yearlyRows": yearly_final_json,
        "dailyDrawdownRows": [
            {
                "date": str(r.get("date", "")),
                "drawdown_pct": float(r.get("drawdown_pct", 0) or 0),
                "peak_value": float(r.get("peak_value", 0) or 0),
                "bottom_value": float(r.get("portfolio_value", r.get("bottom_value", 0)) or 0),
                "drawdown_rupee": float(r.get("drawdown_rupee", 0) or 0),
            }
            for r in daily_drawdown_json
        ],
        "equity_curve_json": equity_curve_json,
        "monthly_roi": monthly_roi,
        "trade_history": trade_history,
        "closed_trades_json": closed_trades_json,
        "monthly_closed_json": monthly_closed_json,
        "monthly_equity_json": monthly_equity_json,
        "daily_json": daily_json,
    }


def run_multi_backtest_from_payloads(strategies: list[dict[str, Any]]) -> dict[str, Any]:
    """Run run_scanner_backtest for each strategy payload — same path as /run_backtest but batched."""
    started_at = time.perf_counter()
    reports: list[dict[str, Any]] = []

    for index, item in enumerate(strategies, start=1):
        payload = dict(item)
        strategy_name = str(payload.pop("_strategy_name", payload.get("strategy_name", f"Strategy {index}"))).strip() or f"Strategy {index}"
        # align min/max price field names for run_scanner_backtest (0 = no filter → None)
        if payload.get("min_price") is None and payload.get("stock_price_min"):
            payload["min_price"] = payload["stock_price_min"]
        if payload.get("max_price") is None and payload.get("stock_price_max"):
            payload["max_price"] = payload["stock_price_max"]

        result = run_scanner_backtest(payload)
        reports.append(_backtest_result_to_report(
            result=result,
            payload=payload,
            strategy_name=strategy_name,
            report_id=f"multi-{index}",
        ))

    combined_report = _build_combined_multi_strategy_report(reports) if len(reports) > 1 else None
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)

    return {
        "status": "success",
        "strategy_count": len(reports),
        "processing_ms": elapsed_ms,
        "reports": reports,
        "combined_report": combined_report,
    }


def run_backtest_from_file_inputs(files: list[dict[str, Any]]) -> dict[str, Any]:
    started_at = time.perf_counter()
    reports: list[dict[str, Any]] = []

    for file_index, item in enumerate(files, start=1):
        file_bytes = item.get("content") or b""
        file_name = str(item.get("file_name") or f"strategy_{file_index}.xls")
        strategy_name = str(item.get("strategy_name") or f"Strategy {file_index}").strip() or f"Strategy {file_index}"

        if not file_bytes:
            raise ValueError(f"Upload file missing for {strategy_name}.")

        try:
            sheet_map = _sheet_rows_from_xml(file_bytes)
        except Exception as exc:
            raise ValueError(f"{file_name} could not be parsed as XML.") from exc

        inputs_rows = sheet_map.get("Inputs", [])
        if not inputs_rows:
            raise ValueError(f"{file_name} is missing the Inputs sheet.")

        input_map: dict[str, str] = {
            str(row[0]).strip(): str(row[1]).strip() if len(row) > 1 else ""
            for row in inputs_rows
            if row and str(row[0]).strip()
        }

        def _int_input(key: str, default: int) -> int:
            try:
                return int(float(input_map.get(key, str(default)) or str(default)))
            except (ValueError, TypeError):
                return default

        def _float_input(key: str, default: float) -> float:
            try:
                return float(input_map.get(key, str(default)) or str(default))
            except (ValueError, TypeError):
                return default

        payload: dict[str, Any] = {
            "strategy_name": input_map.get("Strategy Name", strategy_name),
            "starting_capital": _float_input("Starting Capital", 300000.0),
            "indexes": [p.strip() for p in input_map.get("Universe", "nifty_50").split(",") if p.strip()],
            "entry_rank": _int_input("No Of Stocks in Portfolio", 12),
            "exit_rank": _int_input("Exit Rank", 25),
            "min_price": _float_input("Min Price", 0.0) or None,
            "max_price": _float_input("Max Price", 0.0) or None,
            "rebalance_frequency": input_map.get("Rebalance Frequency", "monthly"),
            "rebalance_date": _int_input("Rebalance Date", 10),
            "alternative_rebalance_day": input_map.get("Alternate Rebalance Day", "next_day"),
            "start_date": input_map.get("Start Date", "2019-01-01"),
            "end_date": input_map.get("End Date", "2026-01-01"),
            "regime_filter": input_map.get("Regime Filter", "supertrend_1_2_5"),
            "regime_filter_action": input_map.get("Regime Filter Action", "go_cash"),
            "regime_filter_indexes": input_map.get("Regime Filter Index", "nifty_500"),
            "uncorrelated_asset_type": input_map.get("Asset Type", "gold_bees"),
            "uncorrelated_asset_allocation": _int_input("Asset Alloc", 100),
            "position_sizing": input_map.get("Position Sizing", "equal_weight"),
            "formula": input_map.get("Formula", DEFAULT_FORMULA),
            "regime_filter_status": True,
            "uncorrelated_asset_status": True,
        }

        result = run_scanner_backtest(payload)
        reports.append(_backtest_result_to_report(
            result=result,
            payload=payload,
            strategy_name=strategy_name,
            report_id=f"backtest-{file_index}",
            file_name=file_name,
        ))

    combined_report = _build_combined_multi_strategy_report(reports) if len(reports) > 1 else None
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)

    return {
        "status": "success",
        "strategy_count": len(reports),
        "processing_ms": elapsed_ms,
        "reports": reports,
        "combined_report": combined_report,
    }
