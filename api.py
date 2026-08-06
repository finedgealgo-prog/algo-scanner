"""
Local Backtest API
──────────────────
Run:
    uvicorn api:app --reload --port 8001

Endpoints:
    GET  /health                    → health check
    POST /backtest                  → run backtest (blocking, waits for result)
    POST /backtest/file             → run backtest using current_backtest_request.json
    POST /backtest/start            → start backtest in background, returns job_id
    GET  /backtest/status/{job_id}  → poll progress: completed_days / total_days
    GET  /backtest/result/{job_id}  → get final result when status=done
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import multiprocessing
import os
import re
import threading

import pathlib as _pathlib
from dotenv import load_dotenv
load_dotenv(_pathlib.Path(__file__).resolve().parent / ".env")

log = logging.getLogger(__name__)
import time
import uuid
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Literal, Optional

from bson import ObjectId
from fastapi import FastAPI, HTTPException, APIRouter, Query, Request, UploadFile, File, Depends
from fastapi.routing import APIRoute
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from starlette.routing import WebSocketRoute

from features.backtest_engine import run_backtest
from features.portfolio_worker import strategy_worker
from features.mongo_data     import MongoData
from features.expiry_config  import seed_expiry_config
from features import auth as app_auth
from features.broker_gateway import (
    broker_get_login_url            as get_login_url,
    broker_generate_session         as generate_session,
    get_broker_rest_client_with_token as get_kite_instance,
    save_broker_session             as save_kite_session,
    get_stored_broker_access_token  as get_stored_access_token,
    broker_ticker_manager           as ticker_manager,
)
from features.mock_ticker import mock_ticker_manager
from features.market_hours_scheduler import run_market_hours_scheduler
from scanner.router import router as scanner_router
from scanner.live_option_chain_collector import collector as _live_option_chain_collector
from scanner.live_chain_snapshot_collector import collector as _live_chain_snapshot_collector
from features.broker_gateway import (
    load_broker_instruments         as _load_kite_instruments,
    BROKER_INDEX_TOKENS             as KITE_INDEX_TOKENS,
    get_broker_expiries             as get_kite_expiries,
    list_broker_option_contracts    as list_kite_option_contracts,
    get_broker_credentials          as get_common_credentials,
    get_broker_ltp_map              as get_ltp_map,
    broker_is_configured            as is_configured,
    load_broker_credentials_from_db as load_credentials_from_db,
)
from features.spot_atm_utils import get_cached_spot_doc
from features.execution_socket import (
    broadcast_backtest_simulation_step,
    emit_broker_settings_for_user,
    queue_execute_order_group_start,
    run_backtest_simulation_step,
    socket_router,
    _fetch_dhan_broker_option_positions,
    _build_message,
    _extract_broker_configuration_label,
)
from features.live_fast_monitor import live_fast_monitor_supervisor
from features.live_monitor_socket import live_monitor_loop
from features import live_entry_monitor
from features.broker_accounts import (
    validate_broker_configuration_session as _validate_broker_configuration_session,
    DEFAULT_APP_USER_ID,
    get_broker_accounts_for_user,
)
from features.mock_kite_socket import mock_kite_socket_router
from features.live_quote_socket import live_quote_socket_router

# ─── Config ───────────────────────────────────────────────────────────────────

REQUEST_JSON_PATH = Path(__file__).parent / "current_backtest_request.json"
SAMPLE_RESULT_PATH = Path(__file__).parent / "sample_backtest_result" / "new_portfolio_result.json"
JOB_STATE_DIR = Path("/tmp/option_algo_backtest_jobs")
CACHE_DIR = Path("/tmp/option_algo_backtest_cache")
API_ROUTE_GROUP_PREFIXES = ("/algo", "/simulator", "/scanner")
API_VERSION_PREFIXES = tuple(
    f"/{segment}"
    for segment in [
        str(value).strip().strip("/")
        for value in os.getenv("API_ROUTE_VERSIONS", "v1,v2").split(",")
    ]
    if segment
)

JOB_TTL_SECONDS = 3600       # auto-delete completed jobs older than 1 hour
MAX_JOBS        = 10         # max jobs kept in memory at once

# ─── Job store (in-memory) ────────────────────────────────────────────────────
# job_id → { status, completed, total, percent, current_day, result, error, created_at }

_jobs: dict = {}
_jobs_lock = multiprocessing.Lock()
_LIST_CACHE_TTL_SECONDS = 30.0
_list_cache: dict[str, dict] = {}
_list_cache_lock = threading.Lock()

_ACTIVE_OPTION_CHAIN_CACHE: dict[str, dict[str, Any]] = {}
_ACTIVE_OPTION_CHAIN_CACHE_LOCK = threading.Lock()
_shared_mongo = MongoData()
IST = timezone(timedelta(hours=5, minutes=30))
ALGO_TRADE_PORTFOLIO_COLLECTION = "algo_trade_portfolio"


def _resolve_app_user_id(value: str | None = None) -> str:
    normalized_value = str(value or "").strip()
    if normalized_value:
        return normalized_value
    return DEFAULT_APP_USER_ID


def _normalize_runtime_activation_mode(value: str | None = None) -> str:
    return str(value or "").strip().lower() or "algo-backtest"


def _default_runtime_trade_date(value: str | None = None, date_hint: str | None = None) -> str:
    normalized_date = str(date_hint or "").strip()
    if normalized_date:
        return normalized_date
    normalized_mode = _normalize_runtime_activation_mode(value)
    if normalized_mode in {"live", "fast-forward", "forward-test"}:
        return datetime.now(IST).strftime("%Y-%m-%d")
    return ""


def _list_cache_get(key: str):
    now = time.time()
    with _list_cache_lock:
        item = _list_cache.get(key)
        if not item:
            return None
        if now - item.get("ts", 0) > _LIST_CACHE_TTL_SECONDS:
            _list_cache.pop(key, None)
            return None
        return deepcopy(item["value"])


def _list_cache_set(key: str, value) -> None:
    with _list_cache_lock:
        _list_cache[key] = {"ts": time.time(), "value": deepcopy(value)}


def _invalidate_list_cache(*keys: str) -> None:
    with _list_cache_lock:
        if not keys:
            _list_cache.clear()
            return
        for key in keys:
            _list_cache.pop(key, None)


def _should_register_version_alias(path: str) -> bool:
    normalized_path = str(path or "").strip()
    if not normalized_path:
        return False
    return any(normalized_path.startswith(prefix) for prefix in API_ROUTE_GROUP_PREFIXES)


def _register_versioned_route_aliases(app_instance: FastAPI) -> None:
    if not API_VERSION_PREFIXES:
        return

    existing_paths = {getattr(route, "path", "") for route in app_instance.routes}
    routes_snapshot = list(app_instance.routes)

    for route in routes_snapshot:
        path = getattr(route, "path", "")
        if not _should_register_version_alias(path):
            continue

        for version_prefix in API_VERSION_PREFIXES:
            alias_path = f"{version_prefix}{path}"
            if alias_path in existing_paths:
                continue

            if isinstance(route, APIRoute):
                app_instance.add_api_route(
                    alias_path,
                    route.endpoint,
                    methods=list(route.methods or []),
                    name=f"{route.name}{version_prefix}",
                    include_in_schema=False,
                    response_model=route.response_model,
                    status_code=route.status_code,
                    tags=list(route.tags),
                    dependencies=list(route.dependencies),
                    summary=route.summary,
                    description=route.description,
                    response_description=route.response_description,
                    responses=dict(route.responses),
                    deprecated=route.deprecated,
                    operation_id=None,
                    response_model_include=route.response_model_include,
                    response_model_exclude=route.response_model_exclude,
                    response_model_by_alias=route.response_model_by_alias,
                    response_model_exclude_unset=route.response_model_exclude_unset,
                    response_model_exclude_defaults=route.response_model_exclude_defaults,
                    response_model_exclude_none=route.response_model_exclude_none,
                    response_class=route.response_class,
                    openapi_extra=route.openapi_extra,
                    generate_unique_id_function=route.generate_unique_id_function,
                )
                existing_paths.add(alias_path)
                continue

            if isinstance(route, WebSocketRoute):
                app_instance.add_api_websocket_route(
                    alias_path,
                    route.endpoint,
                    name=f"{route.name}{version_prefix}",
                )
                existing_paths.add(alias_path)


def _load_active_option_chain_cache() -> dict[str, dict[str, Any]]:
    db = MongoData()
    try:
        contracts = list(
            db._db["active_option_tokens"].find(
                {},
                {
                    "_id": 0,
                    "instrument": 1,
                    "option_type": 1,
                    "expiry": 1,
                    "strike": 1,
                    "exchange": 1,
                    "symbol": 1,
                    "token": 1,
                    "tokens": 1,
                    "created_at": 1,
                    "updated_at": 1,
                },
            ).sort([("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)])
        )
    finally:
        db.close()

    cache: dict[str, dict[str, Any]] = {}
    for contract in contracts:
        instrument = str(contract.get("instrument") or "").strip().upper()
        expiry = str(contract.get("expiry") or "").strip()[:10]
        option_type = str(contract.get("option_type") or "").strip().upper()
        token = str(contract.get("token") or contract.get("tokens") or "").strip()
        if not instrument or not expiry:
            continue

        instrument_bucket = cache.setdefault(
            instrument,
            {
                "instrument": instrument,
                "expiries": [],
                "expiry_count": 0,
                "total_contracts": 0,
                "source": "active_option_tokens",
                "option_chain": [],
                "grouped_option_chain": {},
            },
        )
        if expiry not in instrument_bucket["expiries"]:
            instrument_bucket["expiries"].append(expiry)

        grouped_bucket = instrument_bucket["grouped_option_chain"].setdefault(
            expiry,
            {"CE": [], "PE": []},
        )

        strike_raw = contract.get("strike")
        try:
            strike_value = float(strike_raw)
        except (TypeError, ValueError):
            strike_value = 0.0
        strike = int(strike_value) if strike_value.is_integer() else strike_value

        row = {
            "instrument": instrument,
            "expiry": expiry,
            "strike": strike,
            "option_type": option_type,
            "token": token,
            "tokens": token,
            "symbol": str(contract.get("symbol") or "").strip(),
            "exchange": str(contract.get("exchange") or "").strip(),
            "ltp": 0.0,
            "created_at": str(contract.get("created_at") or "").strip(),
            "updated_at": str(contract.get("updated_at") or "").strip(),
        }
        instrument_bucket["option_chain"].append(row)
        if option_type in {"CE", "PE"}:
            grouped_bucket[option_type].append(row)

    for instrument_bucket in cache.values():
        instrument_bucket["expiries"].sort()
        instrument_bucket["expiry_count"] = len(instrument_bucket["expiries"])
        instrument_bucket["total_contracts"] = len(instrument_bucket["option_chain"])
        for expiry_bucket in instrument_bucket["grouped_option_chain"].values():
            expiry_bucket["CE"].sort(key=lambda item: float(item.get("strike") or 0.0))
            expiry_bucket["PE"].sort(key=lambda item: float(item.get("strike") or 0.0))

    return cache


def _refresh_active_option_chain_cache() -> dict[str, dict[str, Any]]:
    cache = _load_active_option_chain_cache()
    with _ACTIVE_OPTION_CHAIN_CACHE_LOCK:
        _ACTIVE_OPTION_CHAIN_CACHE.clear()
        _ACTIVE_OPTION_CHAIN_CACHE.update(cache)
    return cache


def _get_active_option_chain_cache(instrument: str) -> dict[str, Any] | None:
    normalized_instrument = str(instrument or "").strip().upper()
    with _ACTIVE_OPTION_CHAIN_CACHE_LOCK:
        cached = _ACTIVE_OPTION_CHAIN_CACHE.get(normalized_instrument)
        if cached is not None:
            return cached

    cache = _refresh_active_option_chain_cache()
    return cache.get(normalized_instrument)


def _request_fingerprint(request: dict) -> str:
    payload = json.dumps(request, sort_keys=True, separators=(",", ":"))
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _estimate_total_steps(request: dict) -> int:
    start_date = request.get("start_date")
    end_date = request.get("end_date")
    if not start_date or not end_date:
        return 0
    try:
        db = MongoData()
        holidays = db.get_holidays()
        cur = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        total_days = 0
        while cur <= end_dt:
            if cur.weekday() < 5 and cur.strftime("%Y-%m-%d") not in holidays:
                total_days += 1
            cur += timedelta(days=1)
        db.close()
        return total_days + 1 if total_days > 0 else 0
    except Exception:
        return 0


def _job_state_path(job_id: str) -> Path:
    JOB_STATE_DIR.mkdir(parents=True, exist_ok=True)
    return JOB_STATE_DIR / f"{job_id}.json"


def _cache_path(fingerprint: str) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"{fingerprint}.json"


def _write_job_state(job_id: str, payload: dict) -> None:
    path = _job_state_path(job_id)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(payload, f)
    os.replace(tmp_path, path)


def _read_job_state(job_id: str) -> dict | None:
    path = _job_state_path(job_id)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _read_cached_result(fingerprint: str) -> dict | None:
    path = _cache_path(fingerprint)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return None


def _write_cached_result(fingerprint: str, result: dict) -> None:
    path = _cache_path(fingerprint)
    tmp_path = path.with_suffix(".tmp")
    with open(tmp_path, "w") as f:
        json.dump(result, f)
    os.replace(tmp_path, path)


def _cleanup_old_jobs():
    """Remove finished jobs older than JOB_TTL_SECONDS and enforce MAX_JOBS limit."""
    # Sync in-memory "running" jobs from file — child process only writes files,
    # so _jobs in the parent can be stale (still "running" after the child finishes).
    for jid, job in list(_jobs.items()):
        if job["status"] == "running":
            file_state = _read_job_state(jid)
            if file_state and file_state.get("status") != "running":
                _jobs[jid].update(file_state)

    now = time.time()
    expired = [jid for jid, j in _jobs.items()
               if j["status"] != "running"
               and now - j.get("created_at", now) > JOB_TTL_SECONDS]
    for jid in expired:
        state_path = _job_state_path(jid)
        if state_path.exists():
            state_path.unlink()
        del _jobs[jid]

    # if still over limit, remove oldest completed jobs first
    if len(_jobs) >= MAX_JOBS:
        done = sorted(
            [(jid, j) for jid, j in _jobs.items() if j["status"] != "running"],
            key=lambda x: x[1].get("created_at", 0),
        )
        for jid, _ in done[:len(_jobs) - MAX_JOBS + 1]:
            del _jobs[jid]




def _run_job(job_id: str, request: dict):
    try:
        os.nice(15)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}

    def on_progress(completed: int, total: int, day: str):
        state.update({
            "job_id": job_id,
            "status": "running",
            "completed": completed,
            "total": total,
            "percent": round(completed / total * 100, 1) if total else 0,
            "current_day": day,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)

    try:
        result = run_backtest(request, on_progress=on_progress)
        fingerprint = state.get("fingerprint")
        if fingerprint:
            _write_cached_result(fingerprint, result)
        total = state.get("total", 0)
        state.update({
            "job_id": job_id,
            "status": "done",
            "completed": total,
            "percent": 100.0 if total else 0.0,
            "current_day": "Completed",
            "result": result,
            "error": None,
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)
    except Exception as e:
        state.update({
            "job_id": job_id,
            "status": "error",
            "error": str(e),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


def strategy_worker(args: dict):
    strategy_id_str = str((args or {}).get("strategy_id_str") or "")
    backtest_req = dict((args or {}).get("backtest_req") or {})
    job_id = str((args or {}).get("job_id") or "")

    # Write per-strategy progress to a temp file — avoids Manager IPC complexity
    prog_path = JOB_STATE_DIR / f"{job_id}_{strategy_id_str}.prog" if job_id else None

    def on_progress(completed: int, total: int, day: str):
        if not prog_path:
            return
        try:
            tmp = prog_path.with_suffix(".tmp")
            with open(tmp, "w") as f:
                json.dump({"completed": completed, "total": total, "day": day}, f)
            os.replace(tmp, prog_path)
        except Exception:
            pass

    try:
        result = run_backtest(backtest_req, on_progress=on_progress)
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "completed",
            "error": None,
            "results": result,
        }
    except Exception as exc:
        return {
            "_id": strategy_id_str,
            "item_id": strategy_id_str,
            "status": "error",
            "error": str(exc),
            "results": None,
        }
    finally:
        # Clean up progress file on completion/error
        if prog_path and prog_path.exists():
            try:
                prog_path.unlink()
            except Exception:
                pass


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _parse_datetime_string(value: Any) -> datetime | None:
    normalized_value = str(value or "").strip()
    if not normalized_value:
        return None
    normalized_value = normalized_value.replace("T", " ")
    for pattern in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(normalized_value, pattern)
        except ValueError:
            continue
    return None


def _shift_datetime_string_by_minutes(value: Any, minutes: int) -> Any:
    if not minutes:
        return value
    parsed_value = _parse_datetime_string(value)
    if parsed_value is None:
        return value
    shifted_value = parsed_value - timedelta(minutes=minutes)
    if "." in str(value or ""):
        return shifted_value.strftime("%Y-%m-%d %H:%M:%S.%f")
    return shifted_value.strftime("%Y-%m-%d %H:%M:%S")


def _load_strategy_time_difference_minutes(db: MongoData, activation_mode: str) -> int:
    normalized_mode = str(activation_mode or "").strip()
    if not normalized_mode:
        return 0

    query_candidates = [
        {"activation_mode": normalized_mode, "status": 1},
        {"activation_mode": normalized_mode, "is_active": True},
        {"activation_mode": normalized_mode, "active": True},
        {"activation_mode": normalized_mode},
    ]

    for query in query_candidates:
        try:
            config_doc = db._db["strategy_entry_time_difference"].find_one(
                query,
                {"difference_time_interval": 1},
                sort=[("_id", -1)],
            )
        except Exception:
            config_doc = None
        if config_doc:
            return max(0, _safe_int(config_doc.get("difference_time_interval"), 0))
    return 0


def _load_activation_portfolio_doc(db: MongoData, portfolio_id: str):
    normalized_portfolio_id = str(portfolio_id or "").strip()
    if not normalized_portfolio_id:
        raise HTTPException(status_code=400, detail="portfolio_id is required")
    try:
        portfolio_oid = ObjectId(normalized_portfolio_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid portfolio_id")

    source_doc = db._db["saved_portfolios"].find_one({"_id": portfolio_oid}, {"_id": 1, "name": 1})
    if source_doc:
        return "source", portfolio_oid, source_doc

    daily_doc = db._db[ALGO_TRADE_PORTFOLIO_COLLECTION].find_one(
        {"_id": portfolio_oid},
        {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1},
    )
    if daily_doc:
        return "daily", portfolio_oid, daily_doc

    raise HTTPException(status_code=404, detail="Portfolio not found")


def _get_source_portfolio_id_from_doc(portfolio_kind: str, portfolio_oid, portfolio_doc: dict) -> str:
    if portfolio_kind == "daily":
        resolved = str((portfolio_doc or {}).get("source_portfolio_id") or "").strip()
        if resolved:
            return resolved
    return str(portfolio_oid)


def _load_source_portfolio_root(db: MongoData, portfolio_kind: str, portfolio_oid, portfolio_doc: dict):
    if portfolio_kind == "source":
        return portfolio_oid, portfolio_doc or {}

    source_portfolio_id = str((portfolio_doc or {}).get("source_portfolio_id") or "").strip()
    if source_portfolio_id:
        try:
            source_oid = ObjectId(source_portfolio_id)
            source_doc = db._db["saved_portfolios"].find_one({"_id": source_oid}, {"_id": 1, "name": 1}) or {}
            return source_oid, source_doc
        except Exception:
            pass
    return portfolio_oid, {"_id": portfolio_oid, "name": str((portfolio_doc or {}).get("source_portfolio_name") or (portfolio_doc or {}).get("name") or "").strip()}


def _normalize_trade_index(value: Any) -> str:
    return str(value or "").strip().upper()


def _extract_trade_index(*candidates: Any) -> str:
    for candidate in candidates:
        if isinstance(candidate, dict):
            nested_value = _extract_trade_index(
                candidate.get("trade_index"),
                candidate.get("ticker"),
                candidate.get("underlying"),
                ((candidate.get("config") or {}) if isinstance(candidate.get("config"), dict) else {}).get("Ticker"),
                ((candidate.get("strategy_detail") or {}) if isinstance(candidate.get("strategy_detail"), dict) else {}).get("underlying"),
                ((candidate.get("strategy") or {}) if isinstance(candidate.get("strategy"), dict) else {}).get("Ticker"),
            )
            if nested_value:
                return nested_value
            continue
        normalized = _normalize_trade_index(candidate)
        if normalized:
            return normalized
    return "NIFTY"


def _resolve_daily_portfolio(
    db: MongoData,
    source_portfolio_oid,
    source_portfolio_doc: dict,
    activation_mode: str = "",
    trade_date_hint: str = "",
    trade_index: str = "",
):
    """Find or create a daily runtime portfolio in algo_trade_portfolio.

    Runtime portfolio identity is scoped by:
      trade_date + activation_mode + trade_index

    Returns (portfolio_id_str, portfolio_doc_dict).
    """
    normalized_mode = _normalize_runtime_activation_mode(activation_mode)
    trade_date = _default_runtime_trade_date(normalized_mode, str(trade_date_hint or "").strip()[:10])
    if not trade_date:
        trade_date = datetime.now(IST).strftime("%Y-%m-%d")
    normalized_trade_index = _extract_trade_index(trade_index)

    collection = db._db[ALGO_TRADE_PORTFOLIO_COLLECTION]
    query = {
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "trade_index": normalized_trade_index,
    }
    existing = collection.find_one(
        query,
        {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1, "created_at": 1, "updated_at": 1},
    )
    if existing:
        return str(existing["_id"]), existing

    new_oid = ObjectId()
    now_iso = datetime.utcnow().isoformat()
    sibling_doc = collection.find_one(
        {
            "trade_date": trade_date,
            "activation_mode": normalized_mode,
            "trade_group_portfolio": {"$exists": True, "$ne": ""},
        },
        {"trade_group_portfolio": 1},
    )
    trade_group_portfolio = str((sibling_doc or {}).get("trade_group_portfolio") or "").strip() or str(ObjectId())
    new_doc = {
        "_id": new_oid,
        "trade_portfolio": str(new_oid),
        "trade_group_portfolio": trade_group_portfolio,
        "trade_index": normalized_trade_index,
        "trade_date": trade_date,
        "activation_mode": normalized_mode,
        "created_at": now_iso,
        "updated_at": now_iso,
    }
    try:
        result = collection.insert_one(new_doc)
        return str(result.inserted_id), {
            "_id": result.inserted_id,
            "trade_portfolio": str(new_doc["trade_portfolio"]),
            "trade_group_portfolio": trade_group_portfolio,
            "trade_index": normalized_trade_index,
            "trade_date": trade_date,
            "activation_mode": normalized_mode,
            "created_at": now_iso,
            "updated_at": now_iso,
        }
    except Exception:
        fallback = collection.find_one(
            query,
            {"_id": 1, "trade_portfolio": 1, "trade_group_portfolio": 1, "trade_index": 1, "trade_date": 1, "activation_mode": 1, "created_at": 1, "updated_at": 1},
        )
        if fallback:
            return str(fallback["_id"]), fallback
        return str(source_portfolio_oid), source_portfolio_doc


def _apply_strategy_time_difference_to_trade(trade_doc: dict, difference_minutes: int) -> dict:
    if difference_minutes <= 0 or not isinstance(trade_doc, dict):
        return trade_doc

    adjusted_doc = dict(trade_doc)
    for field_name in ("entry_time", "exit_time", "check_after_ts"):
        if field_name in adjusted_doc:
            adjusted_doc[field_name] = _shift_datetime_string_by_minutes(
                adjusted_doc.get(field_name),
                difference_minutes,
            )
    return adjusted_doc


def _calc_leg_pnl(leg: dict) -> dict:
    entry_trade = leg.get("entry_trade") if isinstance(leg.get("entry_trade"), dict) else {}
    exit_trade = leg.get("exit_trade") if isinstance(leg.get("exit_trade"), dict) else {}
    entry_price = _safe_float(entry_trade.get("price"))
    quantity = _safe_int(leg.get("quantity") or entry_trade.get("quantity"))
    lot_size = _safe_int(leg.get("lot_size"), 1)
    effective_quantity = max(0, quantity) * max(1, lot_size)
    is_sell = "sell" in str(leg.get("position") or "").lower()

    if exit_trade:
        mark_price = _safe_float(exit_trade.get("price"))
        pnl_price_source = "exit_trade"
    else:
        mark_price = _safe_float(leg.get("last_saw_price"))
        pnl_price_source = "last_saw_price"

    if entry_price <= 0 or effective_quantity <= 0:
        pnl_value = 0.0
    else:
        pnl_value = ((entry_price - mark_price) if is_sell else (mark_price - entry_price)) * effective_quantity

    leg_payload = dict(leg)
    leg_payload["entry_price"] = entry_price
    leg_payload["mark_price"] = round(mark_price, 2)
    leg_payload["effective_quantity"] = effective_quantity
    leg_payload["pnl_price_source"] = pnl_price_source
    leg_payload["pnl"] = round(pnl_value, 2)
    return leg_payload


def _populate_history_legs(db_instance, records: list) -> list:
    """
    Batch-fetch all algo_trade_positions_history docs for the given trade records
    by querying trade_id. Groups docs per trade and attaches them as legs[].
    Status counts are derived from history docs:
      status=1 → open_legs_count
      status=2 → closed_legs_count
      status=0 → pending_legs_count
    """
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    # Single batch query: all history docs for all trades at once
    history_by_trade: dict[str, list] = {tid: [] for tid in trade_ids}
    try:
        history_col = db_instance["algo_trade_positions_history"]
        for doc in history_col.find({"trade_id": {"$in": trade_ids}}):
            doc["_id"] = str(doc.get("_id") or "")
            tid = str(doc.get("trade_id") or "")
            if tid in history_by_trade:
                history_by_trade[tid].append(doc)
    except Exception:
        pass

    populated = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        history_legs = history_by_trade.get(trade_id) or []
        new_rec = dict(rec)
        new_rec["legs"] = history_legs
        new_rec["open_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 1)
        new_rec["closed_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 2)
        new_rec["pending_legs_count"] = sum(1 for l in history_legs if _safe_int(l.get("status")) == 0)
        populated.append(new_rec)
    return populated


def _format_feature_status_timestamp(value) -> str:
    raw_value = str(value or "").strip()
    if not raw_value:
        return ""
    return raw_value.replace("T", " ")


def _format_feature_status_price(value) -> str:
    numeric = _safe_float(value)
    if numeric <= 0:
        return "-"
    return f"₹{numeric:.2f}"


def _describe_feature_status_row(row: dict) -> str:
    if not isinstance(row, dict):
        return ""

    description = str(row.get("trigger_description") or "").strip()
    if description:
        return description

    feature_key = str(row.get("feature") or "").strip()
    if feature_key in {"overall_sl", "overall_target"}:
        label = "Overall SL" if feature_key == "overall_sl" else "Overall Target"
        cycle_number = int(row.get("cycle_number") or 1)
        trigger_value = _format_feature_status_price(row.get("trigger_value"))
        next_value = _format_feature_status_price(row.get("next_trigger_value"))
        reentry_type = str(row.get("reentry_type") or "None")
        reentry_count = int(row.get("reentry_count") or 0)
        reentry_done = int(row.get("reentry_done") or 0)
        return (
            f"{label} active for cycle {cycle_number}. "
            f"Current threshold {trigger_value}. "
            f"Re-entry {reentry_type} used {reentry_done}/{reentry_count}. "
            f"Next cycle threshold {next_value}."
        )
    if feature_key == "pending_entry":
        option = str(row.get("option") or "").strip().upper() or "-"
        position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
        strike = str(row.get("strike") or "").strip() or "-"
        queued_at = _format_feature_status_timestamp(row.get("queued_at"))
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        status = str(row.get("status") or "").strip().lower()

        if status == "triggered":
            return (
                f"Pending entry triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
            )

        return (
            f"Pending entry active for {strike} {option} {position} leg since {queued_at or '-'}. "
            f"Waiting for next entry cycle."
        )

    if feature_key != "momentum_pending":
        return ""

    status = str(row.get("status") or "").strip().lower()
    option = str(row.get("option") or "").strip().upper() or "-"
    position = str(row.get("position") or "").split(".")[-1].strip() or "Position"
    strike = str(row.get("strike") or "").strip() or "-"
    momentum_type = str(row.get("momentum_type") or "").split(".")[-1].strip() or "Momentum"
    momentum_value = _safe_float(row.get("momentum_value"))
    base_price = _format_feature_status_price(row.get("momentum_base_price"))
    target_price = _format_feature_status_price(row.get("momentum_target_price"))
    queued_at = _format_feature_status_timestamp(row.get("queued_at"))
    armed_at = _format_feature_status_timestamp(row.get("armed_at"))

    if status == "triggered":
        triggered_at = _format_feature_status_timestamp(row.get("triggered_at"))
        return (
            f"Momentum triggered for {strike} {option} {position} leg at {triggered_at or '-'}."
        )

    if _safe_float(row.get("momentum_base_price")) > 0 and _safe_float(row.get("momentum_target_price")) > 0:
        return (
            f"Momentum waiting for {strike} {option} {position} leg. "
            f"{momentum_type} {momentum_value:g} armed at {armed_at or queued_at or '-'} "
            f"with base {base_price} and target {target_price}."
        )

    return (
        f"Momentum queue active for {strike} {option} {position} leg since {queued_at or '-'}. "
        f"Waiting to arm {momentum_type} {momentum_value:g}."
    )


def _build_pending_feature_leg(row: dict) -> dict:
    row_copy = dict(row)
    description = _describe_feature_status_row(row_copy)
    if description:
        row_copy["trigger_description"] = description

    feature_map = {}
    feature_key = str(row_copy.get("feature") or "").strip()
    if feature_key:
        feature_map[feature_key] = row_copy

    return {
        "id": str(row_copy.get("leg_id") or ""),
        "leg_id": str(row_copy.get("leg_id") or ""),
        "status": 0,
        "position": row_copy.get("position"),
        "option": row_copy.get("option"),
        "strike": row_copy.get("strike"),
        "expiry_date": row_copy.get("expiry_date"),
        "token": row_copy.get("token"),
        "symbol": row_copy.get("symbol"),
        "quantity": 0,
        "lot_config_value": int(row_copy.get("lot_config_value") or 1),
        "entry_trade": None,
        "exit_trade": None,
        "last_saw_price": row_copy.get("momentum_base_price"),
        "is_lazy": True,
        "is_pending_feature_leg": True,
        "queued_at": row_copy.get("queued_at"),
        "armed_at": row_copy.get("armed_at"),
        "triggered_at": row_copy.get("triggered_at"),
        "leg_type": row_copy.get("leg_type"),
        "momentum_base_price": row_copy.get("momentum_base_price"),
        "momentum_target_price": row_copy.get("momentum_target_price"),
        "feature_status_rows": [row_copy],
        "feature_status_map": feature_map,
        "active_trigger_descriptions": [description] if description else [],
    }


def _attach_leg_feature_statuses(db_instance, records: list) -> list:
    if not records:
        return records

    trade_ids = [str(rec.get("_id") or "") for rec in records if rec.get("_id")]
    if not trade_ids:
        return records

    feature_rows_by_key: dict[tuple[str, str], list] = {}
    try:
        feature_col = db_instance["algo_leg_feature_status"]
        for doc in feature_col.find(
            {
                "trade_id": {"$in": trade_ids},
                "enabled": True,
            }
        ):
            trade_id = str(doc.get("trade_id") or "")
            leg_id = str(doc.get("leg_id") or "")
            if not trade_id or not leg_id:
                continue
            doc["_id"] = str(doc.get("_id") or "")
            feature_rows_by_key.setdefault((trade_id, leg_id), []).append(doc)
    except Exception:
        return records

    enriched_records = []
    for rec in records:
        trade_id = str(rec.get("_id") or "")
        legs = rec.get("legs") if isinstance(rec.get("legs"), list) else []
        existing_leg_ids = set()
        enriched_legs = []
        for leg in legs:
            if not isinstance(leg, dict):
                enriched_legs.append(leg)
                continue
            leg_id = str(leg.get("_id") or leg.get("leg_id") or leg.get("id") or "")
            if leg_id:
                existing_leg_ids.add(leg_id)
            feature_rows = feature_rows_by_key.get((trade_id, leg_id), [])
            leg_copy = dict(leg)
            leg_copy["feature_status_rows"] = feature_rows
            feature_map = {}
            active_descriptions = []
            for row in feature_rows:
                feature_key = str(row.get("feature") or "").strip()
                if not feature_key:
                    continue
                row_copy = dict(row)
                description = _describe_feature_status_row(row_copy)
                if description:
                    row_copy["trigger_description"] = description
                feature_map[feature_key] = row_copy
                if description:
                    active_descriptions.append(description)
            leg_copy["feature_status_map"] = feature_map
            leg_copy["feature_status_rows"] = list(feature_map.values()) if feature_map else feature_rows
            leg_copy["active_trigger_descriptions"] = active_descriptions
            enriched_legs.append(leg_copy)

        pending_feature_legs = []
        strategy_feature_rows = []
        for (feature_trade_id, feature_leg_id), feature_rows in feature_rows_by_key.items():
            if feature_trade_id != trade_id or not feature_leg_id or feature_leg_id in existing_leg_ids:
                continue
            if feature_leg_id == "__overall__":
                for row in feature_rows:
                    row_copy = dict(row)
                    description = _describe_feature_status_row(row_copy)
                    if description:
                        row_copy["trigger_description"] = description
                    strategy_feature_rows.append(row_copy)
                continue
            for row in feature_rows:
                if str(row.get("feature") or "").strip() not in {"momentum_pending", "pending_entry"}:
                    continue
                if str(row.get("status") or "").strip().lower() != "active":
                    continue
                pending_feature_legs.append(_build_pending_feature_leg(row))

        new_rec = dict(rec)
        new_rec["legs"] = enriched_legs
        new_rec["pending_feature_legs"] = pending_feature_legs
        new_rec["strategy_feature_status_rows"] = strategy_feature_rows
        enriched_records.append(new_rec)
    return enriched_records


def _extract_broker_configuration_label(document: dict, fallback_broker_id: str = "") -> str:
    if not isinstance(document, dict):
        return fallback_broker_id
    for key in (
        "broker_name",
        "display_name",
        "name",
        "title",
        "broker",
        "broker_type",
        "provider",
        "vendor",
    ):
        value = str(document.get(key) or "").strip()
        if value:
            return value
    return str(fallback_broker_id or "").strip()


def _attach_broker_configuration_details(db_instance, records: list) -> list:
    if not records:
        return records

    broker_ids = []
    broker_object_ids = []
    for record in records:
        broker_id = str((record or {}).get("broker") or "").strip()
        if not broker_id:
            continue
        broker_ids.append(broker_id)
        try:
            broker_object_ids.append(ObjectId(broker_id))
        except Exception:
            continue

    if not broker_ids:
        return records

    broker_docs_by_id = {}
    try:
        cursor = db_instance["broker_configuration"].find(
            {"_id": {"$in": broker_object_ids}},
            {
                "_id": 1,
                "broker_name": 1,
                "display_name": 1,
                "name": 1,
                "title": 1,
                "broker": 1,
                "broker_icon": 1,
                "broker_type": 1,
                "provider": 1,
                "vendor": 1,
            },
        )
        for item in cursor:
            if not item:
                continue
            item_id = str(item.get("_id") or "").strip()
            if item_id:
                broker_docs_by_id[item_id] = item
    except Exception:
        return records

    if not broker_docs_by_id:
        return records

    enriched_records = []
    for record in records:
        new_record = dict(record)
        broker_id = str(new_record.get("broker") or "").strip()
        broker_doc = broker_docs_by_id.get(broker_id)
        if broker_doc:
            broker_details = dict(broker_doc)
            broker_details["_id"] = str(broker_doc.get("_id") or broker_id)
            new_record["broker_details"] = broker_details
            new_record["broker_label"] = _extract_broker_configuration_label(broker_doc, broker_id)
        enriched_records.append(new_record)
    return enriched_records


def _enrich_execution_record_with_pnl(record: dict) -> dict:
    legs = record.get("legs") if isinstance(record.get("legs"), list) else []
    enriched_legs = [_calc_leg_pnl(leg) for leg in legs if isinstance(leg, dict)]
    enriched_record = dict(record)
    enriched_record["legs"] = enriched_legs
    return enriched_record


def _run_portfolio_job(job_id: str, request: dict):
    """
    Subprocess worker for portfolio backtest.
    Runs all strategies in parallel using ProcessPoolExecutor.
    """
    from concurrent.futures import ProcessPoolExecutor, as_completed, wait
    import multiprocessing

    try:
        os.nice(10)
    except Exception:
        pass

    state = _read_job_state(job_id) or {}
    portfolio_id = request.get("portfolio")
    start_date   = request.get("start_date")
    end_date     = request.get("end_date")

    try:
        db = MongoData()
        portfolio = db._db["saved_portfolios"].find_one({"_id": ObjectId(portfolio_id)})
        if not portfolio:
            state.update({"job_id": job_id, "status": "error",
                          "error": f"Portfolio {portfolio_id} not found",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_ids = portfolio.get("strategy_ids", [])
        if not strategy_ids:
            state.update({"job_id": job_id, "status": "error",
                          "error": "Portfolio has no strategies",
                          "updated_at": time.time()})
            _write_job_state(job_id, state)
            db.close()
            return

        strategy_docs = list(db._db["saved_strategies"].find(
            {"_id": {"$in": strategy_ids}},
            {"_id": 1, "name": 1, "full_config": 1},
        ))
        db.close()

        strategy_map     = {str(d["_id"]): d for d in strategy_docs}
        total_strategies = len(strategy_ids)
        name_map         = {}

        # Build per-strategy worker args
        worker_args = []
        error_results = []
        for strategy_id_obj in strategy_ids:
            strategy_id_str = str(strategy_id_obj)
            strategy_doc    = strategy_map.get(strategy_id_str)
            strategy_name   = (strategy_doc or {}).get("name") or strategy_id_str
            name_map[strategy_id_str] = strategy_name

            if not strategy_doc:
                error_results.append({
                    "_id":     strategy_id_str,
                    "item_id": strategy_id_str,
                    "status":  "error",
                    "error":   "Strategy not found",
                    "results": None,
                })
                continue

            full_config  = strategy_doc.get("full_config") or {}
            backtest_req = dict(full_config)
            backtest_req["start_date"] = start_date
            backtest_req["end_date"]   = end_date
            if "weekly_old_regime" in request:
                backtest_req["weekly_old_regime"] = request["weekly_old_regime"]

            worker_args.append({
                "strategy_id_str": strategy_id_str,
                "backtest_req":    backtest_req,
                "job_id":          job_id,
            })

        # Initial progress state
        state.update({
            "job_id":         job_id,
            "status":         "running",
            "strategy_count": total_strategies,
            "completed":      0,
            "total":          total_strategies,
            "percent":        0.0,
            "current_day":    f"Running {total_strategies} strategies in parallel…",
            "error":          None,
            "updated_at":     time.time(),
        })
        _write_job_state(job_id, state)

        results_by_id = {}
        for r in error_results:
            results_by_id[r["item_id"]] = r

        # Run in parallel — use min(strategies, cpu_count, 8) workers
        max_workers  = max(1, min(len(worker_args), os.cpu_count() or 4, 8))
        done_count   = len(error_results)

        def _read_prog_files() -> dict:
            """Read all per-strategy progress files for this job."""
            result = {}
            try:
                for p in JOB_STATE_DIR.glob(f"{job_id}_*.prog"):
                    try:
                        with open(p) as f:
                            data = json.load(f)
                        sid = p.stem[len(job_id) + 1:]
                        result[sid] = data
                    except Exception:
                        pass
            except Exception:
                pass
            return result

        if worker_args:
            with ProcessPoolExecutor(max_workers=max_workers) as pool:
                futures = {
                    pool.submit(strategy_worker, args): args["strategy_id_str"]
                    for args in worker_args
                }
                while futures:
                    done, not_done = wait(futures, timeout=1.0)

                    # Read per-strategy progress from temp files
                    prog_files = _read_prog_files()
                    total_pct  = done_count * 100.0
                    active_day = f"Completed {done_count}/{total_strategies}"
                    for sid, info in prog_files.items():
                        if info.get("total"):
                            worker_pct = (info["completed"] / info["total"]) * 100.0
                            worker_pct = max(worker_pct, 2.0)
                            total_pct += worker_pct
                            if info.get("day"):
                                active_day = info["day"]

                    overall_pct = round(total_pct / total_strategies, 1) if total_strategies else 0.0
                    state.update({
                        "job_id":      job_id,
                        "status":      "running",
                        "completed":   done_count,
                        "percent":     overall_pct,
                        "current_day": active_day,
                        "error":       None,
                        "updated_at":  time.time(),
                    })
                    _write_job_state(job_id, state)

                    for future in done:
                        result_item = future.result()
                        sid         = result_item["item_id"]
                        results_by_id[sid] = result_item
                        done_count += 1
                        del futures[future]

                        # Write immediately after each strategy completes
                        pct = round(done_count / total_strategies * 100, 1) if total_strategies else 0.0
                        state.update({
                            "completed":   done_count,
                            "percent":     pct,
                            "current_day": f"Completed {done_count}/{total_strategies} strategies",
                            "updated_at":  time.time(),
                        })
                        _write_job_state(job_id, state)

        # Preserve original strategy order
        results = [results_by_id[str(sid)] for sid in strategy_ids if str(sid) in results_by_id]

        final_result = {
            "status":   "completed",
            "progress": 100,
            "results":  results,
        }

        state.update({
            "job_id":      job_id,
            "status":      "done",
            "completed":   total_strategies,
            "total":       total_strategies,
            "percent":     100.0,
            "current_day": "Completed",
            "result":      final_result,
            "error":       None,
            "updated_at":  time.time(),
        })
        _write_job_state(job_id, state)

    except Exception as e:
        import traceback
        state.update({
            "job_id":     job_id,
            "status":     "error",
            "error":      traceback.format_exc(),
            "updated_at": time.time(),
        })
        _write_job_state(job_id, state)


# ─── App ──────────────────────────────────────────────────────────────────────

app    = FastAPI(title="Local Backtest API", version="2.0.0")
sim_router = APIRouter()   # simulator routes live at /simulator/... (no extra prefix)

# fno-stocks is a common/shared concern — code lives in shared/features/ but
# is served ONLY from algo.websocket (8003), not mounted here too.
# algo.scanner's own api (and chart_api.py, mounted alongside it) only
# contains scanner/chart-specific routes.


class PTPortfolioIn(BaseModel):
    name: str


class ZerodhaConfigRequest(BaseModel):
    api_key: str
    api_secret: str


class PTPositionIn(BaseModel):
    type: str
    option_type: str
    strike: float = 0.0  # 0.0 for a futures leg (option_type "FUT") — no strike on a future
    expiry: str
    token: Optional[str] = None
    entry_price: float
    entry_time: Optional[str] = None
    lots: Optional[int] = 1
    lot_size: Optional[int] = 75
    quantity: Optional[float] = None
    exited: Optional[bool] = False
    exit_price: Optional[float] = None
    exit_time: Optional[str] = None
    pnl: Optional[float] = None
    pnl_pct: Optional[float] = None


class PTStrategyIn(BaseModel):
    portfolio_name: str
    strategy_name: str
    instrument: Optional[str] = "nifty"
    spot_price: Optional[float] = None
    config: Optional[dict[str, Any]] = None
    positions: Optional[list[PTPositionIn]] = []
    # "backtest" for strategies saved from the historical-data builder
    # (PaperTradeBacktest.tsx), "live" for everything saved from the
    # live-broker/positions views — pure bookkeeping, doesn't gate the risk
    # monitor (that's alert_status, set separately by the "Add Alert" toggle).
    mode: Optional[str] = "live"


class PTWebhookIn(BaseModel):
    strategy_id: str
    adjustment_id: str


class PTTriggerIn(BaseModel):
    broker_id: str
    leg_id: str
    underlying: Optional[str] = None
    expiry: Optional[str] = None
    strike: Optional[float] = None
    option_type: Optional[str] = None
    side: Optional[str] = None
    sl_mode: str
    sl_value: float
    tp_mode: str
    tp_value: float
    entry_price: float
    quantity: int
    exited: Optional[bool] = False


class PortfolioLegSnapshot(BaseModel):
    leg_id: str
    quantity: int


class PTPortfolioTriggerIn(BaseModel):
    broker_id: str
    underlying: str
    sl_upper: Optional[float] = None
    sl_lower: Optional[float] = None
    legs_snapshot: list[PortfolioLegSnapshot] = []


class PTAlertConfigLegSnapshot(BaseModel):
    leg_id: str
    quantity: int
    entry_price: float
    side: str


class PTAlertConfigToggle(BaseModel):
    enabled: bool = False
    unit: str = "points"
    value: float = 0.0


class PTAlertConfigTrailingStop(BaseModel):
    enabled: bool = False
    unit: str = "points"
    x: float = 0.0
    y: float = 0.0


class PTAlertConfigHedgeStrikeType(BaseModel):
    enabled: bool = False
    mode: str = "delta"
    value: float = 0.0
    strike: str = "ATM"


class PTAlertConfigHedgeTimeControl(BaseModel):
    enabled: bool = False
    entry_time: str = "09:15"
    exit_time: str = "15:30"


class PTAlertConfigIn(BaseModel):
    broker_id: str
    underlying: str
    # "alert_only" -> a leg/basket SL-TP hit is logged + Telegrammed to the
    # user (see simulator_risk_monitor.py's notify_user calls) but no real
    # order is placed. "auto" -> today's existing behavior (fires for real,
    # gated only by the global AUTO_FIRE_ENABLED kill-switch).
    trading_mode: str = "auto"
    stoploss: PTAlertConfigToggle
    target: PTAlertConfigToggle
    trailing_stop: PTAlertConfigTrailingStop
    hedge_strike_type: PTAlertConfigHedgeStrikeType
    hedge_time_control: PTAlertConfigHedgeTimeControl
    legs_snapshot: list[PTAlertConfigLegSnapshot] = []


class AdjustmentPositionIn(BaseModel):
    side: str
    lots: int
    qty: int
    strike: float
    option_type: str
    expiry: str
    entry_price: float
    tag: str  # "EXIT" | "NEW"


class PTAdjustmentIn(BaseModel):
    # Live-broker view keys by (broker_id, underlying); a saved/virtual
    # strategy (no broker_id/leg_id) keys by strategy_id instead — exactly
    # one of the two pairs is ever sent by the frontend depending on which
    # view (PaperTradeNew.tsx's isSavedStrategyView) is open.
    broker_id: Optional[str] = None
    underlying: Optional[str] = None
    strategy_id: Optional[str] = None
    trigger_condition: Optional[str] = None
    trigger_price: Optional[float] = None
    positions: list[AdjustmentPositionIn] = []
    # True while this is the live, armed config the risk monitor will act on; the
    # monitor flips it to False (never deletes) once fired, so simulator_adjustments
    # keeps a history of past adjustments instead of losing them.
    status: bool = True


class PTAdjustmentPatchIn(BaseModel):
    positions: list[AdjustmentPositionIn] = []
    trigger_price: Optional[float] = None
    trigger_condition: Optional[str] = None


class SimulatorBrokerPositionsRequest(BaseModel):
    broker_id: Optional[str] = None


class ManualOrderLeg(BaseModel):
    underlying: str
    expiry: str            # "YYYY-MM-DD"
    strike: float = 0.0    # 0.0 for a futures leg (option_type "FUT")
    option_type: str       # "CE" / "PE" / "FUT"
    side: str               # "BUY" / "SELL"
    quantity: int
    order_type: str         # "MARKET" / "LIMIT" / "SL"
    product: str             # "NRML" / "MIS"
    price: float = 0.0
    trigger_price: float = 0.0


class ManualOrderRequest(BaseModel):
    broker_id: str
    orders: list[ManualOrderLeg]


def _normalize_pt_option_type(option_type: str) -> str:
    normalized = str(option_type or "").strip().upper()
    if normalized in {"CALL", "CE"}:
        return "CE"
    if normalized in {"PUT", "PE"}:
        return "PE"
    return normalized


def _resolve_pt_position_token(position: dict, instrument: str = "") -> str:
    direct_token = str(position.get("token") or position.get("tokens") or "").strip()
    if direct_token:
        return direct_token

    normalized_instrument = str(instrument or position.get("instrument") or "").strip().upper()
    normalized_expiry = str(position.get("expiry") or "").strip()[:10]
    normalized_option_type = _normalize_pt_option_type(str(position.get("option_type") or ""))
    is_future = normalized_option_type == "FUT"
    try:
        strike_value = float(position.get("strike") or 0)
    except (TypeError, ValueError):
        strike_value = 0.0

    if not normalized_instrument or not normalized_expiry or not normalized_option_type:
        return ""
    if not is_future and strike_value <= 0:
        return ""

    # _enrich_pt_strategy_positions' own "cross_tokens" fallback (below) already
    # does this exact broker-aware active_option_tokens lookup correctly — but
    # only for positions that already have *some* stored token needing
    # cross-broker resolution. A position with no stored token at all (this
    # function's whole reason to exist) used to only ever try
    # _load_kite_instruments(), a Kite-only instrument master — with Dhan as
    # the active broker that's always empty/wrong, so current_ltp/MTM never
    # got computed and the frontend never even subscribed the leg's token for
    # live updates. Try the active broker's own token collection first.
    try:
        from features.broker_gateway import _active_broker
        if _active_broker() == "dhan":
            # A futures contract has no strike (always stored as 0.0 — see
            # _sync_dhan_index_future_tokens), so the query must omit it entirely
            # rather than matching strike: 0.0 literally against whatever this
            # position happens to carry.
            query = {
                "instrument": normalized_instrument,
                "expiry": {"$regex": f"^{normalized_expiry}"},
                "option_type": normalized_option_type,
                "broker": "dhan",
            }
            if not is_future:
                query["strike"] = strike_value
            doc = _shared_mongo._db["active_option_tokens"].find_one(
                query,
                {"token": 1, "tokens": 1, "_id": 0},
            )
            if doc:
                return str(doc.get("token") or doc.get("tokens") or "").strip()
            return ""
    except Exception:
        pass

    try:
        instrument_doc = (_load_kite_instruments() or {}).get(
            (normalized_instrument, normalized_expiry, strike_value, normalized_option_type)
        ) or {}
        return str(instrument_doc.get("token") or instrument_doc.get("tokens") or "").strip()
    except Exception:
        return ""


def _enrich_pt_strategy_positions(strategy_doc: dict) -> dict:
    enriched = dict(strategy_doc or {})
    instrument = str(enriched.get("instrument") or "").strip().upper()

    # Step 1: resolve tokens
    positions = []
    for raw_position in (enriched.get("positions") or []):
        if not isinstance(raw_position, dict):
            positions.append(raw_position)
            continue
        position = dict(raw_position)
        resolved_token = _resolve_pt_position_token(position, instrument)
        if resolved_token:
            position["token"] = resolved_token
        positions.append(position)

    # Step 2: fetch current LTP for all position tokens
    try:
        from features.broker_gateway import get_broker_ltp_map, get_broker_rest_quotes, _active_broker  # type: ignore
        ws_ltp = get_broker_ltp_map() or {}
        active_broker = _active_broker()

        # Build broker-native token map: stored token → active broker's token
        # Needed when positions have Kite tokens but Dhan is active (or vice-versa)
        broker_token_for: dict[str, str] = {}  # stored_token → broker_token
        ws_seg_for: dict[str, str] = {}         # broker_token → ws_segment

        stored_tokens = [str(p.get("token") or "") for p in positions if isinstance(p, dict) and p.get("token")]
        if stored_tokens:
            db_docs = list(_shared_mongo._db["active_option_tokens"].find(
                {"token": {"$in": stored_tokens}, "broker": active_broker},
                {"_id": 0, "token": 1, "ws_segment": 1},
            ))
            found_broker_tokens = {str(d["token"]) for d in db_docs}
            for d in db_docs:
                t = str(d["token"])
                broker_token_for[t] = t   # already a broker token
                ws_seg_for[t] = str(d.get("ws_segment") or "NSE_FNO")

            # Positions with non-broker tokens → resolve by strike/expiry/option_type
            cross_tokens = [t for t in stored_tokens if t not in found_broker_tokens]
            if cross_tokens:
                # Batch-fetch the position details we need for cross-resolution
                pos_by_token = {str(p.get("token") or ""): p for p in positions if isinstance(p, dict) and p.get("token")}
                for stored_tok in cross_tokens:
                    pos = pos_by_token.get(stored_tok) or {}
                    instr = str(pos.get("instrument") or instrument or "").upper()
                    expiry = str(pos.get("expiry") or "")[:10]
                    strike = pos.get("strike")
                    ot = _normalize_pt_option_type(str(pos.get("option_type") or ""))
                    is_future = ot == "FUT"
                    # A futures position's strike is always 0.0 (falsy) — only CE/PE
                    # positions need a real strike to resolve, see PTPositionIn.
                    if not (instr and expiry and ot) or (not is_future and not strike):
                        continue
                    try:
                        cross_query = {"instrument": instr, "expiry": {"$regex": f"^{expiry}"},
                                        "option_type": ot, "broker": active_broker}
                        if not is_future:
                            cross_query["strike"] = float(strike)
                        dhan_doc = _shared_mongo._db["active_option_tokens"].find_one(
                            cross_query,
                            {"token": 1, "ws_segment": 1, "_id": 0},
                        )
                        if dhan_doc:
                            bt = str(dhan_doc["token"])
                            broker_token_for[stored_tok] = bt
                            ws_seg_for[bt] = str(dhan_doc.get("ws_segment") or "NSE_FNO")
                    except Exception:
                        pass

        # Collect all broker tokens for REST fallback
        all_broker_tokens = list({bt for bt in broker_token_for.values() if bt})
        missing_ltp = [t for t in all_broker_tokens if not ws_ltp.get(t)]
        rest_quotes: dict = {}
        if missing_ltp:
            try:
                rest_quotes = get_broker_rest_quotes(missing_ltp, _shared_mongo._db, ws_seg_for)
            except Exception:
                pass

        for position in positions:
            if not isinstance(position, dict):
                continue
            stored_tok = str(position.get("token") or "")
            if not stored_tok:
                continue
            bt = broker_token_for.get(stored_tok, stored_tok)
            ltp = float(ws_ltp.get(bt) or 0)
            if ltp == 0:
                ltp = float((rest_quotes.get(bt) or {}).get("ltp") or 0)
            if ltp > 0:
                position["current_ltp"] = round(ltp, 2)
    except Exception:
        pass

    enriched["positions"] = positions
    return enriched


_DEFAULT_PAPER_TRADE_SPOT_BROKER_ID = "69e18416c3d234dc8c90e6ca"


def _serialize_instrument_spot_token(doc: dict) -> dict:
    return {
        "_id": str(doc.get("_id") or "").strip(),
        "broker_id": str(doc.get("broker_id") or "").strip(),
        "instrument": str(doc.get("instrument") or "").strip().upper(),
        "code": str(doc.get("code") or "").strip().upper(),
        "token": str(doc.get("token") or "").strip(),
    }


def _get_instrument_spot_token_docs(broker_id: str = "") -> list[dict]:
    resolved_broker_id = str(broker_id or _DEFAULT_PAPER_TRADE_SPOT_BROKER_ID).strip()
    query = {"broker_id": resolved_broker_id} if resolved_broker_id else {}
    docs = list(
        _shared_mongo._db["instrument_spot_token"].find(
            query,
            {"broker_id": 1, "instrument": 1, "code": 1, "token": 1},
        ).sort("instrument", 1)
    )
    return [_serialize_instrument_spot_token(doc) for doc in docs]


def _get_simulator_default_quote_tokens(broker_id: str = "") -> list[str]:
    return [
        str(item.get("token") or "").strip()
        for item in _get_instrument_spot_token_docs(broker_id)
        if str(item.get("token") or "").strip()
    ]


# Plain helper (not a route here — the real /simulator/paper-trade/quotes
# endpoint lives in algo.simulator) kept only because scanner/router.py's
# scanner_quotes() does `from api import simulator_pt_quotes` directly,
# reusing this same quote-resolution logic for scanner's own /scanner/quotes.
async def simulator_pt_quotes(
    tokens: str = "", broker_id: str = Query(default=""), include_index_defaults: bool = True,
) -> dict:
    """
    Reuses the same canonical sources every other simulator page already
    gets correct numbers from (see features/simulator_risk_monitor.py and
    /live-greeks-chain for the same consolidation):
      - index/spot tokens  -> Dhan's own index ids, same Dhan REST/WS path
                              as the FNO legs below (features.broker_gateway.
                              get_broker_rest_quotes, segment "IDX_I")
      - FNO option tokens  -> features.broker_gateway.get_broker_rest_quotes
    """
    from features.broker_gateway import (
        _KITE_INDEX_TOKENS, _DHAN_INDEX_TOKENS,
        _active_broker as _get_active_broker_name,
        get_broker_rest_quotes,
    )
    from features.execution_socket import _fetch_dhan_index_quotes

    index_underlying_by_token: dict[str, str] = {}
    for underlying, tok in _KITE_INDEX_TOKENS.items():
        index_underlying_by_token[str(tok)] = underlying
    for underlying, tok in _DHAN_INDEX_TOKENS.items():
        index_underlying_by_token[str(tok)] = underlying

    requested_tokens = [str(token).strip() for token in str(tokens or "").split(",") if str(token).strip()]
    default_tokens = _get_simulator_default_quote_tokens(str(broker_id or "").strip()) if include_index_defaults else []
    unique_tokens = list(dict.fromkeys(requested_tokens + [token for token in default_tokens if token]))
    if not unique_tokens:
        return {"status": "success", "quotes": {}}

    quotes: dict[str, dict[str, float | str]] = {}
    active_broker = _get_active_broker_name()
    db = MongoData()
    try:
        index_tokens = [t for t in unique_tokens if t in index_underlying_by_token]
        fno_tokens = [t for t in unique_tokens if t not in index_underlying_by_token]

        if index_tokens and active_broker == "dhan":
            dhan_id_by_frontend_token: dict[str, str] = {}
            for token in index_tokens:
                underlying = index_underlying_by_token[token]
                dhan_id = str(_DHAN_INDEX_TOKENS.get(underlying) or "").strip()
                if dhan_id:
                    dhan_id_by_frontend_token[token] = dhan_id
            unresolved_index_tokens = [t for t in index_tokens if t not in dhan_id_by_frontend_token]
        else:
            dhan_id_by_frontend_token = {}
            unresolved_index_tokens = list(index_tokens)

        if unresolved_index_tokens:
            underlyings = {index_underlying_by_token[t] for t in unresolved_index_tokens}
            try:
                index_quotes = await asyncio.to_thread(_fetch_dhan_index_quotes, db, underlyings)
            except Exception as exc:
                log.warning("paper trade index quote error underlyings=%s: %s", underlyings, exc)
                index_quotes = {}
            for token in unresolved_index_tokens:
                underlying = index_underlying_by_token[token]
                spot = float((index_quotes.get(underlying) or {}).get("spot_price") or 0.0)
                if spot > 0:
                    quotes[token] = {"token": token, "ltp": round(spot, 2), "source": "index_quote"}

        dhan_equity_id_by_frontend_token = (
            _resolve_dhan_equity_ids_by_kite_tokens(fno_tokens, db._db) if fno_tokens and active_broker == "dhan" else {}
        )
        pure_fno_tokens = [t for t in fno_tokens if t not in dhan_equity_id_by_frontend_token]

        _dhan_reserved_index_ids = {str(v) for v in _DHAN_INDEX_TOKENS.values()}
        colliding_equity_id_by_frontend_token = {
            frontend_token: dhan_id
            for frontend_token, dhan_id in dhan_equity_id_by_frontend_token.items()
            if dhan_id in _dhan_reserved_index_ids
        }
        safe_equity_id_by_frontend_token = {
            frontend_token: dhan_id
            for frontend_token, dhan_id in dhan_equity_id_by_frontend_token.items()
            if frontend_token not in colliding_equity_id_by_frontend_token
        }

        if (pure_fno_tokens or dhan_id_by_frontend_token or safe_equity_id_by_frontend_token) and active_broker == "dhan":
            segment_by_token = {
                str(row.get("token") or row.get("tokens") or "").strip(): str(row.get("ws_segment") or "NSE_FNO").strip().upper()
                for row in db._db["active_option_tokens"].find(
                    {"broker": "dhan", "token": {"$in": pure_fno_tokens}},
                    {"_id": 0, "token": 1, "tokens": 1, "ws_segment": 1},
                )
            } if pure_fno_tokens else {}
            for dhan_id in dhan_id_by_frontend_token.values():
                segment_by_token[dhan_id] = "IDX_I"
            for dhan_id in safe_equity_id_by_frontend_token.values():
                segment_by_token[dhan_id] = "NSE_EQ"
            combined_tokens = list(dict.fromkeys(
                pure_fno_tokens + list(dhan_id_by_frontend_token.values()) + list(safe_equity_id_by_frontend_token.values())
            ))
            try:
                rest_quotes = await asyncio.to_thread(get_broker_rest_quotes, combined_tokens, db._db, segment_by_token)
            except Exception as exc:
                log.warning("paper trade quote error tokens=%s: %s", ",".join(combined_tokens), exc)
                rest_quotes = {}
            for token, info in rest_quotes.items():
                ltp = float((info or {}).get("ltp") or 0)
                if ltp > 0 and token in unique_tokens:
                    quotes[token] = {"token": token, "ltp": round(ltp, 2), "source": "ws_or_rest"}
            for frontend_token, dhan_id in dhan_id_by_frontend_token.items():
                info = rest_quotes.get(dhan_id)
                ltp = float((info or {}).get("ltp") or 0)
                if ltp > 0:
                    quotes[frontend_token] = {"token": frontend_token, "ltp": round(ltp, 2), "source": "index_quote"}
            for frontend_token, dhan_id in safe_equity_id_by_frontend_token.items():
                info = rest_quotes.get(dhan_id)
                ltp = float((info or {}).get("ltp") or 0)
                if ltp > 0:
                    quotes[frontend_token] = {"token": frontend_token, "ltp": round(ltp, 2), "source": "equity_quote"}

        if colliding_equity_id_by_frontend_token and active_broker == "dhan":
            try:
                cfg = db._db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
                access_token = str(cfg.get("access_token") or "").strip()
                client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
                if access_token and client_id:
                    from features.broker_gateway import dhan_quote_post_blocking
                    sec_ids = [int(dhan_id) for dhan_id in colliding_equity_id_by_frontend_token.values()]
                    response = await asyncio.to_thread(
                        dhan_quote_post_blocking, {"NSE_EQ": sec_ids}, access_token, client_id, 5.0,
                    )
                    payload = response.json() if response is not None and response.ok else {}
                    eq_data = (payload.get("data") or {}).get("NSE_EQ") or payload.get("NSE_EQ") or {}
                    for frontend_token, dhan_id in colliding_equity_id_by_frontend_token.items():
                        info = eq_data.get(str(dhan_id)) if isinstance(eq_data, dict) else None
                        ltp = float((info or {}).get("last_price") or 0) if isinstance(info, dict) else 0.0
                        if ltp > 0:
                            _LAST_GOOD_EQUITY_COLLISION_QUOTE[frontend_token] = ltp
                            quotes[frontend_token] = {"token": frontend_token, "ltp": round(ltp, 2), "source": "equity_quote"}
            except Exception as exc:
                log.warning(
                    "scanner equity index-id-collision quote error tokens=%s: %s",
                    list(colliding_equity_id_by_frontend_token), exc,
                )
            for frontend_token in colliding_equity_id_by_frontend_token:
                if frontend_token not in quotes or not quotes[frontend_token].get("ltp"):
                    cached_ltp = _LAST_GOOD_EQUITY_COLLISION_QUOTE.get(frontend_token)
                    if cached_ltp:
                        quotes[frontend_token] = {"token": frontend_token, "ltp": round(cached_ltp, 2), "source": "equity_quote"}
        if fno_tokens and active_broker != "dhan":
            try:
                if is_configured():
                    api_key, access_token = get_common_credentials()
                    if api_key and access_token:
                        def _kite_quote_call() -> dict:
                            try:
                                kite = get_kite_instance(access_token)
                                return kite.quote([int(token) for token in fno_tokens]) or {}
                            except Exception:
                                return {}
                        quote_docs = await asyncio.to_thread(_kite_quote_call)
                        for quote_key, quote_doc in quote_docs.items():
                            resolved_token = str(
                                quote_doc.get("instrument_token")
                                or quote_key.split(":")[-1]
                                or ""
                            ).strip()
                            if not resolved_token:
                                continue
                            quote_ltp = float(
                                quote_doc.get("last_price")
                                or (quote_doc.get("ohlc") or {}).get("close")
                                or 0.0
                            )
                            if quote_ltp > 0:
                                quotes[resolved_token] = {
                                    "token": resolved_token,
                                    "ltp": round(quote_ltp, 2),
                                    "source": "quote",
                                }
            except Exception as exc:
                log.warning("paper trade quote batch error tokens=%s: %s", ",".join(fno_tokens), exc)
    finally:
        db.close()

    for token in unique_tokens:
        quotes.setdefault(token, {"token": token, "ltp": 0.0, "source": "unavailable"})

    return {"status": "success", "quotes": quotes}


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def _setup_logging():
    from features.app_logger import setup_logging
    setup_logging()
    try:
        MongoData().ensure_core_indexes()
    except Exception:
        log.exception("Failed to ensure MongoDB indexes at startup")
    try:
        _refresh_active_option_chain_cache()
    except Exception:
        log.exception("Failed to preload active option chain cache at startup")


@app.on_event("startup")
async def _auto_start_ticker():
    """Auto-start the broker WebSocket ticker on server startup (for live spot price / VIX)."""
    import asyncio, threading
    async def _bg():
        await asyncio.sleep(5)  # wait for server to fully initialise
        try:
            if ticker_manager.status not in ("running", "connecting"):
                threading.Thread(target=_start_ticker_bg, daemon=True).start()
                log.info("[STARTUP] Broker ticker auto-started.")
        except Exception:
            log.exception("[STARTUP] Broker ticker auto-start failed.")
    asyncio.create_task(_bg())


@app.on_event("startup")
async def _collectors_market_hours_schedule():
    """
    Auto-stop the Live Option Chain Collector and Live Option Chain Snapshot
    (the "day snapshot" stock/option-chain data fetchers) after market
    close, auto-start both again ~09:10 next weekday — see
    features/market_hours_scheduler.py. Their own /live-collector/{start,
    stop} and /live-chain-snapshot/{start,stop} endpoints remain available
    as a manual override at any time.
    """
    asyncio.create_task(run_market_hours_scheduler(
        name="live-option-chain-collector",
        start_fn=_live_option_chain_collector.start,
        stop_fn=_live_option_chain_collector.stop,
        is_running_fn=_live_option_chain_collector.status,
    ))
    asyncio.create_task(run_market_hours_scheduler(
        name="live-chain-snapshot",
        start_fn=_live_chain_snapshot_collector.start,
        stop_fn=_live_chain_snapshot_collector.stop,
        is_running_fn=_live_chain_snapshot_collector.status,
    ))


_SCANNER_SNAPSHOT_IST = timezone(timedelta(hours=5, minutes=30))
_SCANNER_SNAPSHOT_HOUR_IST = 15  # 15:45 IST — 15 min after NSE close, settlement tick has landed by then
_SCANNER_SNAPSHOT_MINUTE_IST = 45


@app.on_event("startup")
async def _auto_daily_scanner_snapshot():
    """
    Was previously a manual-only endpoint (/scanner/sync_daily_market_snapshot) —
    nobody hitting it for a few days silently leaves scanner_stock_historical_data's
    previous-close stuck on whatever day it was last run, which makes every stock's
    change_pct/change_points (see execution_socket._equity_previous_close, used by
    /simulator/positions/all and /simulator/paper-trade/underlying-quotes) compare
    against a stale multi-day-old close instead of yesterday's — silently wrong
    instead of obviously wrong, unlike the index path's 0.00% when pre-market.
    Runs once per trading day, shortly after NSE close, forever for the life of
    this process; weekends/holidays are skipped via the same market_holidays
    collection _previous_session_close() already checks.
    """
    import asyncio

    async def _loop():
        from scanner.service import sync_scanner_daily_market_snapshot
        from features.mongo_data import MongoData

        while True:
            now = datetime.now(_SCANNER_SNAPSHOT_IST)
            target = now.replace(hour=_SCANNER_SNAPSHOT_HOUR_IST, minute=_SCANNER_SNAPSHOT_MINUTE_IST, second=0, microsecond=0)
            if target <= now:
                target += timedelta(days=1)

            db = MongoData()
            try:
                holidays_col = db._db['market_holidays']
                while target.weekday() >= 5 or holidays_col.find_one({'date': target.strftime('%Y-%m-%d')}):
                    target += timedelta(days=1)
            except Exception:
                log.exception("[SCANNER SNAPSHOT] holiday lookup failed — keeping originally computed run time")
            finally:
                db.close()

            await asyncio.sleep(max(1.0, (target - datetime.now(_SCANNER_SNAPSHOT_IST)).total_seconds()))
            try:
                result = await asyncio.to_thread(sync_scanner_daily_market_snapshot)
                log.info("[SCANNER SNAPSHOT] auto run completed: %s", result)
            except Exception:
                log.exception("[SCANNER SNAPSHOT] auto run failed")
            else:
                try:
                    from export_historical_parquet import export_all
                    parquet_result = await asyncio.to_thread(export_all)
                    log.info("[SCANNER SNAPSHOT] parquet re-export completed: %s", parquet_result)
                except Exception:
                    log.exception("[SCANNER SNAPSHOT] parquet re-export failed")
            # Past-due guard: a slow/failed run above shouldn't make the next
            # target-time computation immediately re-fire for the same day.
            await asyncio.sleep(60)

    asyncio.create_task(_loop())


@app.on_event("startup")
async def _auto_start_alert_checker():
    """Continuously evaluate chart price/trendline alerts (tv_chart_state)
    against live spot price (option_chain_index_spot) and fire their
    webhooks — runs for the life of this process regardless of whether any
    browser tab with the chart open is still around. See
    features/alert_checker.py for the actual crossing logic, ported from
    algo-admin's Chart.tsx so server-side and client-side evaluation agree."""
    from features.alert_checker import start_alert_checker_monitor
    start_alert_checker_monitor()



# Indicator-condition alerts (Supertrend/MACD/MA Cross/RSI/Stochastic) are
# deliberately NOT auto-started here, unlike the price/trendline loop above
# — they're controlled on demand via the monitor page/endpoints at
# /signal/indicator-alert-monitor/{start,stop,status} (signal_builder/
# router.py), the same manual start/stop pattern simulator/api_server.py's
# /monitor/{start,stop,status} already uses for the Simulator Monitor. See
# features/alert_checker.py's start_indicator_alert_monitor/
# stop_indicator_alert_monitor.


@app.on_event("startup")
async def _span_params_startup():
    """Seed SPAN defaults to DB (if empty) and load into memory cache."""
    import asyncio
    async def _bg():
        await asyncio.sleep(3)
        try:
            from features.span_file import save_defaults_to_db, fetch_span_file
            await asyncio.to_thread(save_defaults_to_db)   # seed DB if empty
            await asyncio.to_thread(fetch_span_file)       # load DB + any local files
        except Exception:
            log.exception("SPAN params startup failed — hardcoded defaults will be used")
    asyncio.create_task(_bg())


@app.on_event("startup")
async def _redis_prewarm():
    """
    On server startup: if REDIS_MEMORY=True, push all cached pkl5 files to Redis
    in a background thread so the first backtest hits Redis instead of disk.
    Only runs if Redis is reachable and pkl5 cache exists.
    """
    from features.backtest_engine import REDIS_MEMORY, _cache_dir, _pkl5_path, _get_redis, DataIndex
    if not REDIS_MEMORY:
        return
    import threading, pickle, pathlib

    def _warm():
        try:
            r = _get_redis()
        except Exception as e:
            print(f"[prewarm] Redis not available: {e}")
            return

        loaded = 0
        skipped = 0
        base = pathlib.Path.home() / ".backtest_cache"
        for underlying_dir in base.iterdir():
            if not underlying_dir.is_dir():
                continue
            underlying = underlying_dir.name
            for pkl5 in sorted(underlying_dir.glob("*.pkl5")):
                date = pkl5.stem
                key  = f"di:{underlying}:{date}"
                if r.exists(key):
                    skipped += 1
                    continue
                try:
                    with open(pkl5, 'rb') as f:
                        data = pickle.load(f)
                    r.set(key, pickle.dumps(data, protocol=5))
                    loaded += 1
                except Exception:
                    pass

        total = loaded + skipped
        print(f"[prewarm] Redis ready: {total} days ({loaded} loaded, {skipped} already cached)")

    threading.Thread(target=_warm, daemon=True).start()


# ─── Endpoints ────────────────────────────────────────────────────────────────



# ─── App user auth (mobile + password, JWT) ──────────────────────────────────











# ── Blocking endpoints (existing behaviour) ───────────────────────────────────





# ── Background job endpoints (with progress) ──────────────────────────────────





















def _extract_indicator_minutes(node):
    if isinstance(node, list):
        for item in node:
            minutes = _extract_indicator_minutes(item)
            if minutes is not None:
                return minutes
        return None
    if not isinstance(node, dict):
        return None
    value = node.get("Value")
    if isinstance(value, dict) and value.get("IndicatorName") == "IndicatorType.TimeIndicator":
        params = value.get("Parameters") or {}
        try:
            hour = int(params.get("Hour", 0))
            minute = int(params.get("Minute", 0))
            return hour * 60 + minute
        except Exception:
            return None
    if isinstance(value, list):
        nested = _extract_indicator_minutes(value)
        if nested is not None:
            return nested
    children = node.get("children") or node.get("Children")
    if isinstance(children, list):
        return _extract_indicator_minutes(children)
    return None


def _normalize_leg_instrument(option_value, instrument_kind):
    option = str(option_value or "").strip()
    if option.startswith("LegType."):
        return option
    if option in {"CE", "PE", "FUT"}:
        return f"LegType.{option}"
    instrument = str(instrument_kind or "").strip()
    if instrument.startswith("LegType."):
        return instrument
    if instrument in {"CE", "PE", "FUT"}:
        return f"LegType.{instrument}"
    return "LegType.CE"


def _normalize_weekdays_map(values):
    normalized = {
        "monday": False,
        "tuesday": False,
        "wednesday": False,
        "thursday": False,
        "friday": False,
        "saturday": False,
        "sunday": False,
    }
    mapping = {
        "m": "monday",
        "monday": "monday",
        "t": "tuesday",
        "tuesday": "tuesday",
        "w": "wednesday",
        "wednesday": "wednesday",
        "th": "thursday",
        "thu": "thursday",
        "thursday": "thursday",
        "f": "friday",
        "friday": "friday",
        "sat": "saturday",
        "saturday": "saturday",
        "sun": "sunday",
        "sunday": "sunday",
    }
    for value in values if isinstance(values, list) else []:
        key = mapping.get(str(value or "").strip().lower())
        if key:
            normalized[key] = True
    return normalized


def _default_leg_execution_config():
    return {
        "ProductType": "ProductType.NRML",
        "ExitOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {
                    "ModificationFrequency": 5,
                    "ContinuousMonitoring": "True",
                    "MarketOrderAfter": 1,
                },
            },
        },
        "EntryOrder": {
            "Type": "OrderType.Limit",
            "Value": {
                "Buffer": {
                    "Type": "BufferType.Points",
                    "Value": {"TriggerBuffer": 0, "LimitBuffer": 3},
                },
                "Modification": {"MarketOrderAfter": 40},
            },
        },
        "ReferenceForTgtSL": "PriceReferenceType.Trigger",
        "EntryDelay": 0,
    }


def _build_execution_cache(strategy_detail: dict, strategy_state: dict):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    ticker = detail.get("underlying") or strategy.get("Ticker") or strategy_state.get("ticker") or "NIFTY"

    lot_config = []
    expiries = []
    instruments = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        lot = leg.get("LotConfig") if isinstance(leg.get("LotConfig"), dict) else {}
        contract = leg.get("ContractType") if isinstance(leg.get("ContractType"), dict) else {}
        lot_config.append({
            "type": lot.get("Type") or "LotType.Quantity",
            "value": int(lot.get("Value", 1) or 1),
        })
        expiries.append(contract.get("Expiry") or "ExpiryType.Weekly")
        instruments.append(_normalize_leg_instrument(contract.get("Option"), contract.get("InstrumentKind")))

    return {
        "execution_version": "v2",
        "entry_time": _extract_indicator_minutes(strategy.get("EntryIndicators")),
        "exit_time": _extract_indicator_minutes(strategy.get("ExitIndicators")),
        "num_original_legs": len(legs),
        "lot_config": lot_config,
        "expiries": expiries,
        "instruments": instruments,
        "ticker": ticker,
        "strategy_type": strategy.get("StrategyType") or "StrategyType.IntradaySameDay",
        "reentry_restriction": strategy.get("ReentryTimeRestriction"),
    }


def _build_strategy_execution_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []

    execution_config_base = detail.get("execution_config_base") if isinstance(detail.get("execution_config_base"), dict) else {}
    if not execution_config_base:
        execution_config_base = {
            "Multiplier": int(strategy_state.get("qty_multiplier") or 1),
            "LikeBacktester": activation_mode != "live",
            "MarginAutoSquareOff": True,
            "TimeDelta": 0,
        }
    else:
        execution_config_base = dict(execution_config_base)
        execution_config_base.setdefault("Multiplier", int(strategy_state.get("qty_multiplier") or 1))
        execution_config_base.setdefault("LikeBacktester", activation_mode != "live")
        execution_config_base.setdefault("MarginAutoSquareOff", True)
        execution_config_base.setdefault("TimeDelta", 0)

    execution_config_extra = detail.get("execution_config_extra") if isinstance(detail.get("execution_config_extra"), dict) else {}
    if not execution_config_extra or not isinstance(execution_config_extra.get("ListOfLegExecutionConfig"), list):
        execution_config_extra = {
            "ListOfLegExecutionConfig": [_default_leg_execution_config() for _ in legs]
        }
    else:
        execution_config_extra = dict(execution_config_extra)

    return {
        "execution_config_base": execution_config_base,
        "execution_config_extra": execution_config_extra,
        "is_weekdays": bool(strategy_state.get("is_weekdays", True)),
        "dte": strategy_state.get("dte") if isinstance(strategy_state.get("dte"), list) else [],
        "weekdays": _normalize_weekdays_map(strategy_state.get("weekdays") if isinstance(strategy_state.get("weekdays"), list) else []),
        "view_config": detail.get("view_config") if isinstance(detail.get("view_config"), dict) else {"advanced_exec_config_modal": True},
    }


def _normalize_execution_settings_payload(source_detail: dict, payload: dict, activation_mode: str):
    detail = _clone_json_value(source_detail) if isinstance(source_detail, dict) else {}
    incoming = payload if isinstance(payload, dict) else {}

    if isinstance(incoming.get("execution_config_base"), dict):
        detail["execution_config_base"] = _clone_json_value(incoming.get("execution_config_base"))
    if isinstance(incoming.get("execution_config_extra"), dict):
        detail["execution_config_extra"] = _clone_json_value(incoming.get("execution_config_extra"))
    if isinstance(incoming.get("view_config"), dict):
        detail["view_config"] = _clone_json_value(incoming.get("view_config"))

    normalized = _build_strategy_execution_config(
        detail,
        {
            "qty_multiplier": ((incoming.get("execution_config_base") or {}).get("Multiplier") if isinstance(incoming.get("execution_config_base"), dict) else 1) or 1,
            "is_weekdays": incoming.get("is_weekdays", True),
            "dte": incoming.get("dte") if isinstance(incoming.get("dte"), list) else [],
            "weekdays": list((incoming.get("weekdays") or {}).keys()) if isinstance(incoming.get("weekdays"), dict) else [],
        },
        activation_mode,
    )

    if isinstance(incoming.get("weekdays"), dict):
        normalized["weekdays"] = {
            "friday": bool(incoming["weekdays"].get("friday")),
            "monday": bool(incoming["weekdays"].get("monday")),
            "saturday": bool(incoming["weekdays"].get("saturday")),
            "sunday": bool(incoming["weekdays"].get("sunday")),
            "thursday": bool(incoming["weekdays"].get("thursday")),
            "tuesday": bool(incoming["weekdays"].get("tuesday")),
            "wednesday": bool(incoming["weekdays"].get("wednesday")),
        }
    normalized["is_weekdays"] = bool(incoming.get("is_weekdays", normalized.get("is_weekdays", True)))
    normalized["dte"] = incoming.get("dte") if isinstance(incoming.get("dte"), list) else normalized.get("dte", [])
    normalized["view_config"] = incoming.get("view_config") if isinstance(incoming.get("view_config"), dict) else normalized.get("view_config", {"advanced_exec_config_modal": True})
    return normalized


def _clone_json_value(value):
    return deepcopy(value)


def _normalize_optional_config(config):
    if not isinstance(config, dict):
        return None
    normalized = _clone_json_value(config)
    config_type = str(normalized.get("Type") or "").strip()
    if not config_type or config_type == "None":
        return None
    return normalized


def _normalize_reentry_value(config):
    if not isinstance(config, dict):
        return None
    reentry_type = str(config.get("Type") or "").strip()
    if not reentry_type or reentry_type == "None":
        return None

    raw_value = config.get("Value")
    normalized_value = raw_value
    if isinstance(raw_value, dict):
        if "NextLegRef" in raw_value:
            normalized_value = raw_value.get("NextLegRef")
        elif "ReentryCount" in raw_value:
            normalized_value = raw_value.get("ReentryCount")
        elif len(raw_value) == 1:
            normalized_value = next(iter(raw_value.values()))

    return {
        "Type": reentry_type,
        "Value": normalized_value,
    }


def _normalize_option_kind(instrument_kind: str):
    value = str(instrument_kind or "").upper()
    if "PE" in value:
        return "PE"
    return "CE"


def _normalize_contract_strike(value):
    if isinstance(value, (int, float)):
        return value
    raw_value = str(value or "").strip()
    if not raw_value:
        return 0
    if raw_value == "StrikeType.ATM":
        return 0
    numeric_match = re.fullmatch(r"-?\d+(?:\.\d+)?", raw_value)
    if numeric_match:
        parsed = float(raw_value)
        return int(parsed) if parsed.is_integer() else parsed
    return raw_value


def _build_algo_leg_config_entry(leg_config: dict):
    leg = leg_config if isinstance(leg_config, dict) else {}
    stop_loss = _normalize_optional_config(leg.get("LegStopLoss"))
    target = _normalize_optional_config(leg.get("LegTarget"))
    trail = _normalize_optional_config(leg.get("LegTrailSL"))
    momentum = _normalize_optional_config(leg.get("LegMomentum"))
    stop_reentry = _normalize_reentry_value(leg.get("LegReentrySL"))
    target_reentry = _normalize_reentry_value(leg.get("LegReentryTP"))

    if stop_loss and stop_reentry:
        stop_loss["Reentry"] = stop_reentry
    if stop_loss and trail:
        stop_loss["Trail"] = trail
    if target and target_reentry:
        target["Reentry"] = target_reentry

    return {
        "PositionType": leg.get("PositionType") or "PositionType.Sell",
        "ContractType": {
            "Option": _normalize_option_kind(leg.get("InstrumentKind")),
            "Expiry": leg.get("ExpiryKind") or "ExpiryType.Weekly",
            "InstrumentKind": "OPT",
            "StrikeParameter": _normalize_contract_strike(leg.get("StrikeParameter")),
            "EntryKind": leg.get("EntryType") or "EntryType.EntryByStrikeType",
        },
        "LotConfig": _clone_json_value(leg.get("LotConfig")) if isinstance(leg.get("LotConfig"), dict) else {
            "Type": "LotType.Quantity",
            "Value": 1,
        },
        "LegMomentum": momentum,
        "LegTarget": target,
        "LegStopLoss": stop_loss,
    }


def _build_algo_execution_leg_entry(leg_execution_config: dict):
    config = leg_execution_config if isinstance(leg_execution_config, dict) else {}
    entry_order = config.get("EntryOrder") if isinstance(config.get("EntryOrder"), dict) else {}
    exit_order = config.get("ExitOrder") if isinstance(config.get("ExitOrder"), dict) else {}

    entry_order_config = _clone_json_value(entry_order.get("Config")) if isinstance(entry_order.get("Config"), dict) else _clone_json_value(entry_order)
    exit_order_config = _clone_json_value(exit_order.get("Config")) if isinstance(exit_order.get("Config"), dict) else _clone_json_value(exit_order)
    if not entry_order_config:
        entry_order_config = {"Type": "OrderType.Market"}
    if not exit_order_config:
        exit_order_config = {"Type": "OrderType.Market"}

    return {
        "Product": config.get("Product") or config.get("ProductType") or "ProductType.NRML",
        "Reference": config.get("Reference") or config.get("ReferenceForTgtSL") or "PriceReferenceType.Trigger",
        "EntryOrder": {
            "Config": entry_order_config,
            "Delay": int(config.get("EntryDelay") or entry_order.get("Delay") or 0),
        },
        "ExitOrder": {
            "Config": exit_order_config,
            "Delay": int(config.get("ExitDelay") or exit_order.get("Delay") or 0),
        },
    }


def _build_algo_trade_config(strategy_detail: dict, strategy_state: dict, activation_mode: str):
    detail = strategy_detail if isinstance(strategy_detail, dict) else {}
    full_config = detail.get("full_config") if isinstance(detail.get("full_config"), dict) else {}
    strategy = full_config.get("strategy") if isinstance(full_config.get("strategy"), dict) else {}
    if not strategy:
        return None

    parent_legs = strategy.get("ListOfLegConfigs") if isinstance(strategy.get("ListOfLegConfigs"), list) else []
    idle_legs = strategy.get("IdleLegConfigs") if isinstance(strategy.get("IdleLegConfigs"), dict) else {}
    execution_config = _build_strategy_execution_config(detail, strategy_state, activation_mode)
    execution_base = execution_config.get("execution_config_base") if isinstance(execution_config.get("execution_config_base"), dict) else {}
    execution_extra = execution_config.get("execution_config_extra") if isinstance(execution_config.get("execution_config_extra"), dict) else {}
    execution_leg_configs = execution_extra.get("ListOfLegExecutionConfig") if isinstance(execution_extra.get("ListOfLegExecutionConfig"), list) else []

    keyed_leg_configs = {}
    keyed_execution_legs = {}
    for index, leg in enumerate(parent_legs, start=1):
        leg_key = f"og_leg_{index}"
        keyed_leg_configs[leg_key] = _build_algo_leg_config_entry(leg)
        keyed_execution_legs[leg_key] = _build_algo_execution_leg_entry(
            execution_leg_configs[index - 1] if index - 1 < len(execution_leg_configs) else {}
        )

    normalized_idle_legs = {}
    for idle_key, idle_leg in idle_legs.items():
        normalized_idle_legs[str(idle_key)] = _build_algo_leg_config_entry(idle_leg)

    return {
        "ExecutionConfig": {
            "LikeBacktester": bool(execution_base.get("LikeBacktester", activation_mode != "live")),
            "MarginAutoSquareOff": bool(execution_base.get("MarginAutoSquareOff", True)),
            "LotMultiplier": int(execution_base.get("Multiplier") or strategy_state.get("qty_multiplier") or 1),
            "LegsConfig": keyed_execution_legs,
        },
        "Ticker": strategy.get("Ticker") or detail.get("underlying") or strategy_state.get("ticker") or "NIFTY",
        "TakeUnderlyingFromCash": str(strategy.get("TakeUnderlyingFromCashOrNot") or "True").lower() == "true",
        "TrailSLtoBreakeven": _normalize_optional_config(strategy.get("TrailSLtoBreakeven")),
        "SquareOffAllLegs": str(strategy.get("SquareOffAllLegs") or "False").lower() == "true",
        "LegConfigs": keyed_leg_configs,
        "IdleLegConfigs": normalized_idle_legs,
        "OverallSL": _normalize_optional_config(strategy.get("OverallSL")),
        "OverallTgt": _normalize_optional_config(strategy.get("OverallTgt")),
        "LockAndTrail": _normalize_optional_config(strategy.get("LockAndTrail")),
        "OverallTrailSL": _normalize_optional_config(strategy.get("OverallTrailSL")),
        "OverallReentrySL": _normalize_optional_config(strategy.get("OverallReentrySL")),
        "OverallReentryTgt": _normalize_optional_config(strategy.get("OverallReentryTgt")),
        "OverallMomentum": _normalize_optional_config(strategy.get("OverallMomentum")),
    }










def _calculate_margin_sync(body: dict) -> dict:
    """Run all blocking DB + CPU work in a thread — keeps the async event loop free."""
    from features.span_margin import calculate_margin, SpanPosition

    legs_raw = body.get("legs", [])
    positions = []
    resolved_legs: list[dict[str, Any]] = []
    broker_margin: dict[str, Any] | None = None
    db = MongoData()
    try:
        try:
            load_credentials_from_db(db)
        except Exception:
            log.exception("Failed to load Kite credentials for margin calculation")

        for leg in legs_raw:
            underlying = str(leg.get("underlying", "NIFTY")).upper().strip()
            instrument_type = str(leg.get("instrument_type", "CE")).upper().strip()
            expiry = str(leg.get("expiry", "")).strip()
            strike = float(leg.get("strike", 0) or 0)
            transaction_type = str(leg.get("transaction_type", "SELL")).upper().strip()
            quantity = int(leg.get("quantity", 1))
            lot_size = int(leg.get("lot_size", 1))
            ltp = float(leg.get("ltp", 0) or 0)
            spot = float(leg.get("spot", 0) or 0)

            if spot <= 0:
                spot_doc = get_cached_spot_doc(db._db, underlying)
                spot = float(
                    (spot_doc or {}).get("spot_price")
                    or (spot_doc or {}).get("ltp")
                    or (spot_doc or {}).get("close")
                    or 0.0
                )

            if instrument_type in {"CE", "PE"} and ltp <= 0:
                ltp = _resolve_single_option_ltp(
                    db._db, underlying, expiry, strike, instrument_type,
                )
            elif instrument_type == "FUT" and ltp <= 0:
                ltp = spot

            positions.append(SpanPosition(
                underlying=underlying, instrument_type=instrument_type,
                expiry=expiry, strike=strike, transaction_type=transaction_type,
                quantity=quantity, lot_size=lot_size, ltp=ltp, spot=spot,
            ))
            resolved_legs.append({
                "underlying": underlying, "instrument_type": instrument_type,
                "expiry": expiry, "strike": strike, "transaction_type": transaction_type,
                "quantity": quantity, "lot_size": lot_size, "ltp": ltp, "spot": spot,
            })

        use_broker_api = body.get("use_broker_api", True)
        if resolved_legs and use_broker_api:
            broker_margin = _calculate_kite_basket_margin(db._db, resolved_legs)
    finally:
        db.close()

    if not positions:
        return {"span_margin": 0, "exposure_margin": 0, "total_margin": 0, "premium_received": 0, "net_margin": 0, "legs": []}

    product = str(body.get("product", "NRML")).upper()
    broker  = str(body.get("broker",  "kite")).lower()
    result  = calculate_margin(positions, product=product, broker=broker)
    broker_final = (broker_margin or {}).get("final") or {}
    if isinstance(broker_final, dict) and broker_final:
        premium_received_display = 0.0
        for leg in resolved_legs:
            it = str(leg.get("instrument_type") or "").upper()
            if it not in {"CE", "PE"}:
                continue
            leg_premium_value = float(leg.get("ltp") or 0.0) * int(leg.get("quantity") or 0) * int(leg.get("lot_size") or 0)
            if str(leg.get("transaction_type") or "").upper() == "SELL":
                premium_received_display += leg_premium_value
            else:
                premium_received_display -= leg_premium_value
        return {
            "span_margin": float(broker_final.get("span") or 0.0),
            "exposure_margin": float(broker_final.get("exposure") or 0.0),
            "total_margin": float(broker_final.get("total") or 0.0),
            "premium_received": round(premium_received_display, 2),
            "net_margin": float(broker_final.get("total") or 0.0),
            "source": "kite_basket_order_margins",
            "broker_margin": broker_margin,
            "legs": [
                {"underlying": l.underlying, "instrument_type": l.instrument_type,
                 "expiry": l.expiry, "strike": l.strike, "transaction_type": l.transaction_type,
                 "quantity": l.quantity, "lot_size": l.lot_size, "ltp": l.ltp,
                 "span_contribution": l.span_contribution, "exposure_margin": l.exposure_margin,
                 "total_margin": l.total_margin, "implied_vol": l.implied_vol}
                for l in result.legs
            ],
        }
    return {
        "span_margin": result.span_margin, "exposure_margin": result.exposure_margin,
        "total_margin": result.total_margin, "premium_received": result.premium_received,
        "net_margin": result.net_margin, "source": "local_span_engine",
        "legs": [
            {"underlying": l.underlying, "instrument_type": l.instrument_type,
             "expiry": l.expiry, "strike": l.strike, "transaction_type": l.transaction_type,
             "quantity": l.quantity, "lot_size": l.lot_size, "ltp": l.ltp,
             "span_contribution": l.span_contribution, "exposure_margin": l.exposure_margin,
             "total_margin": l.total_margin, "implied_vol": l.implied_vol}
            for l in result.legs
        ],
    }
















def _build_trade_history_payload(db_obj, raw_trade: dict, normalized_status: str):
    normalized_strategy_id = str(raw_trade.get("_id") or "").strip()
    trade_record = {
        "_id": normalized_strategy_id,
        "strategy_id": str(raw_trade.get("strategy_id") or ""),
        "source_strategy_id": str(raw_trade.get("source_strategy_id") or ""),
        "name": raw_trade.get("name") or "",
        "status": raw_trade.get("status") or "",
        "trade_status": raw_trade.get("trade_status"),
        "active_on_server": bool(raw_trade.get("active_on_server")),
        "activation_mode": raw_trade.get("activation_mode") or normalized_status,
        "trade_date": raw_trade.get("trade_date") or "",
        "broker": raw_trade.get("broker") or "",
        "user_id": raw_trade.get("user_id") or "",
        "ticker": raw_trade.get("ticker") or "",
        "creation_ts": raw_trade.get("creation_ts") or "",
        "last_activation_ts": raw_trade.get("last_activation_ts") or "",
        "entry_time": raw_trade.get("entry_time") or "",
        "exit_time": raw_trade.get("exit_time") or "",
        "portfolio": raw_trade.get("portfolio") if isinstance(raw_trade.get("portfolio"), dict) else {},
        "strategy": raw_trade.get("strategy") if isinstance(raw_trade.get("strategy"), dict) else {},
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
    }

    populated_records = _populate_history_legs(db_obj, [trade_record])
    populated_records = _attach_leg_feature_statuses(db_obj, populated_records)
    populated_records = _attach_broker_configuration_details(db_obj, populated_records)
    detailed_trade = _enrich_execution_record_with_pnl((populated_records or [trade_record])[0])

    legs = detailed_trade.get("legs") if isinstance(detailed_trade.get("legs"), list) else []
    pending_feature_legs = detailed_trade.get("pending_feature_legs") if isinstance(detailed_trade.get("pending_feature_legs"), list) else []

    trade_mtm = round(sum(float((leg or {}).get("pnl") or 0) for leg in legs if isinstance(leg, dict)), 2)
    open_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 1]
    closed_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 2]
    pending_legs = [leg for leg in legs if int((leg or {}).get("status") or 0) == 0]

    orders = []
    for doc in (
        db_obj["broker_orders"]
        .find({"trade_id": normalized_strategy_id})
        .sort("placed_at", -1)
        .limit(1000)
    ):
        doc["_id"] = str(doc.get("_id") or "")
        orders.append(doc)

    notifications = []
    feature_filters = [{"trade_id": normalized_strategy_id}]
    related_strategy_ids = {
        str(detailed_trade.get("strategy_id") or "").strip(),
        str(detailed_trade.get("source_strategy_id") or "").strip(),
    }
    related_strategy_ids.discard("")
    for related_id in related_strategy_ids:
        feature_filters.append({"strategy_id": related_id})

    for doc in (
        db_obj["algo_leg_feature_status"]
        .find({"$or": feature_filters})
        .sort("created_at", -1)
        .limit(1000)
    ):
        normalized_doc = dict(doc)
        normalized_doc["_id"] = str(doc.get("_id") or "")
        normalized_doc["type"] = str(doc.get("feature") or "").strip() or "feature_status"
        normalized_doc["event_type"] = normalized_doc["type"]
        normalized_doc["timestamp"] = (
            doc.get("triggered_at")
            or doc.get("updated_at")
            or doc.get("created_at")
            or ""
        )
        notifications.append(normalized_doc)

    notification_status = {}
    for item in notifications:
        event_type = str(item.get("event_type") or item.get("type") or "unknown").strip() or "unknown"
        notification_status[event_type] = notification_status.get(event_type, 0) + 1

    trade_notifications = []
    for doc in (
        db_obj["algo_trade_notification"]
        .find({"$or": feature_filters})
        .sort("timestamp", -1)
        .limit(1000)
    ):
        normalized_doc = dict(doc)
        normalized_doc["_id"] = str(doc.get("_id") or "")
        trade_notifications.append(normalized_doc)

    return {
        "success": True,
        "view_type": "strategy",
        "strategy_id": normalized_strategy_id,
        "group_id": str(((detailed_trade.get("portfolio") or {}).get("group_id")) or "").strip(),
        "activation_mode": str(detailed_trade.get("activation_mode") or normalized_status),
        "trade": detailed_trade,
        "summary": {
            "mtm": trade_mtm,
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": len(pending_legs),
            "broker_orders_count": len(orders),
            "notifications_count": len(notifications),
        },
        "legs": {
            "all": legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": pending_legs,
            "pending_feature_legs": pending_feature_legs,
        },
        "broker_orders": orders,
        "open_orders": [
            order for order in orders
            if str(order.get("status") or "").strip().upper() in {"OPEN", "PENDING", "TRIGGER PENDING"}
        ],
        "notifications": notifications,
        "notification_status": notification_status,
        "trade_notifications": trade_notifications,
        "execution_config_base": raw_trade.get("execution_config_base") if isinstance(raw_trade.get("execution_config_base"), dict) else {},
        "execution_config_extra": raw_trade.get("execution_config_extra") if isinstance(raw_trade.get("execution_config_extra"), dict) else {},
    }


def _aggregate_group_trade_history_payload(group_id: str, normalized_status: str, payloads: list[dict]):
    valid_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    if not valid_payloads:
        raise HTTPException(status_code=404, detail="Strategy trade history not found for this group_id")

    primary_payload = valid_payloads[0]
    primary_trade = primary_payload.get("trade") if isinstance(primary_payload.get("trade"), dict) else {}
    group_name = str(((primary_trade.get("portfolio") or {}).get("group_name")) or "").strip() or f"Group {group_id}"
    strategy_names = []
    tickers = set()
    broker_labels = set()
    user_id = ""
    all_legs = []
    open_legs = []
    closed_legs = []
    pending_legs = []
    pending_feature_legs = []
    broker_orders = []
    notifications = []
    trade_notifications = []
    strategy_execution_configs = []
    notification_status = {}
    total_mtm = 0.0

    for payload in valid_payloads:
        trade = payload.get("trade") if isinstance(payload.get("trade"), dict) else {}
        summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
        legs = payload.get("legs") if isinstance(payload.get("legs"), dict) else {}
        trade_name = str(trade.get("name") or "").strip()
        if trade_name:
            strategy_names.append(trade_name)
        ticker = str(trade.get("ticker") or trade.get("underlying") or "").strip()
        if ticker:
            tickers.add(ticker)
        broker_label = str(
            ((trade.get("broker_details") or {}).get("broker_name"))
            or ((trade.get("broker_details") or {}).get("display_name"))
            or trade.get("broker_label")
            or trade.get("broker")
            or ""
        ).strip()
        if broker_label:
            broker_labels.add(broker_label)
        if not user_id:
            user_id = str(trade.get("user_id") or "").strip()

        total_mtm += float(summary.get("mtm") or 0)
        all_legs.extend(legs.get("all") if isinstance(legs.get("all"), list) else [])
        open_legs.extend(legs.get("open") if isinstance(legs.get("open"), list) else [])
        closed_legs.extend(legs.get("closed") if isinstance(legs.get("closed"), list) else [])
        pending_legs.extend(legs.get("pending") if isinstance(legs.get("pending"), list) else [])
        pending_feature_legs.extend(legs.get("pending_feature_legs") if isinstance(legs.get("pending_feature_legs"), list) else [])
        broker_orders.extend(payload.get("broker_orders") if isinstance(payload.get("broker_orders"), list) else [])
        notifications.extend(payload.get("notifications") if isinstance(payload.get("notifications"), list) else [])
        trade_notifications.extend(payload.get("trade_notifications") if isinstance(payload.get("trade_notifications"), list) else [])

        for key, value in (payload.get("notification_status") or {}).items():
            normalized_key = str(key or "").strip() or "unknown"
            notification_status[normalized_key] = notification_status.get(normalized_key, 0) + int(value or 0)

        strategy_execution_configs.append({
            "strategy_id": str(trade.get("_id") or payload.get("strategy_id") or "").strip(),
            "name": str(trade.get("name") or "").strip(),
            "execution_config_base": payload.get("execution_config_base") if isinstance(payload.get("execution_config_base"), dict) else {},
            "execution_config_extra": payload.get("execution_config_extra") if isinstance(payload.get("execution_config_extra"), dict) else {},
        })

    broker_orders.sort(key=lambda item: str((item or {}).get("placed_at") or ""), reverse=True)
    notifications.sort(
        key=lambda item: str(
            (item or {}).get("timestamp")
            or (item or {}).get("triggered_at")
            or (item or {}).get("updated_at")
            or (item or {}).get("created_at")
            or ""
        ),
        reverse=True,
    )
    trade_notifications.sort(key=lambda item: str((item or {}).get("timestamp") or ""), reverse=True)

    strategy_count = len(valid_payloads)
    tickers_label = ", ".join(sorted(tickers)) if tickers else "Multiple"
    broker_label_text = ", ".join(sorted(broker_labels)) if broker_labels else (primary_trade.get("broker") or "-")
    trade = deepcopy(primary_trade)
    trade["_id"] = group_id
    trade["name"] = f"{group_name} ({strategy_count})"
    trade["ticker"] = tickers_label
    trade["user_id"] = user_id or str(trade.get("user_id") or "")
    trade["activation_mode"] = normalized_status
    trade["broker_label"] = broker_label_text
    portfolio_meta = trade.get("portfolio") if isinstance(trade.get("portfolio"), dict) else {}
    portfolio_meta["group_id"] = group_id
    portfolio_meta["group_name"] = group_name
    portfolio_meta["strategy_count"] = strategy_count
    trade["portfolio"] = portfolio_meta
    trade["strategy_names"] = strategy_names
    trade["status"] = trade.get("status") or "Group"

    return {
        "success": True,
        "view_type": "group",
        "group_id": group_id,
        "strategy_id": "",
        "activation_mode": normalized_status,
        "trade": trade,
        "summary": {
            "mtm": round(total_mtm, 2),
            "open_positions": len(open_legs),
            "closed_positions": len(closed_legs),
            "pending_positions": len(pending_legs),
            "broker_orders_count": len(broker_orders),
            "notifications_count": len(notifications),
            "strategy_count": strategy_count,
        },
        "legs": {
            "all": all_legs,
            "open": open_legs,
            "closed": closed_legs,
            "pending": pending_legs,
            "pending_feature_legs": pending_feature_legs,
        },
        "broker_orders": broker_orders[:1000],
        "open_orders": [
            order for order in broker_orders
            if str(order.get("status") or "").strip().upper() in {"OPEN", "PENDING", "TRIGGER PENDING"}
        ][:1000],
        "notifications": notifications[:1000],
        "notification_status": notification_status,
        "trade_notifications": trade_notifications[:1000],
        "execution_config_base": (strategy_execution_configs[0].get("execution_config_base") if strategy_execution_configs else {}),
        "execution_config_extra": (strategy_execution_configs[0].get("execution_config_extra") if strategy_execution_configs else {}),
        "strategy_execution_configs": strategy_execution_configs,
    }


def _aggregate_portfolio_trade_history_payload(portfolio_id: str, normalized_status: str, payloads: list[dict]):
    valid_payloads = [payload for payload in payloads if isinstance(payload, dict)]
    if not valid_payloads:
        raise HTTPException(status_code=404, detail="Strategy trade history not found for this portfolio")

    # Group individual strategy payloads by group_id
    groups_map: dict[str, list[dict]] = {}
    for payload in valid_payloads:
        gid = str(payload.get("group_id") or ((payload.get("trade") or {}).get("portfolio") or {}).get("group_id") or "").strip()
        if not gid:
            gid = "__no_group__"
        groups_map.setdefault(gid, []).append(payload)

    # Build per-group aggregations
    groups = []
    for gid, group_payloads in groups_map.items():
        group_agg = _aggregate_group_trade_history_payload(gid, normalized_status, group_payloads)
        group_agg["strategies"] = group_payloads
        groups.append(group_agg)

    # Sort groups by group_id for stable ordering
    groups.sort(key=lambda g: str(g.get("group_id") or ""))

    # Portfolio-level aggregation (sum of all strategies)
    portfolio_agg = _aggregate_group_trade_history_payload(portfolio_id, normalized_status, valid_payloads)
    trade = portfolio_agg.get("trade") if isinstance(portfolio_agg.get("trade"), dict) else {}
    portfolio_meta = trade.get("portfolio") if isinstance(trade.get("portfolio"), dict) else {}
    portfolio_name = str(portfolio_meta.get("group_name") or trade.get("name") or "").strip() or f"Portfolio {portfolio_id}"
    strategy_count = len(valid_payloads)
    group_count = len(groups)

    trade["_id"] = portfolio_id
    trade["name"] = f"{portfolio_name} ({strategy_count})"
    portfolio_meta["portfolio"] = portfolio_id
    trade["portfolio"] = portfolio_meta

    portfolio_agg["view_type"] = "portfolio"
    portfolio_agg["portfolio_id"] = portfolio_id
    portfolio_agg["group_id"] = str(portfolio_meta.get("group_id") or "").strip()
    portfolio_agg["strategy_id"] = ""
    portfolio_agg["trade"] = trade
    portfolio_agg["summary"]["group_count"] = group_count
    portfolio_agg["groups"] = groups
    portfolio_agg["strategies"] = valid_payloads
    return portfolio_agg
















# ─── Notification history ──────────────────────────────────────────────────────


















def _str_id(doc: dict | None) -> dict | None:
    if doc and "_id" in doc:
        doc["_id"] = str(doc["_id"])
    return doc





















# ── Simulator risk monitor (SL/Target/hedge auto-exit on simulator_triggers
# / simulator_portfolio_triggers) — separate engine from the monitor above,
# see features/simulator_risk_monitor.py. Same start/stop/status page pattern
# as /simulator/monitor/* but its own toggle, since starting that one must
# never implicitly arm this one (real broker exit orders) or vice versa.











































_manual_order_kite_cache: dict[tuple, dict] = {}
_manual_order_kite_cache_date: str = ""


def _fetch_manual_order_kite_cache(raw_db, kite_doc: dict | None) -> dict[tuple, dict]:
    """
    Same shape/keying as spot_atm_utils._load_kite_instruments(), fetched directly with a
    specific Kite account's own credentials instead of going through that shared helper —
    which silently skips fetching (returns its empty cache) whenever Dhan is the active
    market-data feed broker, a global/unrelated setting that has nothing to do with whether
    a real Kite account is configured for placing this order.
    """
    global _manual_order_kite_cache, _manual_order_kite_cache_date
    today = datetime.now().strftime("%Y-%m-%d")
    if _manual_order_kite_cache_date == today and _manual_order_kite_cache:
        return _manual_order_kite_cache

    doc = kite_doc
    if doc is None:
        for candidate in raw_db["broker_configuration"].find({"broker_type": "live"}):
            name = str(candidate.get("broker_name") or candidate.get("name") or "").lower()
            if ("kite" in name or "zerodha" in name) and candidate.get("api_key") and candidate.get("access_token"):
                doc = candidate
                break
    if not doc:
        return {}

    try:
        from kiteconnect import KiteConnect  # type: ignore

        kite = KiteConnect(api_key=str(doc.get("api_key") or "").strip())
        kite.set_access_token(str(doc.get("access_token") or "").strip())
        new_cache: dict[tuple, dict] = {}
        for segment in ("NFO", "BFO"):
            for inst in kite.instruments(segment):
                name = str(inst.get("name") or "").strip().upper()
                inst_type = str(inst.get("instrument_type") or "").strip().upper()
                exp = inst.get("expiry")
                stk = inst.get("strike")
                sym = str(inst.get("tradingsymbol") or "").strip()
                if not (name and inst_type in ("CE", "PE") and exp and stk is not None and sym):
                    continue
                try:
                    exp_str = exp.strftime("%Y-%m-%d")
                except AttributeError:
                    exp_str = str(exp)[:10]
                new_cache[(name, exp_str, float(stk), inst_type)] = {
                    "symbol": sym,
                    "exchange": str(inst.get("exchange") or segment),
                }
        _manual_order_kite_cache = new_cache
        _manual_order_kite_cache_date = today
        return new_cache
    except Exception as exc:
        log.debug("manual order kite instrument fetch error: %s", exc)
        return {}


def _resolve_manual_order_symbol(leg: "ManualOrderLeg", raw_db, kite_doc: dict | None = None) -> tuple[str, str] | None:
    """
    Kite-native (underlying, expiry, strike, option_type) → (tradingsymbol, exchange).
    Same instrument metadata _to_flattrade_symbol() already uses for the FlatTrade
    conversion — account-agnostic, so it's safe to resolve this way regardless of
    which broker_id is actually placing the order.
    """
    from features.spot_atm_utils import _load_kite_instruments

    cache = _load_kite_instruments()
    if not cache:
        cache = _fetch_manual_order_kite_cache(raw_db, kite_doc)

    key = (
        leg.underlying.strip().upper(),
        leg.expiry.strip()[:10],
        float(leg.strike),
        leg.option_type.strip().upper(),
    )
    inst = cache.get(key)
    if not inst:
        return None
    return str(inst["symbol"]), str(inst["exchange"])


def _resolve_dhan_security(leg: "ManualOrderLeg", raw_db) -> dict | None:
    """
    (underlying, expiry, strike, option_type) → Dhan's own securityId/symbol/exchangeSegment,
    from the same active_option_tokens collection execution_socket.py already keys positions off
    of. Dhan identifies instruments by numeric securityId, not a tradingsymbol string, so this
    doesn't reuse _resolve_manual_order_symbol (that one resolves the Kite-style symbol).
    """
    doc = raw_db["active_option_tokens"].find_one({
        "broker": "dhan",
        "instrument": leg.underlying.strip().upper(),
        "expiry": leg.expiry.strip()[:10],
        "strike": float(leg.strike),
        "option_type": leg.option_type.strip().upper(),
    })
    if not doc:
        return None
    security_id = str(doc.get("token") or "").strip()
    if not security_id:
        return None
    return {
        "security_id": security_id,
        "symbol": str(doc.get("symbol") or "").strip(),
        "exchange_segment": str(doc.get("ws_segment") or "").strip().upper() or "NSE_FNO",
    }


async def _fetch_dhan_quote_for_leg(leg: "ManualOrderLeg", raw_db) -> dict | None:
    """
    Resolves this leg's Dhan security_id and returns its live quote {"symbol","ltp","bid","ask"}.
    Returns None if Dhan has no contract match for this leg at all.

    Shared by _resolve_mpp_price and _resolve_ltp_price — every order's price, regardless of
    which broker (FlatTrade/Kite/Dhan) actually executes it, is read from this one feed. Dhan
    already streams/queries the full F&O chain, whereas Kite's own feed isn't even running
    unless Kite is the active market-data broker (kite_market_config) — and the broker that
    places the order has nothing to do with which one is the best price source.
    """
    resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
    if not resolved:
        return None
    quote = (await asyncio.to_thread(
        _fetch_dhan_market_data, resolved["exchange_segment"], [int(resolved["security_id"])], _shared_mongo,
    )).get(resolved["security_id"], {})
    return {
        "symbol": resolved["symbol"],
        "ltp": float(quote.get("ltp") or 0),
        "bid": float(quote.get("bid") or 0),
        "ask": float(quote.get("ask") or 0),
    }


def _notify_mpp_ltp_price_unresolved(kind: str, message: str) -> None:
    """
    Shared by _resolve_mpp_price/_resolve_ltp_price — every failure to resolve a real,
    fresh price pages admin via Telegram instead of failing silently, since the only other
    signal is a 0.0 return the caller must already be checking for.
    """
    print(f"[{kind} PRICE] {message}", flush=True)
    try:
        from features.telegram_notifier import notify_admin
        notify_admin(f"{kind.lower()}_price_unresolved", message)
    except Exception as exc:
        log.warning("[%s PRICE] notify_admin failed: %s", kind, exc)


async def _resolve_mpp_price(leg: "ManualOrderLeg", raw_db) -> float:
    """
    MPP's bid + protection% / ask - protection% formula, priced off Dhan's feed regardless of
    the execution broker (see _fetch_dhan_quote_for_leg). The order itself still goes out
    through whichever broker/symbol the caller resolved separately.

    Returns 0.0 — NEVER leg.price or ltp as a stand-in for a missing bid/ask — when Dhan has
    no contract match or no live depth on the side this leg needs. Every caller already
    treats a <= 0 return as "unresolved" and aborts the order instead of placing it;
    substituting ltp here would silently hand back a fabricated "protected" price with no
    real depth behind it — exactly the risk that made this whole function worth having.
    """
    from features.live_order_manager import _mpp_protection_pct, _clamp_limit_price

    quote = await _fetch_dhan_quote_for_leg(leg, raw_db)
    if not quote:
        _notify_mpp_ltp_price_unresolved(
            "MPP", f"No Dhan contract match for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0

    ltp = quote["ltp"]
    bid = quote["bid"]
    ask = quote["ask"]
    is_buy = leg.side == "BUY"
    # Only the side this order actually needs (bid for BUY, ask for SELL) has to be live —
    # but never substitute ltp for it if it's missing.
    if (is_buy and bid <= 0) or (not is_buy and ask <= 0):
        _notify_mpp_ltp_price_unresolved(
            "MPP",
            f"No live depth for {quote.get('symbol')} (bid={bid}, ask={ask}) — order NOT placed.",
        )
        return 0.0

    # NSE's MPP protection band is sized differently for options vs futures (tighter for
    # futures — see _mpp_protection_pct's docstring) — a futures leg must not get priced
    # with the wider option band.
    pct = _mpp_protection_pct(ltp, is_option=leg.option_type.strip().upper() != "FUT")
    base_price = bid if is_buy else ask
    raw_price = base_price * (1 + pct / 100) if is_buy else base_price * (1 - pct / 100)
    price = _clamp_limit_price(raw_price, is_buy)
    print(
        f"[MPP PRICE][dhan-feed] symbol={quote['symbol']} ltp={ltp} bid={bid} ask={ask} "
        f"pct={pct}% price={price} is_buy={is_buy}",
        flush=True,
    )
    return price


async def _resolve_ltp_price(leg: "ManualOrderLeg", raw_db) -> float:
    """
    "Execute At LTP" price source — same Dhan-feed-regardless-of-execution-broker principle as
    _resolve_mpp_price, just without the protection-band markup: submits a plain LIMIT order at
    Dhan's current ltp instead of trusting the order pad row's possibly-seconds-stale client-side
    ltp.

    Returns 0.0 — never leg.price — if Dhan has no match/quote yet; see _resolve_mpp_price's
    docstring for why no fallback price is used here.
    """
    quote = await _fetch_dhan_quote_for_leg(leg, raw_db)
    if not quote or quote["ltp"] <= 0:
        _notify_mpp_ltp_price_unresolved(
            "LTP", f"No Dhan quote for {leg.option_type} {leg.strike} exp={leg.expiry} — order NOT placed.",
        )
        return 0.0
    print(f"[LTP PRICE][dhan-feed] symbol={quote['symbol']} ltp={quote['ltp']}", flush=True)
    return quote["ltp"]


async def _simulator_place_manual_order_core(body: ManualOrderRequest) -> dict:
    """
    Places real orders with the broker — this is live money, not a simulation.
    FlatTrade/Kite use their own place_order() already proven elsewhere in this
    codebase. Dhan goes straight to https://api.dhan.co/v2/orders (same direct-
    REST pattern already used for Dhan positions/quotes) — UNVERIFIED against a
    live order, unlike the other two: dhanhq SDK isn't installed, and this is
    adapted from an untested reference in the sibling option-algo repo. Test
    with one small/throwaway order before relying on it for size.
    """
    broker_id = str(body.broker_id or "").strip()
    print(f"[PLACE_ORDER] request broker_id={broker_id} legs={len(body.orders)} orders={[o.model_dump() for o in body.orders]}", flush=True)
    try:
        raw_db = _shared_mongo._db

        dhan_cfg = raw_db["kite_market_config"].find_one({"broker": "dhan"}) or {}
        if broker_id and broker_id == str(dhan_cfg.get("_id") or "").strip():
            dhan_client_id = str(dhan_cfg.get("user_id") or dhan_cfg.get("dhan_client_id") or "").strip()
            dhan_access_token = str(dhan_cfg.get("access_token") or "").strip()
            if not dhan_access_token or not dhan_client_id:
                print("[PLACE_ORDER][dhan] credentials not configured", flush=True)
                return {"status": "error", "message": "Dhan credentials not configured.", "results": []}

            from features.dhan_broker import get_dhan_instance
            from features.order_execution import place_broker_order

            dhan_order_type_map = {"LIMIT": "LIMIT", "MARKET": "MARKET", "SL": "SL"}
            dhan_adapter = get_dhan_instance(_shared_mongo, dhan_client_id, dhan_access_token)

            async def _place_one_dhan_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_dhan_security, leg, raw_db)
                if not resolved:
                    print(f"[PLACE_ORDER][dhan] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}

                price = leg.price
                requested_type = leg.order_type
                if requested_type == "MPP":
                    price = await _resolve_mpp_price(leg, raw_db)
                    if price <= 0:
                        print(f"[PLACE_ORDER][dhan] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"
                elif requested_type == "LTP":
                    price = await _resolve_ltp_price(leg, raw_db)
                    if price <= 0:
                        print(f"[PLACE_ORDER][dhan] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    requested_type = "LIMIT"

                dhan_order_type = dhan_order_type_map.get(requested_type, "LIMIT")
                result = await asyncio.to_thread(
                    place_broker_order,
                    dhan_adapter,
                    tradingsymbol=resolved["symbol"],
                    exchange="NFO",
                    transaction_type="BUY" if leg.side == "BUY" else "SELL",
                    quantity=leg.quantity,
                    order_type=dhan_order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price or 0.0,
                    context={"purpose": "manual_order_pad", "broker": "dhan", "symbol": resolved["symbol"]},
                )
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {"leg": leg.model_dump(), "status": "success", "order_id": result["order_id"]}

            # Every leg in the basket fires at once instead of waiting on the previous leg's
            # broker round-trip — for a multi-leg strategy that's the difference between the
            # whole basket landing together vs. legs getting staggered fills at drifting prices.
            dhan_results: list[dict] = await asyncio.gather(*(_place_one_dhan_leg(leg) for leg in body.orders))

            any_ok = any(r["status"] == "success" for r in dhan_results)
            all_ok = bool(dhan_results) and all(r["status"] == "success" for r in dhan_results)
            overall_status = "success" if all_ok else ("partial" if any_ok else "error")
            print(f"[PLACE_ORDER] done status={overall_status} results={dhan_results}", flush=True)
            return {"status": overall_status, "results": dhan_results}

        try:
            doc = raw_db["broker_configuration"].find_one({"_id": ObjectId(broker_id)})
        except Exception:
            doc = None
        if not doc:
            print(f"[PLACE_ORDER] broker account not found for broker_id={broker_id}", flush=True)
            return {"status": "error", "message": "Broker account not found.", "results": []}

        broker_name = str(doc.get("broker_name") or doc.get("name") or "").strip().lower()
        is_flattrade = "flattrade" in broker_name
        is_kite = "zerodha" in broker_name or "kite" in broker_name
        print(f"[PLACE_ORDER] resolved broker_name={broker_name} is_flattrade={is_flattrade} is_kite={is_kite}", flush=True)
        if not is_flattrade and not is_kite:
            print(f"[PLACE_ORDER] rejected — order placement not supported for broker_name={broker_name}", flush=True)
            return {"status": "error", "message": "Order placement isn't available for this broker yet.", "results": []}

        results: list[dict] = []

        if is_flattrade:
            from features.flattrade_broker import get_flattrade_instance

            adapter = get_flattrade_instance(str(doc.get("user_id") or ""), str(doc.get("access_token") or ""))
            if adapter is None:
                print("[PLACE_ORDER][flattrade] session not available", flush=True)
                return {"status": "error", "message": "FlatTrade session not available.", "results": []}

            async def _place_one_flattrade_leg(leg: "ManualOrderLeg") -> dict:
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db)
                if not resolved:
                    print(f"[PLACE_ORDER][flattrade] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    # FlatTrade has no native MPP order type — "MPP" would silently fall back to
                    # a plain LIMIT at price=0 (rejected by the exchange) if sent through as-is.
                    # Price source is always Dhan's feed (see _resolve_mpp_price), independent of
                    # FlatTrade being the execution broker here.
                    price = await _resolve_mpp_price(leg, raw_db)
                    if price <= 0:
                        print(f"[PLACE_ORDER][flattrade] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    # Same Dhan-feed-regardless-of-execution-broker principle — submit at Dhan's
                    # current ltp instead of trusting a possibly-stale client-side price.
                    price = await _resolve_ltp_price(leg, raw_db)
                    if price <= 0:
                        print(f"[PLACE_ORDER][flattrade] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                print(
                    f"[PLACE_ORDER][flattrade] placing tradingsymbol={symbol} exchange={exchange} "
                    f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    flush=True,
                )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    adapter,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price,
                    trigger_price=leg.trigger_price,
                    context={"purpose": "manual_order_pad", "broker": "flattrade", "symbol": symbol},
                )
                print(f"[PLACE_ORDER][flattrade] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {"leg": leg.model_dump(), "status": "success", "order_id": result["order_id"]}

            # Whole basket fires together instead of one leg waiting on the previous leg's
            # broker round-trip — same reasoning as the Dhan branch above.
            results = await asyncio.gather(*(_place_one_flattrade_leg(leg) for leg in body.orders))
        else:
            from kiteconnect import KiteConnect  # type: ignore

            api_key = str(doc.get("api_key") or "").strip()
            access_token = str(doc.get("access_token") or "").strip()
            if not api_key or not access_token:
                print("[PLACE_ORDER][kite] session not available", flush=True)
                return {"status": "error", "message": "Kite session not available.", "results": []}
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)

            async def _place_one_kite_leg(leg: "ManualOrderLeg") -> dict:
                # Resolve with this exact account's own token — instrument metadata fetched via
                # Dhan's feed wouldn't reflect this Kite account's session, and the shared cache
                # is empty whenever Dhan (not Kite) is the active market-data broker anyway.
                resolved = await asyncio.to_thread(_resolve_manual_order_symbol, leg, raw_db, doc)
                if not resolved:
                    print(f"[PLACE_ORDER][kite] instrument not found for leg={leg.model_dump()}", flush=True)
                    return {"leg": leg.model_dump(), "status": "error", "message": "Instrument not found."}
                symbol, exchange = resolved

                price = leg.price
                order_type = leg.order_type
                if order_type == "MPP":
                    # Kite has no native MPP order type either — price source is always Dhan's
                    # feed (see _resolve_mpp_price), independent of Kite being the execution
                    # broker here.
                    price = await _resolve_mpp_price(leg, raw_db)
                    if price <= 0:
                        print(f"[PLACE_ORDER][kite] MPP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "MPP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"
                elif order_type == "LTP":
                    # Same Dhan-feed-regardless-of-execution-broker principle — submit at Dhan's
                    # current ltp instead of trusting a possibly-stale client-side price.
                    price = await _resolve_ltp_price(leg, raw_db)
                    if price <= 0:
                        print(f"[PLACE_ORDER][kite] LTP price unresolved for leg={leg.model_dump()}", flush=True)
                        return {"leg": leg.model_dump(), "status": "error", "message": "LTP price unavailable — no live quote for this contract."}
                    order_type = "LIMIT"

                print(
                    f"[PLACE_ORDER][kite] placing tradingsymbol={symbol} exchange={exchange} "
                    f"transaction_type={leg.side} quantity={leg.quantity} order_type={order_type} "
                    f"product={leg.product} price={price} trigger_price={leg.trigger_price}",
                    flush=True,
                )
                from features.order_execution import place_broker_order
                result = await asyncio.to_thread(
                    place_broker_order,
                    kite,
                    tradingsymbol=symbol,
                    exchange=exchange,
                    transaction_type=leg.side,
                    quantity=leg.quantity,
                    order_type=order_type,
                    product=leg.product,
                    price=price or 0.0,
                    trigger_price=leg.trigger_price or 0.0,
                    variety=kite.VARIETY_REGULAR,
                    context={"purpose": "manual_order_pad", "broker": "kite", "symbol": symbol},
                )
                print(f"[PLACE_ORDER][kite] response={result}", flush=True)
                if result["status"] != "success":
                    return {"leg": leg.model_dump(), "status": "error", "message": result["message"]}
                return {"leg": leg.model_dump(), "status": "success", "order_id": result["order_id"]}

            # Whole basket fires together instead of one leg waiting on the previous leg's
            # broker round-trip — same reasoning as the Dhan branch above.
            results = await asyncio.gather(*(_place_one_kite_leg(leg) for leg in body.orders))

        any_ok = any(r["status"] == "success" for r in results)
        all_ok = bool(results) and all(r["status"] == "success" for r in results)
        overall_status = "success" if all_ok else ("partial" if any_ok else "error")
        print(f"[PLACE_ORDER] done status={overall_status} results={results}", flush=True)
        return {
            "status": overall_status,
            "results": results,
        }
    except Exception as exc:
        print(f"[PLACE_ORDER] unhandled error={exc}", flush=True)
        return {"status": "error", "message": str(exc), "results": []}


@app.post("/trade/positions/place-order")
async def simulator_place_manual_order(body: ManualOrderRequest, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    """
    Same route + wrapper as algo.trade's (both call the identical
    _simulator_place_manual_order_core defined above) — had no route registered
    here before, only in algo.trade, even though this service already carried
    its own copy of the core function.
    """
    result = await _simulator_place_manual_order_core(body)
    try:
        from features.telegram_notifier import notify_user

        status = str(result.get("status") or "")
        leg_summary = ", ".join(
            f"{o.side} {o.underlying} {o.strike}{o.option_type} x{o.quantity}" for o in body.orders
        )
        if status == "success":
            notify_user("PT_ORDER_PLACED", f"Order placed — {leg_summary}", {"broker": body.broker_id})
        elif status in ("error", "partial"):
            notify_user(
                "PT_ORDER_FAILED" if status == "error" else "PT_ORDER_PARTIAL",
                f"Order {status} — {leg_summary} — {result.get('message', '')}",
                {"broker": body.broker_id},
            )
    except Exception as exc:
        print(f"[PLACE_ORDER] telegram notify error={exc}", flush=True)
    return result


# ════════════════════════════════════════════════════════════════════════════
# Trading Terminal — virtual/paper orders served from the scanner/common host
# (not the dedicated live-order service). Persists to terminal_* collections.
# ════════════════════════════════════════════════════════════════════════════

class TerminalOrderIn(BaseModel):
    kind: Literal["index", "stock", "strike"]
    instrument: str
    underlying: str
    expiry: str = ""
    strike: float = 0.0
    option_type: str = ""
    side: Literal["BUY", "SELL"]
    quantity: int
    product: Literal["MIS", "NRML"] = "MIS"


TERMINAL_STARTING_CASH = 10000.0


def _terminal_user_id(current_user: dict) -> str:
    return str(current_user.get("_id") or "terminal_anonymous")


def _terminal_instrument_key(
    kind: str,
    underlying: str,
    expiry: str = "",
    strike: float = 0.0,
    option_type: str = "",
) -> str:
    base = f"{kind}:{str(underlying or '').strip().upper()}"
    if kind == "strike":
        return f"{base}:{expiry}:{float(strike):.2f}:{str(option_type or '').strip().upper()}"
    return base


def _ensure_terminal_balance(raw_db, user_id: str, now_str: str) -> dict:
    balances = raw_db["terminal_balances"]
    doc = balances.find_one({"user_id": user_id})
    if doc:
        return doc
    balance_doc = {
        "user_id": user_id,
        "starting_cash": TERMINAL_STARTING_CASH,
        "available_cash": TERMINAL_STARTING_CASH,
        "updated_at": now_str,
        "created_at": now_str,
    }
    balances.insert_one(balance_doc)
    return balance_doc


async def _resolve_terminal_price(payload: "TerminalOrderIn", raw_db) -> float:
    def _cached_underlying_price() -> float:
        spot_doc = get_cached_spot_doc(raw_db, payload.underlying or payload.instrument)
        for field_name in ("spot_price", "ltp", "close", "last_price", "price"):
            try:
                value = float((spot_doc or {}).get(field_name) or 0)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0:
                return value
        return 0.0

    if payload.kind == "strike":
        leg = ManualOrderLeg(
            underlying=payload.underlying,
            expiry=payload.expiry,
            strike=payload.strike,
            option_type=payload.option_type,
            side=payload.side,
            quantity=payload.quantity,
            order_type="LTP",
            product=payload.product,
        )
        return await _resolve_ltp_price(leg, raw_db)

    if payload.kind == "stock":
        sec_id = await asyncio.to_thread(_get_dhan_equity_sec_id, payload.underlying)
        if sec_id:
            quote_map = await asyncio.to_thread(_fetch_dhan_market_data, "NSE_EQ", [int(sec_id)], _shared_mongo)
            live_ltp = float((quote_map.get(str(sec_id)) or {}).get("ltp") or 0)
            if live_ltp > 0:
                return live_ltp
        return _cached_underlying_price()

    from features.spot_atm_utils import _get_live_spot_for_underlying

    live_spot = float(await asyncio.to_thread(_get_live_spot_for_underlying, payload.underlying) or 0)
    if live_spot > 0:
        return live_spot
    return _cached_underlying_price()


def _apply_terminal_order_side_effects(raw_db, user_id: str, body: "TerminalOrderIn", price: float, now_str: str) -> dict:
    balances = raw_db["terminal_balances"]
    positions = raw_db["terminal_positions"]
    instrument_key = _terminal_instrument_key(body.kind, body.underlying, body.expiry, body.strike, body.option_type)

    balance_doc = _ensure_terminal_balance(raw_db, user_id, now_str)
    gross_value = float(price) * int(body.quantity)
    cash_delta = -gross_value if body.side == "BUY" else gross_value
    next_cash = float(balance_doc.get("available_cash") or TERMINAL_STARTING_CASH) + cash_delta
    if next_cash < -1e-9:
        raise ValueError("Insufficient available cash.")

    balances.update_one(
        {"user_id": user_id},
        {"$set": {"available_cash": round(next_cash, 2), "updated_at": now_str}},
        upsert=True,
    )

    existing = positions.find_one({"user_id": user_id, "instrument_key": instrument_key})
    existing_qty = float(existing.get("net_quantity") or 0) if existing else 0.0
    existing_avg = float(existing.get("avg_price") or 0) if existing else 0.0
    signed_qty = float(body.quantity if body.side == "BUY" else -body.quantity)
    next_qty = existing_qty + signed_qty

    if abs(existing_qty) < 1e-9:
        next_avg = price
    elif existing_qty * signed_qty > 0:
        next_avg = ((abs(existing_qty) * existing_avg) + (abs(signed_qty) * price)) / abs(next_qty)
    elif abs(existing_qty) > abs(signed_qty):
        next_avg = existing_avg
    elif abs(existing_qty) < abs(signed_qty):
        next_avg = price
    else:
        next_avg = 0.0

    if abs(next_qty) < 1e-9:
        positions.delete_one({"user_id": user_id, "instrument_key": instrument_key})
    else:
        position_doc = {
            "user_id": user_id,
            "instrument_key": instrument_key,
            "kind": body.kind,
            "instrument": body.instrument,
            "underlying": body.underlying,
            "expiry": body.expiry,
            "strike": body.strike,
            "option_type": body.option_type,
            "product": body.product,
            "net_quantity": int(next_qty),
            "avg_price": round(float(next_avg), 4),
            "last_price": round(float(price), 4),
            "last_side": body.side,
            "updated_at": now_str,
            "created_at": (existing or {}).get("created_at", now_str),
        }
        positions.update_one(
            {"user_id": user_id, "instrument_key": instrument_key},
            {"$set": position_doc},
            upsert=True,
        )

    return {
        "instrument_key": instrument_key,
        "available_cash": round(next_cash, 2),
        "net_quantity": int(next_qty),
        "avg_price": round(float(next_avg), 4),
    }


def _terminal_doc_to_price_payload(doc: dict) -> "TerminalOrderIn":
    return TerminalOrderIn(
        kind=str(doc.get("kind") or "stock"),
        instrument=str(doc.get("instrument") or doc.get("underlying") or ""),
        underlying=str(doc.get("underlying") or doc.get("instrument") or ""),
        expiry=str(doc.get("expiry") or ""),
        strike=float(doc.get("strike") or 0),
        option_type=str(doc.get("option_type") or ""),
        side="BUY",
        quantity=max(int(abs(doc.get("net_quantity") or doc.get("quantity") or 1)), 1),
        product=str(doc.get("product") or "MIS"),
    )


async def _build_terminal_dashboard(raw_db, user_id: str) -> dict:
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")

    def _load_snapshot():
        balance = _ensure_terminal_balance(raw_db, user_id, now_str)
        orders = list(raw_db["terminal_orders"].find({"user_id": user_id}).sort("placed_at", -1).limit(200))
        positions = list(raw_db["terminal_positions"].find({"user_id": user_id}).sort("updated_at", -1))
        return balance, orders, positions

    balance_doc, order_docs, position_docs = await asyncio.to_thread(_load_snapshot)

    positions_out = []
    holdings_out = []
    total_pnl = 0.0

    for pos in position_docs:
        current_price = await _resolve_terminal_price(_terminal_doc_to_price_payload(pos), raw_db)
        avg_price = float(pos.get("avg_price") or 0)
        net_quantity = int(pos.get("net_quantity") or 0)
        abs_quantity = abs(net_quantity)
        mtm = round((current_price - avg_price) * net_quantity, 2)
        invested_amount = round(abs_quantity * avg_price, 2)
        current_value = round(abs_quantity * current_price, 2)
        total_pnl += mtm
        row = {
            "instrument_key": pos.get("instrument_key"),
            "instrument": pos.get("instrument"),
            "underlying": pos.get("underlying"),
            "kind": pos.get("kind"),
            "expiry": pos.get("expiry") or "",
            "strike": float(pos.get("strike") or 0),
            "option_type": pos.get("option_type") or "",
            "product": pos.get("product") or "MIS",
            "net_quantity": net_quantity,
            "display_quantity": abs_quantity,
            "side": "BUY" if net_quantity >= 0 else "SELL",
            "avg_price": round(avg_price, 2),
            "current_price": round(current_price, 2),
            "invested_amount": invested_amount,
            "current_value": current_value,
            "pnl": mtm,
            "pnl_pct": round((mtm / invested_amount) * 100, 2) if invested_amount > 0 else 0.0,
            "updated_at": pos.get("updated_at") or "",
        }
        positions_out.append(row)
        if net_quantity > 0:
            holdings_out.append(row)

    orders_out = []
    for order in order_docs:
        current_price = await _resolve_terminal_price(_terminal_doc_to_price_payload(order), raw_db)
        side = str(order.get("side") or "BUY").upper()
        filled_price = float(order.get("price") or 0)
        qty = int(order.get("quantity") or 0)
        signed_qty = qty if side == "BUY" else -qty
        pnl = round((current_price - filled_price) * signed_qty, 2)
        orders_out.append({
            "id": str(order.get("_id") or order.get("id") or ""),
            "instrument": order.get("instrument") or "",
            "underlying": order.get("underlying") or "",
            "kind": order.get("kind") or "stock",
            "product": order.get("product") or "MIS",
            "side": side,
            "quantity": qty,
            "price": round(filled_price, 2),
            "current_price": round(current_price, 2),
            "pnl": pnl,
            "status": order.get("status") or "FILLED",
            "placed_at": order.get("placed_at") or "",
        })

    return {
        "status": "success",
        "balance": {
            "starting_cash": round(float(balance_doc.get("starting_cash") or TERMINAL_STARTING_CASH), 2),
            "available_cash": round(float(balance_doc.get("available_cash") or TERMINAL_STARTING_CASH), 2),
        },
        "summary": {
            "total_pnl": round(total_pnl, 2),
            "positions_count": len(positions_out),
            "holdings_count": len(holdings_out),
            "orders_count": len(orders_out),
        },
        "orders": orders_out,
        "positions": positions_out,
        "holdings": holdings_out,
    }


@app.get("/terminal/dashboard")
async def terminal_dashboard(current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    return await _build_terminal_dashboard(_shared_mongo._db, _terminal_user_id(current_user))


@app.post("/terminal/place-order")
async def terminal_place_order(body: TerminalOrderIn, current_user: dict = Depends(app_auth.get_current_user)) -> dict:
    raw_db = _shared_mongo._db
    price = await _resolve_terminal_price(body, raw_db)
    if price <= 0:
        return {"status": "error", "message": "Price unavailable for this instrument. Try again."}

    user_id = _terminal_user_id(current_user)
    now_str = datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S")
    try:
        side_effects = await asyncio.to_thread(_apply_terminal_order_side_effects, raw_db, user_id, body, price, now_str)
    except ValueError as exc:
        return {"status": "error", "message": str(exc)}

    doc = {
        "user_id": user_id,
        "kind": body.kind,
        "instrument": body.instrument,
        "underlying": body.underlying,
        "expiry": body.expiry,
        "strike": body.strike,
        "option_type": body.option_type,
        "side": body.side,
        "quantity": body.quantity,
        "product": body.product,
        "price": price,
        "status": "FILLED",
        "order_type": "PAPER_MARKET",
        "placed_at": now_str,
        "instrument_key": side_effects["instrument_key"],
        "available_cash_after": side_effects["available_cash"],
        "net_quantity_after": side_effects["net_quantity"],
        "avg_price_after": side_effects["avg_price"],
    }

    def _insert() -> str:
        return str(raw_db["terminal_orders"].insert_one(doc).inserted_id)

    doc["id"] = await asyncio.to_thread(_insert)
    return {"status": "success", "order": doc}



























































app.include_router(scanner_router)


# ─── Kite Broker Endpoints ────────────────────────────────────────────────────

# Temporary in-memory store: session_id → broker_doc_id
# Cleared after use (one-time use per login)
_kite_pending: dict = {}






# ── Dhan OAuth endpoints ──────────────────────────────────────────────────────
_dhan_pending: dict[str, str] = {}


def _dhan_popup_result_html(success: bool, message: str) -> str:
    import json as _json
    payload_js = _json.dumps({"type": "DHAN_LOGIN", "success": success, "message": message})
    icon  = "✓" if success else "✗"
    color = "#22c55e" if success else "#ef4444"
    title = "Login Successful" if success else "Login Failed"
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>Dhan Login</title>
<style>
  body{{font-family:-apple-system,sans-serif;display:flex;align-items:center;
       justify-content:center;min-height:100vh;margin:0;background:#0f172a;color:#f1f5f9;}}
  .card{{text-align:center;padding:2rem;background:#1e293b;border-radius:12px;border:1px solid #334155;}}
  .icon{{font-size:3rem;color:{color};}}
  p{{color:#94a3b8;}}
</style></head>
<body>
  <div class="card">
    <div class="icon">{icon}</div>
    <h2>{title}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) window.opener.postMessage(payload, "*");
    setTimeout(() => window.close(), 1500);
  </script>
</body></html>"""












_FNO_STOCKS_CACHE: dict = {}        # {"data": [...], "fetched_at": float}
_FNO_MASTER_CACHE: dict = {}        # {"rows": {symbol: [contracts]}, "fetched_at": float}
_FNO_CACHE_TTL = 3600               # refresh once per hour

_DHAN_SCRIP_MASTER_CACHE: dict = {}  # {"rows": [csv_row_dict, ...], "date": "YYYY-MM-DD"}


def _get_dhan_scrip_master_rows() -> list[dict]:
    """
    Raw Dhan scrip master CSV rows (~30MB file), downloaded once per calendar day
    and shared by every Dhan contract sync — stocks, indices, anything else —
    so the file is fetched at most once a day no matter how many instruments sync.
    """
    import io as _io, csv as _csv, requests as _req
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_SCRIP_MASTER_CACHE.get("rows") and _DHAN_SCRIP_MASTER_CACHE.get("date") == today_str:
        return _DHAN_SCRIP_MASTER_CACHE["rows"]

    resp = _req.get("https://images.dhan.co/api-data/api-scrip-master.csv", timeout=30)
    resp.raise_for_status()
    rows = list(_csv.DictReader(_io.StringIO(resp.text)))
    _DHAN_SCRIP_MASTER_CACHE["rows"] = rows
    _DHAN_SCRIP_MASTER_CACHE["date"] = today_str
    return rows


def _get_dhan_fno_master() -> dict[str, list[dict]]:
    """
    Returns {symbol: [{sec_id, strike, opt_type, expiry, exchange}]} from
    Dhan security master CSV.  Cached for 1 hour.
    Also populates _FNO_MASTER_CACHE["equity_ids"] = {symbol: sec_id} for spot lookup.
    """
    import time as _t
    if _FNO_MASTER_CACHE.get("rows") and (_t.time() - _FNO_MASTER_CACHE.get("fetched_at", 0)) < _FNO_CACHE_TTL:
        return _FNO_MASTER_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    equity_ids: dict[str, str] = {}
    for row in reader:
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        exch = row.get("SEM_EXM_EXCH_ID", "").strip()
        sec_id = row.get("SEM_SMST_SECURITY_ID", "").strip()
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()

        # Capture NSE equity security IDs for spot price lookup
        # Dhan CSV may use EQUITY, ES, EQ or similar for cash equity
        _deriv_types = {"OPTSTK", "OPTIDX", "FUTSTK", "FUTIDX", "FUTCUR", "OPTCUR", "FUTCOM", "OPTFUT"}
        if exch == "NSE" and inst not in _deriv_types and ts and sec_id:
            sym = ts.split("-")[0].strip()
            if sym and sym not in equity_ids:
                equity_ids[sym] = sec_id

        if inst != "OPTSTK":
            continue
        symbol = ts.split("-")[0].strip() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        entry = {
            "sec_id":   sec_id,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": exch,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        }
        master.setdefault(symbol, []).append(entry)

    _FNO_MASTER_CACHE["rows"] = master
    _FNO_MASTER_CACHE["equity_ids"] = equity_ids
    _FNO_MASTER_CACHE["fetched_at"] = _t.time()
    return master


_LAST_GOOD_EQUITY_COLLISION_QUOTE: dict[str, float] = {}  # frontend (kite) token -> last real ltp


def _get_dhan_equity_sec_id(symbol: str) -> str:
    """Return the NSE equity security ID for a stock symbol from Dhan CSV cache."""
    _get_dhan_fno_master()  # ensure cache is populated
    return str(_FNO_MASTER_CACHE.get("equity_ids", {}).get(symbol.strip().upper()) or "")


def _resolve_dhan_equity_ids_by_kite_tokens(kite_tokens: list[str], db) -> dict[str, str]:
    """
    kite_token -> dhan_security_id for scanner equity holdings, via scanner_stocks_list
    (same dhan_security_id field scanner/service.py's historical-data sync already
    resolves per-row via _resolve_stock_dhan_security_id — this just batches that lookup
    by token for the live quote endpoint). Lets a caller tell a scanner stock's Kite-space
    token apart from a simulator FNO/option token before deciding which Dhan segment to
    query — scanner holdings were always falling into the FNO-only lookup otherwise.
    """
    if not kite_tokens:
        return {}
    docs = db["scanner_stocks_list"].find(
        {"kite_token": {"$in": kite_tokens}},
        {"_id": 0, "kite_token": 1, "dhan_security_id": 1},
    )
    return {
        str(doc["kite_token"]): str(doc["dhan_security_id"])
        for doc in docs
        if doc.get("kite_token") and doc.get("dhan_security_id")
    }


_DHAN_INDEX_OPTION_CACHE: dict = {}  # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}


def _get_dhan_index_option_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for index (OPTIDX) contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV. The CSV is ~30MB, so it's downloaded once per calendar day and
    reused for every call that day, same caching shape as _get_dhan_fno_master() above.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_INDEX_OPTION_CACHE.get("rows") and _DHAN_INDEX_OPTION_CACHE.get("date") == today_str:
        return _DHAN_INDEX_OPTION_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "OPTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   float(row.get("SEM_STRIKE_PRICE") or 0),
            "opt_type": row.get("SEM_OPTION_TYPE", "").strip().upper(),
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_INDEX_OPTION_CACHE["rows"] = master
    _DHAN_INDEX_OPTION_CACHE["date"] = today_str
    return master


_DHAN_INDEX_FUTURE_CACHE: dict = {}  # {"rows": {instrument: [contract, ...]}, "date": "YYYY-MM-DD"}

# token -> last real nonzero LTP ever seen for it via /simulator/paper-trade/futures-chain.
# Never evicted, same "a slightly stale real quote beats showing 0" reasoning as
# execution_socket.py's _LAST_GOOD_UNDERLYING_QUOTE — futures/ATM-option tokens are
# priced via dhan_quote_post_blocking (see simulator_pt_futures_chain), not the
# shared get_broker_rest_quotes/_LAST_GOOD_QUOTE path, so this is this endpoint's own.
_LAST_GOOD_FUTURES_TOKEN_QUOTE: dict[str, float] = {}


def _get_dhan_index_future_master() -> dict[str, list[dict]]:
    """
    Returns {instrument: [{sec_id, symbol, expiry, exchange, lot_size}]} for index
    (FUTIDX) futures contracts — NIFTY, SENSEX, BANKNIFTY, etc. — straight from Dhan's
    scrip master CSV, same caching shape as _get_dhan_index_option_master() above.

    These were never synced into active_option_tokens: _get_dhan_fno_master() and
    _get_dhan_index_option_master() both explicitly skip every FUT* instrument type
    (they only ever kept OPTSTK/OPTIDX), so there's no Mongo collection to query —
    this reads the CSV directly instead, same as the option masters do.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_INDEX_FUTURE_CACHE.get("rows") and _DHAN_INDEX_FUTURE_CACHE.get("date") == today_str:
        return _DHAN_INDEX_FUTURE_CACHE["rows"]

    reader = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in reader:
        if row.get("SEM_INSTRUMENT_NAME", "").strip() != "FUTIDX":
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "expiry":   expiry,
            "exchange": row.get("SEM_EXM_EXCH_ID", "").strip(),
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    for contracts in master.values():
        contracts.sort(key=lambda c: c["expiry"])

    _DHAN_INDEX_FUTURE_CACHE["rows"] = master
    _DHAN_INDEX_FUTURE_CACHE["date"] = today_str
    return master


_ACTIVE_OPTION_TOKENS_INDEX_ENSURED = False


def _ensure_active_option_tokens_index(col) -> None:
    """
    Create the compound index every Dhan contract upsert matches on, once per process.
    Without it, each upsert inside a bulk_write does a full collection scan to check for
    an existing match — that alone turned a multi-thousand-contract sync from under a
    second into ~10s per instrument (measured: NIFTY's 4080 contracts 9.8s -> 0.28s).
    """
    global _ACTIVE_OPTION_TOKENS_INDEX_ENSURED
    if _ACTIVE_OPTION_TOKENS_INDEX_ENSURED:
        return
    try:
        col.create_index(
            [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
            name="idx_active_option_contract_v2",
        )
    except Exception:
        pass
    _ACTIVE_OPTION_TOKENS_INDEX_ENSURED = True


def _sync_dhan_index_option_tokens(instrument: str) -> dict:
    """
    Refresh active_option_tokens for one index instrument from Dhan's scrip master
    (see _get_dhan_index_option_master). Replaces the Kite-instrument-cache path for
    indices when Dhan is the active broker — that path is skipped entirely for Dhan
    and was only ever serving a stale, narrow strike range from whatever was already
    in the DB.
    """
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_option_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index option contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "index",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens sync completed from Dhan scrip master",
        }
    finally:
        db.close()


def _sync_dhan_index_future_tokens(instrument: str) -> dict:
    """
    Refresh active_option_tokens for one index's FUTIDX contracts (see
    _get_dhan_index_future_master). Same `option_type: "FUT", strike: 0.0` shape
    _sync_dhan_commodity_tokens already uses for MCX futures (FUTCOM) — that's
    proof this collection's compound index and every downstream reader already
    tolerate a strike-less contract; this just does the same thing for index
    futures, which were never synced anywhere before (every other index-token
    sync explicitly skips FUT* instrument types).
    """
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_index_future_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan index future contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            exch = c.get("exchange") or ("BSE" if normalized in {"SENSEX", "BANKEX"} else "NSE")
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": 0.0,
                "option_type": "FUT",
            }
            update_payload = {
                **key,
                "instrument_type": "future",
                "exchange": exch,
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-FUT",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "BSE_FNO" if exch == "BSE" else "NSE_FNO",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens FUT sync completed from Dhan scrip master",
        }
    finally:
        db.close()


_DHAN_COMMODITY_MASTER_CACHE: dict = {}  # {"rows": {underlying: [contract, ...]}, "date": "YYYY-MM-DD"}


def _get_dhan_commodity_master() -> dict[str, list[dict]]:
    """
    Returns {underlying: [{sec_id, symbol, strike, opt_type, expiry, exchange, lot_size}]}
    for every MCX commodity — gold, silver, crude oil, copper, and everything else Dhan
    lists on MCX — covering both futures (FUTCOM, opt_type "FUT", strike 0) and options
    on futures (OPTFUT, opt_type CE/PE). Underlyings aren't a fixed list like the indices;
    they're discovered straight from whatever Dhan's scrip master actually carries.
    """
    today_str = datetime.now().strftime("%Y-%m-%d")
    if _DHAN_COMMODITY_MASTER_CACHE.get("rows") and _DHAN_COMMODITY_MASTER_CACHE.get("date") == today_str:
        return _DHAN_COMMODITY_MASTER_CACHE["rows"]

    rows = _get_dhan_scrip_master_rows()
    master: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("SEM_EXM_EXCH_ID", "").strip() != "MCX":
            continue
        inst = row.get("SEM_INSTRUMENT_NAME", "").strip()
        if inst not in ("FUTCOM", "OPTFUT"):
            continue
        ts = row.get("SEM_TRADING_SYMBOL", "").strip()
        symbol = ts.split("-")[0].strip().upper() if "-" in ts else ""
        if not symbol:
            continue
        expiry_raw = row.get("SEM_EXPIRY_DATE", "").strip()
        expiry = expiry_raw[:10] if expiry_raw else ""
        if not expiry:
            continue
        if inst == "FUTCOM":
            opt_type, strike = "FUT", 0.0
        else:
            opt_type = row.get("SEM_OPTION_TYPE", "").strip().upper()
            strike = float(row.get("SEM_STRIKE_PRICE") or 0)
        master.setdefault(symbol, []).append({
            "sec_id":   row.get("SEM_SMST_SECURITY_ID", "").strip(),
            "symbol":   ts,
            "strike":   strike,
            "opt_type": opt_type,
            "expiry":   expiry,
            "lot_size": int(float(row.get("SEM_LOT_UNITS") or 0)),
        })

    _DHAN_COMMODITY_MASTER_CACHE["rows"] = master
    _DHAN_COMMODITY_MASTER_CACHE["date"] = today_str
    return master


def _sync_dhan_commodity_tokens(instrument: str) -> dict:
    """Refresh active_option_tokens for one MCX commodity (futures + options) from Dhan's scrip master."""
    normalized = str(instrument or "").strip().upper()
    master = _get_dhan_commodity_master()
    contracts = master.get(normalized, [])
    if not contracts:
        return {
            "instrument": normalized,
            "expiries": [],
            "contracts_processed": 0,
            "created": 0,
            "updated": 0,
            "message": f"No Dhan commodity contracts found for {normalized} in the scrip master",
        }

    from pymongo import UpdateOne

    db = MongoData()
    try:
        col = db._db["active_option_tokens"]
        _ensure_active_option_tokens_index(col)
        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        expiries: set[str] = set()
        ops = []
        for c in contracts:
            expiries.add(c["expiry"])
            opt_type = c["opt_type"]
            if opt_type not in {"CE", "PE", "FUT"}:
                continue
            sec_id = str(c.get("sec_id") or "").strip()
            if not sec_id:
                continue
            key = {
                "broker": "dhan",
                "instrument": normalized,
                "expiry": c["expiry"],
                "strike": c["strike"],
                "option_type": opt_type,
            }
            update_payload = {
                **key,
                "instrument_type": "commodity",
                "exchange": "MCX",
                "symbol": c.get("symbol") or f"{normalized}-{c['expiry']}-{c['strike']}-{opt_type}",
                "token": sec_id,
                "tokens": sec_id,
                "ws_segment": "MCX_COMM",
                "lot_size": c.get("lot_size"),
                "updated_at": now_ts,
            }
            ops.append(UpdateOne(
                key, {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}}, upsert=True,
            ))

        created = 0
        updated = 0
        if ops:
            result = col.bulk_write(ops, ordered=False)
            created = result.upserted_count
            updated = result.matched_count

        return {
            "instrument": normalized,
            "expiries": sorted(expiries),
            "contracts_processed": len(contracts),
            "created": created,
            "updated": updated,
            "message": "active_option_tokens sync completed from Dhan scrip master (commodity)",
        }
    finally:
        db.close()



















def _kite_popup_html(
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    payload = {
        "type":          "KITE_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    import json as _json
    payload_js = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Kite Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">You can close this window after checking the URL.</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
  </script>
</body>
</html>"""




# ─── FlatTrade postback (order status push) ──────────────────────────────────

async def _parse_flattrade_postback_payload(request: Request) -> dict:
    data: dict = {}
    try:
        query_params = dict(request.query_params or {})
    except Exception:
        query_params = {}

    try:
        body_bytes = await request.body()
        body_str = body_bytes.decode("utf-8", errors="replace")
    except Exception:
        body_str = ""

    try:
        if body_str.startswith("jData="):
            import urllib.parse
            parsed = urllib.parse.parse_qs(body_str)
            jdata_str = (parsed.get("jData") or ["{}"])[0]
            data = json.loads(jdata_str)
        elif body_str.strip():
            data = json.loads(body_str)
    except Exception as exc:
        log.warning("[FLATTRADE POSTBACK] body parse error: %s", exc)
        data = {}

    if not data and query_params:
        if "jData" in query_params:
            try:
                data = json.loads(str(query_params.get("jData") or "{}"))
            except Exception as exc:
                log.warning("[FLATTRADE POSTBACK] query jData parse error: %s", exc)
                data = {}
        else:
            data = query_params
    return data if isinstance(data, dict) else {}


def _process_flattrade_postback_payload(
    *,
    data: dict,
    broker_doc_id: str = "",
    source_tag: str = "FLATTRADE POSTBACK",
) -> None:
    from features.live_order_manager import process_broker_order_update

    order_id = str(data.get("norenordno") or data.get("order_id") or "").strip()
    status_raw = str(data.get("status") or "").upper().strip()
    fill_price = float(data.get("avgprc") or data.get("flprc") or data.get("prc") or 0)
    fill_qty = int(data.get("fillshares") or data.get("filledshares") or data.get("qty") or 0)
    rej_reason = str(data.get("rejreason") or data.get("emsg") or "").lower()
    uid = str(data.get("uid") or data.get("actid") or "").strip()

    log.info(
        "[%s] broker=%s uid=%s order_id=%s status=%s fill=%.2f qty=%d payload=%s",
        source_tag,
        broker_doc_id or "-",
        uid or "-",
        order_id,
        status_raw,
        fill_price,
        fill_qty,
        data,
    )

    if not order_id:
        return

    _status_map = {
        "COMPLETE": "COMPLETE",
        "COMPLETED": "COMPLETE",
        "REJECTED": "REJECTED",
        "CANCELLED": "CANCELLED",
        "CANCELED": "CANCELLED",
        "OPEN": "OPEN",
        "TRIGGER_PENDING": "TRIGGER_PENDING",
    }
    status = _status_map.get(status_raw, status_raw)
    if status not in ("COMPLETE", "REJECTED", "CANCELLED"):
        return

    local_db = MongoData()
    try:
        if broker_doc_id:
            broker_order = local_db._db["broker_orders"].find_one(
                {"order_id": order_id},
                {"trade_id": 1},
            )
            if broker_order:
                trade_id = str(broker_order.get("trade_id") or "").strip()
                trade = local_db._db["algo_trades"].find_one(
                    {"_id": trade_id},
                    {"broker": 1},
                )
                trade_broker = str((trade or {}).get("broker") or "").strip()
                if trade_broker and trade_broker != broker_doc_id:
                    log.warning(
                        "[%s] broker=%s order_id=%s belongs_to=%s - skipping",
                        source_tag, broker_doc_id, order_id, trade_broker,
                    )
                    return

        updated = process_broker_order_update(
            local_db,
            order_id=order_id,
            status=status,
            fill_price=fill_price,
            fill_qty=fill_qty,
            rejection_reason=rej_reason,
            source="postback",
        )
        if not updated and status == "COMPLETE":
            exit_doc = local_db._db["broker_orders"].find_one(
                {"order_id": order_id},
                {"order_side": 1, "status": 1, "trade_id": 1, "leg_id": 1, "exit_reason": 1},
            ) or {}
            if str(exit_doc.get("order_side") or "").strip() == "exit":
                from features.live_order_manager import _sync_live_exit_fill
                trade_id = str(exit_doc.get("trade_id") or "").strip()
                leg_id = str(exit_doc.get("leg_id") or "").strip()
                exit_reason = str(exit_doc.get("exit_reason") or "stoploss").strip() or "stoploss"
                if trade_id and leg_id and fill_price > 0:
                    local_db._db["broker_orders"].update_one(
                        {"order_id": order_id},
                        {"$set": {
                            "status": "COMPLETE",
                            "fill_price": float(fill_price or 0),
                            "fill_qty": int(fill_qty or 0),
                            "filled_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                            "updated_at": datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
                        }},
                    )
                    _sync_live_exit_fill(local_db, trade_id, leg_id, exit_reason, fill_price)
                    updated = True
                    log.info(
                        "[%s] forced exit sync broker=%s order_id=%s trade=%s leg=%s reason=%s fill=%.2f",
                        source_tag, broker_doc_id or "-", order_id, trade_id, leg_id, exit_reason, fill_price,
                    )
        log.info("[%s] broker=%s order_id=%s updated=%s", source_tag, broker_doc_id or "-", order_id, updated)
    except Exception as exc:
        log.error("[%s] processing error broker=%s order_id=%s: %s", source_tag, broker_doc_id or "-", order_id, exc)
    finally:
        try:
            local_db.close()
        except Exception:
            pass










# ─── FlatTrade broker login ───────────────────────────────────────────────────

_flattrade_pending: dict = {}
















def _broker_popup_html(
    broker: str,
    success: bool,
    message: str,
    access_token: str = "",
    user_id: str = "",
    user_name: str = "",
    broker_doc_id: str = "",
) -> str:
    import json as _json
    payload = {
        "type":          f"{broker.upper()}_LOGIN",
        "success":       success,
        "message":       message,
        "access_token":  access_token,
        "user_id":       user_id,
        "user_name":     user_name,
        "broker_doc_id": broker_doc_id,
    }
    payload_js   = _json.dumps(payload)
    status_color = "#22c55e" if success else "#ef4444"
    status_icon  = "✓" if success else "✗"
    return f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>{broker} Login</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      display: flex; align-items: center; justify-content: center;
      min-height: 100vh; margin: 0;
      background: #0f172a; color: #f1f5f9;
    }}
    .card {{
      text-align: center; padding: 2rem;
      background: #1e293b; border-radius: 12px;
      border: 1px solid #334155;
    }}
    .icon {{ font-size: 3rem; color: {status_color}; }}
    h2 {{ margin: 0.5rem 0; }}
    p {{ color: #94a3b8; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="icon">{status_icon}</div>
    <h2>{"Login Successful" if success else "Login Failed"}</h2>
    <p>{message}</p>
    <p style="font-size:0.8rem">This window will close automatically...</p>
  </div>
  <script>
    const payload = {payload_js};
    if (window.opener) {{
      window.opener.postMessage(payload, "*");
    }}
    setTimeout(() => window.close(), 1500);
  </script>
</body>
</html>"""


# ─── Live Market Data (KiteTicker) ───────────────────────────────────────────

_LIVE_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live Trade Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 420px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #94a3b8;
      margin-bottom: 2rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 2rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #22c55e; box-shadow: 0 0 8px #22c55e; animation: pulse 1.5s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #22c55e; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1.1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.5rem 0.75rem;
      display: flex; justify-content: space-between; align-items: center;
    }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #22c55e; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg {
      margin-top: 1rem; font-size: 0.8rem; color: #f87171;
      background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none;
    }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Live Trade Control</div>
  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>
  <button class="btn" id="actionBtn" disabled onclick="handleAction()">...</button>
  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks Received</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
  </div>
  <div class="spot-section">
    <div class="spot-title">Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>
  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>
<script>
  const API = '';

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/live/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn = document.getElementById('actionBtn');
    btn.disabled = false;
    if (status === 'running') {
      btn.textContent = 'Stop Live Trading';
      btn.className   = 'btn btn-stop';
    } else if (status === 'connecting') {
      btn.textContent = 'Connecting...';
      btn.className   = 'btn btn-start';
      btn.disabled    = true;
    } else {
      btn.textContent = 'Start Live Trading';
      btn.className   = 'btn btn-start';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el = document.getElementById('spot-' + sym);
      const v  = spotMap[sym];
      if (!el) return;
      if (v) {
        el.textContent = '\\u20B9' + Number(v).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';
    try {
      const url = status === 'running' ? '/live/stop' : '/live/start';
      await fetch(API + url + '?ui=1');
    } catch(e) { console.error(e); }
    setTimeout(fetchStatus, 800);
    setTimeout(fetchStatus, 2000);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 3000);
</script>
</body>
</html>"""


def _start_ticker_bg():
    """Run in background thread — loads tokens from DB and starts KiteTicker."""
    _db = MongoData()
    try:
        print(
            f'[MONITOR TICKER START] '
            f'current_status={ticker_manager.status} '
            f'tick_count={int(ticker_manager.tick_count or 0)}'
        )
        if ticker_manager.status == "running":
            ticker_manager.restart(_db._db)
        else:
            ticker_manager.start(_db._db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("ticker start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _build_monitor_control_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Live + Fast-Forward Monitor</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(34, 197, 94, 0.14), transparent 34%),
        linear-gradient(160deg, #07111f 0%, #0f172a 55%, #111827 100%);
      color: #e5eefb;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(560px, calc(100vw - 32px));
      background: rgba(10, 19, 34, 0.94);
      border: 1px solid rgba(148, 163, 184, 0.18);
      border-radius: 24px;
      padding: 28px;
      box-shadow: 0 22px 70px rgba(0, 0, 0, 0.35);
    }}
    .eyebrow {{
      color: #7dd3fc;
      font-size: 12px;
      letter-spacing: 0.18em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .title {{
      font-size: 30px;
      font-weight: 700;
      margin-bottom: 10px;
    }}
    .subtitle {{
      color: #94a3b8;
      font-size: 14px;
      line-height: 1.6;
      margin-bottom: 20px;
    }}
    .hero {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      align-items: center;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(125, 211, 252, 0.12);
      border-radius: 18px;
      padding: 18px 20px;
      margin-bottom: 18px;
    }}
    .status-row {{
      display: flex;
      align-items: center;
      gap: 10px;
      font-size: 18px;
      font-weight: 600;
    }}
    .dot {{
      width: 12px;
      height: 12px;
      border-radius: 999px;
      background: #64748b;
      box-shadow: 0 0 0 transparent;
    }}
    .dot.running {{ background: #22c55e; box-shadow: 0 0 12px rgba(34, 197, 94, 0.8); }}
    .dot.connecting {{ background: #f59e0b; box-shadow: 0 0 12px rgba(245, 158, 11, 0.8); }}
    .dot.stopped {{ background: #ef4444; box-shadow: 0 0 12px rgba(239, 68, 68, 0.45); }}
    .clock-box {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    .clock-label {{
      color: #64748b;
      font-size: 11px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
    }}
    .clock-value {{
      margin-top: 6px;
      font-size: 18px;
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }}
    .stat {{
      background: rgba(15, 23, 42, 0.85);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 16px;
      padding: 14px 16px;
    }}
    .stat-label {{
      font-size: 11px;
      color: #64748b;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }}
    .stat-value {{
      font-size: 19px;
      font-weight: 700;
      line-height: 1.35;
      word-break: break-word;
    }}
    .actions {{
      display: flex;
      gap: 12px;
      margin-bottom: 18px;
    }}
    .btn {{
      flex: 1;
      border: none;
      border-radius: 14px;
      padding: 14px 16px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn-primary {{ background: linear-gradient(135deg, #22c55e, #16a34a); color: #04110a; }}
    .btn-danger {{ background: linear-gradient(135deg, #f97316, #ef4444); color: #fff7ed; }}
    .btn-secondary {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148, 163, 184, 0.18); }}
    .btn:disabled {{ opacity: 0.55; cursor: not-allowed; }}
    .panel {{
      background: rgba(15, 23, 42, 0.88);
      border: 1px solid rgba(148, 163, 184, 0.12);
      border-radius: 18px;
      padding: 16px;
    }}
    .panel-title {{
      color: #cbd5e1;
      font-size: 13px;
      font-weight: 700;
      letter-spacing: 0.12em;
      text-transform: uppercase;
      margin-bottom: 10px;
    }}
    .strategies {{
      display: flex;
      flex-direction: column;
      gap: 10px;
      max-height: 220px;
      overflow: auto;
    }}
    .strategy-item {{
      border-radius: 12px;
      padding: 12px 14px;
      background: rgba(8, 15, 28, 0.9);
      border: 1px solid rgba(148, 163, 184, 0.1);
    }}
    .strategy-name {{
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .strategy-meta {{
      color: #94a3b8;
      font-size: 12px;
      line-height: 1.5;
    }}
    .empty {{
      color: #94a3b8;
      font-size: 13px;
      line-height: 1.6;
      padding: 10px 4px 2px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="eyebrow">Auto Monitor</div>
    <div class="title">Live + Fast-Forward Monitor</div>
    <div class="subtitle">
      Single control page for both <b>live</b> and <b>fast-forward</b>. The backend supervisor starts automatically,
      refreshes active strategies every second, and keeps the live execution path highest priority.
    </div>

    <div class="hero">
      <div class="status-row">
        <span class="dot stopped" id="statusDot"></span>
        <span id="statusText">Loading...</span>
      </div>
      <div class="clock-box">
        <div class="clock-label">Server Time</div>
        <div class="clock-value" id="serverTime">--</div>
      </div>
    </div>

    <div class="grid">
      <div class="stat">
        <div class="stat-label">Trade Date</div>
        <div class="stat-value" id="tradeDateValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Live Count</div>
        <div class="stat-value" id="liveCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Started At</div>
        <div class="stat-value" id="startedAtValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Fast-Forward Count</div>
        <div class="stat-value" id="ffCountValue">0</div>
      </div>
      <div class="stat">
        <div class="stat-label">Last Tick</div>
        <div class="stat-value" id="lastTickValue">--</div>
      </div>
      <div class="stat">
        <div class="stat-label">Ticker Ticks</div>
        <div class="stat-value" id="tickCountValue">0</div>
      </div>
    </div>

    <div class="actions">
      <button class="btn btn-primary" id="toggleBtn" onclick="toggleMonitor()" disabled>Loading...</button>
      <button class="btn btn-secondary" onclick="refreshStatus()">Refresh</button>
    </div>

    <div class="panel">
      <div class="panel-title">Live Strategies</div>
      <div class="strategies" id="strategiesBox">
        <div class="empty">Checking active live strategies...</div>
      </div>
    </div>

    <div class="panel" style="margin-top: 14px;">
      <div class="panel-title">Fast-Forward Strategies</div>
      <div class="strategies" id="ffStrategiesBox">
        <div class="empty">Checking active fast-forward strategies...</div>
      </div>
    </div>
  </div>

  <script>
    function formatDateTime(value) {{
      if (!value) return '--';
      return String(value).replace('T', ' ').slice(0, 19);
    }}

    function escapeHtml(value) {{
      return String(value || '')
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
    }}

    async function startMonitorSilently() {{
      try {{
        await fetch('/monitor/start');
      }} catch (err) {{
        console.error(err);
      }}
    }}

    async function refreshStatus() {{
      try {{
        const res = await fetch('/monitor/status');
        const data = await res.json();
        renderStatus(data);
      }} catch (err) {{
        console.error(err);
      }}
    }}

    function renderStatus(data) {{
      const running = !!data.running;
      const status = data.monitor_status || (running ? 'running' : 'stopped');
      const button = document.getElementById('toggleBtn');
      const statusDot = document.getElementById('statusDot');
      const statusText = document.getElementById('statusText');
      const serverTime = document.getElementById('serverTime');
      const tradeDate = document.getElementById('tradeDateValue');
      const startedAt = document.getElementById('startedAtValue');
      const lastTick = document.getElementById('lastTickValue');
      const liveCountValue = document.getElementById('liveCountValue');
      const ffCountValue = document.getElementById('ffCountValue');
      const tickCountValue = document.getElementById('tickCountValue');
      const strategiesBox = document.getElementById('strategiesBox');
      const ffStrategiesBox = document.getElementById('ffStrategiesBox');

      statusDot.className = 'dot ' + (running ? 'running' : 'stopped');
      statusText.textContent = running ? 'Listening' : 'Stopped';
      serverTime.textContent = formatDateTime(data.server_time);
      tradeDate.textContent = data.trade_date || '--';
      startedAt.textContent = formatDateTime(data.started_at);
      lastTick.textContent = formatDateTime(data.last_tick_at);
      liveCountValue.textContent = String(((data.counts || {}).live) || 0);
      ffCountValue.textContent = String(((data.counts || {})['fast-forward']) || 0);
      tickCountValue.textContent = String(data.tick_count || 0);

      button.disabled = false;
      button.textContent = running ? 'Stop Listening' : 'Start Listening';
      button.className = 'btn ' + (running ? 'btn-danger' : 'btn-primary');
      button.dataset.running = running ? '1' : '0';

      const recordsByMode = data.records_by_mode || {{}};
      const liveRecords = Array.isArray(recordsByMode.live) ? recordsByMode.live : [];
      const ffRecords = Array.isArray(recordsByMode['fast-forward']) ? recordsByMode['fast-forward'] : [];

      function renderRecords(records, emptyText) {{
        if (!records.length) {{
          return '<div class="empty">' + emptyText + '</div>';
        }}
        return records.map(function(record) {{
          return (
            '<div class="strategy-item">' +
              '<div class="strategy-name">' + escapeHtml(record.name || '-') + '</div>' +
              '<div class="strategy-meta">' +
                'Group: ' + escapeHtml(record.group_name || '-') + '<br>' +
                'Ticker: ' + escapeHtml(record.ticker || '-') + '<br>' +
                'Mode: ' + escapeHtml(record.activation_mode || '-') + '<br>' +
                'Entry: ' + escapeHtml(record.entry_time || '-') + ' | Exit: ' + escapeHtml(record.exit_time || '-') + '<br>' +
                'Open Legs: ' + escapeHtml(record.open_legs || 0) + '/' + escapeHtml(record.total_legs || 0) +
              '</div>' +
            '</div>'
          );
        }}).join('');
      }}

      strategiesBox.innerHTML = renderRecords(
        liveRecords,
        'No active live strategies right now. Supervisor still keeps checking every second.'
      );
      ffStrategiesBox.innerHTML = renderRecords(
        ffRecords,
        'No active fast-forward strategies right now. Supervisor still keeps checking every second.'
      );
    }}

    async function toggleMonitor() {{
      const button = document.getElementById('toggleBtn');
      const running = button.dataset.running === '1';
      button.disabled = true;
      button.textContent = 'Please wait...';
      try {{
        const path = running ? '/monitor/stop' : '/monitor/start';
        await fetch(path);
      }} catch (err) {{
        console.error(err);
      }}
      setTimeout(refreshStatus, 400);
      setTimeout(refreshStatus, 1200);
    }}

    startMonitorSilently().then(function() {{
      refreshStatus();
      setInterval(refreshStatus, 1000);
    }});
  </script>
</body>
</html>"""


def _start_monitor_services(trade_date: str = '') -> dict:
    import threading
    import asyncio

    normalized_trade_date = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
    print(
        f'[MONITOR START REQUEST] '
        f'trade_date={normalized_trade_date} '
        f'ticker_status={ticker_manager.status} '
        f'tick_count={int(ticker_manager.tick_count or 0)}'
    )
    if ticker_manager.status not in ('running', 'connecting'):
        threading.Thread(target=_start_ticker_bg, daemon=True).start()
    live_fast_monitor_supervisor.start(trade_date=normalized_trade_date)
    try:
        live_entry_monitor.start(asyncio.get_running_loop())
    except RuntimeError:
        pass
    return {
        'ok': True,
        'message': 'Global monitor started',
        'trade_date': live_fast_monitor_supervisor.trade_date,
    }


def _build_monitor_status_payload() -> dict:
    supervisor_status = live_fast_monitor_supervisor.get_status()
    ticker_status = ticker_manager.get_status()
    return {
        'server_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'running': bool(supervisor_status.get('running')),
        'monitor_status': 'running' if bool(supervisor_status.get('running')) else 'stopped',
        'trade_date': str(supervisor_status.get('trade_date') or datetime.now().strftime('%Y-%m-%d')),
        'started_at': str(supervisor_status.get('started_at') or ''),
        'last_tick_at': str(supervisor_status.get('last_tick_at') or ''),
        'last_refresh_at': str(supervisor_status.get('last_refresh_at') or ''),
        'counts': supervisor_status.get('counts') or {},
        'records_by_mode': supervisor_status.get('records_by_mode') or {},
        'ticker_status': str(ticker_status.get('status') or ''),
        'tick_count': ticker_status.get('tick_count'),
        'ltp_count': ticker_status.get('ltp_count'),
        'spot_map': ticker_status.get('spot_map') or {},
        'ticker_error': str(ticker_status.get('error') or ''),
    }


def _build_live_ltp_payload(active_contracts: list[dict], now_ts: str) -> list[dict]:
    payload: list[dict] = []
    for contract in (active_contracts or []):
        token = str(contract.get("token") or "").strip()
        option_type = str(contract.get("option") or "").strip()
        if option_type == "SPOT":
            underlying = str(contract.get("underlying") or "").strip().upper()
            spot_price = float(ticker_manager.get_spot(underlying) or 0.0)
            if spot_price <= 0:
                continue
            payload.append({
                "token": token,
                "timestamp": now_ts,
                "ltp": spot_price,
                "bb_qty": 0,
                "bb_price": 0.0,
                "ba_qty": 0,
                "ba_price": 0.0,
                "vol_in_day": 0,
                "underlying": underlying,
                "option_type": "SPOT",
            })
            continue

        live_ltp = float(ticker_manager.get_ltp(token) or 0.0)
        if live_ltp <= 0:
            continue
        payload.append({
            "token": token,
            "timestamp": now_ts,
            "ltp": live_ltp,
            "bb_qty": 0,
            "bb_price": 0.0,
            "ba_qty": 0,
            "ba_price": 0.0,
            "vol_in_day": 0,
            "expiry": str(contract.get("expiry_date") or ""),
            "strike": contract.get("strike"),
            "option_type": option_type,
        })
    return payload


def _save_market_kite_session(session: dict) -> None:
    api_key = session.get("api_key") or str(getattr(get_kite_instance(), "api_key", "") or "").strip()
    access_token = session.get("access_token")
    login_time = datetime.now().isoformat()
    update_fields = {
        "broker": "kite",
        "api_key": api_key,
        "access_token": access_token,
        "login_time": login_time,
        "user_id": session.get("user_id"),
        "user_name": session.get("user_name"),
        "app_user_id": _resolve_app_user_id(),
    }
    local_db = MongoData()
    try:
        # Match by broker, not by whichever doc currently has enabled:True —
        # that used to match Dhan's doc whenever Dhan was the active broker,
        # overwriting its credentials with this Kite session. Each broker's
        # own login should never be able to touch another broker's doc.
        existing = local_db._db["kite_market_config"].find_one({"broker": "kite"}, {"api_secret": 1}) or {}
        api_secret = str(existing.get("api_secret") or "").strip()
        local_db._db["kite_market_config"].update_one(
            {"broker": "kite"},
            {"$set": update_fields},
            upsert=True,
        )
        from features.kite_broker import sync_kite_access_token_by_credentials
        sync_kite_access_token_by_credentials(
            local_db._db, api_key, api_secret, access_token, login_time,
            skip_collection="kite_market_config",
        )
    finally:
        local_db.close()


def _clear_market_kite_session() -> None:
    local_db = MongoData()
    try:
        local_db._db["kite_market_config"].update_one(
            {"enabled": True},
            {"$set": {"access_token": "", "login_time": datetime.now().isoformat()}},
            upsert=True,
        )
    finally:
        local_db.close()


def _get_kite_market_session_status() -> tuple[bool, str]:
    local_db = MongoData()
    try:
        cfg = local_db._db["kite_market_config"].find_one(
            {"enabled": True},
            {"access_token": 1, "api_key": 1, "user_id": 1, "broker": 1, "login_time": 1},
        ) or {}
        broker = str(cfg.get("broker") or "kite").strip().lower()
        access_token = str(cfg.get("access_token") or "").strip()
        login_time = str(cfg.get("login_time") or "").strip()

        if broker == "dhan":
            user_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
            if not user_id:
                return False, "Dhan config missing user_id in kite_market_config"
            if not access_token:
                return False, "Dhan access_token not found in kite_market_config"
            # Load into dhan_broker_ws cache so the WS can start
            try:
                from features.dhan_broker_ws import set_common_credentials  # type: ignore
                set_common_credentials(user_id, access_token)
            except Exception:
                pass
            # Validate via Dhan profile API
            try:
                import requests as _req  # type: ignore
                resp = _req.get(
                    "https://api.dhan.co/v2/profile",
                    headers={"access-token": access_token, "Content-Type": "application/json"},
                    timeout=10,
                )
                if resp.status_code == 200:
                    return True, "Dhan access token valid"
                return False, f"Dhan token invalid (HTTP {resp.status_code})"
            except Exception as exc:
                return False, f"Dhan token validation error: {exc}"

        # ── Kite path ──
        api_key = str(cfg.get("api_key") or "").strip()
        if not api_key:
            return False, (
                "Kite market config missing api_key"
                + (f" (login_time: {login_time})" if login_time else "")
            )
        if not access_token:
            return False, "Access token not found"
    finally:
        local_db.close()

    try:
        kite = get_kite_instance(access_token)
        kite.profile()
        return True, "Access token valid"
    except Exception as exc:
        try:
            _clear_market_kite_session()
        except Exception:
            pass
        return False, f"Access token invalid or expired: {exc}"


def _has_ready_kite_market_session() -> bool:
    is_ready, _ = _get_kite_market_session_status()
    return is_ready


def _build_monitor_dhan_token_page(trade_date: str = '', reason: str = '', retry_url: str = '') -> str:
    reason_text = str(reason or "Dhan access token not configured").strip()
    if not retry_url:
        retry_url = '/monitor/start' + (f'?trade_date={trade_date}' if trade_date else '')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dhan Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; min-height: 100vh; display: flex; align-items: center;
      justify-content: center;
      background: radial-gradient(circle at top, rgba(249,115,22,0.12), transparent 34%),
                  linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0; font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(480px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(249,115,22,0.22);
      border-radius: 28px; padding: 36px 28px;
      box-shadow: 0 28px 80px rgba(0,0,0,0.38); text-align: center;
    }}
    .badge {{
      display: inline-block; padding: 7px 16px; border-radius: 999px;
      background: rgba(249,115,22,0.12); border: 1px solid rgba(249,115,22,0.28);
      color: #fb923c; letter-spacing: 0.12em; font-size: 11px; text-transform: uppercase;
    }}
    h1 {{ margin: 18px 0 10px; font-size: 26px; line-height: 1.15; color: #f8fafc; }}
    p {{ margin: 0 auto 0; max-width: 400px; color: #94a3b8; line-height: 1.7; font-size: 15px; }}
    .reason {{ margin-top: 10px; color: #f87171; font-size: 13px; }}
    .actions {{ margin-top: 26px; display: flex; justify-content: center; gap: 12px; flex-wrap: wrap; }}
    .btn {{
      display: inline-flex; align-items: center; justify-content: center;
      min-width: 180px; padding: 14px 22px; border-radius: 14px;
      text-decoration: none; border: none; cursor: pointer;
      font-size: 15px; font-weight: 700; transition: opacity .15s;
    }}
    .btn:active {{ opacity: .8; }}
    .btn-dhan {{ background: linear-gradient(135deg, #f97316, #ea580c); color: #fff;
                 box-shadow: 0 8px 24px rgba(249,115,22,0.3); }}
    .btn-retry {{ background: #1e293b; color: #cbd5e1; border: 1px solid rgba(148,163,184,0.18); }}
    .hint {{ margin-top: 18px; color: #64748b; font-size: 13px; min-height: 1.4rem; }}
    .divider {{ margin: 24px 0 16px; border: none; border-top: 1px solid rgba(148,163,184,0.1); }}
    .manual-label {{
      font-size: 12px; color: #475569; cursor: pointer; text-decoration: underline;
      display: block; margin-bottom: 12px;
    }}
    .form-row {{ display: flex; flex-direction: column; gap: 10px; text-align: left; }}
    label {{ font-size: 13px; color: #94a3b8; margin-bottom: 2px; }}
    input {{
      width: 100%; padding: 11px 14px; border-radius: 10px;
      border: 1px solid rgba(148,163,184,0.22); background: rgba(30,41,59,0.8);
      color: #f1f5f9; font-size: 14px;
    }}
    input:focus {{ outline: none; border-color: #f97316; }}
    .btn-save {{
      width: 100%; margin-top: 12px; padding: 12px; border-radius: 10px;
      background: linear-gradient(135deg,#f97316,#ea580c); border: none;
      color: #fff; font-size: 15px; font-weight: 700; cursor: pointer;
    }}
    .err {{ color: #f87171; font-size: 13px; margin-top: 8px; display: none; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Dhan Login Required</div>
    <h1>Connect Dhan Account</h1>
    <p>Login with Dhan to start the live monitor. Your credentials are already saved — just click the button below.</p>
    <p class="reason">Reason: {reason_text}</p>

    <div class="actions">
      <button class="btn btn-dhan" onclick="openDhanLogin()">Login with Dhan →</button>
      <a class="btn btn-retry" href="{retry_url}">Retry</a>
    </div>
    <div class="hint" id="hintText">A Dhan login window will open. Complete login there and return here.</div>

    <hr class="divider">
    <span class="manual-label" onclick="document.getElementById('manualForm').style.display='block';this.style.display='none'">
      Or enter access token manually
    </span>
    <div id="manualForm" style="display:none">
      <div class="form-row">
        <div>
          <label>Dhan Client ID</label>
          <input id="clientId" type="text" placeholder="e.g. HA9835" autocomplete="off" />
        </div>
        <div>
          <label>Access Token</label>
          <input id="accessToken" type="password" placeholder="Paste Dhan access token" autocomplete="off" />
        </div>
      </div>
      <div class="err" id="errMsg"></div>
      <button class="btn-save" onclick="saveToken()">Save &amp; Start Monitor</button>
    </div>
  </div>
  <script>
    let _popup = null;

    function openDhanLogin() {{
      const hint = document.getElementById('hintText');
      hint.textContent = 'Opening Dhan login window...';
      _popup = window.open('/broker/dhan/login', 'DhanLogin',
        'width=420,height=560,resizable=yes,scrollbars=yes');
      if (!_popup) {{
        hint.textContent = 'Popup blocked — allow popups and try again, or use the link below.';
        return;
      }}
      hint.textContent = 'Complete login in the Dhan window. This page will refresh automatically.';
    }}

    window.addEventListener('message', function(e) {{
      if (!e.data || e.data.type !== 'DHAN_LOGIN') return;
      const hint = document.getElementById('hintText');
      if (e.data.success) {{
        hint.textContent = 'Login successful! Redirecting to monitor...';
        hint.style.color = '#22c55e';
        setTimeout(() => {{ window.location.href = {json.dumps(retry_url)}; }}, 1000);
      }} else {{
        hint.textContent = 'Login failed: ' + (e.data.message || 'Unknown error');
        hint.style.color = '#f87171';
      }}
    }});

    async function saveToken() {{
      const clientId    = document.getElementById('clientId').value.trim();
      const accessToken = document.getElementById('accessToken').value.trim();
      const err  = document.getElementById('errMsg');
      err.style.display = 'none';
      if (!clientId || !accessToken) {{
        err.textContent = 'Both Client ID and Access Token are required.';
        err.style.display = 'block';
        return;
      }}
      const hint = document.getElementById('hintText');
      hint.textContent = 'Saving credentials...';
      try {{
        const res = await fetch('/broker/dhan/config', {{
          method: 'POST',
          headers: {{'Content-Type': 'application/json'}},
          body: JSON.stringify({{ client_id: clientId, access_token: accessToken }}),
        }});
        const data = await res.json();
        if (!res.ok || data.status === 'error') {{
          err.textContent = data.message || 'Failed to save credentials.';
          err.style.display = 'block';
          hint.textContent = 'Save failed.';
          return;
        }}
        hint.textContent = 'Saved! Starting monitor...';
        window.location.href = {json.dumps(retry_url)};
      }} catch (e) {{
        err.textContent = 'Network error: ' + e.message;
        err.style.display = 'block';
      }}
    }}
  </script>
</body>
</html>"""


def _build_monitor_kite_login_page(trade_date: str = '', reason: str = '') -> str:
    normalized_trade_date = str(trade_date or '').strip()
    retry_url = "/monitor/start"
    if normalized_trade_date:
        retry_url += f"?trade_date={normalized_trade_date}"
    reason_text = str(reason or "No broker session found").strip()

    # Detect active broker and show appropriate page
    try:
        _local_db = MongoData()
        _cfg = _local_db._db["kite_market_config"].find_one({"enabled": True}, {"broker": 1}) or {}
        _local_db.close()
        if str(_cfg.get("broker") or "kite").strip().lower() == "dhan":
            return _build_monitor_dhan_token_page(
                trade_date=trade_date, reason=reason_text, retry_url=retry_url,
            )
    except Exception:
        pass
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Kite Login Required</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(59, 130, 246, 0.16), transparent 34%),
        linear-gradient(155deg, #07111f 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .card {{
      width: min(520px, calc(100vw - 32px));
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .badge {{
      display: inline-block;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.92);
      border: 1px solid rgba(148, 163, 184, 0.14);
      color: #7dd3fc;
      letter-spacing: 0.14em;
      font-size: 12px;
      text-transform: uppercase;
    }}
    h1 {{
      margin: 18px 0 12px;
      font-size: 32px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      color: #94a3b8;
      line-height: 1.7;
      font-size: 15px;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 220px;
      padding: 16px 22px;
      border-radius: 18px;
      text-decoration: none;
      border: none;
      cursor: pointer;
      font-size: 17px;
      font-weight: 700;
    }}
    .btn.primary {{
      background: linear-gradient(135deg, #38bdf8, #2563eb);
      color: #eff6ff;
      box-shadow: 0 16px 32px rgba(37, 99, 235, 0.24);
    }}
    .btn.secondary {{
      background: #1e293b;
      color: #cbd5e1;
      border: 1px solid rgba(148, 163, 184, 0.18);
    }}
    .hint {{
      margin-top: 18px;
      color: #7dd3fc;
      font-size: 13px;
    }}
  </style>
</head>
<body>
  <div class="card">
    <div class="badge">Kite Required</div>
    <h1>Connect Kite API First</h1>
    <p>
      Monitor start needs a valid Kite access token. Login popup will open, save the access token,
      and then this page will automatically start the server listener.
    </p>
    <p style="margin-top:14px;color:#7dd3fc;font-size:13px;">Reason: {reason_text}</p>
    <div class="actions">
      <button class="btn primary" onclick="openKiteLogin()">Connect Kite API</button>
      <a class="btn secondary" href="/monitor/stop">Open Stop Page</a>
    </div>
    <div class="hint" id="hintText">Waiting for Kite login...</div>
  </div>

  <script>
    let kitePopup = null;

    function openKiteLogin() {{
      kitePopup = window.open('/broker/kite/login', 'kiteLogin', 'width=540,height=720');
      if (!kitePopup) {{
        document.getElementById('hintText').textContent = 'Popup blocked. Please allow popups and click again.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login popup opened. Complete login to continue.';
    }}

    window.addEventListener('message', function(event) {{
      const data = event.data || {{}};
      if (data.type !== 'KITE_LOGIN') return;
      if (!data.success) {{
        document.getElementById('hintText').textContent = data.message || 'Kite login failed.';
        return;
      }}
      document.getElementById('hintText').textContent = 'Kite login successful. Starting monitor...';
      window.location.href = {json.dumps(retry_url)};
    }});

    setTimeout(openKiteLogin, 250);
  </script>
</body>
</html>"""


def _build_monitor_action_page(*, running: bool, trade_date: str = '') -> str:
    title = 'Monitor Running' if running else 'Monitor Stopped'
    status_text = 'Listening is active' if running else 'Listening is stopped'
    button_label = 'Stop Listening' if running else 'Start Listening'
    button_href = '/monitor/stop' if running else '/monitor/start'
    button_class = 'danger' if running else 'success'
    trade_date_text = str(trade_date or '').strip() or datetime.now().strftime('%Y-%m-%d')
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{title}</title>
  <style>
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      background:
        radial-gradient(circle at top, rgba(56, 189, 248, 0.18), transparent 32%),
        linear-gradient(155deg, #06101d 0%, #0f172a 58%, #111827 100%);
      color: #e2e8f0;
      font-family: "Segoe UI", Tahoma, sans-serif;
    }}
    .shell {{
      width: min(540px, calc(100vw - 32px));
      padding: 18px;
    }}
    .card {{
      background: rgba(9, 17, 31, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.16);
      border-radius: 28px;
      padding: 34px 28px;
      box-shadow: 0 28px 80px rgba(0, 0, 0, 0.38);
      text-align: center;
    }}
    .pill {{
      display: inline-flex;
      align-items: center;
      gap: 10px;
      padding: 9px 16px;
      border-radius: 999px;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid rgba(148, 163, 184, 0.14);
      font-size: 13px;
      letter-spacing: 0.14em;
      text-transform: uppercase;
      color: #cbd5e1;
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 999px;
      background: {('#22c55e' if running else '#ef4444')};
      box-shadow: 0 0 14px {('rgba(34, 197, 94, 0.85)' if running else 'rgba(239, 68, 68, 0.7)')};
    }}
    h1 {{
      margin: 20px 0 12px;
      font-size: 34px;
      line-height: 1.15;
      color: #f8fafc;
    }}
    p {{
      margin: 0 auto;
      max-width: 420px;
      font-size: 15px;
      line-height: 1.7;
      color: #94a3b8;
    }}
    .meta {{
      margin-top: 18px;
      font-size: 13px;
      color: #7dd3fc;
      letter-spacing: 0.06em;
      font-variant-numeric: tabular-nums;
    }}
    .actions {{
      margin-top: 28px;
      display: flex;
      justify-content: center;
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 240px;
      padding: 16px 24px;
      border-radius: 18px;
      text-decoration: none;
      font-size: 18px;
      font-weight: 700;
      transition: transform 0.12s ease, opacity 0.2s ease;
    }}
    .btn:active {{ transform: scale(0.985); }}
    .btn.success {{
      background: linear-gradient(135deg, #22c55e, #16a34a);
      color: #04110a;
      box-shadow: 0 16px 32px rgba(22, 163, 74, 0.28);
    }}
    .btn.danger {{
      background: linear-gradient(135deg, #fb7185, #ef4444);
      color: #fff7ed;
      box-shadow: 0 16px 32px rgba(239, 68, 68, 0.24);
    }}
    .link-row {{
      margin-top: 18px;
      font-size: 14px;
    }}
    .link-row a {{
      color: #7dd3fc;
      text-decoration: none;
    }}
  </style>
</head>
<body>
  <div class="shell">
    <div class="card">
      <div class="pill"><span class="dot"></span>{status_text}</div>
      <h1>{title}</h1>
      <p>
        Single monitor service for live and fast-forward is currently
        {'running and checking active strategies every second.' if running else 'stopped. Click below to start listening again.'}
      </p>
      <div class="meta">Trade Date: {trade_date_text}</div>
      <div class="actions">
        <a class="btn {button_class}" href="{button_href}">{button_label}</a>
      </div>
      <div class="link-row"><a href="/monitor">Open Full Monitor</a></div>
    </div>
  </div>
</body>
</html>"""


















# ─── Mock Ticker ──────────────────────────────────────────────────────────────

_MOCK_CONTROL_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Mock Ticker Control</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      background: #0f172a; color: #f1f5f9;
      min-height: 100vh; display: flex;
      align-items: center; justify-content: center;
    }
    .card {
      background: #1e293b; border: 1px solid #334155;
      border-radius: 16px; padding: 2.5rem 3rem;
      width: 460px; text-align: center;
    }
    .title {
      font-size: 1.25rem; font-weight: 600; color: #a78bfa;
      margin-bottom: 0.5rem; letter-spacing: 0.05em; text-transform: uppercase;
    }
    .subtitle {
      font-size: 0.75rem; color: #475569;
      margin-bottom: 2rem;
    }
    .status-row {
      display: flex; align-items: center; justify-content: center;
      gap: 0.6rem; margin-bottom: 1.5rem;
    }
    .dot {
      width: 10px; height: 10px; border-radius: 50%;
      background: #475569; transition: background 0.3s;
    }
    .dot.running    { background: #a78bfa; box-shadow: 0 0 8px #a78bfa; animation: pulse 1.5s infinite; }
    .dot.connecting { background: #f59e0b; animation: pulse 0.8s infinite; }
    .dot.stopped    { background: #ef4444; }
    .dot.error      { background: #ef4444; }
    @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.4} }
    .status-text { font-size: 1rem; font-weight: 500; color: #cbd5e1; text-transform: capitalize; }
    .mock-time-badge {
      font-size: 0.78rem; color: #a78bfa; margin-bottom: 1.25rem;
      font-variant-numeric: tabular-nums; min-height: 1.2em;
    }
    /* Time picker row — only shown when stopped */
    .time-row {
      display: flex; gap: 0.5rem; margin-bottom: 1.25rem;
    }
    .time-input {
      flex: 1; padding: 0.65rem 0.75rem;
      background: #0f172a; border: 1px solid #334155;
      border-radius: 8px; color: #e2e8f0; font-size: 0.875rem; outline: none;
      color-scheme: dark;
    }
    .time-input:focus { border-color: #7c3aed; }
    .btn {
      width: 100%; padding: 1rem; border: none; border-radius: 10px;
      font-size: 1.1rem; font-weight: 600; cursor: pointer;
      transition: opacity 0.2s, transform 0.1s; letter-spacing: 0.03em;
    }
    .btn:active { transform: scale(0.98); }
    .btn:disabled { opacity: 0.5; cursor: not-allowed; }
    .btn-start { background: #7c3aed; color: #fff; }
    .btn-start:hover:not(:disabled) { opacity: 0.9; }
    .btn-stop  { background: #ef4444; color: #fff; }
    .btn-stop:hover:not(:disabled)  { opacity: 0.9; }
    .stats {
      display: grid; grid-template-columns: 1fr 1fr 1fr;
      gap: 0.75rem; margin-top: 1.75rem;
    }
    .stat-box {
      background: #0f172a; border: 1px solid #1e293b;
      border-radius: 8px; padding: 0.75rem;
    }
    .stat-label { font-size: 0.65rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.3rem; }
    .stat-value { font-size: 1rem; font-weight: 700; color: #e2e8f0; }
    .spot-section { margin-top: 1.5rem; text-align: left; }
    .spot-title { font-size: 0.7rem; color: #64748b; text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
    .spot-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem; }
    .spot-item { background: #0f172a; border: 1px solid #1e293b; border-radius: 8px; padding: 0.5rem 0.75rem; display: flex; justify-content: space-between; align-items: center; }
    .spot-name  { font-size: 0.75rem; color: #94a3b8; font-weight: 600; }
    .spot-price { font-size: 0.85rem; color: #a78bfa; font-weight: 700; }
    .spot-price.na { color: #475569; }
    .error-msg { margin-top: 1rem; font-size: 0.8rem; color: #f87171; background: #1a0a0a; border-radius: 6px; padding: 0.5rem 0.75rem; display: none; }
    .started-at { margin-top: 1rem; font-size: 0.72rem; color: #475569; }
  </style>
</head>
<body>
<div class="card">
  <div class="title">Mock Ticker</div>
  <div class="subtitle">Simulates Kite WebSocket using historical DB data</div>

  <div class="status-row">
    <div class="dot" id="dot"></div>
    <span class="status-text" id="statusText">Loading...</span>
  </div>

  <div class="mock-time-badge" id="mockTimeBadge"></div>

  <!-- Time picker — hidden when running -->
  <div class="time-row" id="timeRow">
    <input class="time-input" type="datetime-local" id="mockTimeInput" step="60" />
  </div>

  <button class="btn btn-start" id="actionBtn" disabled onclick="handleAction()">...</button>

  <div class="stats">
    <div class="stat-box">
      <div class="stat-label">Ticks</div>
      <div class="stat-value" id="tickCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">LTP Tokens</div>
      <div class="stat-value" id="ltpCount">—</div>
    </div>
    <div class="stat-box">
      <div class="stat-label">Subscribed</div>
      <div class="stat-value" id="subCount">—</div>
    </div>
  </div>

  <div class="spot-section">
    <div class="spot-title">Mock Spot Prices</div>
    <div class="spot-grid">
      <div class="spot-item"><span class="spot-name">NIFTY</span><span class="spot-price na" id="spot-NIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">BANKNIFTY</span><span class="spot-price na" id="spot-BANKNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">FINNIFTY</span><span class="spot-price na" id="spot-FINNIFTY">—</span></div>
      <div class="spot-item"><span class="spot-name">SENSEX</span><span class="spot-price na" id="spot-SENSEX">—</span></div>
    </div>
  </div>

  <div class="error-msg" id="errorMsg"></div>
  <div class="started-at" id="startedAt"></div>
</div>

<script>
  const API = '';   // same origin

  async function fetchStatus() {
    try {
      const res  = await fetch(API + '/mock/status');
      const data = await res.json();
      renderStatus(data);
    } catch(e) {
      renderStatus({ status: 'error', error: 'Cannot reach server' });
    }
  }

  function renderStatus(data) {
    const status = data.status || 'stopped';
    document.getElementById('dot').className       = 'dot ' + status;
    document.getElementById('statusText').textContent = status;

    const btn      = document.getElementById('actionBtn');
    const timeRow  = document.getElementById('timeRow');
    const badgeEl  = document.getElementById('mockTimeBadge');

    btn.disabled = false;

    if (status === 'running' || status === 'connecting') {
      btn.textContent = 'Stop Mock Server';
      btn.className   = 'btn btn-stop';
      if (status === 'connecting') btn.disabled = true;
      timeRow.style.display = 'none';
      badgeEl.textContent   = data.mock_time
        ? '\\u25B6 Simulating: ' + data.mock_time.replace('T', ' ')
        : '';
    } else {
      btn.textContent       = 'Start Listening';
      btn.className         = 'btn btn-start';
      timeRow.style.display = 'flex';
      const inputEl = document.getElementById('mockTimeInput');
      if (inputEl && data.mock_time) {
        inputEl.value = data.mock_time.slice(0, 16);
      }
      badgeEl.textContent   = data.mock_time
        ? 'Last stopped at: ' + data.mock_time.replace('T', ' ')
        : 'Set simulation start time above';
    }

    document.getElementById('tickCount').textContent =
      data.tick_count !== undefined ? data.tick_count.toLocaleString() : '—';
    document.getElementById('ltpCount').textContent =
      data.ltp_count !== undefined ? data.ltp_count.toLocaleString() : '—';
    document.getElementById('subCount').textContent =
      data.subscribed_tokens !== undefined ? data.subscribed_tokens.toLocaleString() : '—';

    const spotMap = data.spot_map || {};
    ['NIFTY','BANKNIFTY','FINNIFTY','SENSEX'].forEach(sym => {
      const el  = document.getElementById('spot-' + sym);
      const val = spotMap[sym];
      if (!el) return;
      if (val) {
        el.textContent = '\\u20B9' + Number(val).toLocaleString('en-IN', { minimumFractionDigits: 2 });
        el.className = 'spot-price';
      } else {
        el.textContent = '—';
        el.className = 'spot-price na';
      }
    });

    const errEl = document.getElementById('errorMsg');
    if (data.error) { errEl.textContent = data.error; errEl.style.display = 'block'; }
    else            { errEl.style.display = 'none'; }

    const startEl = document.getElementById('startedAt');
    startEl.textContent = data.started_at
      ? 'Started: ' + data.started_at.replace('T',' ').slice(0,19)
      : '';
  }

  async function handleAction() {
    const btn    = document.getElementById('actionBtn');
    const status = document.getElementById('statusText').textContent;
    btn.disabled    = true;
    btn.textContent = 'Please wait...';

    try {
      if (status === 'running') {
        await fetch(API + '/mock/stop');
      } else {
        const raw = document.getElementById('mockTimeInput').value;
        if (!raw) {
          await fetch(API + '/mock/start');
        } else {
          const timeStr = raw.length === 16 ? raw + ':00' : raw;
          await fetch(API + '/mock/start?time=' + encodeURIComponent(timeStr));
        }
      }
    } catch(e) { console.error(e); }

    setTimeout(fetchStatus, 600);
    setTimeout(fetchStatus, 1800);
    setTimeout(fetchStatus, 4000);
  }

  fetchStatus();
  setInterval(fetchStatus, 2000);
</script>
</body>
</html>"""


def _start_mock_bg(time_str: str) -> None:
    """Run in a daemon thread — sets mock time then starts MockTicker."""
    result = mock_ticker_manager.set_mock_time(time_str)
    if not result.get("ok"):
        import logging
        logging.getLogger(__name__).error("mock set_mock_time failed: %s", result)
        return
    _db = MongoData()
    try:
        mock_ticker_manager.start(_db)
    except Exception as exc:
        import logging
        logging.getLogger(__name__).error("mock start error: %s", exc)
    finally:
        try:
            _db.close()
        except Exception:
            pass


def _upsert_contracts_into_col(
    active_tokens_col,
    contracts: list[dict],
    stock_name: str,
    now_ts: str,
    broker: str = "",
) -> tuple[int, int]:
    if not contracts:
        return 0, 0

    from pymongo import UpdateOne

    _idx_set = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    instrument_type = "index" if stock_name.upper() in _idx_set else "stock"
    ops = []
    for contract in contracts:
        expiry_val = str(contract.get("expiry") or "").strip()[:10]
        opt_type_val = str(contract.get("option_type") or contract.get("opt_type") or "").strip().upper()
        strike_val = contract.get("strike")
        query: dict = {
            "instrument": stock_name,
            "expiry": expiry_val,
            "strike": strike_val,
            "option_type": opt_type_val,
        }
        if broker:
            query["broker"] = broker
        update_payload: dict = {
            "instrument": stock_name,
            "instrument_type": instrument_type,
            "expiry": expiry_val,
            "strike": strike_val,
            "option_type": opt_type_val,
            "token": str(contract.get("token") or "").strip(),
            "tokens": str(contract.get("tokens") or contract.get("token") or "").strip(),
            "symbol": str(contract.get("symbol") or "").strip(),
            "exchange": str(contract.get("exchange") or "").strip(),
            "updated_at": now_ts,
        }
        if broker:
            update_payload["broker"] = broker
        ops.append(UpdateOne(
            query,
            {"$set": update_payload, "$setOnInsert": {"created_at": now_ts}},
            upsert=True,
        ))

    # Batched in one round-trip per call (ordered=False so one bad op can't stall the rest)
    # instead of one update_one() per contract — this is what made syncing thousands of
    # contracts take tens of seconds.
    result = active_tokens_col.bulk_write(ops, ordered=False)
    created = result.upserted_count
    updated = result.matched_count
    return created, updated


def _sync_active_option_tokens(instrument: str) -> dict:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    today_str = datetime.now().strftime("%Y-%m-%d")
    db = MongoData()
    try:
        credentials_loaded = load_credentials_from_db(db)
        active_tokens_col = db._db["active_option_tokens"]
        try:
            active_tokens_col.create_index(
                [("broker", 1), ("instrument", 1), ("expiry", 1), ("strike", 1), ("option_type", 1)],
                name="idx_active_option_contract_v2",
            )
        except Exception:
            pass

        from features.broker_gateway import _active_broker as _sync_get_broker  # type: ignore
        active_broker = _sync_get_broker()

        _INDEX_SET = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}

        # Special case: iterate ALL non-index FNO stock underlyings
        if normalized_instrument == "FNO-STOCKS":
            now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            created_count = 0
            updated_count = 0
            contracts_processed = 0
            all_expiries: set[str] = set()

            if active_broker == "dhan":
                # Clear existing FNO stock contracts (expired + stale) before re-inserting
                deleted = active_tokens_col.delete_many({
                    "broker": "dhan",
                    "instrument_type": "stock",
                })
                if deleted.deleted_count == 0:
                    # First run — no instrument_type field yet, clear by excluding known indices
                    active_tokens_col.delete_many({
                        "broker": "dhan",
                        "instrument": {"$nin": list(_INDEX_SET)},
                    })

                # Dhan: use CSV master directly (avoids circular DB read)
                master = _get_dhan_fno_master()
                for symbol, all_contracts in master.items():
                    for c in all_contracts:
                        exp = str(c.get("expiry") or "").strip()[:10]
                        if not exp or exp < today_str:
                            continue
                        all_expiries.add(exp)
                        contracts_processed += 1
                        opt_type = str(c.get("opt_type") or "").strip().upper()
                        query = {
                            "broker": "dhan",
                            "instrument": symbol,
                            "expiry": exp,
                            "strike": c.get("strike"),
                            "option_type": opt_type,
                        }
                        payload = {
                            "broker": "dhan",
                            "instrument": symbol,
                            "instrument_type": "stock",
                            "expiry": exp,
                            "strike": c.get("strike"),
                            "option_type": opt_type,
                            "token": str(c.get("sec_id") or "").strip(),
                            "tokens": str(c.get("sec_id") or "").strip(),
                            "symbol": f"{symbol}{int(c['strike']) if float(c['strike']).is_integer() else c['strike']}{opt_type}",
                            "exchange": str(c.get("exchange") or "NSE").strip(),
                            "lot_size": c.get("lot_size"),
                            "updated_at": now_ts,
                        }
                        res = active_tokens_col.update_one(
                            query,
                            {"$set": payload, "$setOnInsert": {"created_at": now_ts}},
                            upsert=True,
                        )
                        if res.upserted_id is not None:
                            created_count += 1
                        elif res.matched_count:
                            updated_count += 1
            else:
                # Kite: load from Kite REST instruments API
                from features.spot_atm_utils import (  # type: ignore
                    _load_kite_instruments as _kite_inst_load,
                    list_kite_option_contracts as _kite_list_contracts,
                )
                known_indices = set(KITE_INDEX_TOKENS.keys())
                cache = _kite_inst_load(force=True)
                if not cache:
                    return {
                        "instrument": "FNO-STOCKS",
                        "expiries": [],
                        "contracts_processed": 0,
                        "created": 0,
                        "updated": 0,
                        "message": "No active option contracts found",
                        "credentials_loaded": credentials_loaded,
                        "hint": "Check kite_market_config access_token/login if this instrument should have live contracts",
                    }

                underlyings: dict[str, set[str]] = {}
                for (name, exp, _strike, _type) in cache:
                    if name not in known_indices and exp >= today_str:
                        underlyings.setdefault(name, set()).add(exp)

                for stock_name, expiry_set in underlyings.items():
                    for expiry in sorted(expiry_set):
                        contracts = _kite_list_contracts(stock_name, expiry)
                        all_expiries.update(expiry_set)
                        contracts_processed += len(contracts)
                        c, u = _upsert_contracts_into_col(
                            active_tokens_col, contracts, stock_name, now_ts, broker="kite"
                        )
                        created_count += c
                        updated_count += u

            return {
                "instrument": "FNO-STOCKS",
                "underlyings_count": contracts_processed,
                "expiries": sorted(all_expiries),
                "contracts_processed": contracts_processed,
                "created": created_count,
                "updated": updated_count,
                "credentials_loaded": credentials_loaded,
                "message": "active_option_tokens sync completed" if contracts_processed else "No active option contracts found",
            }

        expiries = get_kite_expiries(normalized_instrument, today_str, force_refresh=True)
        if not expiries:
            return {
                "instrument": normalized_instrument,
                "expiries": [],
                "contracts_processed": 0,
                "created": 0,
                "updated": 0,
                "message": "No active option contracts found",
                "credentials_loaded": credentials_loaded,
                "hint": (
                    "Check kite_market_config access_token/login if this instrument should have live contracts"
                ),
            }

        now_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        created_count = 0
        updated_count = 0
        contracts_processed = 0

        for expiry_index, expiry in enumerate(expiries):
            contracts = list_kite_option_contracts(
                normalized_instrument,
                expiry,
                force_refresh=(expiry_index == 0),
            )
            contracts_processed += len(contracts)
            c, u = _upsert_contracts_into_col(
                active_tokens_col, contracts, normalized_instrument, now_ts, broker=active_broker
            )
            created_count += c
            updated_count += u

        return {
            "instrument": normalized_instrument,
            "expiries": expiries,
            "contracts_processed": contracts_processed,
            "created": created_count,
            "updated": updated_count,
            "credentials_loaded": credentials_loaded,
            "message": "active_option_tokens sync completed",
        }
    finally:
        db.close()


def _get_live_index_spot_price(normalized_instrument: str) -> float:
    index_token = KITE_INDEX_TOKENS.get(normalized_instrument)
    if not index_token:
        return 0.0
    try:
        from features.broker_gateway import get_broker_ltp_map  # type: ignore

        ltp_value = (get_broker_ltp_map() or {}).get(str(index_token), 0.0)
        return float(ltp_value or 0.0)
    except Exception:
        return 0.0


def _resolve_single_option_ltp(
    db,
    underlying: str,
    expiry: str,
    strike: float,
    option_type: str,
) -> float:
    normalized_underlying = str(underlying or "").strip().upper()
    normalized_expiry = str(expiry or "").strip()[:10]
    normalized_option_type = str(option_type or "").strip().upper()

    contract = {}
    try:
        contract = db["active_option_tokens"].find_one(
            {
                "instrument": normalized_underlying,
                "expiry": normalized_expiry,
                "strike": strike,
                "option_type": normalized_option_type,
            },
            {
                "_id": 0,
                "token": 1,
                "tokens": 1,
                "symbol": 1,
            },
        ) or {}
    except Exception:
        contract = {}

    token = str(contract.get("token") or contract.get("tokens") or "").strip()
    symbol = str(contract.get("symbol") or "").strip()
    if not token:
        try:
            inst = (_load_kite_instruments() or {}).get(
                (normalized_underlying, normalized_expiry, float(strike), normalized_option_type)
            ) or {}
            token = str(inst.get("token") or "").strip()
            symbol = str(inst.get("symbol") or "").strip()
        except Exception:
            token = ""

    if not token:
        log.warning(
            "margin quote token not found underlying=%s expiry=%s strike=%s option_type=%s",
            normalized_underlying,
            normalized_expiry,
            strike,
            normalized_option_type,
        )
        return 0.0

    try:
        live_ltp = float((get_ltp_map() or {}).get(token, 0.0) or 0.0)
        if live_ltp > 0:
            return live_ltp
    except Exception:
        pass

    try:
        if not is_configured():
            return 0.0
        api_key, access_token = get_common_credentials()
        if not api_key or not access_token:
            return 0.0
        kite = get_kite_instance(access_token)
        quotes = kite.quote([int(token)]) or {}
        for _quote_key, quote_doc in quotes.items():
            quote_ltp = float(
                quote_doc.get("last_price")
                or (quote_doc.get("ohlc") or {}).get("close")
                or 0.0
            )
            if quote_ltp > 0:
                print(
                    f"[MARGIN SINGLE QUOTE] underlying={normalized_underlying} "
                    f"expiry={normalized_expiry} strike={strike} type={normalized_option_type} "
                    f"token={token} symbol={symbol or '-'} ltp={quote_ltp}",
                    flush=True,
                )
                return quote_ltp
    except Exception as exc:
        log.warning(
            "margin single quote error underlying=%s expiry=%s strike=%s option_type=%s token=%s: %s",
            normalized_underlying,
            normalized_expiry,
            strike,
            normalized_option_type,
            token,
            exc,
        )

    return 0.0


def _resolve_margin_order_contract(
    db,
    underlying: str,
    instrument_type: str,
    expiry: str,
    strike: float,
) -> dict[str, Any]:
    normalized_underlying = str(underlying or "").strip().upper()
    normalized_instrument_type = str(instrument_type or "").strip().upper()
    normalized_expiry = str(expiry or "").strip()[:10]

    if normalized_instrument_type in {"CE", "PE"}:
        contract = db["active_option_tokens"].find_one(
            {
                "instrument": normalized_underlying,
                "expiry": normalized_expiry,
                "strike": strike,
                "option_type": normalized_instrument_type,
            },
            {
                "_id": 0,
                "symbol": 1,
                "exchange": 1,
            },
        ) or {}
        symbol = str(contract.get("symbol") or "").strip()
        exchange = str(contract.get("exchange") or "").strip() or ("BFO" if normalized_underlying in {"SENSEX", "BANKEX"} else "NFO")
        if symbol:
            return {"tradingsymbol": symbol, "exchange": exchange}
    return {}


def _calculate_kite_basket_margin(db, legs: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not legs or not is_configured():
        return None

    api_key, access_token = get_common_credentials()
    if not api_key or not access_token:
        return None

    orders: list[dict[str, Any]] = []
    for leg in legs:
        contract = _resolve_margin_order_contract(
            db,
            leg.get("underlying"),
            leg.get("instrument_type"),
            leg.get("expiry"),
            float(leg.get("strike") or 0.0),
        )
        tradingsymbol = str(contract.get("tradingsymbol") or "").strip()
        exchange = str(contract.get("exchange") or "").strip()
        quantity = int(leg.get("quantity") or 0) * int(leg.get("lot_size") or 0)
        if not tradingsymbol or not exchange or quantity <= 0:
            return None
        orders.append(
            {
                "exchange": exchange,
                "tradingsymbol": tradingsymbol,
                "transaction_type": str(leg.get("transaction_type") or "SELL").upper(),
                "variety": "regular",
                "product": "NRML",
                "order_type": "MARKET",
                "quantity": quantity,
                "price": 0,
                "trigger_price": 0,
            }
        )

    try:
        kite = get_kite_instance(access_token)
        return kite.basket_order_margins(orders, consider_positions=False) or {}
    except Exception as exc:
        log.warning("kite basket margin error: %s", exc)
        return None


def _build_full_option_chain_response(instrument: str) -> dict[str, Any]:
    normalized_instrument = str(instrument or "").strip().upper()
    if not normalized_instrument:
        raise HTTPException(status_code=400, detail="Instrument is required")

    allowed_instruments = {"NIFTY", "BANKNIFTY", "SENSEX", "FINNIFTY", "MIDCPNIFTY"}
    if normalized_instrument not in allowed_instruments:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported instrument '{normalized_instrument}'. "
                "Use one of: NIFTY, BANKNIFTY, SENSEX, FINNIFTY, MIDCPNIFTY"
            ),
        )

    cached_base = _get_active_option_chain_cache(normalized_instrument)
    if not cached_base:
        raise HTTPException(
            status_code=404,
            detail=f"No option chain rows found in active_option_tokens for instrument {normalized_instrument}",
        )

    response = deepcopy(cached_base)
    return {
        **response,
        "spot_price": _get_live_index_spot_price(normalized_instrument),
    }


















_INDEX_KITE_SYMBOLS: dict[str, str] = {
    "NIFTY":      "NSE:NIFTY 50",
    "BANKNIFTY":  "NSE:NIFTY BANK",
    "FINNIFTY":   "NSE:NIFTY FIN SERVICE",
    "SENSEX":     "BSE:SENSEX",
    "MIDCPNIFTY": "NSE:NIFTY MID SELECT",
}


def _get_kite_rest_client():
    """Return a configured broker REST client using DB credentials, or None."""
    try:
        from features.broker_gateway import get_broker_rest_client  # type: ignore
        return get_broker_rest_client()
    except Exception:
        return None


# NSE option chain in-process cache — keyed by "SYMBOL:YYYY-MM-DD"
_nse_chain_cache: dict[str, tuple[float, dict]] = {}
_nse_chain_cache_lock = threading.Lock()
_NSE_CHAIN_CACHE_TTL = 60.0  # seconds

# India VIX NSE-API fallback cache — see get_live_greeks_chain's VIX section.
_india_vix_cache: dict[str, tuple[float, float]] = {}
_INDIA_VIX_CACHE_TTL = 60.0  # seconds


def _resolve_chain_reference_spot(
    rows_by_side: dict[str, dict[float, dict]],
    spot_price: float,
    T: float,
    r: float,
    q: float,
) -> float:
    """
    Convert the ATM synthetic future into a spot-equivalent reference price.

    When spot_price is 0 (equity spot fetch failed), estimates spot from
    put-call parity by finding the strike where |CE_ltp - PE_ltp| is minimum.
    """
    ce_by_strike = rows_by_side.get("CE") or {}
    pe_by_strike = rows_by_side.get("PE") or {}
    common_strikes = [
        strike
        for strike in set(ce_by_strike) & set(pe_by_strike)
        if float((ce_by_strike.get(strike) or {}).get("ltp") or 0) > 0
        and float((pe_by_strike.get(strike) or {}).get("ltp") or 0) > 0
    ]
    if not common_strikes:
        return spot_price

    if spot_price > 0:
        atm_strike = min(common_strikes, key=lambda strike: abs(strike - spot_price))
    else:
        # Estimate ATM via put-call parity: strike where |CE - PE| is minimized
        atm_strike = min(
            common_strikes,
            key=lambda strike: abs(
                float((ce_by_strike.get(strike) or {}).get("ltp") or 0)
                - float((pe_by_strike.get(strike) or {}).get("ltp") or 0)
            ),
        )

    ce_ltp = float((ce_by_strike.get(atm_strike) or {}).get("ltp") or 0)
    pe_ltp = float((pe_by_strike.get(atm_strike) or {}).get("ltp") or 0)
    synthetic_future = atm_strike + ce_ltp - pe_ltp
    if synthetic_future <= 0:
        return spot_price

    # Convert forward/synthetic reference back to a BSM-compatible spot input.
    return synthetic_future * math.exp(-(r - q) * max(T, 0.0))


def _fetch_nse_chain_data(symbol: str, expiry_iso: str) -> dict:
    """
    Fetch LTP + OI + spot from NSE option chain for a symbol + expiry.
    Returns {"spot": float, "chain": {"24500_CE": {"ltp": 22.3, "oi": 131000}, ...}}
    Results are cached for 60 seconds to avoid repeated slow HTTP calls.
    """
    import requests as _req
    from datetime import datetime as _dt

    cache_key = f"{symbol.upper()}:{expiry_iso[:10]}"
    _now = time.monotonic()
    with _nse_chain_cache_lock:
        _hit = _nse_chain_cache.get(cache_key)
        if _hit and (_now - _hit[0]) < _NSE_CHAIN_CACHE_TTL:
            return _hit[1]

    try:
        expiry_dt = _dt.strptime(expiry_iso[:10], "%Y-%m-%d")
        _day = expiry_dt.strftime("%d").lstrip("0")
        _mon = expiry_dt.strftime("%b")
        _yr  = expiry_dt.strftime("%Y")
        expiry_nse_dash  = f"{_day}-{_mon}-{_yr}"   # "23-Jun-2026"
        expiry_nse_space = f"{_day} {_mon} {_yr}"   # "23 Jun 2026"
    except Exception:
        expiry_nse_dash = expiry_nse_space = ""

    _INDICES = {"NIFTY", "BANKNIFTY", "FINNIFTY", "MIDCPNIFTY", "SENSEX", "BANKEX"}
    is_index = symbol.upper() in _INDICES
    url = (
        f"https://www.nseindia.com/api/option-chain-indices?symbol={symbol.upper()}"
        if is_index
        else f"https://www.nseindia.com/api/option-chain-equities?symbol={symbol.upper()}"
    )

    empty: dict = {"spot": 0.0, "chain": {}}
    try:
        sess = _req.Session()
        sess.get("https://www.nseindia.com", timeout=5,
                 headers={"User-Agent": "Mozilla/5.0", "Accept": "*/*"})
        r = sess.get(url, timeout=10, headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.nseindia.com",
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
        })
        if r.status_code != 200:
            log.warning("[NSE CHAIN] %s HTTP %s", symbol, r.status_code)
            return empty
        records = r.json().get("records") or {}
        data_rows = records.get("data") or []
        spot = float(records.get("underlyingValue") or 0)
        chain: dict[str, dict] = {}
        for row in data_rows:
            _row_expiry = str(row.get("expiryDate") or "").strip()
            if expiry_nse_dash and _row_expiry not in (expiry_nse_dash, expiry_nse_space):
                continue
            strike = row.get("strikePrice")
            if strike is None:
                continue
            strike_int = int(float(strike))
            if not spot:
                spot = float(row.get("CE", {}).get("underlyingValue") or row.get("PE", {}).get("underlyingValue") or 0)
            for opt_type in ("CE", "PE"):
                opt_data = row.get(opt_type) or {}
                chain[f"{strike_int}_{opt_type}"] = {
                    "ltp": float(opt_data.get("lastPrice") or 0),
                    "oi":  int(opt_data.get("openInterest") or 0),
                }
        result = {"spot": spot, "chain": chain}
        if chain:
            with _nse_chain_cache_lock:
                _nse_chain_cache[cache_key] = (time.monotonic(), result)
        return result
    except Exception as _e:
        log.warning("[NSE CHAIN] %s error: %s", symbol, _e)
        return empty


def _fetch_nse_oi_map(symbol: str, expiry_iso: str) -> dict[str, int]:
    """Backward-compat wrapper — returns only OI map."""
    return {k: v["oi"] for k, v in _fetch_nse_chain_data(symbol, expiry_iso).get("chain", {}).items()}


# f"{segment}:{sec_id}" → last-seen-good market-data dict. Never evicted —
# see the resilience note in _fetch_dhan_market_data()'s docstring below.
_DHAN_MARKET_DATA_LAST_GOOD: dict[str, dict] = {}


def _fetch_dhan_market_data(segment: str, sec_ids: list[int], db) -> dict[str, dict]:
    """
    Fetch LTP + OI + best bid/ask from Dhan /marketfeed/quote for a list of security IDs.
    Returns {str(sec_id): {"ltp": float, "oi": int, "bid": float, "ask": float, "prev_close": float}}.
    Dhan /quote supports up to 1000 per segment — send as few requests as possible.

    WS-first + last-good fallback, same resilience as
    features.broker_gateway.get_broker_rest_quotes: Dhan's REST quote
    endpoint rate-limits to ~1 req/sec per account, and this function used
    to retry a 429 with a blocking time.sleep(1s/2s/3s) per batch — on
    /live-greeks-chain, which calls this 2+ times sequentially (equity
    spot, then the whole NSE_FNO/BSE_FNO chain), that alone could add
    several seconds to one page load. A WS ltp_map hit resolves a sec_id
    with zero REST round trip; a 429/failed REST attempt now falls straight
    back to the last real value seen for that sec_id instead of blocking
    to retry.
    """
    if not sec_ids:
        return {}
    raw_db = db._db if hasattr(db, "_db") else db
    cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    access_token = str(cfg.get("access_token") or "").strip()
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    if not access_token or not client_id:
        return {}

    result: dict[str, dict] = {}

    # WS ltp_map/oi_map are keyed by bare numeric security id regardless of
    # segment (index/equity/FNO ticks all land there — see dhan_ticker.py's
    # binary parser), so a hit here is an in-memory read, no REST call at all.
    try:
        from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore
        for sid in sec_ids:
            sid_str = str(sid)
            ws_ltp = float(_dtm.ltp_map.get(sid_str) or 0)
            if ws_ltp > 0:
                cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}") or {}
                result[sid_str] = {
                    "ltp": ws_ltp,
                    "oi": int(_dtm.oi_map.get(sid_str) or cached.get("oi", 0)),
                    "bid": cached.get("bid", 0.0),
                    "ask": cached.get("ask", 0.0),
                    "prev_close": cached.get("prev_close", 0.0),
                }
    except Exception:
        pass

    missing = [sid for sid in sec_ids if str(sid) not in result]
    if missing:
        from features.broker_gateway import dhan_quote_post_blocking

        _BATCH = 500  # Dhan /quote supports up to 1000 per segment
        batches = [missing[i: i + _BATCH] for i in range(0, len(missing), _BATCH)]

        for batch in batches:
            # Up to 3 tries: a single transient 429/5xx from Dhan (real
            # per-account rate limit, not just our internal gate — momentary
            # under genuinely heavy concurrent demand from other features
            # sharing this same gate) used to surface as a flat ltp=0 for the
            # whole chain with no second chance. wait_for_dhan_slot() inside
            # dhan_quote_post_blocking() already spaces retries >=1.05s apart.
            for _attempt in range(3):
                try:
                    # Blocking, not skip-on-busy: this is usually called right
                    # after a spot-price quote on the same rate gate (e.g.
                    # get_live_greeks_chain fetches index spot, then the whole
                    # chain, microseconds apart) — skip-on-busy made the second
                    # call lose that race almost every time, rendering the
                    # whole chain as ltp=0. See dhan_quote_post_blocking's docstring.
                    r = dhan_quote_post_blocking({segment: batch}, access_token, client_id, timeout=15.0)
                    if r is None:
                        continue
                    if r.status_code == 200:
                        raw = r.json()
                        data = (raw.get("data") or raw).get(segment) or {}
                        for sid, info in data.items():
                            if not isinstance(info, dict):
                                continue
                            depth = info.get("depth") or {}
                            buy_levels = depth.get("buy") or []
                            sell_levels = depth.get("sell") or []
                            entry = {
                                "ltp": float(info.get("last_price") or 0),
                                "oi":  int(info.get("oi") or 0),
                                # Best bid/ask (level 0) — 0 if that side of the book is empty.
                                "bid": float((buy_levels[0] or {}).get("price") or 0) if buy_levels else 0.0,
                                "ask": float((sell_levels[0] or {}).get("price") or 0) if sell_levels else 0.0,
                                # Previous trading day's close — Dhan's own quote response
                                # already carries this in ohlc.close, additive field so
                                # nothing keying off just ['ltp']/['oi'] etc. is affected.
                                "prev_close": float((info.get("ohlc") or {}).get("close") or 0),
                            }
                            result[str(sid)] = entry
                            if entry["ltp"] > 0:
                                _DHAN_MARKET_DATA_LAST_GOOD[f"{segment}:{sid}"] = entry
                        break
                    else:
                        # Most commonly a 429 — retry a couple times (spaced
                        # by the shared gate) before giving up to the
                        # last-good backfill below.
                        log.warning("[DHAN QUOTE] segment=%s status=%d attempt=%d body=%s",
                                    segment, r.status_code, _attempt, r.text[:200])
                except Exception as _e:
                    log.warning("[DHAN QUOTE] error=%s attempt=%d", _e, _attempt)

    for sid in sec_ids:
        sid_str = str(sid)
        if sid_str not in result or not result[sid_str].get("ltp"):
            cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}")
            if cached:
                result[sid_str] = cached

    return result


def _fetch_dhan_ltp(segment: str, sec_ids: list[int], db) -> dict[str, float]:
    """Convenience wrapper — returns {str(sec_id): ltp}."""
    return {k: v["ltp"] for k, v in _fetch_dhan_market_data(segment, sec_ids, db).items()}


@app.on_event("startup")
def _dhan_equity_master_prewarm_startup():
    """
    Warm _FNO_MASTER_CACHE (this process's own copy — module-level dict,
    NOT shared with algo.websocket's separate prewarm of the same CSV) in a
    background thread at boot, so the first real caller of
    _get_dhan_equity_sec_id()/_get_dhan_fno_master() (e.g. /rest-option-chain)
    doesn't pay the ~5-6s Dhan scrip-master CSV download+parse cost inline.
    Confirmed live (2026-08-06, on algo.trade before this endpoint moved
    here): this was ~82% of one /rest-option-chain request's total time
    (5.6s of 6.8s) — the Dhan quote call itself was ~1.2s. Every process
    keeps its own process-local cache, so this needs to run in this
    process too, not just in algo.websocket's dhan_ticker.py.
    """
    def _warm():
        try:
            t0 = time.perf_counter()
            master = _get_dhan_fno_master()
            equity_count = len(_FNO_MASTER_CACHE.get("equity_ids") or {})
            print(f"[EQUITY MASTER PREWARM] symbols={len(master)} equities={equity_count} "
                  f"took={(time.perf_counter() - t0) * 1000:.0f}ms", flush=True)
        except Exception as exc:
            log.warning("[EQUITY MASTER PREWARM] error: %s", exc)

    threading.Thread(target=_warm, daemon=True, name="dhan_equity_master_prewarm").start()


def _fetch_dhan_market_data_multi_nowait(sids_by_segment: dict[str, list[int]], db) -> dict[str, dict[str, dict]]:
    """
    Sends EVERY segment (e.g. NSE_FNO chain tokens + NSE_EQ spot token) in
    ONE Dhan /marketfeed/quote request body instead of one call per segment —
    Dhan's quote endpoint accepts multiple top-level segment keys in a
    single request and returns data keyed by segment, which our parsing
    already assumed (see `data.get(segment)` below). This uses exactly ONE
    shared rate-gate slot for the whole /rest-option-chain response instead
    of two, roughly halving the total wait versus a two-separate-calls version.

    Up to 2 blocking attempts (dhan_quote_post_blocking, which waits for its
    own rate-gate slot each time) — NOT a pure skip-if-busy single attempt:
    that returned instantly but empty (0/no data) far too often for a stock
    queried for the first time in this process (no last-good cache yet to
    fall back on), confirmed live. The consistently observed pattern that
    day was "attempt 1 gets 429, attempt 2 succeeds" (Dhan's real
    per-account window running ~2x our assumed 1.05s interval) — so 2
    attempts reliably gets real data in ~2.1-2.2s.

    Returns {segment: {sec_id_str: entry}}.
    """
    sids_by_segment = {seg: ids for seg, ids in sids_by_segment.items() if ids}
    if not sids_by_segment:
        return {}
    raw_db = db._db if hasattr(db, "_db") else db
    cfg = raw_db["kite_market_config"].find_one({"broker": "dhan", "enabled": True}) or {}
    access_token = str(cfg.get("access_token") or "").strip()
    client_id = str(cfg.get("user_id") or cfg.get("dhan_client_id") or "").strip()
    if not access_token or not client_id:
        return {}

    result: dict[str, dict[str, dict]] = {seg: {} for seg in sids_by_segment}

    # WS ltp_map is bare-numeric-keyed regardless of segment — resolve
    # whatever's already warm before touching Dhan at all.
    remaining: dict[str, list[int]] = {}
    try:
        from features.dhan_ticker import dhan_ticker_manager as _dtm  # type: ignore
        for segment, sids in sids_by_segment.items():
            still_missing = []
            for sid in sids:
                sid_str = str(sid)
                ws_ltp = float(_dtm.ltp_map.get(sid_str) or 0)
                if ws_ltp > 0:
                    result[segment][sid_str] = {"ltp": ws_ltp, "oi": int(_dtm.oi_map.get(sid_str) or 0), "bid": 0.0, "ask": 0.0, "prev_close": 0.0}
                else:
                    still_missing.append(sid)
            if still_missing:
                remaining[segment] = still_missing
    except Exception:
        remaining = dict(sids_by_segment)

    if remaining:
        from features.broker_gateway import dhan_quote_post_blocking
        for _attempt in range(2):
            try:
                r = dhan_quote_post_blocking(remaining, access_token, client_id, timeout=15.0)
                if r is None:
                    continue
                if r.status_code == 200:
                    raw = r.json()
                    data_by_segment = raw.get("data") or raw
                    for segment in remaining:
                        data = data_by_segment.get(segment) or {}
                        for sid, info in data.items():
                            if not isinstance(info, dict):
                                continue
                            entry = {
                                "ltp": float(info.get("last_price") or 0),
                                "oi":  int(info.get("oi") or 0),
                                "bid": 0.0, "ask": 0.0,
                                "volume": int(info.get("volume") or 0),
                                "prev_close": float((info.get("ohlc") or {}).get("close") or 0),
                            }
                            result[segment][str(sid)] = entry
                            if entry["ltp"] > 0:
                                _DHAN_MARKET_DATA_LAST_GOOD[f"{segment}:{sid}"] = entry
                    break
            except Exception as _e:
                log.warning("[DHAN QUOTE MULTI NOWAIT] error=%s attempt=%d", _e, _attempt)

    for segment, sids in sids_by_segment.items():
        for sid in sids:
            sid_str = str(sid)
            if sid_str not in result[segment] or not result[segment][sid_str].get("ltp"):
                cached = _DHAN_MARKET_DATA_LAST_GOOD.get(f"{segment}:{sid_str}")
                if cached:
                    result[segment][sid_str] = cached
    return result


# Confirmed live against Dhan's own published reference
# (https://dhan.co/commodities-lot-size/, fetched 2026-08-06) — overrides
# active_option_tokens.lot_size for these, which is unreliable: every MCX
# commodity in that collection reports "1" regardless of the real contract
# size (that's the SEM_LOT_UNITS field straight from Dhan's scrip-master
# CSV, not meant for this). Dhan's own lot-size page itself shows "NA" for
# several commodities (ALUMINI, ALUMINIUM, CARDAMOM, COTTON, COTTONOIL,
# ELECDMBL, GOLDGUINEA, GOLDPETAL, GOLDTEN, KAPAS, LEAD, LEADMINI,
# MENTHAOIL, NICKEL, SILVER100, SILVERMIC, STEELREBAR, ZINCMINI) —
# deliberately NOT guessing values for those; they fall back to the
# (known-unreliable) DB value until a real source turns up.
_COMMODITY_LOT_SIZE_OVERRIDES: dict[str, int] = {
    "GOLD":        1,      # 1 KGS
    "GOLDM":       100,    # 100 GRMS
    "COPPER":      2500,   # 2500 KGS
    "CRUDEOIL":    100,    # 100 BBL
    "CRUDEOILM":   10,     # 10 BBL
    "SILVER":      30,     # 30 KGS
    "SILVERM":     5,      # 5 KGS
    "NATURALGAS":  1250,   # 1250 mmBtu
    "NATGASMINI":  250,    # 250 mmBtu
    "ZINC":        5,      # 5 MT
}


def _build_chain_payload_sync(normalized: str, expiry: str) -> dict:
    """
    Pure-REST F&O option chain for any Dhan-listed underlying (built for
    individual stocks, but works for indices too — the query is generic).

    Reads spot price from the live ticker's spot_map first (instant,
    in-memory — populated for every F&O stock by dhan_ticker.py's
    _prewarm_all_stock_spots at ticker startup, same mechanism the 6
    indices already used), falling back to a REST resolve + combined Dhan
    quote call only if spot_map doesn't have it yet (e.g. algo.websocket
    just restarted, or the ticker is stopped after market hours — Dhan's
    REST quote still returns the last traded price either way).

    Contracts come from active_option_tokens (broker=dhan) — same source
    /algo/get_active_tokens/{instrument} populates. If that's empty for
    this underlying, run that sync first.

    Synchronous/blocking (network + Mongo I/O) by design — called via
    asyncio.to_thread from both the REST endpoint (rest_option_chain, below)
    and the shared background refresh loop (_chain_refresh_loop), so neither
    blocks the event loop. Takes an already-normalized (uppercased)
    instrument — callers own that normalization since it's also the cache
    key both call sites share.
    """
    def _lap(label: str) -> None:
        pass  # timing prints removed — re-add a body here if diagnosing slowness again

    db = MongoData()
    today = datetime.now().strftime("%Y-%m-%d")
    tok_col = db._db["active_option_tokens"]
    _lap("db_connect")

    expiries = sorted({
        str(e)[:10]
        for e in tok_col.distinct(
            "expiry",
            {"broker": "dhan", "instrument": normalized, "expiry": {"$gte": today}},
        )
        if e
    })
    _lap("expiries_query")
    resolved_expiry = str(expiry or "").strip()[:10] or (expiries[0] if expiries else "")
    if not resolved_expiry:
        return {
            "instrument": normalized, "expiry": "", "expiries": [],
            "spot_price": 0.0, "lot_size": 0, "chain": {"CE": [], "PE": []},
            "message": "no contracts found in active_option_tokens — run /algo/get_active_tokens/" + normalized + " first",
        }

    docs = list(tok_col.find(
        {"broker": "dhan", "instrument": normalized, "expiry": {"$regex": f"^{resolved_expiry}"}, "option_type": {"$in": ["CE", "PE"]}},
        {"_id": 0, "token": 1, "tokens": 1, "strike": 1, "option_type": 1, "symbol": 1, "ws_segment": 1, "lot_size": 1, "instrument_type": 1},
    ))
    _lap(f"strikes_query (docs={len(docs)})")

    sids_by_segment: dict[str, list[int]] = {}
    doc_by_sid: dict[str, dict] = {}
    for d in docs:
        tok = str(d.get("token") or d.get("tokens") or "").strip()
        if not tok.isdigit():
            continue
        segment = str(d.get("ws_segment") or "NSE_FNO")
        sids_by_segment.setdefault(segment, []).append(int(tok))
        doc_by_sid[tok] = d

    # Fast path: read straight from the live ticker's spot_map — instant,
    # in-memory, zero REST call.
    spot_price_from_ws = 0.0
    try:
        from features.broker_gateway import broker_ticker_manager as _btm
        spot_price_from_ws = float(_btm.spot_map.get(normalized) or 0.0)
    except Exception:
        spot_price_from_ws = 0.0
    _lap(f"spot_from_ws_spot_map={spot_price_from_ws}")

    # Resolve spot_sec_id/segment regardless of whether spot_price already
    # came from WS — needed as a key into prev_close_map (previous day's
    # close) below even when we skip the REST spot-quote call itself.
    #
    # is_stock gates which lookup runs FIRST — confirmed live that
    # _get_dhan_equity_sec_id (Dhan's NSE cash-equity CSV rows) is not
    # actually scoped to real equities: its own exclusion filter only
    # drops OPTSTK/OPTIDX/FUTSTK/FUTIDX/etc, not plain "INDEX" rows, so
    # _get_dhan_equity_sec_id('NIFTY') returns '13' — the SAME security id
    # as NIFTY's real IDX_I spot token, just miscategorized. Trying the
    # equity path first for an index therefore doesn't fail (which would
    # correctly fall through to the index branch) — it "succeeds" with a
    # coincidentally-valid-looking id, so NIFTY's spot silently got quoted
    # under NSE_EQ instead of IDX_I and returned a wrong LTP. Checking
    # instrument_type first and skipping the equity path entirely for
    # non-stocks avoids relying on that lookup ever failing correctly.
    is_stock = bool(docs) and str(docs[0].get("instrument_type") or "").strip().lower() == "stock"
    spot_segment = ""
    spot_sec_id = ""
    if is_stock:
        try:
            spot_sec_id = _get_dhan_equity_sec_id(normalized)
            if spot_sec_id:
                spot_segment = "NSE_EQ"
        except Exception:
            spot_sec_id = ""
    if not spot_sec_id:
        try:
            from features.market_feed_tokens import ensure_seeded, get_spot_tokens
            ensure_seeded(db._db)
            spot_tokens = get_spot_tokens(db._db, "dhan") or {}
            idx_sid = next((s for s, u in spot_tokens.items() if u == normalized), None)
            if idx_sid:
                spot_sec_id = idx_sid
                spot_segment = "IDX_I"
        except Exception:
            pass
    # Commodities (MCX options-on-futures, instrument_type "commodity") have
    # no index/equity spot token anywhere — Dhan doesn't publish a true spot
    # price for these, only the futures contract itself. Use the nearest
    # FUTCOM contract's own LTP as the pricing reference instead, same
    # fallback live_greeks_chain_socket.py's _resolve_spot_price already
    # uses for this same reason. Was missing entirely from this endpoint
    # until now — confirmed live: GOLD/CRUDEOIL had no spot_price path at
    # all (not stock, not a known index, so both branches above left
    # spot_sec_id empty and the request never priced them).
    if not spot_sec_id:
        try:
            fut_doc = tok_col.find_one(
                {"broker": "dhan", "instrument": normalized, "option_type": "FUT", "expiry": {"$gte": today}},
                {"_id": 0, "token": 1, "tokens": 1, "ws_segment": 1},
                sort=[("expiry", 1)],
            )
            if fut_doc:
                fut_tok = str(fut_doc.get("token") or fut_doc.get("tokens") or "").strip()
                if fut_tok.isdigit():
                    spot_sec_id = fut_tok
                    spot_segment = str(fut_doc.get("ws_segment") or "MCX_COMM")
        except Exception:
            pass
    _lap(f"spot_sec_id_resolved={spot_sec_id} segment={spot_segment} is_stock={is_stock}")

    combined_request: dict[str, list[int]] = {seg: list(sids) for seg, sids in sids_by_segment.items()}
    if spot_sec_id and (spot_price_from_ws <= 0 or spot_segment == "NSE_EQ"):
        # Indices: skip the REST call once WS already has spot_price (their
        # previous_close reliably comes from prev_close_map, confirmed live
        # for NIFTY/SENSEX). Stocks: always include it even when WS already
        # has spot_price — needed for ohlc.close below, since prev_close_map
        # is unreliable for individual NSE_EQ tokens (Dhan doesn't seem to
        # send RESP_PREV_CLOSE packets for cash-equity subscriptions as
        # consistently as it does for index/F&O), confirmed live: many
        # stocks' change_pct stayed 0 with only the prev_close_map + DB
        # fallback. Still one combined call either way (this rides along
        # with the chain-quote tokens already in the request).
        combined_request.setdefault(spot_segment, []).append(int(spot_sec_id))

    all_quotes = _fetch_dhan_market_data_multi_nowait(combined_request, db)
    _lap("combined_quote_done")

    quotes: dict[str, dict] = {}
    for segment in sids_by_segment:
        quotes.update(all_quotes.get(segment, {}))

    spot_price = spot_price_from_ws
    spot_quote_entry = (all_quotes.get(spot_segment, {}) or {}).get(str(spot_sec_id)) or {}
    if spot_price <= 0 and spot_sec_id:
        spot_price = float(spot_quote_entry.get("ltp") or 0.0)
    _lap(f"spot_resolution_done spot_price={spot_price}")

    # previous_close: prefer Dhan's own RESP_PREV_CLOSE WS packet (in-memory,
    # populated for these tokens since dhan_ticker.py's _prewarm_all_stock_
    # spots subscribes with REQ_FULL_SUB, which includes prev-close packets)
    # — same primary source live_greeks_chain_socket.py's _resolve_previous_
    # close already uses. That WS packet is unreliable for individual NSE_EQ
    # (stock) tokens though — confirmed live many stocks' change_pct stayed
    # 0 with only this + the DB fallback — so for stocks specifically, fall
    # back to this request's own REST spot quote's ohlc.close next. NOT for
    # indices: confirmed live that IDX_I's ohlc.close tracks today's
    # current/live reference, not yesterday's close (the exact bug
    # live_greeks_chain_socket.py's own _resolve_previous_close docstring
    # warns about — "made change_pct/change_points round to ~0 every
    # time" — but that warning is specific to IDX_I; for NSE_EQ, ohlc.close
    # is the standard previous-day close, confirmed against real values).
    previous_close = 0.0
    try:
        from features.broker_gateway import broker_ticker_manager as _btm2
        previous_close = float(_btm2.prev_close_map.get(str(spot_sec_id)) or 0.0)
    except Exception:
        previous_close = 0.0
    # DB backfill BEFORE ohlc.close: confirmed live that ohlc.close returns
    # a non-zero but WRONG value for stocks once market is closed (today's
    # final print, same as ltp — no way to distinguish "closed, this is
    # today's close" from "closed, this is still just current price" from
    # that field alone), which would otherwise short-circuit this chain
    # before ever reaching the DB's genuine historical close.
    # option_chain_index_spot does carry real per-stock history (confirmed:
    # 12 backfilled rows for ADANIGREEN) despite the index-sounding name.
    if previous_close <= 0:
        try:
            day_start = f"{today}T00:00:00"
            doc = db._db["option_chain_index_spot"].find_one(
                {"underlying": normalized, "timestamp": {"$lt": day_start}},
                {"_id": 0, "close": 1, "spot_price": 1},
                sort=[("timestamp", -1)],
            ) or {}
            previous_close = float(doc.get("spot_price") or doc.get("close") or 0.0)
        except Exception:
            previous_close = 0.0
    if previous_close <= 0 and spot_segment in ("NSE_EQ", "MCX_COMM"):
        # MCX_COMM (commodity futures) added alongside NSE_EQ — same
        # reasoning: confirmed zero DB backfill for GOLD/CRUDEOIL, and
        # unlike IDX_I this segment's ohlc.close isn't known to have the
        # current-price-echo quirk, so it's a reasonable fallback here too.
        previous_close = float(spot_quote_entry.get("prev_close") or 0.0)
    change_pct = round((spot_price - previous_close) / previous_close * 100, 2) if previous_close else 0.0
    change_points = round(spot_price - previous_close, 2) if previous_close else 0.0
    _lap(f"previous_close_resolved={previous_close}")

    india_vix = 0.0
    try:
        vix_doc = (
            db._db["option_chain_index_spot"].find_one(
                {"underlying": "INDIAVIX"}, {"_id": 0, "close": 1, "spot_price": 1}, sort=[("timestamp", -1)],
            )
            or db._db["option_chain_index_spot"].find_one(
                {"token": "NSE_00"}, {"_id": 0, "close": 1, "spot_price": 1}, sort=[("timestamp", -1)],
            )
            or {}
        )
        india_vix = round(float(vix_doc.get("spot_price") or vix_doc.get("close") or 0), 2)
    except Exception:
        india_vix = 0.0

    token_ok, token_msg = True, ""
    try:
        from features.broker_gateway import get_active_broker_token_status
        token_ok, token_msg = get_active_broker_token_status()
    except Exception:
        pass

    from features.broker_gateway import get_bs_helpers
    _calc_iv, _calc_greeks, _time_to_expiry, _RISK_FREE_RATE, _DIVIDEND_YIELDS, _DEFAULT_DIVIDEND_YIELD = get_bs_helpers()
    T = _time_to_expiry(resolved_expiry)
    r = _RISK_FREE_RATE
    q_yield = _DIVIDEND_YIELDS.get(normalized, _DEFAULT_DIVIDEND_YIELD)

    chain: dict[str, list[dict]] = {"CE": [], "PE": []}
    for tok, doc in doc_by_sid.items():
        side = str(doc.get("option_type") or "").upper()
        if side not in chain:
            continue
        q = quotes.get(tok) or {}
        ltp = float(q.get("ltp") or 0.0)
        leg_prev_close = float(q.get("prev_close") or 0.0)
        if ltp > 0 and spot_price > 0:
            strike_f = float(doc.get("strike") or 0.0)
            try:
                iv = _calc_iv(ltp, spot_price, strike_f, T, r, side, q_yield)
                greeks = _calc_greeks(spot_price, strike_f, T, r, iv, side, q_yield)
            except Exception:
                iv, greeks = 0.0, {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
        else:
            iv, greeks = 0.0, {"delta": 0.0, "gamma": 0.0, "theta": 0.0, "vega": 0.0}
        ltp_change_pct = round((ltp - leg_prev_close) / leg_prev_close * 100, 2) if leg_prev_close else 0.0
        chain[side].append({
            "strike":  doc.get("strike"),
            "token":   tok,
            "symbol":  str(doc.get("symbol") or ""),
            "ltp":     ltp,
            "iv":      round(iv * 100, 2),
            "delta":   round(greeks.get("delta", 0.0), 4),
            "gamma":   round(greeks.get("gamma", 0.0), 4),
            "theta":   round(greeks.get("theta", 0.0), 4),
            "vega":    round(greeks.get("vega", 0.0), 4),
            "oi":      int(q.get("oi") or 0),
            "bid":     float(q.get("bid") or 0.0),
            "ask":     float(q.get("ask") or 0.0),
            "oi_change_pct": 0.0,
            "ltp_change_pct": ltp_change_pct,
            "volume":  int(q.get("volume") or 0),
        })
    for side in chain:
        chain[side].sort(key=lambda r: r["strike"] or 0)
    _lap("greeks_computed")

    from collections import Counter as _Counter
    all_strikes = sorted({float(row.get("strike") or 0) for side in ("CE", "PE") for row in chain.get(side, [])})
    strike_interval = 0.0
    if len(all_strikes) >= 2:
        diffs = [all_strikes[i + 1] - all_strikes[i] for i in range(len(all_strikes) - 1)]
        strike_interval = float(_Counter(diffs).most_common(1)[0][0])
    atm_strike = 0.0
    if all_strikes and spot_price > 0:
        atm_strike = min(all_strikes, key=lambda s: abs(s - spot_price))
    elif all_strikes:
        atm_strike = all_strikes[len(all_strikes) // 2]
    _lap("TOTAL (response ready)")

    return {
        "type":        "chain",
        "instrument":  normalized,
        "expiry":      resolved_expiry,
        "expiries":    expiries,
        "spot_price":  spot_price,
        "pricing_spot": spot_price,
        "previous_close": round(previous_close, 2),
        "change_pct":  change_pct,
        "change_points": change_points,
        "atm_strike":  int(atm_strike) if atm_strike == int(atm_strike) else atm_strike,
        "strike_interval": int(strike_interval) if strike_interval == int(strike_interval) else strike_interval,
        "india_vix":   india_vix,
        "lot_size":    _COMMODITY_LOT_SIZE_OVERRIDES.get(normalized) or (int(docs[0].get("lot_size") or 0) if docs else 0),
        "chain":       chain,
        "broker_session_expired": not token_ok,
        "broker_session_message": token_msg if not token_ok else "",
        "quote_source": "rest",
    }


# ── Shared chain cache + background refresh loop ─────────────────────────────
# Same principle the WS hub (live_greeks_chain_socket.py) already uses for
# /ws/live-greeks-chain — "N clients watching the same chain cost one fetch
# per refresh interval, not N REST calls" — applied here for the plain REST
# endpoint below. Without this, every single request calls Dhan directly
# (see _build_chain_payload_sync → _fetch_dhan_market_data_multi_nowait),
# which is fine for occasional/admin use but cannot survive real concurrent
# traffic: Dhan's rate limit is per-ACCOUNT (~1 req/1-2s for this whole app,
# all 5 processes combined — see broker_gateway.py's _DHAN_QUOTE_MIN_INTERVAL),
# not per-user, so any real number of simultaneous requesters would mostly
# 429/timeout each other and risk the same account-level block this app hit
# earlier today from just its own testing traffic.
#
# Design: the endpoint never blocks on a fresh Dhan call except the very
# first time a given (instrument, expiry) pair is ever requested. After
# that, it's served instantly from _CHAIN_CACHE, which _chain_refresh_loop
# keeps warm in the background — one Dhan call per *distinct active pair*
# per refresh cycle, regardless of how many users are reading that pair.
# Dhan call volume scales with how many different stocks are being watched
# right now, never with how many users are watching them.
_CHAIN_CACHE: dict[tuple[str, str], dict] = {}          # (instrument, expiry_hint) -> last built payload
_CHAIN_CACHE_TS: dict[tuple[str, str], float] = {}      # same key -> time.time() it was last refreshed
_CHAIN_WATCH_LAST_SEEN: dict[tuple[str, str], float] = {}  # same key -> time.time() it was last requested
_CHAIN_INFLIGHT: dict[tuple[str, str], asyncio.Task] = {}  # same key -> in-progress first-fetch task, if any
_CHAIN_WATCH_LOCK = threading.Lock()
_CHAIN_WATCH_TTL_SECONDS = 180.0   # stop refreshing a pair nobody's asked for in 3 minutes
_CHAIN_REFRESH_IDLE_SLEEP_SECONDS = 2.0  # poll pace when there's nothing active to refresh


async def _chain_refresh_loop() -> None:
    """
    Runs forever in the background (started at app startup, see
    _chain_refresh_loop_startup below). Cycles through every currently-
    watched (instrument, expiry) pair, refreshing each one's cached payload
    in turn. Pacing between individual Dhan calls comes from the shared
    rate gate itself (dhan_quote_post_blocking's wait_for_dhan_slot) — this
    loop doesn't add its own extra throttle beyond a small yield, since
    doing so on top of the shared gate would only slow refreshes down
    further for no benefit.
    """
    while True:
        now = time.time()
        with _CHAIN_WATCH_LOCK:
            stale_keys = [k for k, ts in _CHAIN_WATCH_LAST_SEEN.items() if now - ts >= _CHAIN_WATCH_TTL_SECONDS]
            for k in stale_keys:
                _CHAIN_WATCH_LAST_SEEN.pop(k, None)
                _CHAIN_CACHE.pop(k, None)
                _CHAIN_CACHE_TS.pop(k, None)
            active_pairs = sorted(_CHAIN_WATCH_LAST_SEEN.keys())

        if not active_pairs:
            await asyncio.sleep(_CHAIN_REFRESH_IDLE_SLEEP_SECONDS)
            continue

        for (normalized, expiry_hint) in active_pairs:
            try:
                payload = await asyncio.to_thread(_build_chain_payload_sync, normalized, expiry_hint)
                _CHAIN_CACHE[(normalized, expiry_hint)] = payload
                _CHAIN_CACHE_TS[(normalized, expiry_hint)] = time.time()
            except Exception as exc:
                log.warning("[CHAIN REFRESH LOOP] error pair=%s: %s", (normalized, expiry_hint), exc)
            await asyncio.sleep(0)  # yield to the event loop between pairs


@app.on_event("startup")
def _chain_refresh_loop_startup() -> None:
    asyncio.get_event_loop().create_task(_chain_refresh_loop())


@app.get("/rest-option-chain/{instrument}")
async def rest_option_chain(instrument: str, expiry: str = ""):
    """
    Thin cache-aware wrapper — see _build_chain_payload_sync for what
    actually builds the response, and the "Shared chain cache" block above
    for why this doesn't call Dhan directly on every request.

    _CHAIN_INFLIGHT coalesces concurrent first-requests for the same pair:
    without it, N simultaneous requests for a brand-new (instrument, expiry)
    — e.g. many users opening the same never-before-seen stock at once —
    would each see an empty cache and fire their own Dhan call in parallel,
    exactly the pile-up this whole cache exists to prevent. Only the first
    request to arrive actually fetches; every concurrent other one awaits
    that same in-flight task and shares its result.
    """
    normalized = str(instrument or "").strip().upper()
    if not normalized:
        raise HTTPException(status_code=400, detail="instrument is required")
    expiry_hint = str(expiry or "").strip()[:10]
    key = (normalized, expiry_hint)

    with _CHAIN_WATCH_LOCK:
        _CHAIN_WATCH_LAST_SEEN[key] = time.time()

    cached = _CHAIN_CACHE.get(key)
    if cached is not None:
        return cached

    with _CHAIN_WATCH_LOCK:
        inflight = _CHAIN_INFLIGHT.get(key)
        is_fetcher = inflight is None
        if is_fetcher:
            inflight = asyncio.get_event_loop().create_task(
                asyncio.to_thread(_build_chain_payload_sync, normalized, expiry)
            )
            _CHAIN_INFLIGHT[key] = inflight

    try:
        payload = await inflight
    finally:
        if is_fetcher:
            with _CHAIN_WATCH_LOCK:
                _CHAIN_INFLIGHT.pop(key, None)

    if is_fetcher:
        _CHAIN_CACHE[key] = payload
        _CHAIN_CACHE_TS[key] = time.time()
    return payload








# ── Background sync state ─────────────────────────────────────────────────────
_bg_sync_state: dict = {
    "running": False,
    "instrument": "",
    "started_at": "",
    "finished_at": "",
    "result": None,
    "error": "",
}
_bg_sync_thread: threading.Thread | None = None


def _run_bg_sync(instrument: str) -> None:
    global _bg_sync_state
    _bg_sync_state["running"] = True
    _bg_sync_state["instrument"] = instrument
    _bg_sync_state["started_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _bg_sync_state["finished_at"] = ""
    _bg_sync_state["result"] = None
    _bg_sync_state["error"] = ""
    try:
        result = _sync_active_option_tokens(instrument)
        _bg_sync_state["result"] = result
    except Exception as exc:
        _bg_sync_state["error"] = str(exc)
    finally:
        _bg_sync_state["running"] = False
        _bg_sync_state["finished_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")














# ─── MTM Historical Data ──────────────────────────────────────────────────────







_TRADE_DATA_DIR = Path(__file__).resolve().parent.parent / "algoreq" / "trade-data"


def _read_trade_static_json(filename: str):
    path = _TRADE_DATA_DIR / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail=f"{filename} not found")
    import json as _json_mod
    return _json_mod.loads(path.read_text(encoding="utf-8"))












# ─── Data Migration ───────────────────────────────────────────────────────────



_register_versioned_route_aliases(app)
