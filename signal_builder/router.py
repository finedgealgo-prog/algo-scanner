from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException

from features import auth as app_auth

from .equity_trading import (
    EquityOrderIn,
    exit_equity_position,
    get_equity_quotes,
    list_equity_positions,
    place_equity_order,
    search_equity_symbols,
)
from .service import (
    backfill_native_schema_key_links,
    get_order_config_schema,
    get_signal_schemas_catalog,
    merge_candlestick_patterns_into_tv_indicators,
    merge_price_action_into_tv_indicators,
    seed_indicator_schemas,
    seed_order_config_schema,
)
from .stock_historical import get_stock_option_future_historical_data

router = APIRouter(prefix="/signal", tags=["signal"])


@router.get("/indicator-catalog")
async def signal_indicator_catalog() -> list[dict[str, Any]]:
    """Return the full indicator catalog from MongoDB (signal_indicator_catalog collection)."""
    try:
        return get_signal_schemas_catalog()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seed-indicator-schemas")
async def signal_seed_indicator_schemas() -> dict[str, Any]:
    """Parse indicators.json (pipesSchema) and upsert all indicators into MongoDB."""
    try:
        return seed_indicator_schemas()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/order-config-schema")
async def signal_order_config_schema() -> dict[str, Any]:
    """Return the entry/exit condition + transaction-leg schema (signal_order_config_schema collection)."""
    try:
        return get_order_config_schema()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/seed-order-config-schema")
async def signal_seed_order_config_schema() -> dict[str, Any]:
    """Parse indicators.json (orderConfigSchema) and upsert it into MongoDB."""
    try:
        return seed_order_config_schema()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/merge-candlestick-patterns")
async def signal_merge_candlestick_patterns() -> dict[str, Any]:
    """Add candlestick-pattern pipes into tv_indicator_alert_conditions so the chart's indicator
    picker lists them alongside TradingView's native studies."""
    try:
        return merge_candlestick_patterns_into_tv_indicators()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/merge-price-action")
async def signal_merge_price_action() -> dict[str, Any]:
    """Add price_action pipes (AVWAP, CPR, ORB, gap strategy, candle references, ...) into
    tv_indicator_alert_conditions so the chart's indicator picker lists them too."""
    try:
        return merge_price_action_into_tv_indicators()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/backfill-native-schema-links")
async def signal_backfill_native_schema_links() -> dict[str, Any]:
    """Stamp schema_key onto native TradingView docs that already correspond to a
    signal_indicator_catalog pipe, so future merges don't duplicate them under a new name."""
    try:
        return backfill_native_schema_key_links()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Stock option/future historical data (straight from Dhan, nothing persisted) ──

@router.get("/stock/fno-historical-data")
async def signal_stock_fno_historical_data(
    symbol: str,
    years: float = 1.0,
    strike_offsets: str = "0",
    include_option_chain: bool = True,
) -> dict[str, Any]:
    """Given a stock symbol: `years` of equity daily OHLCV, every currently-listed
    FUTSTK contract's own daily history, up to 5 years of ATM (+/- strike_offsets)
    CE/PE daily history via Dhan's Expired Options Data API, and (optionally) the
    live OPTSTK chain snapshot for the nearest expiry. Every candle is fetched
    live from Dhan on each call — nothing is cached or stored in Mongo.

    strike_offsets: comma-separated integers within +-3 of ATM, e.g. "-1,0,1".
    """
    try:
        offsets = [int(o.strip()) for o in strike_offsets.split(",") if o.strip()]
        return get_stock_option_future_historical_data(
            symbol, years, strike_offsets=offsets, include_option_chain=include_option_chain,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


# ── Equity trading panel (paper/virtual only — no real broker order ever placed) ──

def _equity_user_id(current_user: dict) -> str:
    return str(current_user.get("_id") or "anonymous")


@router.get("/equity/search")
async def signal_equity_search(q: str) -> list[dict[str, Any]]:
    """Stock picker — regex search over scanner_stocks_list (symbol prefix or company name)."""
    try:
        return search_equity_symbols(q)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/equity/quote")
async def signal_equity_quote(symbols: str) -> dict[str, dict[str, float]]:
    """Live LTP/prev_close/change_pct for a comma-separated symbol list, via Dhan's NSE_EQ feed."""
    try:
        return get_equity_quotes([s for s in symbols.split(",") if s.strip()])
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/equity/orders")
async def signal_equity_place_order(
    body: EquityOrderIn, current_user: dict = Depends(app_auth.get_current_user)
) -> dict[str, Any]:
    """Place a virtual market order — fills instantly at the current LTP, no real broker order."""
    try:
        return place_equity_order(_equity_user_id(current_user), body)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/equity/positions")
async def signal_equity_positions(current_user: dict = Depends(app_auth.get_current_user)) -> dict[str, Any]:
    """Current user's open (with live unrealized P&L) and recently-closed virtual equity positions."""
    try:
        return list_equity_positions(_equity_user_id(current_user))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/equity/positions/{position_id}/exit")
async def signal_equity_exit_position(
    position_id: str, current_user: dict = Depends(app_auth.get_current_user)
) -> dict[str, Any]:
    """Square off an open virtual position at the current LTP."""
    try:
        return exit_equity_position(_equity_user_id(current_user), position_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
