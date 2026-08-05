from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException

from .service import (
    backfill_native_schema_key_links,
    get_order_config_schema,
    get_signal_schemas_catalog,
    merge_candlestick_patterns_into_tv_indicators,
    merge_price_action_into_tv_indicators,
    seed_indicator_schemas,
    seed_order_config_schema,
)

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
