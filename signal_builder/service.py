from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime
from typing import Any

from features.mongo_data import MongoData

from .indicator_catalog import SCHEMA_PATH, build_indicator_catalog

INDICATOR_CATALOG_COLLECTION = "signal_indicator_catalog"
ORDER_CONFIG_SCHEMA_COLLECTION = "signal_order_config_schema"
ORDER_CONFIG_SCHEMA_KEY = "quantman_order_config_schema"
TV_INDICATOR_ALERT_CONDITIONS_COLLECTION = "tv_indicator_alert_conditions"
CANDLESTICK_PATTERN_SOURCE = "quantman_pipes_schema"

# Curated name-matches between TradingView's native indicator list and QuantMan's pipesSchema —
# e.g. tv's "vwap" IS signal_indicator_catalog's volumeWeightedAveragePrice pipe, just under a
# different name string. Persisting this as schema_key on the *native* tv doc (see
# backfill_native_schema_key_links) means merge_pipes_into_tv_indicators can skip a pipe that's
# already represented natively, instead of inserting a second, differently-named duplicate.
NATIVE_TV_SCHEMA_KEY_ALIASES: dict[str, str] = {
    "aroon": "aroon",
    "average directional index": "averageDirectionalIndex",
    "average true range": "averageTrueRange",
    "awesome oscillator": "awesomeOscillator",
    "bollinger bands": "bollingerBand",
    "commodity channel index": "commodityChannelIndex",
    "moving average exponential": "exponentialMovingAverage",
    "ichimoku cloud": "ichimokuCloud",
    "macd": "movingAverageConvergenceDivergence",
    "rate of change": "rateOfChange",
    "relative strength index": "relativeStrengthIndex",
    "moving average": "simpleMovingAverage",
    "hull moving average": "simpleMovingAverage",
    "stochastic": "stochasticOscillator",
    "stochastic rsi": "stochasticRSI",
    "supertrend": "superTrend",
    "moving average weighted": "weightedMovingAverage",
    "smoothed moving average": "wilderSmoothingAverage",
    "williams %r": "williamsR",
    "williams alligator": "williamsAlligator",
    "pivot points standard": "pivotPoints",
    "vwap": "volumeWeightedAveragePrice",
    "coppock curve": "coppockcurve",
    "fisher transform": "ft",
    "keltner channels": "keltnerChannel",
    "money flow index": "mfi",
    "parabolic sar": "parabolicSAR",
    "ratio": "ratio",
    "donchian channels": "simpleMovingAverage",
}


def backfill_native_schema_key_links() -> dict[str, Any]:
    """Stamp schema_key onto the native TradingView docs that already correspond 1:1 to a
    signal_indicator_catalog pipe (see NATIVE_TV_SCHEMA_KEY_ALIASES), so that link lives in
    MongoDB instead of only in a one-off comparison script — and so merge_pipes_into_tv_indicators
    can defensively skip re-adding those pipes under a second, differently-named doc."""
    db = MongoData()._db
    tv_col = db[TV_INDICATOR_ALERT_CONDITIONS_COLLECTION]

    updated = 0
    for indicator_name, schema_key in NATIVE_TV_SCHEMA_KEY_ALIASES.items():
        result = tv_col.update_one(
            {"indicator": indicator_name},
            {"$set": {"schema_key": schema_key, "linked_via": "name_match"}},
        )
        if result.matched_count:
            updated += 1

    return {"status": "success", "updated": updated}


def _clean_indicator_name(label: str) -> str:
    """'AVWAP (Anchored Volume Weighted Average Price)' -> 'anchored volume weighted average
    price' — TradingView's own indicator names are always the full descriptive phrase, never the
    abbreviation, so prefer whatever is inside the parens when there is one."""
    if "(" in label and label.endswith(")"):
        return label[label.index("(") + 1 : -1].strip().lower()
    return label.strip().lower()


def _alert_conditions_from_outputs(outputs: list[dict[str, Any]], indicator_label: str) -> list[dict[str, Any]]:
    """Turn a pipe's pipesOutputSchema entries into tv_indicator_alert_conditions-style plots:
    a plain numeric output (e.g. '.upperBand') becomes a native-style plot line
    {histogramBase, isHidden, joinPoints, title}, exactly like the real 'bollinger bands' /
    'aroon' docs already in the collection; an output with an enum: [0, 1] (a computed flag, e.g.
    '.isNarrow') becomes a qualitative {title, text} event, like the candlestick patterns."""
    conditions: list[dict[str, Any]] = []
    for output in outputs:
        name = (output.get("name") or "").lstrip(".") or indicator_label.lower()
        if output.get("enum"):
            conditions.append({
                "title": name,
                "text": f"{indicator_label} {name} condition met",
            })
        else:
            conditions.append({
                "histogramBase": 0,
                "isHidden": False,
                "joinPoints": False,
                "title": name,
            })
    return conditions


def merge_pipes_into_tv_indicators(category: str, tv_category_label: str) -> dict[str, Any]:
    """Add every pipe in `category` (a signal_indicator_catalog category, e.g. 'price_action')
    into tv_indicator_alert_conditions, deriving each doc's alert_conditions from that pipe's own
    pipesOutputSchema (see _alert_conditions_from_outputs). Same custom-source pattern as
    merge_candlestick_patterns_into_tv_indicators: no native TradingView study backs these, so the
    chart drives them from our own computed candle data instead of chart.createStudy(...)."""
    db = MongoData()._db
    catalog_col = db[INDICATOR_CATALOG_COLLECTION]
    tv_col = db[TV_INDICATOR_ALERT_CONDITIONS_COLLECTION]

    already_linked = {
        d["schema_key"] for d in tv_col.find({"schema_key": {"$exists": True}}, {"_id": 0, "schema_key": 1})
    }
    pipes = list(catalog_col.find({"category": category}, {"_id": 0, "schema_key": 1, "label": 1, "outputs": 1}))
    merged_at = datetime.utcnow()

    upserted = 0
    skipped_already_linked: list[str] = []
    for pipe in pipes:
        if pipe["schema_key"] in already_linked:
            skipped_already_linked.append(pipe["schema_key"])
            continue
        label = pipe["label"]
        indicator_name = _clean_indicator_name(label)
        doc = {
            "indicator": indicator_name,
            "category": tv_category_label,
            "shortDescription": "",
            "alert_conditions": _alert_conditions_from_outputs(pipe.get("outputs") or [], label),
            "schema_key": pipe["schema_key"],
            "source": CANDLESTICK_PATTERN_SOURCE,
            "merged_at": merged_at,
        }
        tv_col.update_one(
            {"indicator": indicator_name},
            {"$set": doc},
            upsert=True,
        )
        upserted += 1

    return {"status": "success", "upserted": upserted, "skipped_already_linked": skipped_already_linked}


def merge_price_action_into_tv_indicators() -> dict[str, Any]:
    """Add the price_action pipes (AVWAP, CPR, ORB, gap strategy, candle references, ...) from
    signal_indicator_catalog into tv_indicator_alert_conditions."""
    return merge_pipes_into_tv_indicators("price_action", "price action")


def seed_indicator_schemas() -> dict[str, Any]:
    """Parse indicators.json (pipesSchema + pipesOutputSchema) and upsert every pipe/indicator into MongoDB."""
    build_indicator_catalog.cache_clear()
    catalog = build_indicator_catalog()

    db = MongoData()._db
    col = db[INDICATOR_CATALOG_COLLECTION]
    seeded_at = datetime.utcnow()

    upserted = 0
    for defn in catalog:
        doc = {
            "schema_key": defn.schema_key,
            "label": defn.label,
            "status": defn.status,
            "access_tier": defn.access_tier,
            "tier_reason": defn.tier_reason,
            "fields": [asdict(f) for f in defn.fields],
            "category": defn.category,
            "outputs": list(defn.outputs),
            "seeded_at": seeded_at,
        }
        # Keyed on label, not schema_key — several distinct labels (e.g. "SMA
        # (Simple Moving Average)" and "HMA (Hull Moving Average)") share one
        # schema_key because they reuse the same pipesSchema entry. Upserting
        # on schema_key alone made every later label in _ALL_INDICATOR_LABELS
        # silently clobber the earlier one's doc, dropping it from the catalog
        # the API serves entirely.
        col.update_one(
            {"label": defn.label},
            {"$set": doc},
            upsert=True,
        )
        upserted += 1

    return {"status": "success", "upserted": upserted}


def get_indicator_schemas() -> list[dict[str, Any]]:
    """Return all indicator schemas from MongoDB; fall back to the built-in catalog if empty."""
    db = MongoData()._db
    col = db[INDICATOR_CATALOG_COLLECTION]
    items = list(col.find({}, {"_id": 0}).sort("label", 1))
    if items:
        return items

    catalog = build_indicator_catalog()
    return [
        {
            "schema_key": d.schema_key,
            "label": d.label,
            "status": d.status,
            "access_tier": d.access_tier,
            "tier_reason": d.tier_reason,
            "fields": [asdict(f) for f in d.fields],
            "category": d.category,
            "outputs": list(d.outputs),
        }
        for d in catalog
    ]


get_signal_schemas_catalog = get_indicator_schemas


def seed_order_config_schema() -> dict[str, Any]:
    """Upsert the global entry/exit condition + transaction-leg schema (orderConfigSchema in
    indicators.json) into MongoDB. Unlike the per-pipe indicator catalog, this schema is a single
    document: it describes the condition operators (isAbove/crossesAbove/...) and the
    equity/future/option transaction-leg shape used to build & evaluate a strategy's orders,
    exactly as QuantMan's strategy builder does."""
    schema_json = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    order_config_schema = schema_json.get("orderConfigSchema", {})

    db = MongoData()._db
    col = db[ORDER_CONFIG_SCHEMA_COLLECTION]
    doc = {
        "schema_key": ORDER_CONFIG_SCHEMA_KEY,
        "schema": order_config_schema,
        "seeded_at": datetime.utcnow(),
    }
    col.update_one(
        {"schema_key": ORDER_CONFIG_SCHEMA_KEY},
        {"$set": doc},
        upsert=True,
    )
    return {"status": "success", "upserted": 1}


def get_order_config_schema() -> dict[str, Any]:
    """Return the seeded order-config schema; fall back to reading indicators.json directly if empty."""
    db = MongoData()._db
    col = db[ORDER_CONFIG_SCHEMA_COLLECTION]
    doc = col.find_one({"schema_key": ORDER_CONFIG_SCHEMA_KEY}, {"_id": 0})
    if doc:
        return doc

    schema_json = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return {"schema_key": ORDER_CONFIG_SCHEMA_KEY, "schema": schema_json.get("orderConfigSchema", {})}


def merge_candlestick_patterns_into_tv_indicators() -> dict[str, Any]:
    """Add every candlestick-pattern pipe from signal_indicator_catalog into
    tv_indicator_alert_conditions, so the chart's indicator/alert picker offers candlestick
    patterns alongside TradingView's native studies.

    Candlestick patterns have no TradingView study to back them (tv_indicator_alert_conditions
    has zero candlestick entries today) — they're detected entirely on our own OHLC data and
    plotted as chart markers (createShape), never as a native chart.createStudy(...) line. Each
    merged doc keeps the same {indicator, category, shortDescription, alert_conditions} shape the
    chart already reads, plus two extra fields that only apply to these custom entries:
      - schema_key: links back to signal_indicator_catalog for the pipe's config/output schema
      - source: "quantman_pipes_schema", so chart code can tell "needs a custom detector +
        createShape marker" apart from a native TradingView study.
    """
    db = MongoData()._db
    catalog_col = db[INDICATOR_CATALOG_COLLECTION]
    tv_col = db[TV_INDICATOR_ALERT_CONDITIONS_COLLECTION]

    patterns = list(catalog_col.find({"category": "candlestick_pattern"}, {"_id": 0, "schema_key": 1, "label": 1}))
    merged_at = datetime.utcnow()

    upserted = 0
    for pattern in patterns:
        label = pattern["label"]
        indicator_name = label.lower()
        doc = {
            "indicator": indicator_name,
            "category": "candlestick pattern",
            "shortDescription": "",
            "alert_conditions": [
                {"title": f"{indicator_name} detected", "text": f"{label} candlestick pattern detected"},
            ],
            "schema_key": pattern["schema_key"],
            "source": CANDLESTICK_PATTERN_SOURCE,
            "merged_at": merged_at,
        }
        tv_col.update_one(
            {"indicator": indicator_name},
            {"$set": doc},
            upsert=True,
        )
        upserted += 1

    return {"status": "success", "upserted": upserted}
