from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


# algo.scanner/signal_builder/indicator_catalog.py -> algo.scanner/scanner/algoreq/...
ROOT_DIR = Path(__file__).resolve().parent.parent / "scanner"
SCHEMA_PATH = ROOT_DIR / "algoreq" / "indicators.json"
DROPDOWN_HTML_PATH = ROOT_DIR / "algoreq" / "trade-data" / "missing-token.txt"


@dataclass(frozen=True)
class IndicatorField:
    key: str
    label: str
    kind: str
    required: bool = False
    options: tuple[str, ...] = ()
    placeholder: str = ""
    helper: str = ""


@dataclass(frozen=True)
class IndicatorDefinition:
    label: str
    schema_key: str
    status: str
    access_tier: str
    tier_reason: str
    fields: tuple[IndicatorField, ...]
    category: str = "uncategorized"
    outputs: tuple[dict[str, Any], ...] = ()


# Groups every schema_key present in pipesSchema (indicators.json) the same way
# QuantMan groups them in its own pipe picker UI. Anything not listed here (labels
# that only exist in the legacy hardcoded list, with no matching pipesSchema entry)
# falls back to "uncategorized".
_CANDLESTICK_PATTERN_KEYS: frozenset[str] = frozenset({
    "bearishEngulfing", "bearishHammer", "bearishInvertedHammer", "bearishMarubozu",
    "bearishSpinningTop", "bullishEngulfing", "bullishHammer", "bullishInvertedHammer",
    "bullishMarubozu", "bullishSpinningTop", "darkCloudCover", "doji", "dragonFlyDoji",
    "eveningStar", "graveStoneDoji", "heikinashiBearish", "heikinashiBearishIndecision",
    "heikinashiBullish", "heikinashiBullishIndecision", "heikinashiVeryBearish",
    "heikinashiVeryBullish", "longLeggedDoji", "morningStar", "piercingLine",
    "renkoBearishBrick", "renkoBullishBrick", "threeBlackCrows", "threeWhiteSoldiers",
})
_STATISTICAL_KEYS: frozenset[str] = frozenset({
    "augmentedDickeyFuller", "densityCurve", "ratio", "ratioStandardDeviation", "zScore",
    "indiaVIXPercentile",
})
_PRICE_ACTION_KEYS: frozenset[str] = frozenset({
    "anchoredVolumeWeightedAveragePrice", "centralPivotRange", "currentCandle", "gapStrategy",
    "lastNCandles", "openingRangeBreakout", "pivotPoints", "previousCandle", "priceCandle",
    "signalCandle", "todayCandle", "volumeWeightedAveragePrice", "yesterdayCandle",
})
_OPTIONS_DATA_KEYS: frozenset[str] = frozenset({
    "longBuildup", "longUnwidening", "shortBuildup", "shortCovering",
})
_TECHNICAL_INDICATOR_KEYS: frozenset[str] = frozenset({
    "aroon", "averageDirectionalIndex", "averageTrueRange", "awesomeOscillator", "bollingerBand",
    "commodityChannelIndex", "exponentialMovingAverage", "ichimokuCloud",
    "movingAverageConvergenceDivergence", "rateOfChange", "relativeStrengthIndex",
    "simpleMovingAverage", "stochasticOscillator", "stochasticRSI", "superTrend",
    "weightedMovingAverage", "wilderSmoothingAverage", "williamsR", "williamsAlligator",
})


def _category_for_schema_key(schema_key: str) -> str:
    if schema_key == "snippet":
        return "reference"
    if schema_key in _CANDLESTICK_PATTERN_KEYS:
        return "candlestick_pattern"
    if schema_key in _STATISTICAL_KEYS:
        return "statistical"
    if schema_key in _PRICE_ACTION_KEYS:
        return "price_action"
    if schema_key in _OPTIONS_DATA_KEYS:
        return "options_data"
    if schema_key in _TECHNICAL_INDICATOR_KEYS:
        return "technical_indicator"
    return "uncategorized"


_BASE_FIELDS: tuple[IndicatorField, ...] = (
    IndicatorField("name", "Name", "text", required=True, placeholder="indicatorName"),
    IndicatorField("chartType", "Chart Type", "select", required=True, options=("Candle", "Heikin Ashi", "Open Interest", "Volume")),
)

_FIELD_META: dict[str, IndicatorField] = {
    "candleInterval": IndicatorField(
        "candleInterval",
        "Candle Interval",
        "select",
        required=True,
        options=("1 minute", "3 minutes", "5 minutes", "10 minutes", "15 minutes", "30 minutes", "1 hour", "1 day", "1 week"),
    ),
    "valuePaths": IndicatorField(
        "valuePaths",
        "Field",
        "select",
        required=True,
        options=("Equity", "Open", "High", "Low", "Close", "HLC3", "Volume", "Open Interest"),
    ),
    "noOfCandles": IndicatorField("noOfCandles", "Period", "number", required=True),
    "multiplier": IndicatorField("multiplier", "Multiplier", "number", required=True),
    "fastPeriod": IndicatorField("fastPeriod", "Fast Period", "number", required=True),
    "slowPeriod": IndicatorField("slowPeriod", "Slow Period", "number", required=True),
    "signalPeriod": IndicatorField("signalPeriod", "Signal Period", "number", required=True),
    "DILength": IndicatorField("DILength", "DI Length", "number", required=True),
    "ADXSmoothing": IndicatorField("ADXSmoothing", "ADX Smoothing", "number", required=True),
    "conversionPeriod": IndicatorField("conversionPeriod", "Conversion Period", "number", required=True),
    "basePeriod": IndicatorField("basePeriod", "Base Period", "number", required=True),
    "spanPeriod": IndicatorField("spanPeriod", "Span Period", "number", required=True),
    "displacement": IndicatorField("displacement", "Displacement", "number", required=True),
    "rsiPeriod": IndicatorField("rsiPeriod", "RSI Period", "number", required=True),
    "stochasticPeriod": IndicatorField("stochasticPeriod", "Stochastic Period", "number", required=True),
    "kPeriod": IndicatorField("kPeriod", "K Period", "number", required=True),
    "dPeriod": IndicatorField("dPeriod", "D Period", "number", required=True),
    "basedOn": IndicatorField("basedOn", "Based On", "select", required=True, options=("expiry", "signal", "dateTime")),
    "source": IndicatorField("source", "Source", "select", options=("open", "high", "low", "close", "hlc3")),
    "date": IndicatorField("date", "Date", "date"),
    "time": IndicatorField("time", "Time", "time"),
    "timeFrame": IndicatorField("timeFrame", "Time Frame", "select", options=("day", "week")),
    "category": IndicatorField(
        "category",
        "Category",
        "select",
        options=("traditional", "fibonacci", "classic", "demarks", "camarilla", "woodie"),
    ),
    "narrowRange": IndicatorField("narrowRange", "Narrow Range", "number", required=True),
    "moderatelyRange": IndicatorField("moderatelyRange", "Moderately Range", "number", required=True),
    "wideRange": IndicatorField("wideRange", "Wide Range", "number", required=True),
    "maxDiffBetweenOpenHighAndOpenLowInPercentage": IndicatorField("maxDiffBetweenOpenHighAndOpenLowInPercentage", "Max Diff %", "number"),
    "candleType": IndicatorField("candleType", "Candle Type", "select", options=("current", "previous", "signal")),
    "period": IndicatorField("period", "Period", "number"),
    "backOffDays": IndicatorField("backOffDays", "Back Off Days", "number"),
    "accuracy": IndicatorField("accuracy", "Accuracy", "number"),
    "jawLength": IndicatorField("jawLength", "Jaw Length", "number"),
    "teethLength": IndicatorField("teethLength", "Teeth Length", "number"),
    "lipsLength": IndicatorField("lipsLength", "Lips Length", "number"),
    "jawOffset": IndicatorField("jawOffset", "Jaw Offset", "number"),
    "teethOffset": IndicatorField("teethOffset", "Teeth Offset", "number"),
    "lipsOffset": IndicatorField("lipsOffset", "Lips Offset", "number"),
    "breakoutSide": IndicatorField("breakoutSide", "Breakout Side", "select", options=("High", "Low")),
    "trackingMode": IndicatorField("trackingMode", "Tracking", "select", options=("Underlying", "Strike Price")),
    "externalSource": IndicatorField("externalSource", "Source Name", "text", placeholder="Tradingview / Chartink webhook"),
    "acceleration": IndicatorField("acceleration", "Acceleration", "number"),
    "maximum": IndicatorField("maximum", "Maximum", "number"),
    "stopLoss": IndicatorField("stopLoss", "Stop Loss", "number"),
}

_EXPLICIT_SCHEMA_KEY_BY_LABEL: dict[str, str] = {
    "SMA (Simple Moving Average)": "simpleMovingAverage",
    "Current Candle": "currentCandle",
    "Entry Candle": "currentCandle",
    "Super Trend": "superTrend",
    "EMA (Exponential Moving Average)": "exponentialMovingAverage",
    "RSI (Relative Strength Index)": "relativeStrengthIndex",
    "HMA (Hull Moving Average)": "simpleMovingAverage",
    "Bollinger Band": "bollingerBand",
    "Previous Candle": "previousCandle",
    "MACD (Moving Average Convergence Divergence)": "movingAverageConvergenceDivergence",
    "Pivot Points": "pivotPoints",
    "ADX (Average Directional Index)": "averageDirectionalIndex",
    "Range Breakout": "openingRangeBreakout",
    "CCI (Commodity Channel Index)": "commodityChannelIndex",
    "VWAP (Volume Weighted Average Price)": "volumeWeightedAveragePrice",
    "CPR (Central Pivot Range)": "centralPivotRange",
    "ORB (Opening Range Breakout)": "openingRangeBreakout",
    "Signal Candle": "signalCandle",
    "Previous Nth Candle": "previousCandle",
    "Last N Candles": "lastNCandles",
    "AVWAP (Anchored Volume Weighted Average Price)": "anchoredVolumeWeightedAveragePrice",
    "Aroon Oscillator": "aroon",
    "Aroon": "aroon",
    "ATR (Average True Range)": "averageTrueRange",
    "Awesome Oscillator": "awesomeOscillator",
    "Bearish Engulfing": "bearishEngulfing",
    "Bearish Hammer": "bearishHammer",
    "Bearish Inverted Hammer": "bearishInvertedHammer",
    "Bearish Marubozu": "bearishMarubozu",
    "Bearish Spinning Top": "bearishSpinningTop",
    "Bullish Engulfing": "bullishEngulfing",
    "Bullish Hammer": "bullishHammer",
    "Bullish Inverted Hammer": "bullishInvertedHammer",
    "Bullish Marubozu": "bullishMarubozu",
    "Bullish Spinning Top": "bullishSpinningTop",
    "Dark Cloud Cover": "darkCloudCover",
    "Doji": "doji",
    "Don Chian Channel": "simpleMovingAverage",
    "Dragon Fly Doji": "dragonFlyDoji",
    "Evening Star": "eveningStar",
    "Gap Strategy": "gapStrategy",
    "Grave Stone Doji": "graveStoneDoji",
    "Heikinashi Bearish": "heikinashiBearish",
    "Heikinashi Bearish Indecision": "heikinashiBearishIndecision",
    "Heikinashi Bullish": "heikinashiBullish",
    "Heikinashi Bullish Indecision": "heikinashiBullishIndecision",
    "Heikinashi Very Bearish": "heikinashiVeryBearish",
    "Heikinashi Very Bullish": "heikinashiVeryBullish",
    "Ichimoku Cloud": "ichimokuCloud",
    "Long Buildup": "longBuildup",
    "Long Legged Doji": "longLeggedDoji",
    "Long Unwinding": "longUnwidening",
    "Morning Star": "morningStar",
    "Piercing Line": "piercingLine",
    "ROC (Rate of Change)": "rateOfChange",
    "Ratio": "ratio",
    "Short Buildup": "shortBuildup",
    "Short Covering": "shortCovering",
    "SMMA (Smoothed Moving Average)": "wilderSmoothingAverage",
    "Stochastic Oscillator": "stochasticOscillator",
    "Stochastic RSI": "stochasticRSI",
    "Today Candle": "todayCandle",
    "WMA (Weighted Moving Average)": "weightedMovingAverage",
    "Wilder Smoothing Average": "wilderSmoothingAverage",
    "Williams Alligator": "williamsAlligator",
    "Williams R": "williamsR",
    "Previous Day Candle": "yesterdayCandle",
    "Yesterday Candle": "yesterdayCandle",
}

_INFERRED_FIELDS: dict[str, tuple[str, ...]] = {
    "Signal": ("externalSource",),
    "Condition": ("externalSource",),
    "CandleAt": ("candleInterval", "time", "valuePaths"),
    "External Signal": ("externalSource",),
    "Tradingview": ("externalSource",),
    "Chartink": ("externalSource",),
    "Manual Trigger": ("externalSource",),
    "Basket": ("valuePaths",),
    "Combined Premium": ("valuePaths",),
    "Options Basket": ("valuePaths",),
    "Chandelier Exit": ("candleInterval", "noOfCandles", "multiplier", "valuePaths"),
    "coppockCurve": ("noOfCandles", "valuePaths"),
    "Don Chian Channel": ("candleInterval", "noOfCandles", "valuePaths"),
    "FT (Fisher Transform)": ("candleInterval", "noOfCandles", "valuePaths"),
    "Half Trend": ("candleInterval", "noOfCandles", "multiplier", "valuePaths"),
    "Keltner Channel": ("candleInterval", "noOfCandles", "multiplier", "valuePaths"),
    "MRC (Mean Reversion Channel)": ("candleInterval", "noOfCandles", "multiplier", "valuePaths"),
    "MFI (Money Flow Index)": ("candleInterval", "noOfCandles", "valuePaths"),
    "Moving Average For ATR": ("candleInterval", "noOfCandles", "valuePaths"),
    "Moving Average For RSI": ("candleInterval", "noOfCandles", "valuePaths"),
    "Parabolic SAR": ("candleInterval", "acceleration", "maximum", "valuePaths"),
    "Qqe Signals": ("candleInterval", "rsiPeriod", "multiplier", "valuePaths"),
    "Price Ration": ("valuePaths",),
    "Transaction StopLoss": ("stopLoss",),
    "Trend Intensity Index": ("candleInterval", "noOfCandles", "valuePaths"),
}

_COMMON_FREE_PUBLIC_LABELS: set[str] = {
    "SMA (Simple Moving Average)",
    "Current Candle",
    "Entry Candle",
    "Super Trend",
    "EMA (Exponential Moving Average)",
    "RSI (Relative Strength Index)",
    "Bollinger Band",
    "Previous Candle",
    "MACD (Moving Average Convergence Divergence)",
    "Pivot Points",
    "ADX (Average Directional Index)",
    "Range Breakout",
    "CCI (Commodity Channel Index)",
    "VWAP (Volume Weighted Average Price)",
    "CPR (Central Pivot Range)",
    "ORB (Opening Range Breakout)",
    "Signal Candle",
    "Previous Nth Candle",
    "Last N Candles",
    "AVWAP (Anchored Volume Weighted Average Price)",
    "Aroon Oscillator",
    "Aroon",
    "ATR (Average True Range)",
    "Awesome Oscillator",
    "Bearish Engulfing",
    "Bearish Hammer",
    "Bearish Inverted Hammer",
    "Bearish Marubozu",
    "Bearish Spinning Top",
    "Bullish Engulfing",
    "Bullish Hammer",
    "Bullish Inverted Hammer",
    "Bullish Marubozu",
    "Bullish Spinning Top",
    "Dark Cloud Cover",
    "Doji",
    "Dragon Fly Doji",
    "Evening Star",
    "Gap Strategy",
    "Grave Stone Doji",
    "Heikinashi Bearish",
    "Heikinashi Bearish Indecision",
    "Heikinashi Bullish",
    "Heikinashi Bullish Indecision",
    "Heikinashi Very Bearish",
    "Heikinashi Very Bullish",
    "Ichimoku Cloud",
    "Long Buildup",
    "Long Legged Doji",
    "Long Unwinding",
    "Morning Star",
    "Piercing Line",
    "ROC (Rate of Change)",
    "Ratio",
    "Short Buildup",
    "Short Covering",
    "SMMA (Smoothed Moving Average)",
    "Stochastic Oscillator",
    "Stochastic RSI",
    "Three Black Crows",
    "Three White Soldiers",
    "Today Candle",
    "WMA (Weighted Moving Average)",
    "Wilder Smoothing Average",
    "Williams Alligator",
    "Williams R",
    "Previous Day Candle",
    "Yesterday Candle",
}

_CUSTOM_REPRODUCIBLE_LABELS: set[str] = {
    "HMA (Hull Moving Average)",
    "Signal",
    "Condition",
    "CandleAt",
    "Basket",
    "Combined Premium",
    "Options Basket",
    "Chandelier Exit",
    "coppockCurve",
    "Don Chian Channel",
    "FT (Fisher Transform)",
    "Half Trend",
    "Keltner Channel",
    "MRC (Mean Reversion Channel)",
    "MFI (Money Flow Index)",
    "Moving Average For ATR",
    "Moving Average For RSI",
    "Parabolic SAR",
    "Qqe Signals",
    "Price Ration",
    "Transaction StopLoss",
    "Trend Intensity Index",
}

_EXTERNAL_OR_UNCERTAIN_LABELS: set[str] = {
    "External Signal",
    "Tradingview",
    "Chartink",
    "Manual Trigger",
}


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _parse_dropdown_labels(path: Path) -> list[str]:
    html = path.read_text(encoding="utf-8")
    labels = re.findall(r'<a[^>]*data-test-id="[^"]+"[^>]*>([^<]+)</a>', html)
    # Keep original order while removing duplicates.
    seen: set[str] = set()
    ordered: list[str] = []
    for label in labels:
        if label not in seen:
            seen.add(label)
            ordered.append(label)
    return ordered


def _to_schema_name(label: str) -> str:
    cleaned = re.sub(r"\([^)]*\)", "", label)
    cleaned = re.sub(r"[^A-Za-z0-9]+", " ", cleaned).strip()
    parts = cleaned.split()
    if not parts:
        return "indicator"
    first, *rest = parts
    return first.lower() + "".join(part[:1].upper() + part[1:] for part in rest)


def _field_from_schema(field_key: str, required: bool, property_schema: dict[str, Any] | None = None) -> IndicatorField:
    base = _FIELD_META.get(field_key, IndicatorField(field_key, field_key, "text"))
    kind = base.kind
    options = base.options

    # Prefer the real pipesSchema enum (proper mapping) over the hand-guessed options table.
    enum_values = (property_schema or {}).get("enum") or (property_schema or {}).get("items", {}).get("enum")
    if enum_values:
        kind = "select"
        options = tuple(str(v) for v in enum_values)

    return IndicatorField(
        key=base.key,
        label=base.label,
        kind=kind,
        required=required or base.required,
        options=options,
        placeholder=base.placeholder,
        helper=base.helper,
    )


def _build_confirmed_fields(schema_key: str, schema: dict[str, Any]) -> tuple[IndicatorField, ...]:
    config = schema.get("properties", {}).get("config", {})
    properties = config.get("properties", {})
    required = set(config.get("required", []))
    field_keys = list(properties.keys())

    # Small schema correction: williamsAlligator references candleInterval in required, but the property is absent.
    if schema_key == "williamsAlligator" and "candleInterval" not in properties:
        field_keys.insert(1, "candleInterval")
        required.add("candleInterval")

    return tuple(
        _field_from_schema(field_key, field_key in required, properties.get(field_key))
        for field_key in field_keys
    )


def _build_fields_for_indicator(label: str, schema_key: str, schema_map: dict[str, Any]) -> tuple[str, tuple[IndicatorField, ...]]:
    schema = schema_map.get(schema_key)
    if schema is not None:
        return "confirmed", _BASE_FIELDS + _build_confirmed_fields(schema_key, schema)

    inferred_keys = _INFERRED_FIELDS.get(label)
    if inferred_keys:
        return "inferred", _BASE_FIELDS + tuple(_field_from_schema(key, False) for key in inferred_keys)

    return "unmapped", _BASE_FIELDS


def _classify_access_tier(label: str) -> tuple[str, str]:
    if label in _COMMON_FREE_PUBLIC_LABELS:
        return (
            "common_free_public",
            "Widely known indicator formula or standard candle-pattern logic; typically reproducible without paid access.",
        )
    if label in _CUSTOM_REPRODUCIBLE_LABELS:
        return (
            "custom_reproducible",
            "Likely reproducible, but implementation details may vary across vendors and need manual validation.",
        )
    if label in _EXTERNAL_OR_UNCERTAIN_LABELS:
        return (
            "external_or_uncertain",
            "Depends on external platform, webhook, vendor logic, or protected implementation; paid/free status is uncertain.",
        )
    return (
        "external_or_uncertain",
        "No reliable paid/free visibility in local files; treat as uncertain until verified from source platform.",
    )


_ALL_INDICATOR_LABELS: list[str] = [
    "SMA (Simple Moving Average)", "Current Candle", "Entry Candle", "Super Trend",
    "EMA (Exponential Moving Average)", "RSI (Relative Strength Index)", "HMA (Hull Moving Average)",
    "Bollinger Band", "Previous Candle", "MACD (Moving Average Convergence Divergence)",
    "Pivot Points", "ADX (Average Directional Index)", "Signal", "Condition", "Range Breakout",
    "CandleAt", "CCI (Commodity Channel Index)", "VWAP (Volume Weighted Average Price)",
    "CPR (Central Pivot Range)", "ORB (Opening Range Breakout)", "Signal Candle",
    "External Signal", "Tradingview", "Chartink", "Manual Trigger", "Previous Nth Candle",
    "Last N Candles", "AVWAP (Anchored Volume Weighted Average Price)", "Aroon Oscillator",
    "Aroon", "ATR (Average True Range)", "Awesome Oscillator", "Basket", "Combined Premium",
    "Options Basket", "Bearish Engulfing", "Bearish Hammer", "Bearish Inverted Hammer",
    "Bearish Marubozu", "Bearish Spinning Top", "Bullish Engulfing", "Bullish Hammer",
    "Bullish Inverted Hammer", "Bullish Marubozu", "Bullish Spinning Top", "Chandelier Exit",
    "coppockCurve", "Dark Cloud Cover", "Doji", "Don Chian Channel", "Dragon Fly Doji",
    "Evening Star", "FT (Fisher Transform)", "Gap Strategy", "Grave Stone Doji", "Half Trend",
    "Heikinashi Bearish", "Heikinashi Bearish Indecision", "Heikinashi Bullish",
    "Heikinashi Bullish Indecision", "Heikinashi Very Bearish", "Heikinashi Very Bullish",
    "Ichimoku Cloud", "Keltner Channel", "Long Buildup", "Long Legged Doji", "Long Unwinding",
    "MRC (Mean Reversion Channel)", "MFI (Money Flow Index)", "Morning Star",
    "Moving Average For ATR", "Moving Average For RSI", "Parabolic SAR", "Piercing Line",
    "Qqe Signals", "ROC (Rate of Change)", "Price Ration", "Ratio", "Short Buildup",
    "Short Covering", "SMMA (Smoothed Moving Average)", "Stochastic Oscillator",
    "Stochastic RSI", "Three Black Crows", "Three White Soldiers", "Today Candle",
    "Transaction StopLoss", "Trend Intensity Index", "WMA (Weighted Moving Average)",
    "Wilder Smoothing Average", "Williams Alligator", "Williams R", "Previous Day Candle",
    "Yesterday Candle",
]


@lru_cache(maxsize=1)
def build_indicator_catalog() -> tuple[IndicatorDefinition, ...]:
    # Load pipesSchema if file exists; fall back to empty dict so indicators
    # still appear with inferred/unmapped status.
    try:
        schema_json = _read_json(SCHEMA_PATH)
        schema_map = schema_json.get("pipesSchema", {})
        output_schema_map = schema_json.get("pipesOutputSchema", {})
    except (FileNotFoundError, json.JSONDecodeError):
        schema_map = {}
        output_schema_map = {}

    # Use explicit label list; fall back to parsing HTML dropdown if file looks valid.
    labels: list[str] = _ALL_INDICATOR_LABELS
    try:
        from_html = _parse_dropdown_labels(DROPDOWN_HTML_PATH)
        if from_html:
            labels = from_html
    except (FileNotFoundError, Exception):
        pass

    definitions: list[IndicatorDefinition] = []
    for label in labels:
        schema_key = _EXPLICIT_SCHEMA_KEY_BY_LABEL.get(label, _to_schema_name(label))
        status, fields = _build_fields_for_indicator(label, schema_key, schema_map)
        access_tier, tier_reason = _classify_access_tier(label)
        outputs = tuple(output_schema_map.get(schema_key) or ())
        definitions.append(
            IndicatorDefinition(
                label=label,
                schema_key=schema_key,
                status=status,
                access_tier=access_tier,
                tier_reason=tier_reason,
                fields=fields,
                category=_category_for_schema_key(schema_key),
                outputs=outputs,
            )
        )
    return tuple(definitions)


def get_indicator_labels() -> list[str]:
    return [definition.label for definition in build_indicator_catalog()]


def get_indicator_definition(label: str) -> IndicatorDefinition | None:
    for definition in build_indicator_catalog():
        if definition.label == label:
            return definition
    return None


def _default_value(field: IndicatorField, definition: IndicatorDefinition) -> Any:
    if field.key == "name":
        return definition.schema_key
    if field.key == "chartType":
        return "Candle"
    if field.key == "candleInterval":
        return "5 minutes"
    if field.key == "valuePaths":
        return ["equity"]
    if field.kind == "select":
        return field.options[0] if field.options else ""
    if field.kind == "date":
        return "2026-05-26"
    if field.kind == "time":
        return "09:15"
    if field.kind == "number":
        defaults = {
            "noOfCandles": 9,
            "multiplier": 2,
            "fastPeriod": 12,
            "slowPeriod": 26,
            "signalPeriod": 9,
            "DILength": 14,
            "ADXSmoothing": 14,
            "conversionPeriod": 9,
            "basePeriod": 26,
            "spanPeriod": 52,
            "displacement": 26,
            "rsiPeriod": 14,
            "stochasticPeriod": 14,
            "kPeriod": 3,
            "dPeriod": 3,
            "narrowRange": 0.3,
            "moderatelyRange": 0.6,
            "wideRange": 1.2,
            "maxDiffBetweenOpenHighAndOpenLowInPercentage": 1,
            "acceleration": 0.02,
            "maximum": 0.2,
            "stopLoss": 100,
        }
        return defaults.get(field.key, 1)
    return ""


def build_prefilled_indicator_payload(label: str) -> dict[str, Any]:
    definition = get_indicator_definition(label)
    if definition is None:
        raise KeyError(f"Unknown indicator label: {label}")

    config: dict[str, Any] = {}
    name = definition.schema_key
    chart_type = "Candle"

    for field in definition.fields:
        value = _default_value(field, definition)
        if field.key == "name":
            name = value
            continue
        if field.key == "chartType":
            chart_type = value
            continue
        config[field.key] = value

    return {
        "type": definition.schema_key,
        "name": name,
        "chartType": chart_type,
        "schemaStatus": definition.status,
        "config": config,
    }


def as_serializable_catalog() -> list[dict[str, Any]]:
    return [
        {
            "label": definition.label,
            "schema_key": definition.schema_key,
            "status": definition.status,
            "access_tier": definition.access_tier,
            "tier_reason": definition.tier_reason,
            "fields": [asdict(field) for field in definition.fields],
            "category": definition.category,
            "outputs": list(definition.outputs),
        }
        for definition in build_indicator_catalog()
    ]


def get_indicators_grouped_by_tier() -> dict[str, list[str]]:
    grouped = {
        "common_free_public": [],
        "custom_reproducible": [],
        "external_or_uncertain": [],
    }
    for definition in build_indicator_catalog():
        grouped.setdefault(definition.access_tier, []).append(definition.label)
    return grouped


if __name__ == "__main__":
    catalog = build_indicator_catalog()
    print(f"Loaded {len(catalog)} indicators from {DROPDOWN_HTML_PATH.name}")
    for item in catalog[:10]:
        print(f"- {item.label} [{item.status}] [{item.access_tier}] -> {item.schema_key}")
