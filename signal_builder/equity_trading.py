from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from bson import ObjectId
from pydantic import BaseModel

from features.mongo_data import MongoData

STOCKS_COLLECTION = "scanner_stocks_list"
EQUITY_POSITIONS_COLLECTION = "signal_equity_paper_positions"


class EquityOrderIn(BaseModel):
    symbol: str
    side: Literal["BUY", "SELL"]
    quantity: int
    product: Literal["MIS", "CNC"] = "MIS"


def _db():
    return MongoData()._db


def _now_iso() -> str:
    return datetime.now().isoformat()


def search_equity_symbols(query: str, limit: int = 20) -> list[dict[str, Any]]:
    q = query.strip()
    if not q:
        return []
    db = _db()
    rows = db[STOCKS_COLLECTION].find(
        {"$or": [
            {"symbol": {"$regex": f"^{q}", "$options": "i"}},
            {"company_name": {"$regex": q, "$options": "i"}},
        ]},
        {"_id": 0, "symbol": 1, "company_name": 1},
    ).limit(limit)
    return [
        {"symbol": row.get("symbol", ""), "company_name": row.get("company_name") or row.get("symbol", "")}
        for row in rows
    ]


def get_equity_quotes(symbols: list[str]) -> dict[str, dict[str, float]]:
    """symbol -> {ltp, prev_close, change_pct}. Reuses the same Dhan NSE_EQ helpers
    execution_socket.py's stock-underlying pricing branch already relies on
    (_get_dhan_equity_sec_id / _fetch_dhan_market_data), so no new Dhan wiring here.

    prev_close deliberately does NOT come from that Dhan quote's own
    info['prev_close'] (sourced from ohlc.close) — confirmed elsewhere
    (execution_socket.py's _fetch_dhan_index_quotes, same NSE_EQ quirk) that
    Dhan's ohlc.close mirrors the live price all day for cash-equity quotes,
    which would make change_pct round to ~0.00% on every refresh. Using the
    scanner's own daily EOD sync (_load_latest_closes, scanner_stock_historical_data)
    gives the real previous-trading-day close instead.
    """
    from api import _fetch_dhan_market_data, _get_dhan_equity_sec_id  # lazy: avoids a hard circular import at module load
    from scanner.service import _load_latest_closes  # lazy: same reason

    db = _db()
    symbols = [s.strip().upper() for s in symbols if s.strip()]
    sec_id_by_symbol = {s: sec_id for s in symbols if (sec_id := _get_dhan_equity_sec_id(s))}
    if not sec_id_by_symbol:
        return {}

    quote_by_sec_id = _fetch_dhan_market_data("NSE_EQ", [int(v) for v in sec_id_by_symbol.values()], db)
    prev_close_by_symbol = _load_latest_closes(list(sec_id_by_symbol.keys()))

    result: dict[str, dict[str, float]] = {}
    for symbol, sec_id in sec_id_by_symbol.items():
        info = quote_by_sec_id.get(str(sec_id))
        if not info or not info.get("ltp"):
            continue
        ltp = float(info["ltp"])
        prev_close = float(prev_close_by_symbol.get(symbol) or 0)
        change_pct = ((ltp - prev_close) / prev_close * 100) if prev_close > 0 else 0.0
        result[symbol] = {"ltp": ltp, "prev_close": prev_close, "change_pct": change_pct}
    return result


def _require_ltp(symbol: str) -> float:
    quotes = get_equity_quotes([symbol])
    quote = quotes.get(symbol.strip().upper())
    if not quote:
        raise ValueError(f"No live quote available for {symbol}")
    return quote["ltp"]


def _serialize_position(doc: dict[str, Any]) -> dict[str, Any]:
    doc = dict(doc)
    doc["id"] = str(doc.pop("_id"))
    doc.pop("user_id", None)
    return doc


def _signed_pnl(side: str, entry_price: float, exit_price: float, quantity: float) -> float:
    direction = 1 if side == "BUY" else -1
    return (exit_price - entry_price) * quantity * direction


def place_equity_order(user_id: str, order: EquityOrderIn) -> dict[str, Any]:
    symbol = order.symbol.strip().upper()
    if order.quantity <= 0:
        raise ValueError("quantity must be positive")
    ltp = _require_ltp(symbol)
    db = _db()
    col = db[EQUITY_POSITIONS_COLLECTION]

    existing = col.find_one({"user_id": user_id, "symbol": symbol, "exited": False})

    if not existing:
        doc = {
            "user_id": user_id,
            "symbol": symbol,
            "side": order.side,
            "quantity": order.quantity,
            "entry_price": ltp,
            "entry_time": _now_iso(),
            "product": order.product,
            "exited": False,
            "exit_price": None,
            "exit_time": None,
            "realized_pnl": 0.0,
        }
        inserted = col.insert_one(doc)
        doc["_id"] = inserted.inserted_id
        return _serialize_position(doc)

    if existing["side"] == order.side:
        new_quantity = existing["quantity"] + order.quantity
        new_avg_price = (
            existing["entry_price"] * existing["quantity"] + ltp * order.quantity
        ) / new_quantity
        col.update_one(
            {"_id": existing["_id"]},
            {"$set": {"quantity": new_quantity, "entry_price": new_avg_price}},
        )
        existing["quantity"] = new_quantity
        existing["entry_price"] = new_avg_price
        return _serialize_position(existing)

    # Opposite side of an open position — reduce, close, or close-and-flip.
    if order.quantity < existing["quantity"]:
        realized = _signed_pnl(existing["side"], existing["entry_price"], ltp, order.quantity)
        remaining_qty = existing["quantity"] - order.quantity
        col.update_one(
            {"_id": existing["_id"]},
            {"$set": {"quantity": remaining_qty}, "$inc": {"realized_pnl": realized}},
        )
        existing["quantity"] = remaining_qty
        existing["realized_pnl"] = existing.get("realized_pnl", 0.0) + realized
        return _serialize_position(existing)

    realized = _signed_pnl(existing["side"], existing["entry_price"], ltp, existing["quantity"])
    col.update_one(
        {"_id": existing["_id"]},
        {"$set": {"exited": True, "exit_price": ltp, "exit_time": _now_iso()}, "$inc": {"realized_pnl": realized}},
    )

    leftover_qty = order.quantity - existing["quantity"]
    if leftover_qty == 0:
        existing["exited"] = True
        existing["exit_price"] = ltp
        existing["realized_pnl"] = existing.get("realized_pnl", 0.0) + realized
        return _serialize_position(existing)

    flipped = {
        "user_id": user_id,
        "symbol": symbol,
        "side": order.side,
        "quantity": leftover_qty,
        "entry_price": ltp,
        "entry_time": _now_iso(),
        "product": order.product,
        "exited": False,
        "exit_price": None,
        "exit_time": None,
        "realized_pnl": 0.0,
    }
    inserted = col.insert_one(flipped)
    flipped["_id"] = inserted.inserted_id
    return _serialize_position(flipped)


def list_equity_positions(user_id: str) -> dict[str, list[dict[str, Any]]]:
    db = _db()
    col = db[EQUITY_POSITIONS_COLLECTION]

    open_docs = list(col.find({"user_id": user_id, "exited": False}))
    closed_docs = list(
        col.find({"user_id": user_id, "exited": True}).sort("exit_time", -1).limit(50)
    )

    quotes = get_equity_quotes([doc["symbol"] for doc in open_docs]) if open_docs else {}

    open_positions = []
    for doc in open_docs:
        serialized = _serialize_position(doc)
        quote = quotes.get(doc["symbol"])
        ltp = quote["ltp"] if quote else doc["entry_price"]
        serialized["ltp"] = ltp
        serialized["unrealized_pnl"] = _signed_pnl(doc["side"], doc["entry_price"], ltp, doc["quantity"])
        open_positions.append(serialized)

    closed_positions = [_serialize_position(doc) for doc in closed_docs]
    return {"open": open_positions, "closed": closed_positions}


def exit_equity_position(user_id: str, position_id: str) -> dict[str, Any]:
    db = _db()
    col = db[EQUITY_POSITIONS_COLLECTION]
    doc = col.find_one({"_id": ObjectId(position_id), "user_id": user_id, "exited": False})
    if not doc:
        raise ValueError("Open position not found")

    ltp = _require_ltp(doc["symbol"])
    realized = _signed_pnl(doc["side"], doc["entry_price"], ltp, doc["quantity"])
    col.update_one(
        {"_id": doc["_id"]},
        {"$set": {"exited": True, "exit_price": ltp, "exit_time": _now_iso()}, "$inc": {"realized_pnl": realized}},
    )
    doc["exited"] = True
    doc["exit_price"] = ltp
    doc["realized_pnl"] = doc.get("realized_pnl", 0.0) + realized
    return _serialize_position(doc)
