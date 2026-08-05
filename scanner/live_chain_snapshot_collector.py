"""
live_chain_snapshot_collector.py
──────────────────────────────────
Manual start/stop wrapper around service.sync_live_option_chain_snapshot —
does NOT auto-start on server boot. It only runs once armed from the admin
Monitors page (algo-admin's src/pages/Admin/Monitors.tsx), mirroring
live_option_chain_collector.py's start()/stop()/status() shape so the same
admin-page pattern (GET/POST .../start, POST .../stop, GET .../status)
works here too.

Separate destination from live_option_chain_collector.py on purpose: that
one writes stock_data.option_chain (NIFTY-only, 30-strike window) for
OptionChainManager's algo.simulator backtests. This one writes
option_chain_historical_data / option_chain_index_spot (every F&O index
underlying, full unwindowed chain) for strike_selector.py's backtest path —
different consumer, different shape, not a duplicate of that collector.

Once started, the background thread checks every ~60s whether it's inside
NSE market hours (09:15-15:30 IST) on a trading day (weekday, not in
market_holidays) and only then calls sync_live_option_chain_snapshot() once.
Outside that window it just idles and rechecks — stop() must be called
explicitly to end the thread; market-hours gating is a filter on top of the
armed/disarmed state, not a substitute for it.
"""

import logging
import threading
from datetime import datetime, timedelta, timezone
from typing import Any

logger = logging.getLogger(__name__)

_IST = timezone(timedelta(hours=5, minutes=30))
_MARKET_OPEN = (9, 15)
_MARKET_CLOSE = (15, 30)
_POLL_INTERVAL_S = 60


class LiveChainSnapshotCollector:
    def __init__(self) -> None:
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_run_at: str | None = None
        self._last_result: dict[str, Any] | None = None

    def start(self) -> dict[str, Any]:
        if self._thread and self._thread.is_alive():
            return {"status": "already_running"}
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run_loop, daemon=True, name="live_chain_snapshot_collector",
        )
        self._thread.start()
        logger.info("[LIVE CHAIN SNAPSHOT] collector started")
        return {"status": "started"}

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=5)
        logger.info("[LIVE CHAIN SNAPSHOT] collector stopped")
        return {"status": "stopped", "last_run_at": self._last_run_at}

    def status(self) -> dict[str, Any]:
        return {
            "running": bool(self._thread and self._thread.is_alive()),
            "last_run_at": self._last_run_at,
            "last_result": self._last_result,
        }

    def _in_market_window(self, db) -> bool:
        now = datetime.now(_IST)
        if now.weekday() >= 5:
            return False
        if not (_MARKET_OPEN <= (now.hour, now.minute) <= _MARKET_CLOSE):
            return False
        try:
            return not bool(db._db["market_holidays"].find_one({"date": now.strftime("%Y-%m-%d")}))
        except Exception:
            logger.exception("[LIVE CHAIN SNAPSHOT] holiday lookup failed — proceeding as trading day")
            return True

    def _run_loop(self) -> None:
        from features.mongo_data import MongoData
        from .service import sync_live_option_chain_snapshot

        while not self._stop_event.is_set():
            db = MongoData()
            try:
                if self._in_market_window(db):
                    try:
                        self._last_result = sync_live_option_chain_snapshot()
                    except Exception as exc:
                        logger.exception("[LIVE CHAIN SNAPSHOT] pass failed")
                        self._last_result = {"status": "error", "message": str(exc)}
                    self._last_run_at = datetime.now(_IST).strftime("%Y-%m-%dT%H:%M:%S")
            finally:
                db.close()
            if self._stop_event.wait(timeout=_POLL_INTERVAL_S):
                break


collector = LiveChainSnapshotCollector()
