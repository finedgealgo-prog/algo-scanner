"""
Scanner-only entrypoint for the split backend.

`api.py` here is a trimmed copy of the original monolith: every /algo,
/trade, /broker, /live, /monitor, /mock route plus the simulator/signal_builder/
common imports and mounts have been removed from the source itself (not just
hidden), and the simulator/signal_builder/common packages are not present in
this folder at all. What's left only serves the "Scanner" frontend pages
(EodScore, Portfolio, PortfolioDetail, ScannerBacktest, CombainedBacktest,
MultiStrategy) — the /scanner/* REST API, including the daily market
snapshot background loop (_auto_daily_scanner_snapshot) that those pages'
change_pct/change_points figures depend on staying fresh.

No websocket here — research confirmed no Scanner page uses /ws/live-quotes
or any other socket; if that ever changes, point it at algo.websocket
(ws_main.py) like every other domain, not a local mount.

Shared dependency code (features/, zerodha_config.json, .env) lives in
../shared and is symlinked into this folder rather than duplicated — see
/media/ashok-innoppl/7CD60970D6092C48/algo-backend/shared/.

Run (from /media/ashok-innoppl/7CD60970D6092C48/algo-backend/algo.scanner):
    uvicorn scanner_main:app --reload --port 8002
"""

from api import app  # noqa: E402  (runs api.py's module-level setup as-is)

# Algo-only background loops; features/span_file.py and
# features/backtest_engine.py stay on disk (shared features/ module) but the
# scanner process doesn't need them running. api.py's own
# _auto_start_alert_checker is skipped below — the chart domain (REST +
# alert-checker background loop) has moved to algo.signals entirely (see
# signal_main.py), so this process no longer mounts chart_api.py at all.
SKIP_STARTUP_FUNCS = {"_span_params_startup", "_redis_prewarm", "_auto_start_alert_checker", "_auto_start_ticker"}

app.router.on_startup = [f for f in app.router.on_startup if f.__name__ not in SKIP_STARTUP_FUNCS]
