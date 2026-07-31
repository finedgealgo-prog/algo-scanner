from typing import Any, List, Optional, Union

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from pydantic import BaseModel

from .service import (
    backfill_missing_tokens,
    backfill_stocks_list_exchange,
    delete_portfolio,
    exit_goldbees_strategy,
    generate_combained_stock_scores,
    generate_stock_scores,
    get_combained_portfolio_detail,
    get_live_combained_portfolio,
    get_live_portfolio,
    get_combained_portfolio_detail,
    get_index_historical_chart_bars,
    get_indexes,
    get_scanner_portfolio_options,
    get_portfolio_settings,
    get_portfolio_detail,
    get_portfolio_summary,
    get_sectors,
    invest_goldbees_strategy,
    rebalance_combained_portfolio,
    rebalance_portfolio,
    run_scanner_backtest,
    save_portfolio,
    backfill_stocks_list_kite_tokens,
    backfill_index_stocks_kite_tokens,
    get_scanner_historical_sync_status,
    get_previous_universe_stocks,
    remove_universe_field_from_stocks_list,
    request_stop_scanner_historical_sync,
    start_scanner_historical_data_sync,
    sync_scanner_daily_market_snapshot,
    sync_market_holidays,
    parse_multi_strategy_reports,
    run_backtest_from_file_inputs,
    parse_single_file_inputs,
    run_multi_backtest_from_payloads,
    kite_sync_universe_stock_list,
    sync_universe_stock_list,
    update_portfolio_investments,
    get_kite_universe_stocks,
    sync_kite_universe_to_db,
    sync_all_kite_universes_to_db,
    sync_stocks_industry_from_nse,
)

router = APIRouter(prefix="/scanner", tags=["scanner"])


class RunBacktestRequest(BaseModel):
    indexes: Optional[List[str]] = ["nifty_50"]
    sectors: Optional[List[str]] = []
    formula: Optional[str] = None
    starting_capital: float = 1_000_000
    entry_rank: int = 12
    exit_rank: int = 25
    rebalance_frequency: str = "monthly"
    rebalance_date: Union[str, int] = "10"
    alternative_rebalance_day: str = "next_day"
    position_sizing: str = "equal_weight"
    strategy_name: str = "My Strategy"
    start_date: str = "2020-01-01"
    end_date: str = "2026-01-01"
    regime_filter_status: bool = True
    regime_filter: str = "supertrend_1_2_5"
    regime_filter_action: str = "go_cash"
    regime_filter_indexes: str = "nifty_500"
    uncorrelated_asset_status: bool = True
    uncorrelated_asset_type: str = "gold_bees"
    uncorrelated_asset_allocation: int = 100
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    stock_price_min: Optional[float] = None
    stock_price_max: Optional[float] = None
    score_model: Optional[str] = "current"


class EodScoringRequest(BaseModel):
    index_name: Optional[Union[List[str], str]] = None
    index_names: Optional[Union[List[str], str]] = None
    sectors: Optional[Union[List[str], str]] = None
    min_price: Optional[Union[float, int, str]] = None
    max_price: Optional[Union[float, int, str]] = None
    top_n: int = 12
    total_capital: float = 1_000_000
    score_date: Optional[str] = None
    formula: Optional[str] = None
    index_name_1: Optional[Union[List[str], str]] = None
    index_names_1: Optional[Union[List[str], str]] = None
    sectors_1: Optional[Union[List[str], str]] = None
    min_price_1: Optional[Union[float, int, str]] = None
    max_price_1: Optional[Union[float, int, str]] = None
    top_n_1: Optional[int] = None
    total_capital_1: Optional[float] = None
    score_date_1: Optional[str] = None
    formula_1: Optional[str] = None
    score_model: Optional[str] = "current"


class UpdatePortfolioRequest(BaseModel):
    portfolio_strategy_id: str
    invest_stock_data: Optional[list[dict[str, Any]]] = None
    score_data: Optional[list[dict[str, Any]]] = None
    total_capital: Optional[float] = None
    top_n: Optional[int] = None


class SavePortfolioRequest(BaseModel):
    portfolio_settings: dict[str, Any]
    invest_stock_data: list[dict[str, Any]]


@router.get("/indexes")
async def scanner_indexes() -> dict[str, Any]:
    return {"status": "success", "items": get_indexes()}

@router.get("/quotes")
async def scanner_quotes(tokens: str = "", broker_id: str = Query(default="")) -> dict[str, Any]:
    requested_tokens = [
        str(token).strip()
        for token in str(tokens or "").split(",")
        if str(token).strip()
    ]
    requested_token_set = set(requested_tokens)
    try:
        from api import simulator_pt_quotes as _simulator_pt_quotes
    except Exception:
        from backend.api import simulator_pt_quotes as _simulator_pt_quotes
    payload = await _simulator_pt_quotes(tokens=tokens, broker_id=broker_id, include_index_defaults=False)
    if not requested_token_set:
        return payload
    quotes = payload.get("quotes") if isinstance(payload, dict) else None
    if isinstance(quotes, dict):
        payload["quotes"] = {
            token: value
            for token, value in quotes.items()
            if str(token).strip() in requested_token_set
        }
    return payload



@router.get("/sectors")
async def scanner_sectors() -> dict[str, Any]:
    return {"status": "success", "items": get_sectors()}


@router.get("/index_historical_chart")
async def scanner_index_historical_chart(
    i_symbol: str = Query(..., description="Normalized index symbol, e.g. nifty_50"),
    from_ts: Optional[int] = Query(default=None, alias="from"),
    to_ts: Optional[int] = Query(default=None, alias="to"),
    resolution: str = Query(default="1D", description="5/15/30/60/240/480 (minutes) or 1D"),
) -> dict[str, Any]:
    try:
        return get_index_historical_chart_bars(i_symbol, from_ts=from_ts, to_ts=to_ts, resolution=resolution)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_universe_stocks")
async def scanner_sync_universe_stocks() -> dict[str, Any]:
    try:
        return sync_universe_stock_list()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/kite_sync_universe_stocks")
async def scanner_kite_sync_universe_stocks() -> dict[str, Any]:
    try:
        return kite_sync_universe_stock_list()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/universe_stocks/{filter_symbol}")
async def scanner_universe_stocks(filter_symbol: str) -> dict[str, Any]:
    try:
        return get_kite_universe_stocks(filter_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_universe_to_db/{filter_symbol}")
async def scanner_sync_universe_to_db(filter_symbol: str) -> dict[str, Any]:
    try:
        return sync_kite_universe_to_db(filter_symbol)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_all_universes_to_db")
async def scanner_sync_all_universes_to_db() -> dict[str, Any]:
    try:
        return sync_all_kite_universes_to_db()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_stocks_industry")
async def scanner_sync_stocks_industry() -> dict[str, Any]:
    try:
        return sync_stocks_industry_from_nse()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/backfill_stocks_kite_tokens")
async def scanner_backfill_stocks_kite_tokens() -> dict[str, Any]:
    try:
        return backfill_stocks_list_kite_tokens()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/backfill_missing_tokens")
async def scanner_backfill_missing_tokens() -> dict[str, Any]:
    try:
        return backfill_missing_tokens()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/backfill_stocks_exchange")
async def scanner_backfill_stocks_exchange() -> dict[str, Any]:
    try:
        return backfill_stocks_list_exchange()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/backfill_index_stocks_kite_tokens")
async def scanner_backfill_index_stocks_kite_tokens() -> dict[str, Any]:
    try:
        return backfill_index_stocks_kite_tokens()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_market_holidays")
async def scanner_sync_market_holidays(
    year: int = Query(default=2026, ge=2000, le=2100),
) -> dict[str, Any]:
    try:
        return sync_market_holidays(year)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_historical_data")
async def scanner_sync_historical_data(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
) -> dict[str, Any]:
    try:
        return start_scanner_historical_data_sync(start_date, end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/sync_universe_historical_data")
async def scanner_sync_universe_historical_data(
    start_date: str = Query(..., description="YYYY-MM-DD"),
    end_date: str = Query(..., description="YYYY-MM-DD"),
) -> dict[str, Any]:
    try:
        return start_scanner_historical_data_sync(start_date, end_date, use_universe_list=True)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/sync_daily_market_snapshot")
async def scanner_sync_daily_market_snapshot() -> dict[str, Any]:
    try:
        return sync_scanner_daily_market_snapshot()
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/stop_historical_data_sync")
async def scanner_stop_historical_data_sync() -> dict[str, Any]:
    try:
        return request_stop_scanner_historical_sync()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/historical_data_sync_status")
async def scanner_historical_data_sync_status() -> dict[str, Any]:
    try:
        return get_scanner_historical_sync_status()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/remove_universe_field")
async def scanner_remove_universe_field() -> dict[str, Any]:
    try:
        return remove_universe_field_from_stocks_list()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/previous_universe_stocks/{filter_symbol}")
async def scanner_previous_universe_stocks(filter_symbol: str) -> dict[str, Any]:
    try:
        return get_previous_universe_stocks(filter_symbol)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/eod_scoring")
async def scanner_eod_scoring(body: EodScoringRequest) -> dict[str, Any]:
    try:
        result = generate_stock_scores(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


@router.post("/eod_scoring_combained")
async def scanner_eod_scoring_combained(body: EodScoringRequest) -> dict[str, Any]:
    try:
        result = generate_combained_stock_scores(body.model_dump())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    return result


@router.post("/update_portfolio")
async def scanner_update_portfolio(body: UpdatePortfolioRequest) -> dict[str, Any]:
    try:
        return update_portfolio_investments(
            body.portfolio_strategy_id,
            body.invest_stock_data,
            score_data=body.score_data,
            total_capital=body.total_capital,
            top_n=body.top_n,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/save_portfolio")
async def scanner_save_portfolio(body: SavePortfolioRequest) -> dict[str, Any]:
    try:
        return save_portfolio(body.portfolio_settings, body.invest_stock_data)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.delete("/portfolio/{portfolio_id}")
async def scanner_delete_portfolio(portfolio_id: str) -> dict[str, Any]:
    try:
        return delete_portfolio(portfolio_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/portfolio_summary")
async def scanner_portfolio_summary(portfolio_master_id: str = Query(default="")) -> list[dict[str, Any]]:
    try:
        return get_portfolio_summary(portfolio_master_id=portfolio_master_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/get_portfolio")
async def scanner_get_portfolio() -> list[dict[str, str]]:
    try:
        return get_scanner_portfolio_options()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/portfolio_settings/{strategy_id}")
async def scanner_portfolio_settings(strategy_id: str) -> dict[str, Any]:
    try:
        return get_portfolio_settings(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/detailPortfolio/{strategy_id}")
async def scanner_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    try:
        return get_portfolio_detail(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/combainedDetailPortfolio/{strategy_id}")
async def scanner_combained_portfolio_detail(strategy_id: str) -> dict[str, Any]:
    try:
        return get_combained_portfolio_detail(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/live_prices/strategy/{strategy_id}")
async def scanner_live_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return get_live_portfolio(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/live_prices/combained_strategy/{strategy_id}")
async def scanner_live_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return get_live_combained_portfolio(strategy_id)
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if "not found" in message.lower() else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/rebalance/{strategy_id}")
async def scanner_rebalance_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return rebalance_portfolio(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/combained_rebalance/{strategy_id}")
async def scanner_rebalance_combained_portfolio(strategy_id: str) -> dict[str, Any]:
    try:
        return rebalance_combained_portfolio(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/invest_goldbees/{strategy_id}")
async def scanner_invest_goldbees(strategy_id: str) -> dict[str, Any]:
    try:
        return invest_goldbees_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/prtfolio/exit_goldbees/{strategy_id}")
async def scanner_exit_goldbees(strategy_id: str) -> dict[str, Any]:
    try:
        return exit_goldbees_strategy(strategy_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/run_backtest")
async def scanner_run_backtest(body: RunBacktestRequest) -> dict[str, Any]:
    try:
        payload = body.model_dump()
        if payload.get("min_price") is None and payload.get("stock_price_min"):
            payload["min_price"] = payload["stock_price_min"]
        if payload.get("max_price") is None and payload.get("stock_price_max"):
            payload["max_price"] = payload["stock_price_max"]
        return run_scanner_backtest(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/multi_strategy_reports")
async def scanner_multi_strategy_reports(
    files: list[UploadFile] = File(...),
    strategy_names: list[str] = Form(...),
) -> dict[str, Any]:
    try:
        if len(files) != len(strategy_names):
            raise ValueError("Each uploaded file must have a matching strategy name.")
        payload_files = []
        for file, strategy_name in zip(files, strategy_names):
            payload_files.append({
                "file_name": file.filename or "strategy_report.xls",
                "strategy_name": strategy_name,
                "content": await file.read(),
            })
        return parse_multi_strategy_reports(payload_files)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/rerun_backtest_from_files")
async def scanner_rerun_backtest_from_files(
    files: list[UploadFile] = File(...),
    strategy_names: list[str] = Form(...),
) -> dict[str, Any]:
    try:
        if len(files) != len(strategy_names):
            raise ValueError("Each uploaded file must have a matching strategy name.")
        payload_files = []
        for file, strategy_name in zip(files, strategy_names):
            payload_files.append({
                "file_name": file.filename or "strategy_report.xls",
                "strategy_name": strategy_name,
                "content": await file.read(),
            })
        return run_backtest_from_file_inputs(payload_files)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/parse_file_input")
async def scanner_parse_file_input(
    file: UploadFile = File(...),
) -> dict[str, Any]:
    try:
        content = await file.read()
        return parse_single_file_inputs(content, file.filename or "strategy.xls")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.post("/run_backtest_multi")
async def scanner_run_backtest_multi(body: dict[str, Any]) -> dict[str, Any]:
    try:
        strategies = body.get("strategies")
        if not isinstance(strategies, list) or not strategies:
            raise ValueError("strategies must be a non-empty array.")
        return run_multi_backtest_from_payloads(strategies)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
