"""Margin analysis service layer."""
import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, date

from core.duckdb_store import get_store
from core.filters import parse_period as _parse_period_core

logger = logging.getLogger(__name__)


def parse_period(period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, str]:
    return _parse_period_core(period, start_date, end_date).as_str_tuple()


def _parse_dates(start_date: str, end_date: str) -> Tuple[date, date]:
    return datetime.strptime(start_date, "%Y-%m-%d").date(), datetime.strptime(end_date, "%Y-%m-%d").date()


async def get_margin_overview(start_date: str, end_date: str, sales_type: str = "retail") -> Dict[str, Any]:
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_margin_overview(start, end, sales_type=sales_type)


async def get_margin_by_brand(start_date: str, end_date: str, sales_type: str = "retail", limit: int = 20) -> List[Dict[str, Any]]:
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_margin_by_brand(start, end, sales_type=sales_type, limit=limit)


async def get_margin_by_category(start_date: str, end_date: str, sales_type: str = "retail") -> List[Dict[str, Any]]:
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_margin_by_category(start, end, sales_type=sales_type)


async def get_margin_trend(start_date: str, end_date: str, sales_type: str = "retail") -> List[Dict[str, Any]]:
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_margin_trend(start, end, sales_type=sales_type)


async def get_margin_brand_category(start_date: str, end_date: str, sales_type: str = "retail", min_revenue: float = 500) -> List[Dict[str, Any]]:
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_margin_brand_category(start, end, sales_type=sales_type, min_revenue=min_revenue)


async def get_margin_alerts(start_date: str, end_date: str, sales_type: str = "retail", margin_floor: float = 30.0) -> List[Dict[str, Any]]:
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_margin_alerts(start, end, sales_type=sales_type, margin_floor=margin_floor)
