"""
Dashboard service for analytics data.

Uses DuckDB as the primary data source for fast, persistent queries.
No in-memory caching needed - DuckDB queries are <10ms.
"""
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Any, Sequence, Tuple

from core.store import get_store
from core.filters import parse_period as _parse_period_core

logger = logging.getLogger(__name__)


def parse_period(period: Optional[str], start_date: Optional[str], end_date: Optional[str]) -> Tuple[str, str]:
    """Parse period shortcut or dates into (start_date, end_date) tuple."""
    return _parse_period_core(period, start_date, end_date).as_str_tuple()


def stack_forecast_on_actuals(
    start: date, revenue: List[float], predictions: List[Dict[str, Any]],
) -> Dict[str, float]:
    """The trend's forecast badge: what the chart's bars add up to.

    `revenue` is the trend's daily series, one value a day from `start`. A
    day stands at its actual, and a day with a forecast at the larger of its
    actual and its forecast: the chart stacks `max(0, forecast − actual)` on
    the day's actual (`useRevenueTrendChartData`). So today's revenue so far
    is not counted again inside its full-day forecast, a day that already
    beat its forecast adds nothing, and the week counts the days behind it.
    Until 2026-11-05 the month's badge added today's actual to its whole
    forecast — ₴116 546 twice on 2026-11-04 — and the week's held only the
    days from today on.
    """
    actual = {(start + timedelta(days=i)).isoformat(): float(v or 0)
              for i, v in enumerate(revenue)}
    actual_to_date = round(sum(actual.values()), 2)
    remaining = round(sum(
        max(0.0, float(p["predicted_revenue"]) - actual.get(p["date"], 0.0))
        for p in predictions), 2)
    return {
        "actual_to_date": actual_to_date,
        "predicted_remaining": remaining,
        "predicted_total": round(actual_to_date + remaining, 2),
    }


def last_day_of_period(period: Optional[str], start: date, end: date) -> date:
    """The last day of the period a trend request names.

    `month` and `week` run from their first day to today (`parse_period`),
    and the chart draws them to the month's or the week's end: a forecast
    fills the days ahead, and the comparison is the whole period's (owner's
    decision, 2026-11-12). Any other period ends where it says.
    """
    if period == "month":
        first_of_next = (start.replace(day=28) + timedelta(days=4)).replace(day=1)
        return first_of_next - timedelta(days=1)
    if period == "week":
        return start + timedelta(days=6)
    return end


def fit_trend(
    result: Dict[str, Any], start: date, forecast_days: Sequence[date] = (),
) -> None:
    """Lay the trend out over the days the chart draws, in place.

    The series' own days come first, then `forecast_days`: a forecast's days
    past them, at zero revenue and orders, which the chart draws as forecast
    bars. Beside every one of those days the comparison holds the day at the
    same position of its window, day `d` beside position `d − start`. The
    store's comparison runs over the whole period (`period_end`), longer
    than the series, so this also cuts it back to the days drawn. A position
    past the window draws 0: 31 October beside a 30-day September has no
    day there.

    Until 2026-11-12 a forecast day was set beside the day as many days back
    as there were forecast days, read from Silver while the rest came from
    Gold: on 12 November the line under 19–30 November drew 1–12 November.
    """
    labels = result.get("labels")
    if labels is None:
        return
    positions = list(range(len(labels))) + [(d - start).days for d in forecast_days]
    for d in forecast_days:
        labels.append(d.strftime("%d.%m"))
        result["revenue"].append(0)
        result["orders"].append(0)
    comparison = result.get("comparison")
    if comparison:
        window = list(comparison["revenue"])
        # In place: the same list is the "Previous Period" dataset's data.
        comparison["revenue"][:] = [
            window[k] if k < len(window) else 0 for k in positions]


def _parse_dates(start_date: str, end_date: str) -> Tuple[date, date]:
    """Parse date strings to date objects."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    return start, end


# ─── Dashboard Query Functions ────────────────────────────────────────────────


async def get_revenue_trend(
    start_date: str,
    end_date: str,
    granularity: str = "daily",
    include_comparison: bool = True,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail",
    compare_type: str = "previous_period",
    promocode: Optional[str] = None,
    period_end: Optional[date] = None,
    through: Optional[date] = None,
) -> Dict[str, Any]:
    """Get revenue data over time for line chart. `period_end` and
    `through` are the store's: the whole period the comparison is of, and
    the last day the growth badge counts."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_revenue_trend(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        include_comparison=include_comparison,
        sales_type=sales_type,
        compare_type=compare_type,
        promocode=promocode,
        granularity=granularity,
        period_end=period_end,
        through=through,
    )


async def get_forecast_data(sales_type: str = "retail") -> Optional[Dict[str, Any]]:
    """Get ML revenue forecast for the current month.

    None is "no forecast": no model yet, nothing predicted, or a read that
    failed. A read refused because its engine failed
    (`read_fallback.refuse`) is not one of those (DN-20b): it passes
    through, so `/api/revenue/forecast` answers 503
    naming `goals` instead of "Forecast not available yet", which read an
    outage as a model nobody had trained. The trend does not call this: its
    overlay is `get_range_forecast` over the period (2026-11-12), which reads
    no actuals, and its handler drops the overlay, not the chart.
    """
    from core.read_fallback import ReadUnavailable

    try:
        from core.prediction_service import get_prediction_service
        service = get_prediction_service()
        return await service.get_forecast(sales_type)
    except ReadUnavailable:
        raise
    except Exception as e:
        logger.warning(f"Forecast data unavailable: {e}")
        return None


async def get_sales_by_source(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail",
    promocode: Optional[str] = None,
) -> Dict[str, Any]:
    """Get sales data aggregated by source for bar/pie chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_sales_by_source(
        start, end,
        category_id=category_id,
        brand=brand,
        sales_type=sales_type,
        promocode=promocode,
    )


async def get_top_products(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    limit: int = 10,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    sales_type: str = "retail",
    promocode: Optional[str] = None,
) -> Dict[str, Any]:
    """Get top products for horizontal bar chart."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_top_products(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        limit=limit,
        sales_type=sales_type,
        promocode=promocode,
    )


async def get_summary_stats(
    start_date: str,
    end_date: str,
    category_id: Optional[int] = None,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail",
    promocode: Optional[str] = None,
) -> Dict[str, Any]:
    """Get summary statistics for dashboard cards."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_summary_stats(
        start, end,
        source_id=source_id,
        category_id=category_id,
        brand=brand,
        sales_type=sales_type,
        promocode=promocode,
    )


async def get_customer_insights(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail",
    promocode: Optional[str] = None,
    granularity: str = "daily",
) -> Dict[str, Any]:
    """Get customer insights: new vs returning, AOV trend, repeat rate."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_customer_insights(
        start, end,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type,
        promocode=promocode,
        granularity=granularity,
    )


async def get_product_performance(
    start_date: str,
    end_date: str,
    brand: Optional[str] = None,
    source_id: Optional[int] = None,
    sales_type: str = "retail",
    promocode: Optional[str] = None,
) -> Dict[str, Any]:
    """Get product performance: top by revenue, category breakdown."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_product_performance(
        start, end,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type,
        promocode=promocode,
    )


async def get_brand_analytics(start_date: str, end_date: str, sales_type: str = "retail") -> Dict[str, Any]:
    """Get brand analytics: top brands by revenue and quantity."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_brand_analytics(start, end, sales_type=sales_type)


async def get_promocode_analytics(start_date: str, end_date: str, sales_type: str = "retail") -> Dict[str, Any]:
    """Get promocode performance overview."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_promocode_analytics(start, end, sales_type=sales_type)


async def get_subcategory_breakdown(
    start_date: str,
    end_date: str,
    parent_category: str,
    source_id: Optional[int] = None,
    brand: Optional[str] = None,
    sales_type: str = "retail",
    promocode: Optional[str] = None,
) -> Dict[str, Any]:
    """Get sales breakdown by subcategories for a given parent category."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_subcategory_breakdown(
        start, end,
        parent_category_name=parent_category,
        source_id=source_id,
        brand=brand,
        sales_type=sales_type,
        promocode=promocode,
    )


async def get_expense_types() -> List[Dict[str, Any]]:
    """Get list of expense types for filter dropdown."""
    store = await get_store()
    return await store.get_expense_types()


async def get_expense_summary(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    expense_type_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get expense summary: breakdown by type, daily trend."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_expense_summary(
        start, end,
        source_id=source_id,
        expense_type_id=expense_type_id,
        sales_type=sales_type
    )


async def get_profit_analysis(
    start_date: str,
    end_date: str,
    source_id: Optional[int] = None,
    sales_type: str = "retail"
) -> Dict[str, Any]:
    """Get profit analysis: revenue vs expenses."""
    start, end = _parse_dates(start_date, end_date)
    store = await get_store()
    return await store.get_profit_analysis(
        start, end,
        source_id=source_id,
        sales_type=sales_type
    )
