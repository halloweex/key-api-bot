"""Shared dependencies for API route modules."""
import logging
import time

from web.ratelimit import limiter  # single app-wide limiter instance

from core.duckdb_store import get_store
from core.validators import (
    validate_period,
    validate_source_id,
    validate_category_id,
    validate_brand_name,
    validate_limit,
    validate_sales_type,
    validate_promocode,
)
from core.exceptions import ValidationError

# Shared logger factory
def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

# Track startup time for uptime calculation
START_TIME = time.time()
