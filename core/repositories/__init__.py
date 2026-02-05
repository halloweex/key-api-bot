"""
Repository layer for DuckDB store.

This module splits the monolithic DuckDBStore into focused repositories:
- BaseRepository: Connection management and schema initialization
- SyncRepository: Upsert operations and sync metadata
- AnalyticsRepository: Revenue, sales, and customer analytics
- CatalogRepository: Categories, brands, products queries
"""
from core.repositories.base import BaseRepository
from core.repositories.sync_repo import SyncRepository
from core.repositories.analytics_repo import AnalyticsRepository
from core.repositories.catalog_repo import CatalogRepository

__all__ = [
    "BaseRepository",
    "SyncRepository",
    "AnalyticsRepository",
    "CatalogRepository",
]
