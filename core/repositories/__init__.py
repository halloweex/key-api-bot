"""
Repository layer for DuckDB store.

This module splits the monolithic DuckDBStore into focused repositories:
- BaseRepository: Connection management and schema initialization
- SyncRepository: Upsert operations and sync metadata
- InventoryRepository: Stock and inventory management
- AnalyticsRepository: Revenue, sales, and customer analytics
- CatalogRepository: Categories, brands, products queries
- GoalsRepository: Goal setting and seasonality calculations
"""
from core.repositories.base import BaseRepository
from core.repositories.sync_repo import SyncRepository
from core.repositories.inventory_repo import InventoryRepository
from core.repositories.analytics_repo import AnalyticsRepository
from core.repositories.catalog_repo import CatalogRepository
from core.repositories.goals_repo import GoalsRepository

__all__ = [
    "BaseRepository",
    "SyncRepository",
    "InventoryRepository",
    "AnalyticsRepository",
    "CatalogRepository",
    "GoalsRepository",
]
