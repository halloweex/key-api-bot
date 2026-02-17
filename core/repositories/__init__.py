"""Repository mixins for DuckDBStore."""
from .customers import CustomersMixin
from .expenses import ExpensesMixin
from .goals import GoalsMixin
from .inventory import InventoryMixin
from .revenue import RevenueMixin
from .traffic import TrafficMixin
from .users import UsersMixin

__all__ = [
    "CustomersMixin",
    "ExpensesMixin",
    "GoalsMixin",
    "InventoryMixin",
    "RevenueMixin",
    "TrafficMixin",
    "UsersMixin",
]
