"""
Catalog repository for categories, brands, and product queries.

Handles product catalog metadata queries.
"""
from typing import List, Dict, Any, Optional

from core.repositories.base import BaseRepository
from core.observability import get_logger

logger = get_logger(__name__)


class CatalogRepository(BaseRepository):
    """Repository for catalog data - categories, brands, products."""

    async def get_categories(self) -> List[Dict[str, Any]]:
        """
        Get root categories (no parent).

        Returns:
            List of root categories with id and name
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT id, name
                FROM categories
                WHERE parent_id IS NULL
                ORDER BY name
            """).fetchall()

            return [{"id": r[0], "name": r[1]} for r in result]

    async def get_child_categories(self, parent_id: int) -> List[Dict[str, Any]]:
        """
        Get child categories for a parent.

        Args:
            parent_id: Parent category ID

        Returns:
            List of child categories
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT id, name
                FROM categories
                WHERE parent_id = ?
                ORDER BY name
            """, [parent_id]).fetchall()

            return [{"id": r[0], "name": r[1]} for r in result]

    async def get_category_with_children(
        self, category_id: int
    ) -> List[int]:
        """
        Get category ID and all its descendants.

        Used for filtering orders/products by category including subcategories.

        Args:
            category_id: Root category ID

        Returns:
            List of category IDs (root + all descendants)
        """
        async with self.connection() as conn:
            # Recursive CTE to get all descendants
            result = conn.execute("""
                WITH RECURSIVE category_tree AS (
                    SELECT id FROM categories WHERE id = ?
                    UNION ALL
                    SELECT c.id
                    FROM categories c
                    JOIN category_tree ct ON c.parent_id = ct.id
                )
                SELECT id FROM category_tree
            """, [category_id]).fetchall()

            return [r[0] for r in result]

    async def get_brands(self) -> List[Dict[str, str]]:
        """
        Get unique brands from products.

        Returns:
            List of brand names
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT DISTINCT brand
                FROM products
                WHERE brand IS NOT NULL AND brand != ''
                ORDER BY brand
            """).fetchall()

            return [{"name": r[0]} for r in result]

    async def get_product(self, product_id: int) -> Optional[Dict[str, Any]]:
        """
        Get product by ID.

        Args:
            product_id: Product ID

        Returns:
            Product dict or None
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT
                    p.id,
                    p.name,
                    p.sku,
                    p.brand,
                    p.price,
                    p.category_id,
                    c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.id = ?
            """, [product_id]).fetchone()

            if not result:
                return None

            return {
                "id": result[0],
                "name": result[1],
                "sku": result[2],
                "brand": result[3],
                "price": float(result[4] or 0),
                "category_id": result[5],
                "category_name": result[6],
            }

    async def search_products(
        self,
        query: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search products by name or SKU.

        Args:
            query: Search query
            limit: Max results

        Returns:
            List of matching products
        """
        async with self.connection() as conn:
            search_pattern = f"%{query}%"
            result = conn.execute(f"""
                SELECT
                    p.id,
                    p.name,
                    p.sku,
                    p.brand,
                    p.price,
                    c.name as category_name
                FROM products p
                LEFT JOIN categories c ON p.category_id = c.id
                WHERE p.name ILIKE ?
                   OR p.sku ILIKE ?
                ORDER BY p.name
                LIMIT {limit}
            """, [search_pattern, search_pattern]).fetchall()

            return [{
                "id": r[0],
                "name": r[1],
                "sku": r[2],
                "brand": r[3],
                "price": float(r[4] or 0),
                "category_name": r[5],
            } for r in result]

    async def get_expense_types(self) -> List[Dict[str, Any]]:
        """
        Get all expense types.

        Returns:
            List of expense types
        """
        async with self.connection() as conn:
            result = conn.execute("""
                SELECT id, name, alias, is_active
                FROM expense_types
                ORDER BY name
            """).fetchall()

            return [{
                "id": r[0],
                "name": r[1],
                "alias": r[2],
                "is_active": r[3],
            } for r in result]

    @staticmethod
    def wrap_label(text: str, max_chars: int = 25) -> List[str]:
        """
        Wrap long text for chart labels.

        Args:
            text: Text to wrap
            max_chars: Max characters per line

        Returns:
            List of lines
        """
        if not text or len(text) <= max_chars:
            return [text] if text else []

        words = text.split()
        lines = []
        current_line = ""

        for word in words:
            if len(current_line) + len(word) + 1 <= max_chars:
                current_line += (" " if current_line else "") + word
            else:
                if current_line:
                    lines.append(current_line)
                current_line = word

        if current_line:
            lines.append(current_line)

        return lines
