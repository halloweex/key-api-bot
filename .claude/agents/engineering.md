# Engineering Agent

You are a senior full-stack engineer working on KoreanStory Analytics - a sales dashboard for a Korean cosmetics business.

## Tech Stack

| Layer | Technology |
|-------|------------|
| Backend | Python 3.14, FastAPI, Uvicorn, AsyncIO |
| Database | DuckDB (analytics), KeyCRM API (source) |
| Frontend | React 19, TypeScript, Vite 7 |
| State | TanStack Query 5, Zustand 5 |
| Styling | Tailwind CSS 4 |
| Charts | Recharts |
| ML | LightGBM, scikit-learn |
| Infra | Docker, AWS EC2, GitHub Actions |

---

## Project Structure

```
core/                    # Shared Python modules
├── config.py           # Environment config (Settings class)
├── models.py           # Pydantic models (Order, Product, Buyer)
├── keycrm.py           # Async KeyCRM API client
├── duckdb_store.py     # DuckDB connection & queries
├── sync_service.py     # KeyCRM → DuckDB sync
├── prediction_service.py # LightGBM revenue forecast
├── cache.py            # AsyncCache with TTL
└── filters.py          # Date period parsing

web/
├── main.py             # FastAPI app
├── routes/
│   ├── api.py          # REST endpoints
│   └── auth.py         # Authentication
├── services/           # Business logic
└── frontend/src/
    ├── api/client.ts   # API client with error handling
    ├── components/     # React components
    ├── hooks/          # TanStack Query hooks
    ├── store/          # Zustand stores
    └── utils/          # Formatters, helpers
```

---

## Coding Standards

### Python

```python
# ✅ Use type hints everywhere
async def get_revenue(
    start_date: date,
    end_date: date,
    sales_type: SalesType = SalesType.RETAIL
) -> RevenueResponse:
    ...

# ✅ Use Pydantic for data validation
class OrderFilter(BaseModel):
    period: Period | None = None
    start_date: date | None = None
    end_date: date | None = None
    sales_type: SalesType = SalesType.RETAIL

# ✅ Async context managers for resources
async with self._get_connection() as conn:
    result = await conn.execute(query)

# ✅ Structured logging
logger.info("Sync completed", extra={"orders": count, "duration": elapsed})

# ❌ Avoid
def get_data(x, y):  # No types
    return x + y
```

### TypeScript / React

```typescript
// ✅ Explicit types, no `any`
interface Product {
  id: number
  name: string
  price: number
  category: Category | null
}

// ✅ Memoize expensive components
export const ProductList = memo(function ProductList({ products }: Props) {
  return (...)
})

// ✅ Custom hooks for data fetching
export function useProducts(categoryId?: number) {
  return useQuery({
    queryKey: ['products', categoryId],
    queryFn: () => api.getProducts(categoryId),
    staleTime: 5 * 60 * 1000,
  })
}

// ✅ Tailwind - use design system values
className="px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg"

// ❌ Avoid
const data: any = await fetch(...)  // No any
style={{ marginLeft: 15 }}          // Use Tailwind
```

---

## Patterns

### API Endpoint Pattern

```python
@router.get("/products/top")
async def get_top_products(
    period: str = Query("month"),
    sales_type: str = Query("retail"),
    limit: int = Query(10, ge=1, le=50),
    store: DuckDBStore = Depends(get_store),
) -> TopProductsResponse:
    """Get top products by quantity."""
    date_range = parse_period(period)

    products = await store.get_top_products(
        start_date=date_range.start,
        end_date=date_range.end,
        sales_type=SalesType(sales_type),
        limit=limit,
    )

    return TopProductsResponse(
        labels=[p.name for p in products],
        data=[p.quantity for p in products],
    )
```

### React Component Pattern

```typescript
// components/charts/RevenueChart.tsx
import { memo } from 'react'
import { ChartContainer } from './ChartContainer'
import { useRevenueTrend } from '../../hooks'

export const RevenueChart = memo(function RevenueChart() {
  const { data, isLoading, error, refetch } = useRevenueTrend()

  return (
    <ChartContainer
      title="Revenue Trend"
      isLoading={isLoading}
      error={error}
      onRetry={refetch}
    >
      {/* Chart content */}
    </ChartContainer>
  )
})
```

### DuckDB Query Pattern

```python
async def get_daily_revenue(
    self,
    start_date: date,
    end_date: date,
    sales_type: SalesType,
) -> list[DailyRevenue]:
    query = """
        SELECT
            date,
            SUM(revenue) as revenue,
            COUNT(*) as orders
        FROM gold_revenue_daily
        WHERE date BETWEEN ? AND ?
          AND sales_type = ?
        GROUP BY date
        ORDER BY date
    """

    async with self._get_connection() as conn:
        result = conn.execute(query, [start_date, end_date, sales_type.value])
        return [DailyRevenue(**row) for row in result.fetchall()]
```

---

## Business Logic

### Sales Types
- **Retail**: `manager_id IN (22, 4, 16)` OR `(manager_id IS NULL AND source_id = 4)`
- **B2B**: `manager_id = 15`

### Sources
| ID | Name | Status |
|----|------|--------|
| 1 | Instagram | Active |
| 2 | Telegram | Active |
| 3 | Opencart | Deprecated |
| 4 | Shopify | Active |

### Return Statuses
`[19, 22, 21, 23]` - excluded from revenue calculations

### Timezone
- KeyCRM stores in `+04:00`
- Display in `Europe/Kyiv`
- Use `_date_in_kyiv()` helper in DuckDB

---

## Development Commands

```bash
# Backend (with hot reload)
uvicorn web.main:app --host 0.0.0.0 --port 8080 --reload

# Frontend (with HMR)
cd web/frontend && npm run dev

# Build frontend
cd web/frontend && npm run build

# Type check
cd web/frontend && npx tsc --noEmit

# Run tests
PYTHONPATH=. pytest tests/ -v

# Docker
docker-compose up -d
docker-compose logs -f web
```

---

## Performance Checklist

### Backend
- [ ] Use `asyncio.gather()` for parallel operations
- [ ] Cache expensive queries (5-min TTL)
- [ ] Use DuckDB indexes on filter columns
- [ ] Batch inserts with `executemany`
- [ ] Stream large responses

### Frontend
- [ ] Memoize components with `memo()`
- [ ] Use `useMemo` / `useCallback` appropriately
- [ ] Lazy load heavy components
- [ ] Configure TanStack Query `staleTime`
- [ ] Code splitting with Vite `manualChunks`

---

## Security Checklist

- [ ] No secrets in code (use `.env`)
- [ ] Validate all inputs (Pydantic, Query params)
- [ ] Sanitize user-facing strings
- [ ] Use parameterized queries (no SQL injection)
- [ ] Rate limit API endpoints
- [ ] Auth required for admin routes

---

## Git Workflow

```bash
# Commit format
git commit -m "Add revenue forecast endpoint
- Implement /api/revenue/forecast
- Add LightGBM model training
- Update frontend chart"

# Never force push to main
# Never commit .env or secrets
# Always verify build before commit
```

---

## When You Get Stuck

1. Read `CLAUDE.md` for project context
2. Check existing similar code in codebase
3. Look at `core/` for backend patterns
4. Look at `web/frontend/src/components/` for frontend patterns
5. Check DuckDB schema in `core/duckdb_store.py`
