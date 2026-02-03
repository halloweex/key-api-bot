# KoreanStory Sales Dashboard

Automated sales reporting system for KoreanStory e-commerce with interactive web dashboard and Telegram bot. Uses KeyCRM as data source with DuckDB for fast analytics.

![Dashboard](https://img.shields.io/badge/Dashboard-FastAPI-009688?style=flat-square)
![Bot](https://img.shields.io/badge/Bot-Telegram-26A5E4?style=flat-square)
![Python](https://img.shields.io/badge/Python-3.14-3776AB?style=flat-square)
![Docker](https://img.shields.io/badge/Docker-Containerized-2496ED?style=flat-square)

## Features

### Web Dashboardad
- **Revenue Analytics** - Daily/weekly/monthly trends with period comparison
- **Sales by Source** - Instagram, Telegram, Shopify breakdown (bar + doughnut charts)
- **Top Products** - By quantity and revenue (horizontal bar charts)
- **Category Performance** - Interactive drill-down from parent to subcategories
- **Customer Insights** - New vs returning customers, AOV trends, repeat rate
- **Brand Analytics** - Top brands by revenue and quantity
- **Expense Tracking** - Profit analysis, expense breakdown by type
- **Filters** - Period, sales type (Retail/B2B/All), source, category, brand

### Telegram Bot
- **Sales Reports** - Summary by source with totals
- **Excel Export** - Formatted spreadsheets sent via Telegram
- **TOP-10 Products** - Best sellers with percentages
- **Date Filtering** - Today, yesterday, week, month, custom range
- **User Management** - Authorization system for access control

## Tech Stack

| Component | Technology |
|-----------|------------|
| **Backend** | Python 3.14, FastAPI, Uvicorn |
| **Bot** | python-telegram-bot v22 |
| **Database** | DuckDB (analytics), SQLite (bot auth) |
| **Charts** | Chart.js + chartjs-plugin-datalabels |
| **API** | KeyCRM OpenAPI |
| **Proxy** | Nginx |
| **Deploy** | Docker, GitHub Actions, AWS EC2 |

## Project Structure

```
key-api-bot/
├── core/                    # Shared modules
│   ├── keycrm.py           # Async KeyCRM API client
│   ├── duckdb_store.py     # DuckDB analytics store
│   ├── sync_service.py     # Data synchronization
│   ├── models.py           # Data models
│   ├── filters.py          # Date period parsing
│   └── validators.py       # Input validation
│
├── bot/                     # Telegram bot
│   ├── main.py             # Entry point
│   ├── handlers.py         # Command handlers
│   ├── services.py         # Business logic
│   ├── keyboards.py        # Telegram keyboards
│   └── formatters.py       # Message formatting
│
├── web/                     # Web dashboard
│   ├── main.py             # FastAPI app
│   ├── routes/
│   │   ├── api.py          # REST API endpoints
│   │   └── pages.py        # HTML routes
│   ├── services/
│   │   └── dashboard_service.py
│   ├── static/
│   │   ├── css/styles.css
│   │   └── js/charts.js
│   └── templates/
│       └── dashboard.html
│
├── nginx/
│   └── nginx.conf
│
├── docker-compose.yml
├── Dockerfile              # Bot container
├── Dockerfile.web          # Web container
└── requirements.txt
```

## Quick Start

### Prerequisites
- Python 3.14+
- Docker & Docker Compose
- KeyCRM API key
- Telegram Bot Token

### Environment Variables

Create `.env` file:
```env
BOT_TOKEN=your_telegram_bot_token
KEYCRM_API_KEY=your_keycrm_api_key
ADMIN_USER_IDS=123456789,987654321
DASHBOARD_URL=http://localhost
```

### Local Development

```bash
# Clone repository
git clone https://github.com/halloweex/key-api-bot.git
cd key-api-bot

# Install dependencies
pip install -r requirements.txt

# Run web dashboard
uvicorn web.main:app --host 0.0.0.0 --port 8080 --reload

# Run bot (in another terminal)
python -m bot.main
```

### Docker Deployment

```bash
# Build and run all services
docker-compose up -d

# View logs
docker-compose logs -f web
docker-compose logs -f bot

# Restart services
docker-compose restart
```

## API Endpoints

### Dashboard Data

| Endpoint | Description |
|----------|-------------|
| `GET /api/summary` | Summary statistics (orders, revenue, avg check) |
| `GET /api/revenue/trend` | Revenue time series for line chart |
| `GET /api/sales/by-source` | Sales breakdown by source |
| `GET /api/products/top` | Top products by quantity |
| `GET /api/products/performance` | Top by revenue + category breakdown |
| `GET /api/categories/breakdown` | Subcategory drill-down |
| `GET /api/customers/insights` | New vs returning, AOV trend |
| `GET /api/brands/analytics` | Top brands by revenue/quantity |
| `GET /api/expenses/summary` | Expense breakdown |
| `GET /api/expenses/profit` | Profit analysis |

### Query Parameters

| Parameter | Values | Description |
|-----------|--------|-------------|
| `period` | today, yesterday, week, last_week, month, last_month | Date shortcut |
| `start_date` | YYYY-MM-DD | Custom start date |
| `end_date` | YYYY-MM-DD | Custom end date |
| `sales_type` | retail, b2b, all | Sales channel filter |
| `source_id` | 1, 2, 4 | Source filter (Instagram, Telegram, Shopify) |
| `category_id` | number | Category filter |
| `brand` | string | Brand name filter |

### Metadata

| Endpoint | Description |
|----------|-------------|
| `GET /api/health` | Health check with uptime and stats |
| `GET /api/categories` | Root categories list |
| `GET /api/categories/{id}/children` | Subcategories |
| `GET /api/brands` | All brands list |
| `GET /api/expense-types` | Expense type list |

## Bot Commands

| Command | Description |
|---------|-------------|
| `/start` | Start bot, request access |
| `/report` | Generate sales report |
| `/top` | TOP-10 products |
| `/excel` | Download Excel report |
| `/search` | Search orders by phone/email |
| `/dashboard` | Open web dashboard |
| `/help` | Show help message |
| `/users` | User management (admin) |
| `/settings` | Bot settings (admin) |

## Dashboard Features

### Interactive Category Chart

The "Sales by Category" chart supports drill-down:
1. **Click** on a parent category slice to see subcategories
2. **Click** anywhere on subcategory view to go back

### Filters

- **Period** - Quick buttons (Today, Week, Month) or custom date range
- **Sales Type** - Retail (default), B2B, or All
- **Source** - Filter by sales channel
- **Category** - Two-level cascading filter
- **Brand** - Filter by product brand

### Milestone Celebrations

Revenue milestones trigger confetti animations:
- Daily: 200K UAH
- Weekly: 500K, 700K, 1M UAH
- Monthly: 1M, 2M, 3M, 5M UAH

## Deployment

### GitHub Actions (CI/CD)

Push to `main` branch triggers automatic deployment:
1. Builds Docker images (bot + web)
2. Pushes to Docker Hub
3. SSH to EC2 and pulls latest images
4. Restarts containers

### Required Secrets

```
DOCKER_USERNAME
DOCKER_PASSWORD
EC2_HOST
EC2_USER
EC2_SSH_KEY
```

### Manual Deployment

```bash
ssh -i keycrm_key.pem ec2-user@your-ec2-ip
cd key-api-bot
docker-compose pull && docker-compose up -d
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    KeyCRM API (Data Source)                 │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┴───────────────┐
              │                               │
       ┌──────▼──────┐                 ┌──────▼──────┐
       │  Telegram   │                 │    Web      │
       │    Bot      │                 │  Dashboard  │
       └──────┬──────┘                 └──────┬──────┘
              │                               │
       ┌──────▼──────┐                 ┌──────▼──────┐
       │   SQLite    │                 │   DuckDB    │
       │ (User Auth) │                 │ (Analytics) │
       └─────────────┘                 └─────────────┘
```

### Data Flow

1. **Sync Service** fetches orders from KeyCRM API on startup
2. Data stored in **DuckDB** for fast analytics queries (<10ms)
3. **Web Dashboard** queries DuckDB via FastAPI endpoints
4. **Bot** can use both direct API calls and cached data

## Performance

| Metric | Value |
|--------|-------|
| API query time | <10ms (DuckDB) |
| Page load | <500ms |
| Response compression | ~70% (gzip) |
| Data sync | Every startup + incremental |

## License

Private repository - KoreanStory internal use only.

## Support

- **Dashboard**: http://34.252.178.223
- **Repository**: https://github.com/hсalloweex/key-api-bot
- **Docker Hub**: https://hub.docker.com/u/halloweex
