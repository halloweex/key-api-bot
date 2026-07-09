# Traffic Attribution — глубокий разбор

_Дата анализа: 2026-07-07 (Claude Code). Все ссылки file:line актуальны на коммит 0d7bf25._

## Что это

Система атрибуции трафика: определяет для каждого заказа, откуда пришёл покупатель
(платная реклама / органика / менеджер и т.д.), и строит отчёты с разбивкой по
платформам и ROAS. Источник данных нестандартный: UTM-метки лежат внутри текстового
поля `orders.manager_comment` из KeyCRM в формате
`UTM: utm_source: value; utm_medium: value; ...` вместе с пиксельными куками
(`_fbp`, `_fbc`, `ttp`, `fbclid`).

## Полный поток данных

```
KeyCRM API (manager_comment в заказе)
  → инкрементальный синк каждые 60 сек / полный синк вс 02:00 (core/sync_service.py:717)
  → upsert_orders → orders.manager_comment (core/duckdb_store.py:2487)
  → mark_warehouse_dirty
  → warehouse refresh job каждые 2 мин (core/scheduler.py)
  → silver_orders (order_date в Kyiv TZ, sales_type по manager_id, is_return по status_id,
     is_active_source: source_id IN (1,2,4) = Instagram/Telegram/Shopify)
  → refresh_utm_silver_layer → silver_order_utm (парсинг + классификация)
  → refresh_traffic_gold_layer → gold_daily_traffic (LEFT JOIN + COALESCE-фоллбеки)
  → API /traffic/* → страница Traffic (веб)
```

`manager_comment` подтягивается **обычным синком** (часть каждого 60-секундного
инкремента). Backfill (`scripts/backfill_utm.py` и `POST /traffic/backfill-utm`)
нужен только для исторических заказов, засинканных до появления колонки.
CLI-скрипт сносит `silver_order_utm` целиком и ре-парсит с нуля; API-endpoint
делает инкрементальный рефреш.

## Пайплайн (medallion), core/repositories/traffic.py

1. **Bronze** — сырой `manager_comment` в таблице `orders`.
2. **Silver** — `silver_order_utm` (схема: core/duckdb_store.py:774): регэксом
   парсятся UTM-пары и пиксели (`_parse_utm_from_comment`, traffic.py:14), затем
   `_classify_traffic` (traffic.py:43) выводит `traffic_type` + `platform`.
   Обновление инкрементальное (только новые/изменённые заказы), батчами по 1000
   с отпусканием блокировки БД между батчами.
3. **Gold** — `gold_daily_traffic` (duckdb_store.py:804): дневная агрегация по ключу
   `(date, source_id, sales_type, platform, traffic_type)` с `orders_count` и `revenue`.
   Всегда полный rebuild (транзакционный DELETE+INSERT) — инкрементально нельзя,
   парсинг UTM может задеть даты вне обновлённого диапазона. GROUP BY обязан
   повторять COALESCE-выражения (не алиасы) — иначе PK violation (см. комментарий
   traffic.py:284).

Обе таблицы обновляются в хвосте каждого warehouse-рефреша
(duckdb_store.py:2227), ошибки — non-critical warning.

## Классификация (`_classify_traffic`, traffic.py:43)

Шесть типов с приоритетом «явные UTM > куки/пиксели» (кука `_fbc` живёт 90 дней
и не доказывает текущую сессию):

- **paid_confirmed** — явные признаки: `fbads*`/Advantage+, TikTok-воронки
  (TOF/MOF/BOF в campaign), `google + cpc*`, `facebook/fb + cpc/paid/paid_social/sales`.
- **paid_likely** — косвенные: `_fbc`/`fbclid` без UTM, либо generic `medium=cpc/paid`
  без известного паттерна.
- **manager** — campaign начинается с `sales_manager_`.
- **organic** — email/Klaviyo, Instagram, соцсети с `medium=social/organic`,
  Google Shopping `product_sync`, ИИ-ассистенты (ChatGPT и др. по `utm_source`,
  добавлено 29.04.2026).
- **pixel_only** — только пассивный пиксель `_fbp`/`ttp`.
- **unknown** — трекинга нет.

Платформы: facebook, tiktok, google, instagram, telegram, email, ai, manager, other.
Фоллбек для заказов без UTM в gold: `source_id=1` → instagram organic,
`source_id=2` → telegram organic, иначе other/unknown.

## API (web/routes/api/traffic.py)

| Endpoint | Что делает |
|---|---|
| `GET /traffic/analytics` | Сводка: totals, paid/organic/manager/pixel_only/unknown, by_platform, by_traffic_type |
| `GET /traffic/trend` | Дневной тренд paid / organic / other |
| `GET /traffic/transactions` | Заказы постранично + `evidence` (какие метки/куки дали классификацию) |
| `GET /traffic/roas` | Blended + per-platform ROAS, бонусные тиры |
| `POST /traffic/refresh` | Admin: форс-рефреш silver+gold |
| `POST /traffic/reclassify` | Admin: полный ре-парсинг после изменения правил (DELETE silver_order_utm) |
| `POST /traffic/backfill-utm` + `/status` | Фоновая догрузка manager_comment из KeyCRM (чанки 30 дней, до ~2 лет) |

ROAS (traffic.py:669): общая выручка из `gold_daily_revenue` + платная выручка по
платформам из `gold_daily_traffic` + расходы из `manual_expenses`
(category=marketing, platform). Бонусные тиры: ≥7 → +30%, ≥6 → +20%, ≥5 → +10%,
≥4 → base, <4 → no bonus (traffic.py:661).

## Фронтенд (web/frontend/src/components/)

Страница `TrafficPage.tsx` (роут `/traffic`), **без AdminGuard** — доступна всем
аутентифицированным (в отличие от MarginPage). Секции:

- **TrafficSummaryCards** — 5 карточек (Paid/Organic/Manager/PixelOnly/Unknown);
  Paid Ads кликабельна → split Confirmed vs Likely; collapsible-легенда
  «What is this?» с объяснениями типов (locales/en.json:448–485).
- **TrafficTrendChart** — stacked bar по дням (paid/organic/other) + пиллы с итогами.
- **PlatformBreakdownChart** — donut (revenue) + горизонтальные бары (orders) по платформам.
- **TrafficTransactionsTable** — фильтры по типу/платформе, Load More по 50,
  evidence-пиллы (до 3 видимых, «+N more» в tooltip).
- **ROASSection** — blended ROAS-карта (зелёный ≥5 / жёлтый ≥3 / красный <3),
  таблица бонусных тиров, per-platform карты, и **форма ввода расходов**:
  «Edit Ad Spend» → `POST /api/expenses` (category=marketing, платформы
  facebook/tiktok/google, permission `expenses:edit`).

Все хуки берут фильтры из Zustand `filterStore` (period, sales_type, source_id,
category_id, brand, promocode); кэш react-query 5 мин (CACHE_TTL.STANDARD).
По умолчанию `sales_type=retail`.

`manual_expenses.platform` добавлена миграцией (duckdb_store.py:1120–1138)
с бэкфиллом из `expense_type` по LIKE-паттернам.

## Находки ультраглубокого аудита (2026-07-10)

1. **Исправлено — парсер терял нестандартные форматы** (query-string, UTM в URL,
   без префикса, переносы строк; коммит 1cd3fa6). После изменения правил парсинга
   всегда нужен `POST /traffic/reclassify`.
2. **Исправлено — NULL-перезапись `manager_comment`** (коммит eb2f66a): апсерт
   безусловно писал NULL из payload поверх сохранённого комментария — цикл
   «бэкфилл восстановил → синк стёр». Теперь COALESCE.
3. **Открыто — корзина `sales_type='other'` невидима**: заказы менеджеров вне
   `RETAIL_MANAGER_IDS` (константа не обновлялась с 01.2026) и без `is_retail=TRUE`
   в таблице managers падают в 'other' (duckdb_store.py:1861). UI позволяет только
   retail/b2b/all; мониторинга размера корзины нет. Новый менеджер = невидимые
   заказы. `set_manager_retail_status()` существует (duckdb_store.py:3426), но не
   выведен в API.
4. **Открыто — источники вне 1/2/4** отфильтрованы как `is_active_source=FALSE`
   во всех traffic-отчётах; распределения по source_id в API нет.
5. Полная карта из 13 точек исключения заказа — см. историю сессии/агентский отчёт.

## Пробелы и риски

1. **Нет тестов на трафик.** `_parse_utm_from_comment`, `_classify_traffic`
   (15+ правил с приоритетами), gold-агрегация и ROAS не покрыты юнит-тестами.
   Классификатор трижды чинили (17.02, 16–17.04.2026) — самый хрупкий код фичи.
2. **Data Quality framework не валидирует traffic-таблицы** — `silver_order_utm`
   и `gold_daily_traffic` вне охвата Layer 1/2; ошибки UTM-рефреша только warning
   (duckdb_store.py:2238) → возможна тихая деградация.
3. **Окно синка 24 ч**: если `manager_comment` дописали в KeyCRM позже суток после
   создания заказа, обычный синк это не поймает; постоянного джоба перечитывания нет.
4. **ROAS без алертов на пустые расходы**: нет spend → blended ROAS = None;
   авто-синка из рекламных кабинетов нет, всё вручную.
5. **Происхождение UTM-блока в manager_comment не задокументировано** — пишет,
   судя по формату, интеграция сайта/KeyCRM; точка инъекции вне этого репо.
   Если UTM переедут в другое поле — атрибуция молча опустеет.
6. **В Telegram-боте трафика нет** — только веб.

## Хронология (git)

- 17.02.2026 — фича появилась («target analysis»): классификатор, backfill,
  фиксы приоритета UTM над куками, фоновый backfill со статус-поллингом.
- 16–17.04.2026 — фиксы PK-violation в GROUP BY, снятие блокировки БД на 37K строк;
  расширение классификатора (Advantage+, fbsales, telegram, cpc*).
- 29.04.2026 — платформа `ai` (ChatGPT по utm_source).
- 14.05.2026 — security hardening.

## Возможные следующие шаги

- Юнит-тесты на `_classify_traffic` / `_parse_utm_from_comment` (риск №1).
- Добавить traffic-таблицы в Data Quality framework (риск №2).
