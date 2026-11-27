import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { MetricCard } from './MetricCard'
import { TileGrid } from './TileGrid'
import { useIsPhone } from '../hooks/useMediaQuery'
import { formatNumber } from '../utils/formatters'
import {
  PROBLEM_FLAGS, bucketLabel, moneyShort, percent, type StockMarginView,
} from '../utils/stockMargin'
import type { StockMarginResponse } from '../types/api'

// ─── StockMarginSummary ──────────────────────────────────────────────────────
//
// The upper half of the stock-margin block: four tiles — the stock's margin,
// the margin the last 90 days actually made, the profit on the shelf, and
// what to check — a line saying what they cover and what they leave out, and
// how many items sit at what margin. Every figure is the server's; a tap on a
// bar or a "show" link asks the section to narrow the table below.
//
// The tiles cover goods only, and the margin only the goods with a sale and a
// purchase price: a certificate would read as a margin of nearly 100 % and a
// ₴1 giveaway as a loss many times its price, so they are counted apart and
// said so.

interface StockMarginSummaryProps {
  data: StockMarginResponse
  /** The bar the table is narrowed to, if any. */
  bucket: number | null
  onBucket: (bucket: number | null) => void
  /** List one of the table's views from scratch (the "show" links). */
  onShow: (view: StockMarginView) => void
}

export const StockMarginSummary = memo(function StockMarginSummary({
  data, bucket, onBucket, onShow,
}: StockMarginSummaryProps) {
  const { t } = useTranslation()
  const isPhone = useIsPhone()
  const g = data.goods
  const vars = { days: data.window_days, high: percent(data.thresholds.high_margin, 0) }
  const problems = PROBLEM_FLAGS
    .filter((f) => (g.problems[f] ?? 0) > 0)
    .map((f) => `${t(`margin.stock.flag.${f}`, vars)}: ${formatNumber(g.problems[f] ?? 0)}`)
  const maxCount = Math.max(1, ...g.buckets.map((b) => b.count))
  const excluded = data.excluded.certificate.positions + data.excluded.promo.positions
  const link = (view: StockMarginView) => (
    <button
      type="button"
      onClick={() => onShow(view)}
      className="text-xs font-medium text-purple-700 underline underline-offset-2 hover:text-purple-900"
    >
      {t('margin.stock.show')}
    </button>
  )

  return (
    <div>
      <TileGrid columns={4}>
        <MetricCard
          surface="tile-gradient"
          tone="purple"
          label={t('margin.stock.tile.margin')}
          value={percent(g.margin)}
          sub={isPhone
            ? t('margin.stock.tile.marginSubShort', { markup: percent(g.markup, 0) })
            : t('margin.stock.tile.marginSub', { n: formatNumber(g.costed), markup: percent(g.markup, 0) })}
        />
        <MetricCard
          surface="tile-gradient"
          tone="purple"
          label={t('margin.stock.tile.realized', vars)}
          value={percent(g.realized_margin_90d)}
          sub={isPhone
            ? t('margin.stock.tile.realizedSubShort', { list: percent(g.list_margin_90d) })
            : t('margin.stock.tile.realizedSub', {
              n: formatNumber(g.sold), list: percent(g.list_margin_90d), discount: percent(g.discount_90d, 0),
            })}
        />
        <MetricCard
          surface="tile-gradient"
          tone="purple"
          label={t('margin.stock.tile.profit')}
          value={moneyShort(g.profit)}
          sub={t(isPhone ? 'margin.stock.tile.profitSubShort' : 'margin.stock.tile.profitSub', {
            slow: moneyShort(g.slow_profit),
          })}
        />
        <MetricCard
          surface="tile-gradient"
          tone={g.problem_items > 0 ? 'orange' : 'neutral'}
          label={t('margin.stock.tile.check')}
          value={formatNumber(g.problem_items)}
          sub={problems.length === 0
            ? t('margin.stock.tile.checkNone')
            : isPhone && problems.length > 2 ? `${problems.slice(0, 2).join(' · ')} …` : problems.join(' · ')}
          valueExtra={g.problem_items > 0 ? link('problems') : undefined}
        />
      </TileGrid>

      <p className="mt-3 text-xs leading-relaxed text-slate-500">
        {t('margin.stock.scope', { positions: formatNumber(g.positions), costed: formatNumber(g.costed) })}
        {g.reserved > 0 && ` ${t('margin.stock.scopeReserved', { n: formatNumber(g.reserved) })}`}
        {excluded > 0 && (
          <>
            {' '}
            {t('margin.stock.scopeExcluded', {
              certs: formatNumber(data.excluded.certificate.positions),
              promo: formatNumber(data.excluded.promo.positions),
            })}
            {' '}
            {link('other')}
          </>
        )}
      </p>

      <div className="mt-5">
        <h4 className="text-sm font-semibold text-slate-800">{t('margin.stock.dist.title')}</h4>
        <p className="mt-0.5 text-xs text-slate-500">{t('margin.stock.dist.sub', { n: formatNumber(g.costed) })}</p>
        <ul className="mt-2 space-y-1">
          {g.buckets.map((b, i) => {
            const picked = bucket === i
            const label = bucketLabel(b, t)
            return (
              <li key={label}>
                <button
                  type="button"
                  aria-pressed={picked}
                  onClick={() => onBucket(picked ? null : i)}
                  className="grid min-h-8 w-full grid-cols-[6.5rem_1fr_3rem] items-center gap-2 rounded-md px-1
                             text-left text-xs hover:bg-purple-50 max-sm:min-h-11"
                >
                  <span className={picked ? 'font-semibold text-purple-700' : 'text-slate-600'}>{label}</span>
                  <span className={`relative h-3.5 overflow-hidden rounded bg-purple-50
                                    ${picked ? 'ring-2 ring-purple-600 ring-offset-1' : ''}`}>
                    <span
                      className="absolute inset-y-0 left-0 rounded-r bg-purple-600"
                      style={{ width: `${(100 * b.count) / maxCount}%` }}
                    />
                  </span>
                  <span className="text-right tabular-nums text-slate-700">{formatNumber(b.count)}</span>
                </button>
              </li>
            )
          })}
        </ul>
      </div>
    </div>
  )
})
