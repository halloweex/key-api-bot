import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { InfoPopover } from '../ui/InfoPopover'
import { useInventoryTurnover } from '../../hooks'
import { formatNumber, formatCurrency } from '../../utils/formatters'
import type { TopExcessItem, ABCClassData } from '../../types/api'

// ─── Helpers ────────────────────────────────────────────────────────────────

type Severity = 'good' | 'warning' | 'danger'

function kpiColor(value: number, benchmarks: [number, number], inverse = false): Severity {
  const [lo, hi] = benchmarks
  if (inverse) {
    if (value <= lo) return 'good'
    if (value <= hi) return 'warning'
    return 'danger'
  }
  if (value >= hi) return 'good'
  if (value >= lo) return 'warning'
  return 'danger'
}

const severityStyles: Record<Severity, { text: string; bg: string; border: string }> = {
  good:    { text: 'text-emerald-600', bg: 'bg-emerald-50', border: 'border-emerald-200' },
  warning: { text: 'text-amber-600',   bg: 'bg-amber-50',   border: 'border-amber-200' },
  danger:  { text: 'text-red-600',     bg: 'bg-red-50',     border: 'border-red-200' },
}

// ─── Sub-components ─────────────────────────────────────────────────────────

function KpiCard({ label, value, subLabel, severity }: {
  label: string; value: string; subLabel?: string; severity: Severity
}) {
  const s = severityStyles[severity]
  return (
    <div className={`rounded-lg border p-3 ${s.bg} ${s.border}`}>
      <div className="text-[11px] font-medium text-slate-500 uppercase tracking-wide">{label}</div>
      <div className={`text-xl font-bold mt-1 ${s.text}`}>{value}</div>
      {subLabel && <div className="text-[11px] text-slate-500 mt-0.5">{subLabel}</div>}
    </div>
  )
}

function ABCCard({ cls, data, t }: { cls: string; data: ABCClassData; t: (k: string) => string }) {
  const colors: Record<string, string> = {
    A: 'border-emerald-300 bg-emerald-50',
    B: 'border-amber-300 bg-amber-50',
    C: 'border-red-300 bg-red-50',
  }
  return (
    <div className={`rounded-lg border p-3 ${colors[cls] || 'border-slate-200 bg-slate-50'}`}>
      <div className="flex items-center justify-between">
        <span className="text-sm font-bold">{t('inventory.turnover.class')} {cls}</span>
        <span className="text-xs text-slate-500">{data.skuCount} SKUs</span>
      </div>
      <div className="mt-2 grid grid-cols-2 gap-2 text-xs">
        <div>
          <div className="text-slate-500">{t('inventory.turnover.stockPct')}</div>
          <div className="font-semibold">{data.stockPct}%</div>
        </div>
        <div>
          <div className="text-slate-500">{t('inventory.turnover.revenuePct')}</div>
          <div className="font-semibold">{data.revenuePct}%</div>
        </div>
        <div>
          <div className="text-slate-500">{t('inventory.turnover.stockVal')}</div>
          <div className="font-semibold">{formatCurrency(data.stockValue)}</div>
        </div>
        <div>
          <div className="text-slate-500">{t('inventory.turnover.revenue')}</div>
          <div className="font-semibold">{formatCurrency(data.revenue)}</div>
        </div>
      </div>
    </div>
  )
}

function ExcessRow({ item }: { item: TopExcessItem }) {
  return (
    <div className="flex items-center gap-2 py-2 border-b border-slate-100 last:border-0 text-xs">
      <div className="flex-1 min-w-0">
        <div className="font-medium truncate">{item.name || item.sku}</div>
        <div className="text-slate-400 truncate">{item.brand} {item.categoryName ? `· ${item.categoryName}` : ''}</div>
      </div>
      <div className="text-right shrink-0 w-20">
        <div className="font-semibold">{formatCurrency(item.value)}</div>
        <div className="text-slate-400">{item.units} pcs</div>
      </div>
      <div className="text-right shrink-0 w-16">
        <div className="font-medium">
          {item.daysOfSupply != null ? `${item.daysOfSupply}d` : '∞'}
        </div>
        <div className="text-slate-400">{item.sellThroughRate}%</div>
      </div>
    </div>
  )
}

function StockGauge({ optimal, max, current, t }: {
  optimal: number; max: number; current: number; t: (k: string) => string
}) {
  const total = Math.max(current, max) * 1.1
  const optPct = Math.min((optimal / total) * 100, 100)
  const maxPct = Math.min(((max - optimal) / total) * 100, 100 - optPct)
  const excessPct = Math.max(0, Math.min(((current - max) / total) * 100, 100 - optPct - maxPct))
  const markerPct = Math.min((current / total) * 100, 99)

  return (
    <div className="space-y-2">
      <div className="relative h-6 rounded-full overflow-hidden bg-slate-100 flex">
        <div className="h-full bg-emerald-400" style={{ width: `${optPct}%` }} />
        <div className="h-full bg-amber-300" style={{ width: `${maxPct}%` }} />
        {excessPct > 0 && <div className="h-full bg-red-300" style={{ width: `${excessPct}%` }} />}
        {/* Current marker */}
        <div
          className="absolute top-0 h-full w-0.5 bg-slate-800"
          style={{ left: `${markerPct}%` }}
        >
          <div className="absolute -top-5 left-1/2 -translate-x-1/2 text-[10px] font-bold whitespace-nowrap">
            {formatCurrency(current)}
          </div>
        </div>
      </div>
      <div className="flex justify-between text-[10px] text-slate-500">
        <span>{t('inventory.turnover.optimal')}: {formatCurrency(optimal)}</span>
        <span>{t('inventory.turnover.maxAcceptable')}: {formatCurrency(max)}</span>
      </div>
    </div>
  )
}

// ─── Main Component ─────────────────────────────────────────────────────────

function InventoryTurnoverChartComponent() {
  const { t } = useTranslation()
  const [days, setDays] = useState(30)
  const { data, isLoading, error } = useInventoryTurnover(days)

  const periodOptions = [
    { value: 30, label: '30d' },
    { value: 60, label: '60d' },
    { value: 90, label: '90d' },
  ]

  return (
    <ChartContainer
      title={t('inventory.turnover.title')}
      titleExtra={
        <InfoPopover title={t('inventory.turnover.title')}>
          <div className="space-y-2">
            <p className="text-xs text-slate-300">{t('inventory.turnover.info1')}</p>
            <p className="text-xs text-slate-300">{t('inventory.turnover.info2')}</p>
            <p className="text-xs text-slate-300">{t('inventory.turnover.info3')}</p>
            <p className="text-xs text-slate-300">{t('inventory.turnover.info4')}</p>
          </div>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error}
      action={
        <div className="flex gap-1">
          {periodOptions.map(opt => (
            <button
              key={opt.value}
              onClick={() => setDays(opt.value)}
              className={`px-2.5 py-1 text-xs rounded-md transition-colors ${
                days === opt.value
                  ? 'bg-indigo-600 text-white'
                  : 'bg-slate-100 text-slate-600 hover:bg-slate-200'
              }`}
            >
              {opt.label}
            </button>
          ))}
        </div>
      }
      ariaLabel={t('inventory.turnover.title')}
    >
      {data && (
        <div className="space-y-5">
          {/* Section 1: KPI Cards */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <KpiCard
              label={t('inventory.turnover.dsi')}
              value={`${data.kpis.dsi}d`}
              subLabel={`${t('inventory.turnover.benchmark')}: ${data.kpis.benchmarks.dsi[0]}–${data.kpis.benchmarks.dsi[1]}d`}
              severity={kpiColor(data.kpis.dsi, data.kpis.benchmarks.dsi as [number, number], true)}
            />
            <KpiCard
              label={t('inventory.turnover.turnoverRatio')}
              value={`${data.kpis.turnoverRatio}x`}
              subLabel={`${t('inventory.turnover.benchmark')}: ${data.kpis.benchmarks.turnoverRatio[0]}–${data.kpis.benchmarks.turnoverRatio[1]}x`}
              severity={kpiColor(data.kpis.turnoverRatio, data.kpis.benchmarks.turnoverRatio as [number, number])}
            />
            <KpiCard
              label={t('inventory.turnover.stockToSales')}
              value={`${data.kpis.stockToSales} ${t('inventory.turnover.months')}`}
              subLabel={`${t('inventory.turnover.benchmark')}: ${data.kpis.benchmarks.stockToSales[0]}–${data.kpis.benchmarks.stockToSales[1]}`}
              severity={kpiColor(data.kpis.stockToSales, data.kpis.benchmarks.stockToSales as [number, number], true)}
            />
            <KpiCard
              label={t('inventory.turnover.dailyRevenue')}
              value={formatCurrency(data.turnover.dailyRevenue)}
              subLabel={`${data.turnover.actualDays}d ${t('inventory.turnover.ofData')}`}
              severity="good"
            />
          </div>

          {/* Section 2: Stock Gauge */}
          <div className="rounded-lg border border-slate-200 p-4 bg-white">
            <h4 className="text-xs font-semibold text-slate-600 uppercase mb-3">
              {t('inventory.turnover.stockLevel')}
            </h4>
            <StockGauge
              optimal={data.optimal.totalValue}
              max={data.optimal.maxAcceptableValue}
              current={data.currentStock.valueSale}
              t={t}
            />
            <div className="grid grid-cols-3 gap-3 mt-3 text-xs text-center">
              <div>
                <div className="text-slate-500">{t('inventory.turnover.leadTime')}</div>
                <div className="font-semibold">{data.optimal.leadTimeDays}d</div>
              </div>
              <div>
                <div className="text-slate-500">{t('inventory.turnover.safety')}</div>
                <div className="font-semibold">{data.optimal.safetyDays}d</div>
              </div>
              <div>
                <div className="text-slate-500">{t('inventory.turnover.buffer')}</div>
                <div className="font-semibold">{data.optimal.bufferDays}d</div>
              </div>
            </div>
          </div>

          {/* Section 3: Excess / Frozen Capital */}
          {data.excess.excessValue > 0 && (
            <div className="rounded-lg border border-red-200 bg-red-50 p-4">
              <h4 className="text-xs font-semibold text-red-700 uppercase mb-2">
                {t('inventory.turnover.frozenCapital')}
              </h4>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
                <div>
                  <div className="text-red-500">{t('inventory.turnover.excessValue')}</div>
                  <div className="font-bold text-red-700">{formatCurrency(data.excess.excessValue)}</div>
                </div>
                <div>
                  <div className="text-red-500">{t('inventory.turnover.excessRatio')}</div>
                  <div className="font-bold text-red-700">{data.excess.excessRatio}x</div>
                </div>
                <div>
                  <div className="text-red-500">{t('inventory.turnover.excessDays')}</div>
                  <div className="font-bold text-red-700">+{data.excess.excessDays}d</div>
                </div>
                <div>
                  <div className="text-red-500">{t('inventory.turnover.carryingCost')}</div>
                  <div className="font-bold text-red-700">{formatCurrency(data.excess.carryingCostAnnual)}/yr</div>
                </div>
              </div>
            </div>
          )}

          {/* Section 4: ABC Distribution */}
          <div>
            <h4 className="text-xs font-semibold text-slate-600 uppercase mb-2">
              {t('inventory.turnover.abcAnalysis')}
              {data.abc.imbalanceScore > 3 && (
                <span className="ml-2 text-red-500 normal-case font-normal">
                  {t('inventory.turnover.imbalanceWarning')}
                </span>
              )}
            </h4>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <ABCCard cls="A" data={data.abc.A} t={t} />
              <ABCCard cls="B" data={data.abc.B} t={t} />
              <ABCCard cls="C" data={data.abc.C} t={t} />
            </div>
          </div>

          {/* Section 5: Sell-through Velocity */}
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
            <div className="rounded-lg border border-emerald-200 bg-emerald-50 p-3">
              <div className="text-[11px] text-slate-500">{t('inventory.turnover.fastMovers')}</div>
              <div className="text-lg font-bold text-emerald-600">{formatNumber(data.sellThrough.fastMovers)}</div>
              <div className="text-[10px] text-slate-400">STR &ge; 20%</div>
            </div>
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-3">
              <div className="text-[11px] text-slate-500">{t('inventory.turnover.slowMovers')}</div>
              <div className="text-lg font-bold text-amber-600">{formatNumber(data.sellThrough.slowMovers)}</div>
              <div className="text-[10px] text-slate-400">0 &lt; STR &lt; 20%</div>
            </div>
            <div className="rounded-lg border border-red-200 bg-red-50 p-3">
              <div className="text-[11px] text-slate-500">{t('inventory.turnover.zeroVelocity')}</div>
              <div className="text-lg font-bold text-red-600">{formatNumber(data.sellThrough.zeroVelocity)}</div>
              <div className="text-[10px] text-slate-400">STR = 0%</div>
            </div>
            <div className="rounded-lg border border-slate-200 bg-white p-3">
              <div className="text-[11px] text-slate-500">{t('inventory.turnover.avgSTR')}</div>
              <div className="text-lg font-bold">{data.sellThrough.avgSellThroughRate}%</div>
              <div className="text-[10px] text-slate-400">{t('inventory.turnover.avg30d')}</div>
            </div>
            <div className="rounded-lg border border-slate-200 bg-white p-3">
              <div className="text-[11px] text-slate-500">{t('inventory.turnover.medianDoS')}</div>
              <div className="text-lg font-bold">
                {data.sellThrough.medianDaysOfSupply != null ? `${data.sellThrough.medianDaysOfSupply}d` : '—'}
              </div>
              <div className="text-[10px] text-slate-400">{t('inventory.turnover.daysOfSupply')}</div>
            </div>
          </div>

          {/* Section 6: Top Excess SKUs */}
          {data.topExcess.length > 0 && (
            <div>
              <h4 className="text-xs font-semibold text-slate-600 uppercase mb-2">
                {t('inventory.turnover.topExcess')} ({data.topExcess.length})
              </h4>
              <div className="max-h-[300px] overflow-y-auto rounded-lg border border-slate-200 bg-white px-3">
                {data.topExcess.map(item => (
                  <ExcessRow key={item.offerId} item={item} />
                ))}
              </div>
            </div>
          )}

          {/* Section 7: Conclusion */}
          <ConclusionBox data={data} t={t} />
        </div>
      )}
    </ChartContainer>
  )
}

function ConclusionBox({ data, t }: {
  data: NonNullable<ReturnType<typeof useInventoryTurnover>['data']>
  t: (k: string, opts?: Record<string, string>) => string
}) {
  const { kpis, optimal, excess, currentStock } = data
  const dsi = kpis.dsi

  let severity: Severity
  let messageKey: string
  if (dsi <= optimal.totalDays) {
    severity = 'good'
    messageKey = 'inventory.turnover.conclusionGood'
  } else if (dsi <= optimal.maxAcceptableDays) {
    severity = 'warning'
    messageKey = 'inventory.turnover.conclusionWarning'
  } else {
    severity = 'danger'
    messageKey = 'inventory.turnover.conclusionDanger'
  }

  const s = severityStyles[severity]
  return (
    <div className={`rounded-lg border p-4 ${s.bg} ${s.border}`}>
      <div className={`text-sm font-semibold ${s.text}`}>
        {t(messageKey)}
      </div>
      <div className="text-xs text-slate-600 mt-1">
        {t('inventory.turnover.conclusionDetails', {
          stockValue: formatCurrency(currentStock.valueSale),
          optimalValue: formatCurrency(optimal.totalValue),
          excessValue: formatCurrency(excess.excessValue),
          dsi: String(dsi),
          targetDays: String(optimal.totalDays),
        })}
      </div>
    </div>
  )
}

export const InventoryTurnoverChart = memo(InventoryTurnoverChartComponent)
