import { memo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { InfoPopover } from './InfoPopover'
import { MetricCard } from './MetricCard'
import { useInventoryAnalysis, useStockActions } from '../hooks'
import { formatNumber, formatCurrency } from '../utils/formatters'
import type {
  InventoryAnalysisItem,
  QuadrantMatrix,
  AbcClass,
  VelocityTier,
  CostQuality,
  Concentration,
  LiquidationSummary,
  GmroiDistribution,
} from '../types/api'

// ─── Component ───────────────────────────────────────────────────────────────

function DeadStockChartComponent() {
  const { t } = useTranslation()
  const { data, isLoading, error } = useInventoryAnalysis()
  const { data: actions } = useStockActions()
  const [activeTab, setActiveTab] = useState<'summary' | 'matrix' | 'aging' | 'items' | 'actions'>('summary')

  return (
    <ChartContainer
      title={t('inventory.health')}
      titleExtra={
        <InfoPopover title={t('inventory.health')}>
          <div className="space-y-3 max-w-md">
            <div className="space-y-1">
              <p className="text-xs text-slate-300 font-semibold">Что считаем</p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-blue-300">Cost basis</strong> — реально замороженный кэш (units × purchased_price).
                Если purchased_price нет, fallback на portfolio cost ratio (помечено `~est`).
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-blue-300">Excess capital</strong> — стоимость остатков сверх 60-дневного оптимального запаса (cost basis).
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-blue-300">DOS</strong> — days of supply, на сколько дней хватит остатков при темпе продаж за 90 дней.
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-blue-300">GMROI</strong> — annualized gross profit / cost basis.
                {' '}&lt;100% = SKU теряет деньги на хранении. Бенчмарк cosmetics 200–400%.
              </p>
            </div>

            <div className="space-y-1 border-t border-slate-700 pt-2">
              <p className="text-xs text-slate-300 font-semibold">Velocity tiers</p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-emerald-400">hot</strong> ≤30d · <strong className="text-emerald-400">healthy</strong> 30–90 ·{' '}
                <strong className="text-amber-400">warm</strong> 90–180 ·{' '}
                <strong className="text-orange-400">cold</strong> 180–365 ·{' '}
                <strong className="text-red-400">frozen</strong> &gt;365 (или 0 продаж 90д)
              </p>
            </div>

            <div className="space-y-1 border-t border-slate-700 pt-2">
              <p className="text-xs text-slate-300 font-semibold">Decision (NPV-based)</p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-slate-300">HOLD</strong> — натуральный sell-through выгоднее ликвидации
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-amber-400">PROMO</strong> — frozen/cold, но hold-NPV пока выше — нужно стимулировать
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-red-400">LIQUIDATE</strong> — продать со скидкой выгоднее: NPV(liq @ -50%) &gt; NPV(hold)
              </p>
              <p className="text-[10px] text-slate-400">
                Carrying cost 25%/год. Tunable через query: `?carrying_rate=0.25&liquidation_discount=0.50`.
              </p>
            </div>

            <div className="space-y-1 border-t border-slate-700 pt-2">
              <p className="text-xs text-slate-300 font-semibold">Старые статусы (legacy)</p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-emerald-400">{t('inventory.healthy')}:</strong> {t('inventory.healthInfo4')}
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-indigo-400">{t('inventory.overstocked')}:</strong> {t('inventory.healthInfo8')}
              </p>
              <p className="text-[11px] text-slate-300">
                <strong className="text-red-400">{t('inventory.deadStock')}:</strong> {t('inventory.healthInfo6')}
              </p>
            </div>
          </div>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error}
      ariaLabel={t('inventory.healthDesc')}
    >
      {data && (
        <div className="space-y-4 min-h-[420px]">
          {/* Summary Stats */}
          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3">
            <MetricCard
              surface="tile-tinted"
              tone="green"
              label={t('inventory.healthy')}
              value={String(data.summary.healthy.skuCount)}
              sub={`${data.summary.healthy.valuePercent}%`}
            />
            <MetricCard
              surface="tile-tinted"
              tone="indigo"
              label={t('inventory.overstocked')}
              value={String(data.summary.overstocked.skuCount)}
              sub={`${data.summary.overstocked.valuePercent}%`}
            />
            <MetricCard
              surface="tile-tinted"
              tone="orange"
              label={t('inventory.atRisk')}
              value={String(data.summary.atRisk.skuCount)}
              sub={`${data.summary.atRisk.valuePercent}%`}
            />
            <MetricCard
              surface="tile-tinted"
              tone="red"
              label={t('inventory.deadStock')}
              value={String(data.summary.deadStock.skuCount)}
              sub={`${data.summary.deadStock.valuePercent}%`}
            />
            <MetricCard
              surface="tile-tinted"
              tone="neutral"
              label={t('inventory.neverSold')}
              value={String(data.summary.neverSold.skuCount)}
              sub={`${data.summary.neverSold.valuePercent}%`}
            />
          </div>

          {/* Capital-at-risk: cost-basis frozen capital + liquidation P&L */}
          <CapitalAtRiskPanel
            concentration={data.concentration}
            costQuality={data.costQuality}
            liquidation={data.liquidationSummary}
            gmroi={data.gmroiDistribution}
          />

          {/* Tabs */}
          <div className="flex gap-1 border-b border-slate-200 overflow-x-auto">
            <TabButton active={activeTab === 'summary'} onClick={() => setActiveTab('summary')}>
              {t('inventory.summaryTab')}
            </TabButton>
            <TabButton active={activeTab === 'matrix'} onClick={() => setActiveTab('matrix')}>
              ABC × Velocity
            </TabButton>
            <TabButton active={activeTab === 'aging'} onClick={() => setActiveTab('aging')}>
              {t('inventory.agingTab')}
            </TabButton>
            <TabButton active={activeTab === 'items'} onClick={() => setActiveTab('items')}>
              {t('inventory.itemsTab')} ({data.items.length})
            </TabButton>
            <TabButton active={activeTab === 'actions'} onClick={() => setActiveTab('actions')}>
              {t('inventory.actionsTab')}
            </TabButton>
          </div>

          {/* Tab Content */}
          <div className="min-h-[200px]">
            {activeTab === 'summary' && <SummaryTab data={data} />}
            {activeTab === 'matrix' && <MatrixTab matrix={data.quadrantMatrix} />}
            {activeTab === 'aging' && <AgingTab buckets={data.agingBuckets} />}
            {activeTab === 'items' && <ItemsTab items={data.items} />}
            {activeTab === 'actions' && <ActionsTab actions={actions || []} />}
          </div>
        </div>
      )}
    </ChartContainer>
  )
}

// ─── Capital-at-Risk panel ───────────────────────────────────────────────────

interface CapitalAtRiskPanelProps {
  concentration: Concentration
  costQuality: CostQuality
  liquidation: LiquidationSummary
  gmroi: GmroiDistribution
}

function CapitalAtRiskPanel({ concentration, costQuality, liquidation, gmroi }: CapitalAtRiskPanelProps) {
  const liqGain = liquidation.recoveryAtDiscount - liquidation.costBasis
  const top20Share = concentration.top20.share * 100
  const fallbackPct = 100 - costQuality.actualPct

  return (
    <div className="rounded-lg border border-slate-200 bg-gradient-to-br from-slate-50 to-white p-3 space-y-3">
      <div className="flex items-center justify-between">
        <h4 className="text-sm font-semibold text-slate-700">Capital at Risk (cost basis)</h4>
        {fallbackPct > 0 && (
          <span
            className="text-[10px] text-amber-600 bg-amber-50 px-2 py-0.5 rounded"
            title={`${costQuality.fallbackSkus} SKU без покупочной цены — fallback на portfolio cost ratio`}
          >
            {fallbackPct.toFixed(0)}% оценочно
          </span>
        )}
      </div>

      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
        <div className="bg-red-50 rounded p-2">
          <div className="text-[11px] text-slate-500">Excess capital</div>
          <div className="text-lg font-bold text-red-700">
            {formatCurrency(concentration.totalExcessCapitalCost)}
          </div>
          <div className="text-[10px] text-slate-500">
            TOP-20 = {top20Share.toFixed(0)}% избытка
          </div>
        </div>
        <div className="bg-slate-50 rounded p-2">
          <div className="text-[11px] text-slate-500">Total cost basis</div>
          <div className="text-lg font-bold text-slate-700">
            {formatCurrency(concentration.totalCostBasis)}
          </div>
          <div className="text-[10px] text-slate-500">
            cost / sale ratio инвентаря
          </div>
        </div>
        <div className="bg-emerald-50 rounded p-2">
          <div className="text-[11px] text-slate-500">
            Liquidation @ -{(liquidation.discount * 100).toFixed(0)}%
          </div>
          <div className="text-lg font-bold text-emerald-700">
            {formatCurrency(liquidation.recoveryAtDiscount)}
          </div>
          <div className={`text-[10px] ${liqGain >= 0 ? 'text-emerald-600' : 'text-red-500'}`}>
            {liqGain >= 0 ? '+' : ''}{formatCurrency(liqGain)} к cost
          </div>
        </div>
        <div className="bg-blue-50 rounded p-2">
          <div className="text-[11px] text-slate-500">GMROI median</div>
          <div className="text-lg font-bold text-blue-700">
            {(gmroi.median * 100).toFixed(0)}%
          </div>
          <div className="text-[10px] text-slate-500">
            {gmroi.under100Count} SKU теряют деньги
          </div>
        </div>
      </div>

      {liquidation.skuCount > 0 && (
        <div className="text-[11px] text-slate-600 bg-amber-50 border border-amber-200 rounded p-2">
          <strong className="text-amber-700">{liquidation.skuCount} SKU</strong> рекомендуется ликвидировать
          (NPV-positive): высвобождает {formatCurrency(liquidation.recoveryAtDiscount)} кэша
          + экономит {formatCurrency(liquidation.carryingCostSavedPerYear)}/год storage.
        </div>
      )}
    </div>
  )
}

// ─── Sub Components ──────────────────────────────────────────────────────────

interface TabButtonProps {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}

function TabButton({ active, onClick, children }: TabButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`px-3 py-2 text-sm font-medium transition-colors whitespace-nowrap ${
        active
          ? 'text-blue-600 border-b-2 border-blue-600'
          : 'text-slate-500 hover:text-slate-700'
      }`}
    >
      {children}
    </button>
  )
}

interface SummaryTabProps {
  data: NonNullable<ReturnType<typeof useInventoryAnalysis>['data']>
}

function SummaryTab({ data }: SummaryTabProps) {
  const { t } = useTranslation()
  const total = data.summary.total
  const deadAndNever = data.summary.deadStock.value + data.summary.neverSold.value
  const deadPercent = total.value > 0 ? (deadAndNever / total.value * 100) : 0

  return (
    <div className="space-y-3">
      {/* Progress Bar */}
      <div>
        <div className="flex justify-between text-xs text-slate-500 mb-1">
          <span>{t('inventory.health')}</span>
          <span>{data.summary.healthy.valuePercent}% {t('inventory.pctHealthy')}</span>
        </div>
        <div className="h-3 bg-slate-100 rounded-full overflow-hidden flex">
          <div
            className="bg-emerald-500 transition-all"
            style={{ width: `${data.summary.healthy.valuePercent}%` }}
          />
          <div
            className="bg-indigo-400 transition-all"
            style={{ width: `${data.summary.overstocked.valuePercent}%` }}
          />
          <div
            className="bg-amber-500 transition-all"
            style={{ width: `${data.summary.atRisk.valuePercent}%` }}
          />
          <div
            className="bg-red-500 transition-all"
            style={{ width: `${data.summary.deadStock.valuePercent}%` }}
          />
          <div
            className="bg-slate-400 transition-all"
            style={{ width: `${data.summary.neverSold.valuePercent}%` }}
          />
        </div>
        <div className="flex gap-3 mt-2 text-xs flex-wrap">
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
            {t('inventory.healthy')}
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-indigo-400 rounded-full"></span>
            {t('inventory.overstocked')}
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-amber-500 rounded-full"></span>
            {t('inventory.atRisk')}
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-red-500 rounded-full"></span>
            {t('inventory.deadStock')}
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-slate-400 rounded-full"></span>
            {t('inventory.neverSold')}
          </span>
        </div>
      </div>

      {/* Details Grid */}
      <div className="grid grid-cols-2 gap-2 text-sm">
        <div className="bg-slate-50 rounded p-2">
          <div className="text-slate-500 text-xs">{t('inventory.totalSkus')}</div>
          <div className="font-medium">{formatNumber(total.skuCount)}</div>
        </div>
        <div className="bg-slate-50 rounded p-2">
          <div className="text-slate-500 text-xs">{t('inventory.totalUnits')}</div>
          <div className="font-medium">{formatNumber(total.quantity)}</div>
        </div>
        <div className="bg-slate-50 rounded p-2">
          <div className="text-slate-500 text-xs">{t('inventory.totalValue')}</div>
          <div className="font-medium">{formatCurrency(total.value)}</div>
        </div>
        <div className="bg-red-50 rounded p-2">
          <div className="text-slate-500 text-xs">{t('inventory.deadStockPct')}</div>
          <div className="font-medium text-red-600">{deadPercent.toFixed(1)}%</div>
        </div>
      </div>

      {/* Category Thresholds */}
      {data.categoryThresholds.length > 0 && (
        <div>
          <h4 className="text-sm font-semibold text-slate-700 mb-1">{t('inventory.categoryThresholds')}</h4>
          <p className="text-[11px] text-slate-400 mb-2">{t('inventory.categoryThresholdsDesc')}</p>
          <div className="space-y-1 max-h-32 overflow-y-auto">
            {data.categoryThresholds.slice(0, 8).map((cat) => (
              <div
                key={cat.categoryId}
                className="flex justify-between items-center text-xs py-1 px-2 bg-slate-50 rounded"
              >
                <span className="text-slate-600 truncate flex-1">{cat.categoryName}</span>
                <span className="text-slate-500 ml-2">
                  P75: {cat.p75 || '-'}d → <span className="font-medium">{cat.thresholdDays}d</span>
                </span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}

interface AgingTabProps {
  buckets: NonNullable<ReturnType<typeof useInventoryAnalysis>['data']>['agingBuckets']
}

function AgingTab({ buckets }: AgingTabProps) {
  const totalValue = buckets.reduce((sum, b) => sum + b.value, 0)

  const getBucketColor = (bucket: string) => {
    if (bucket.includes('0-30')) return 'bg-emerald-500'
    if (bucket.includes('31-90')) return 'bg-emerald-400'
    if (bucket.includes('91-180')) return 'bg-amber-400'
    if (bucket.includes('181-365')) return 'bg-amber-500'
    if (bucket.includes('365+')) return 'bg-red-500'
    return 'bg-slate-400'
  }

  return (
    <div className="space-y-3">
      <div className="text-xs text-slate-500 mb-2">
        Distribution of inventory by days since last sale
      </div>

      {/* Stacked Bar */}
      <div className="h-6 bg-slate-100 rounded-full overflow-hidden flex">
        {buckets.map((bucket) => {
          const pct = totalValue > 0 ? (bucket.value / totalValue * 100) : 0
          return (
            <div
              key={bucket.bucket}
              className={`${getBucketColor(bucket.bucket)} transition-all`}
              style={{ width: `${pct}%` }}
              title={`${bucket.bucket}: ${formatCurrency(bucket.value)}`}
            />
          )
        })}
      </div>

      {/* Buckets List */}
      <div className="space-y-2">
        {buckets.map((bucket) => {
          const pct = totalValue > 0 ? (bucket.value / totalValue * 100) : 0
          return (
            <div key={bucket.bucket} className="flex items-center gap-2 text-sm">
              <div className={`w-3 h-3 rounded ${getBucketColor(bucket.bucket)}`} />
              <span className="flex-1 text-slate-600">
                {bucket.bucket.replace(/^\d+\.\s*/, '')}
              </span>
              <span className="text-slate-500">{bucket.skuCount} SKUs</span>
              <span className="text-slate-700 font-medium w-24 text-right">
                {formatCurrency(bucket.value)}
              </span>
              <span className="text-slate-400 w-12 text-right">
                {pct.toFixed(1)}%
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}

interface ItemsTabProps {
  items: InventoryAnalysisItem[]
}

const TIER_BG: Record<VelocityTier, string> = {
  hot: 'bg-emerald-50',
  healthy: 'bg-emerald-50',
  warm: 'bg-amber-50',
  cold: 'bg-orange-50',
  frozen: 'bg-red-50',
}
const TIER_LABEL: Record<VelocityTier, string> = {
  hot: 'hot', healthy: 'healthy', warm: 'warm', cold: 'cold', frozen: 'frozen',
}
const DECISION_BADGE: Record<'HOLD' | 'PROMO' | 'LIQUIDATE', string> = {
  HOLD: 'bg-slate-100 text-slate-600',
  PROMO: 'bg-amber-100 text-amber-700',
  LIQUIDATE: 'bg-red-100 text-red-700',
}

function ItemsTab({ items }: ItemsTabProps) {
  const { t } = useTranslation()

  if (items.length === 0) {
    return (
      <div className="text-center text-slate-500 py-8">
        {t('inventory.noDeadStock')}
      </div>
    )
  }

  return (
    <div className="space-y-1 max-h-72 overflow-y-auto text-sm">
      {/* Header row */}
      <div className="grid grid-cols-12 gap-2 text-[10px] uppercase tracking-wide text-slate-400 px-2 pb-1 border-b border-slate-100 sticky top-0 bg-white">
        <div className="col-span-5">SKU / Brand</div>
        <div className="col-span-1 text-right" title="Доступные единицы (units − reserved)">Units</div>
        <div className="col-span-2 text-right" title="Замороженный капитал по себестоимости (units × purchased_price). Под cost basis — excess сверх 60-дневной нормы.">
          Cost ₴
        </div>
        <div className="col-span-1 text-right" title="Days of supply: на сколько дней хватит остатков при темпе продаж за 90 дней">
          DOS
        </div>
        <div className="col-span-1 text-right" title="Annualized gross profit / cost basis. Бенчмарк 200-400% для cosmetics. <100% = убыточно при carrying cost 25%.">
          GMROI
        </div>
        <div className="col-span-2 text-right" title="HOLD: hold-NPV выше · PROMO: нужно подтолкнуть · LIQUIDATE: NPV(liq @ -50%) > NPV(hold)">
          Decision
        </div>
      </div>

      {items.map((item) => {
        const tier = item.velocityTier
        const decision = item.decision
        const gmroiPct = item.gmroi != null ? Math.round(item.gmroi * 100) : null
        const gmroiColor =
          gmroiPct == null ? 'text-slate-400' :
          gmroiPct < 100 ? 'text-red-600' :
          gmroiPct < 200 ? 'text-amber-600' : 'text-emerald-600'

        return (
          <div
            key={item.offerId}
            className={`grid grid-cols-12 gap-2 items-center py-1.5 px-2 rounded ${TIER_BG[tier]}`}
          >
            <div className="col-span-5 min-w-0">
              <div className="truncate text-slate-700" title={item.name || item.sku}>
                {item.name || item.sku}
              </div>
              <div className="text-[11px] text-slate-500 flex flex-wrap gap-x-1.5">
                {item.brand && <span className="font-medium">{item.brand}</span>}
                {item.brand && <span>·</span>}
                <span>{item.categoryName || t('inventory.uncategorized')}</span>
                <span>·</span>
                <span className="font-mono">{item.abcClass}</span>
                <span>·</span>
                <span className="capitalize">{TIER_LABEL[tier]}</span>
                {item.daysSinceSale != null && (
                  <>
                    <span>·</span>
                    <span>{item.daysSinceSale}{t('inventory.daysAgo')}</span>
                  </>
                )}
                {item.costQuality === 'fallback' && (
                  <>
                    <span>·</span>
                    <span className="text-amber-600" title="Cost basis derived from portfolio ratio">~est</span>
                  </>
                )}
              </div>
            </div>
            <div className="col-span-1 text-right text-slate-600 tabular-nums">
              {formatNumber(item.units)}
            </div>
            <div className="col-span-2 text-right font-medium text-slate-800 tabular-nums">
              {formatCurrency(item.costBasis)}
              <div className="text-[10px] text-slate-400">
                excess {formatCurrency(item.excessCapitalCost)}
              </div>
            </div>
            <div className="col-span-1 text-right text-slate-600 tabular-nums">
              {item.daysOfSupply != null ? `${item.daysOfSupply}d` : '—'}
            </div>
            <div className={`col-span-1 text-right font-medium tabular-nums ${gmroiColor}`}>
              {gmroiPct != null ? `${gmroiPct}%` : '—'}
            </div>
            <div className="col-span-2 text-right">
              <span className={`text-[11px] font-semibold px-2 py-0.5 rounded ${DECISION_BADGE[decision]}`}>
                {decision}
              </span>
            </div>
          </div>
        )
      })}
    </div>
  )
}

// ─── ABC × Velocity matrix tab ───────────────────────────────────────────────

const VELOCITY_TIERS: VelocityTier[] = ['hot', 'healthy', 'warm', 'cold', 'frozen']
const ABC_CLASSES: AbcClass[] = ['A', 'B', 'C']

interface MatrixTabProps {
  matrix: QuadrantMatrix
}

function MatrixTab({ matrix }: MatrixTabProps) {
  // Find max cost basis for color scaling
  let maxCost = 0
  for (const abc of ABC_CLASSES) {
    for (const tier of VELOCITY_TIERS) {
      maxCost = Math.max(maxCost, matrix[abc][tier].costBasis)
    }
  }

  // Cell color: progressive red intensity for cold/frozen, green for hot/healthy
  const cellColor = (tier: VelocityTier, cost: number): string => {
    if (cost === 0) return 'bg-slate-50 text-slate-400'
    const intensity = maxCost > 0 ? cost / maxCost : 0
    const isBad = tier === 'cold' || tier === 'frozen'
    const isWarn = tier === 'warm'
    if (isBad) {
      if (intensity > 0.6) return 'bg-red-200 text-red-900'
      if (intensity > 0.3) return 'bg-red-100 text-red-800'
      if (intensity > 0.1) return 'bg-red-50 text-red-700'
      return 'bg-red-50/60 text-red-600'
    }
    if (isWarn) {
      if (intensity > 0.4) return 'bg-amber-100 text-amber-800'
      return 'bg-amber-50 text-amber-700'
    }
    if (intensity > 0.4) return 'bg-emerald-100 text-emerald-800'
    return 'bg-emerald-50 text-emerald-700'
  }

  // Row totals
  const rowTotal = (abc: AbcClass) =>
    VELOCITY_TIERS.reduce((sum, t) => sum + matrix[abc][t].costBasis, 0)
  const colTotal = (tier: VelocityTier) =>
    ABC_CLASSES.reduce((sum, a) => sum + matrix[a][tier].costBasis, 0)
  const grandTotal = ABC_CLASSES.reduce((s, a) => s + rowTotal(a), 0)

  return (
    <div className="space-y-3">
      <div className="text-xs text-slate-500">
        Где осел капитал по cost basis. Цель — минимизировать <span className="text-red-600 font-medium">правый-нижний угол</span> (C·frozen) и нарастить <span className="text-emerald-600 font-medium">A·hot/healthy</span>.
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-[11px] uppercase tracking-wide text-slate-500">
              <th className="text-left p-1 font-medium">ABC</th>
              {VELOCITY_TIERS.map((tier) => (
                <th key={tier} className="text-right p-1 font-medium capitalize">
                  {tier}
                </th>
              ))}
              <th className="text-right p-1 font-medium">Σ</th>
            </tr>
          </thead>
          <tbody>
            {ABC_CLASSES.map((abc) => (
              <tr key={abc}>
                <td className="p-1 font-bold text-slate-700">{abc}</td>
                {VELOCITY_TIERS.map((tier) => {
                  const cell = matrix[abc][tier]
                  return (
                    <td
                      key={tier}
                      className={`p-1 text-right tabular-nums rounded ${cellColor(tier, cell.costBasis)}`}
                      title={`${cell.skuCount} SKU · ${formatNumber(cell.units)} units · 90d rev ${formatCurrency(cell.revenue90d)}`}
                    >
                      <div className="font-medium">{formatCurrency(cell.costBasis)}</div>
                      <div className="text-[10px] opacity-70">{cell.skuCount} SKU</div>
                    </td>
                  )
                })}
                <td className="p-1 text-right font-semibold text-slate-700 tabular-nums">
                  {formatCurrency(rowTotal(abc))}
                </td>
              </tr>
            ))}
            <tr className="text-[11px] text-slate-500 border-t border-slate-200">
              <td className="p-1">Σ</td>
              {VELOCITY_TIERS.map((tier) => (
                <td key={tier} className="p-1 text-right tabular-nums">
                  {formatCurrency(colTotal(tier))}
                </td>
              ))}
              <td className="p-1 text-right font-bold text-slate-700">
                {formatCurrency(grandTotal)}
              </td>
            </tr>
          </tbody>
        </table>
      </div>

      <div className="text-[11px] text-slate-500 leading-relaxed bg-slate-50 rounded p-2">
        <span className="font-medium text-slate-700">Чтение:</span> hot ≤ 30 дн supply · healthy 30-90 · warm 90-180 · cold 180-365 · frozen &gt; 365 (или 0 продаж 90д). C-class — нижние 5% выручки. Идеально: A-hot/healthy ≥ 60% капитала, C-frozen &lt; 5%.
      </div>
    </div>
  )
}

interface ActionsTabProps {
  actions: ReturnType<typeof useStockActions>['data'] extends infer T ? NonNullable<T> : never
}

function ActionsTab({ actions }: ActionsTabProps) {
  const { t } = useTranslation()

  if (!actions || actions.length === 0) {
    return (
      <div className="text-center text-slate-500 py-8">
        {t('inventory.noActions')}
      </div>
    )
  }

  const getActionColor = (action: string) => {
    if (action.includes('Return')) return 'text-red-600 bg-red-50'
    if (action.includes('70%') || action.includes('50%')) return 'text-amber-600 bg-amber-50'
    if (action.includes('Bundle')) return 'text-blue-600 bg-blue-50'
    if (action.includes('Promote')) return 'text-purple-600 bg-purple-50'
    return 'text-slate-600 bg-slate-50'
  }

  return (
    <div className="space-y-2 max-h-64 overflow-y-auto">
      {actions.map((item) => (
        <div
          key={item.offerId}
          className="flex justify-between items-center text-sm py-2 px-2 bg-slate-50 rounded gap-2"
        >
          <div className="flex-1 min-w-0">
            <div className="truncate text-slate-700" title={item.name || item.sku}>
              {item.name || item.sku}
            </div>
            <div className="text-xs text-slate-500">
              {item.units} units · {formatCurrency(item.value)}
            </div>
          </div>
          <div className={`text-xs font-medium px-2 py-1 rounded ${getActionColor(item.action)}`}>
            {item.action}
          </div>
        </div>
      ))}
    </div>
  )
}

// ─── Export ──────────────────────────────────────────────────────────────────

export const DeadStockChart = memo(DeadStockChartComponent)
