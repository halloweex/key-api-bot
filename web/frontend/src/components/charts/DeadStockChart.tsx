import { memo, useState } from 'react'
import { ChartContainer } from './ChartContainer'
import { useDeadStockAnalysis } from '../../hooks'
import { formatNumber, formatCurrency } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

function DeadStockChartComponent() {
  const { data, isLoading, error } = useDeadStockAnalysis()
  const [activeTab, setActiveTab] = useState<'summary' | 'items' | 'thresholds'>('summary')

  return (
    <ChartContainer
      title="Dead Stock Analysis"
      isLoading={isLoading}
      error={error}
      className="col-span-1"
      ariaLabel="Dead stock analysis"
    >
      {data && (
        <div className="space-y-4">
          {/* Summary Stats */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <StatCard
              label="Healthy"
              value={data.summary.healthy.skuCount}
              subValue={`${data.summary.healthy.valuePercent}%`}
              color="text-emerald-600"
              bgColor="bg-emerald-50"
            />
            <StatCard
              label="At Risk"
              value={data.summary.atRisk.skuCount}
              subValue={`${data.summary.atRisk.valuePercent}%`}
              color="text-amber-600"
              bgColor="bg-amber-50"
            />
            <StatCard
              label="Dead Stock"
              value={data.summary.deadStock.skuCount}
              subValue={`${data.summary.deadStock.valuePercent}%`}
              color="text-red-600"
              bgColor="bg-red-50"
            />
            <StatCard
              label="Never Sold"
              value={data.summary.neverSold.skuCount}
              subValue={`${data.summary.neverSold.valuePercent}%`}
              color="text-slate-600"
              bgColor="bg-slate-100"
            />
          </div>

          {/* Value Summary */}
          <div className="grid grid-cols-2 gap-3">
            <div className="text-center py-2 bg-red-50 rounded-lg">
              <div className="text-lg font-semibold text-red-700">
                {formatCurrency(data.summary.deadStock.value + data.summary.neverSold.value)}
              </div>
              <div className="text-xs text-slate-500">Dead Stock Value</div>
            </div>
            <div className="text-center py-2 bg-amber-50 rounded-lg">
              <div className="text-lg font-semibold text-amber-700">
                {formatCurrency(data.summary.atRisk.value)}
              </div>
              <div className="text-xs text-slate-500">At Risk Value</div>
            </div>
          </div>

          {/* Tabs */}
          <div className="flex gap-1 border-b border-slate-200">
            <TabButton
              active={activeTab === 'summary'}
              onClick={() => setActiveTab('summary')}
            >
              Summary
            </TabButton>
            <TabButton
              active={activeTab === 'items'}
              onClick={() => setActiveTab('items')}
            >
              Items ({data.items.length})
            </TabButton>
            <TabButton
              active={activeTab === 'thresholds'}
              onClick={() => setActiveTab('thresholds')}
            >
              Thresholds
            </TabButton>
          </div>

          {/* Tab Content */}
          <div className="min-h-[200px]">
            {activeTab === 'summary' && (
              <SummaryTab data={data} />
            )}
            {activeTab === 'items' && (
              <ItemsTab items={data.items} />
            )}
            {activeTab === 'thresholds' && (
              <ThresholdsTab
                thresholds={data.categoryThresholds}
                methodology={data.methodology}
              />
            )}
          </div>
        </div>
      )}
    </ChartContainer>
  )
}

// ─── Sub Components ──────────────────────────────────────────────────────────

interface StatCardProps {
  label: string
  value: number | string
  subValue?: string
  color: string
  bgColor: string
}

function StatCard({ label, value, subValue, color, bgColor }: StatCardProps) {
  return (
    <div className={`${bgColor} rounded-lg p-3 text-center`}>
      <div className={`text-xl font-bold ${color}`}>{value}</div>
      <div className="text-xs text-slate-500">{label}</div>
      {subValue && <div className="text-xs text-slate-400 mt-0.5">{subValue}</div>}
    </div>
  )
}

interface TabButtonProps {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}

function TabButton({ active, onClick, children }: TabButtonProps) {
  return (
    <button
      onClick={onClick}
      className={`px-3 py-2 text-sm font-medium transition-colors ${
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
  data: NonNullable<ReturnType<typeof useDeadStockAnalysis>['data']>
}

function SummaryTab({ data }: SummaryTabProps) {
  const total = data.summary.total
  const deadAndNever = data.summary.deadStock.value + data.summary.neverSold.value
  const deadPercent = total.value > 0 ? (deadAndNever / total.value * 100) : 0

  return (
    <div className="space-y-3">
      {/* Progress Bar */}
      <div>
        <div className="flex justify-between text-xs text-slate-500 mb-1">
          <span>Inventory Health</span>
          <span>{data.summary.healthy.valuePercent}% healthy</span>
        </div>
        <div className="h-3 bg-slate-100 rounded-full overflow-hidden flex">
          <div
            className="bg-emerald-500 transition-all"
            style={{ width: `${data.summary.healthy.valuePercent}%` }}
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
        <div className="flex gap-3 mt-2 text-xs">
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-emerald-500 rounded-full"></span>
            Healthy
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-amber-500 rounded-full"></span>
            At Risk
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-red-500 rounded-full"></span>
            Dead
          </span>
          <span className="flex items-center gap-1">
            <span className="w-2 h-2 bg-slate-400 rounded-full"></span>
            Never Sold
          </span>
        </div>
      </div>

      {/* Details Grid */}
      <div className="grid grid-cols-2 gap-2 text-sm">
        <div className="bg-slate-50 rounded p-2">
          <div className="text-slate-500 text-xs">Total SKUs</div>
          <div className="font-medium">{formatNumber(total.skuCount)}</div>
        </div>
        <div className="bg-slate-50 rounded p-2">
          <div className="text-slate-500 text-xs">Total Units</div>
          <div className="font-medium">{formatNumber(total.quantity)}</div>
        </div>
        <div className="bg-slate-50 rounded p-2">
          <div className="text-slate-500 text-xs">Total Value</div>
          <div className="font-medium">{formatCurrency(total.value)}</div>
        </div>
        <div className="bg-red-50 rounded p-2">
          <div className="text-slate-500 text-xs">Dead Stock %</div>
          <div className="font-medium text-red-600">{deadPercent.toFixed(1)}%</div>
        </div>
      </div>
    </div>
  )
}

interface ItemsTabProps {
  items: NonNullable<ReturnType<typeof useDeadStockAnalysis>['data']>['items']
}

function ItemsTab({ items }: ItemsTabProps) {
  if (items.length === 0) {
    return (
      <div className="text-center text-slate-500 py-8">
        No dead stock or at-risk items found
      </div>
    )
  }

  return (
    <div className="space-y-1 max-h-64 overflow-y-auto">
      {items.map((item) => (
        <div
          key={item.id}
          className={`flex justify-between items-center text-sm py-2 px-2 rounded gap-2 ${
            item.status === 'never_sold'
              ? 'bg-slate-100'
              : item.status === 'dead_stock'
              ? 'bg-red-50'
              : 'bg-amber-50'
          }`}
        >
          <div className="flex-1 min-w-0">
            <div className="truncate text-slate-700" title={item.name || item.sku}>
              {item.name || item.sku}
            </div>
            <div className="text-xs text-slate-500 flex flex-wrap gap-x-2">
              {item.brand && <span className="font-medium">{item.brand}</span>}
              {item.brand && <span>·</span>}
              <span>{item.categoryName || 'Uncategorized'}</span>
              <span>·</span>
              <span>
                {item.lastSaleDate
                  ? `${item.daysSinceSale}d ago`
                  : 'Never sold'}
              </span>
            </div>
          </div>
          <div className="text-right whitespace-nowrap">
            <div className={`font-medium ${
              item.status === 'never_sold'
                ? 'text-slate-600'
                : item.status === 'dead_stock'
                ? 'text-red-600'
                : 'text-amber-600'
            }`}>
              {formatCurrency(item.value)}
            </div>
            <div className="text-xs text-slate-500">
              {item.quantity} units
            </div>
          </div>
        </div>
      ))}
    </div>
  )
}

interface ThresholdsTabProps {
  thresholds: NonNullable<ReturnType<typeof useDeadStockAnalysis>['data']>['categoryThresholds']
  methodology: NonNullable<ReturnType<typeof useDeadStockAnalysis>['data']>['methodology']
}

function ThresholdsTab({ thresholds, methodology }: ThresholdsTabProps) {
  return (
    <div className="space-y-3">
      {/* Methodology Info */}
      <div className="bg-blue-50 rounded-lg p-3 text-sm">
        <div className="font-medium text-blue-700 mb-1">Dynamic Thresholds</div>
        <p className="text-blue-600 text-xs">{methodology.description}</p>
        <div className="mt-2 text-xs text-blue-500 flex gap-3">
          <span>Min: {methodology.minimumThreshold}d</span>
          <span>Default: {methodology.defaultThreshold}d</span>
          <span>At-risk: 70%</span>
        </div>
      </div>

      {/* Category Thresholds */}
      {thresholds.length > 0 ? (
        <div className="space-y-1 max-h-48 overflow-y-auto">
          {thresholds.map((cat) => (
            <div
              key={cat.categoryId}
              className="flex justify-between items-center text-sm py-2 px-2 bg-slate-50 rounded"
            >
              <div className="flex-1 min-w-0">
                <div className="truncate text-slate-700">{cat.categoryName}</div>
                <div className="text-xs text-slate-500">
                  {cat.productsWithSales} products · median {cat.medianDays}d
                </div>
              </div>
              <div className="text-right">
                <div className="font-medium text-slate-700">
                  {cat.thresholdDays}d
                </div>
                <div className="text-xs text-slate-500">threshold</div>
              </div>
            </div>
          ))}
        </div>
      ) : (
        <div className="text-center text-slate-500 py-4 text-sm">
          Not enough sales data to calculate category thresholds.
          Using default {methodology.defaultThreshold}-day threshold.
        </div>
      )}
    </div>
  )
}

// ─── Export ──────────────────────────────────────────────────────────────────

export const DeadStockChart = memo(DeadStockChartComponent)
