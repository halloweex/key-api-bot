import { memo } from 'react'
import { AlertTriangle } from 'lucide-react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { InfoPopover } from '../ui/InfoPopover'
import { MetricCard } from '../MetricCard'
import { useStockSummary } from '../../hooks'
import { formatNumber, formatCurrency } from '../../utils/formatters'

// ─── Component ───────────────────────────────────────────────────────────────

function StockSummaryChartComponent() {
  const { t } = useTranslation()
  const { data, isLoading, error } = useStockSummary(15)

  return (
    <ChartContainer
      title={t('inventory.stockLevels')}
      titleExtra={
        <InfoPopover title={t('inventory.stockLevels')}>
          <div className="space-y-2">
            <p className="text-xs text-slate-300">
              <strong className="text-emerald-400">{t('inventory.inStock')}:</strong> {t('inventory.stockLevelsInfo1')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-blue-400">{t('inventory.available')}:</strong> {t('inventory.stockLevelsInfo2')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-amber-400">{t('inventory.lowStock')}:</strong> {t('inventory.stockLevelsInfo3')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-purple-400">{t('inventory.stockValue')}:</strong> {t('inventory.stockLevelsInfo4')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-slate-400">{t('inventory.avgInventory30d')}:</strong> {t('inventory.stockLevelsInfo5')}
            </p>
          </div>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error}
      ariaLabel={t('inventory.stockLevelsDesc')}
    >
      {data && (
        <div className="space-y-4 min-h-[420px]">
          {/* Summary Stats */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
            <MetricCard
              surface="tile-tinted"
              tone="green"
              label={t('inventory.inStock')}
              value={String(data.summary.inStockCount)}
            />
            <MetricCard
              surface="tile-tinted"
              tone="orange"
              label={t('inventory.lowStock')}
              value={String(data.summary.lowStockCount)}
            />
            <MetricCard
              surface="tile-tinted"
              tone="red"
              label={t('inventory.outOfStock')}
              value={String(data.summary.outOfStockCount)}
            />
            <MetricCard
              surface="tile-tinted"
              tone="blue"
              label={t('inventory.available')}
              value={formatNumber(data.summary.totalQuantity)}
              sub={`${formatNumber(data.summary.totalReserve)} ${t('inventory.reserved')}`}
            />
          </div>

          {/* Value Stats */}
          <div className="grid grid-cols-1 sm:grid-cols-2 gap-3">
            <MetricCard
              surface="tile-tinted"
              tone="neutral"
              label={t('inventory.stockValue')}
              value={formatCurrency(data.summary.totalValue)}
              sub={data.summary.reserveValue > 0 ? `${formatCurrency(data.summary.reserveValue)} ${t('inventory.reserved')}` : undefined}
            />
            <MetricCard
              surface="tile-tinted"
              tone="purple"
              label={
                data.summary.avgDataPoints > 0
                  ? `${t('inventory.avgInventory30d')} · ${data.summary.avgDataPoints} ${t('inventory.pts')}`
                  : t('inventory.avgInventory30d')
              }
              value={formatCurrency(data.summary.averageValue)}
            />
          </div>

          {/* Lists */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Low Stock Alert */}
            {data.lowStock.length > 0 && (
              <div>
                <h4 className="text-sm font-semibold text-slate-700 mb-2 flex items-center gap-1">
                  <AlertTriangle className="w-4 h-4" />
                  {t('inventory.lowStock')} ({data.lowStock.length})
                </h4>
                <div className="space-y-1 max-h-64 overflow-y-auto">
                  {data.lowStock.slice(0, 10).map((item) => (
                    <div
                      key={item.sku}
                      className="flex justify-between items-center text-sm py-1 px-2 bg-amber-50 rounded gap-2"
                    >
                      <span className="text-slate-600 truncate flex-1" title={item.name || item.sku}>
                        {item.name || item.sku}
                      </span>
                      <span className="font-medium text-amber-700 whitespace-nowrap">
                        {item.quantity} {t('inventory.left')}
                        {item.reserve > 0 && (
                          <span className="text-amber-500 text-xs ml-1">
                            ({item.reserve} {t('inventory.res')})
                          </span>
                        )}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Top by Quantity */}
            <div>
              <h4 className="text-sm font-semibold text-slate-700 mb-2">{t('inventory.topByQuantity')}</h4>
              <div className="space-y-1 max-h-64 overflow-y-auto">
                {data.topByQuantity.slice(0, 10).map((item, index) => (
                  <div
                    key={item.sku}
                    className="flex justify-between items-center text-sm py-1 px-2 bg-slate-50 rounded gap-2"
                  >
                    <span className="flex items-center gap-2 flex-1 min-w-0">
                      <span className="text-xs text-slate-400 w-4 flex-shrink-0">{index + 1}.</span>
                      <span className="text-slate-600 truncate" title={item.name || item.sku}>
                        {item.name || item.sku}
                      </span>
                    </span>
                    <span className="font-medium text-slate-700 whitespace-nowrap">
                      {formatNumber(item.quantity)}
                      {item.reserve > 0 && (
                        <span className="text-slate-400 text-xs ml-1">
                          (-{item.reserve})
                        </span>
                      )}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Last Sync */}
          {data.lastSync && (
            <div className="text-xs text-slate-400 text-center pt-2">
              {t('inventory.lastSynced')} {new Date(data.lastSync).toLocaleString()}
            </div>
          )}
        </div>
      )}
    </ChartContainer>
  )
}

// ─── Export ──────────────────────────────────────────────────────────────────

export const StockSummaryChart = memo(StockSummaryChartComponent)
