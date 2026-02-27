import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { ShoppingCart, Layers, TrendingUp, Link } from 'lucide-react'
import { StatCard, StatCardSkeleton } from '../cards/StatCard'
import { useBasketSummary } from '../../hooks/useApi'
import { formatNumber, formatCurrency } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'

export const BasketSummaryCards = memo(function BasketSummaryCards() {
  const { t } = useTranslation()
  const { data, isLoading } = useBasketSummary()

  if (isLoading || !data) {
    return (
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
        {Array.from({ length: 4 }).map((_, i) => <StatCardSkeleton key={i} />)}
      </div>
    )
  }

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
      <StatCard
        label={t('products.avgBasketSize')}
        value={data.avgBasketSize}
        formatter={(v) => `${v.toFixed(1)} ${t('products.items')}`}
        icon={<ShoppingCart className="w-5 h-5" />}
        variant="blue"
        subtitle={`${formatNumber(data.totalOrders)} ${t('products.orders')}`}
        labelExtra={
          <InfoPopover>
            <p className="text-xs text-slate-300">{t('products.avgBasketSizeDesc')}</p>
          </InfoPopover>
        }
      />
      <StatCard
        label={t('products.multiItemOrders')}
        value={data.multiItemPct}
        formatter={(v) => `${v.toFixed(1)}%`}
        icon={<Layers className="w-5 h-5" />}
        variant="purple"
        subtitle={`${formatNumber(data.multiItemOrders)} ${t('products.of')} ${formatNumber(data.totalOrders)}`}
        labelExtra={
          <InfoPopover>
            <p className="text-xs text-slate-300">{t('products.multiItemDesc')}</p>
          </InfoPopover>
        }
      />
      <StatCard
        label={t('products.aovUplift')}
        value={data.aovUplift}
        formatter={(v) => `${v.toFixed(1)}x`}
        icon={<TrendingUp className="w-5 h-5" />}
        variant="green"
        subtitle={`${formatCurrency(data.multiAov)} ${t('products.vs')} ${formatCurrency(data.singleAov)}`}
        labelExtra={
          <InfoPopover>
            <p className="text-xs text-slate-300">{t('products.aovUpliftDesc', { value: data.aovUplift.toFixed(1) })}</p>
          </InfoPopover>
        }
      />
      <StatCard
        label={t('products.topPair')}
        value={data.topPairCount}
        formatter={formatNumber}
        icon={<Link className="w-5 h-5" />}
        variant="orange"
        subtitle={data.topPair}
        labelExtra={
          <InfoPopover>
            <p className="text-xs text-slate-300">{t('products.topPairDesc')}</p>
          </InfoPopover>
        }
      />
    </div>
  )
})
