import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { StatCard, StatCardSkeleton } from '../cards/StatCard'
import { useBasketSummary } from '../../hooks/useApi'
import { formatNumber, formatCurrency } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'

const BasketIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M3 3h2l.4 2M7 13h10l4-8H5.4M7 13L5.4 5M7 13l-2.293 2.293c-.63.63-.184 1.707.707 1.707H17m0 0a2 2 0 100 4 2 2 0 000-4zm-8 2a2 2 0 100 4 2 2 0 000-4z" />
  </svg>
)

const MultiIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" />
  </svg>
)

const UpliftIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
  </svg>
)

const PairIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M13.828 10.172a4 4 0 00-5.656 0l-4 4a4 4 0 105.656 5.656l1.102-1.101m-.758-4.899a4 4 0 005.656 0l4-4a4 4 0 00-5.656-5.656l-1.1 1.1" />
  </svg>
)

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
        icon={<BasketIcon />}
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
        icon={<MultiIcon />}
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
        icon={<UpliftIcon />}
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
        icon={<PairIcon />}
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
