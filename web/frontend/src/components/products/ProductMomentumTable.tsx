import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { useProductMomentum } from '../../hooks/useApi'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'

export const ProductMomentumTable = memo(function ProductMomentumTable() {
  const { t } = useTranslation()
  const { data, isLoading } = useProductMomentum()

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
        <div className="flex items-center gap-1.5 mb-3">
          <h3 className="text-sm font-semibold text-slate-800">{t('products.momentumTitle')}</h3>
          <InfoPopover>
            <p className="text-xs text-slate-300">{t('products.momentumDesc')}</p>
          </InfoPopover>
        </div>
        <div className="h-[280px] flex items-center justify-center">
          <div className="w-6 h-6 border-2 border-purple-500 border-t-transparent rounded-full animate-spin" />
        </div>
      </div>
    )
  }

  const gainers = data?.gainers ?? []
  const losers = data?.losers ?? []
  const hasData = gainers.length > 0 || losers.length > 0

  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
      <div className="flex items-center gap-1.5 mb-3">
        <h3 className="text-sm font-semibold text-slate-800">{t('products.momentumTitle')}</h3>
        <InfoPopover title={t('products.momentumTitle')}>
          <p className="text-xs text-slate-300 mb-2">
            {t('products.momentumDesc')}
          </p>
          <p className="text-xs text-slate-300 mb-2">
            <strong className="text-green-400">{t('products.gainers')}:</strong> {t('products.momentumGainersDesc')}
          </p>
          <p className="text-xs text-slate-300">
            <strong className="text-red-400">{t('products.losers')}:</strong> {t('products.momentumLosersDesc')}
          </p>
        </InfoPopover>
      </div>

      {!hasData ? (
        <div className="h-[280px] flex items-center justify-center text-sm text-slate-400">
          {t('products.noMomentumData')}
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {/* Gainers */}
          <div>
            <div className="flex items-center gap-1.5 mb-2">
              <span className="w-2 h-2 rounded-full bg-green-500" />
              <span className="text-xs font-medium text-green-700 uppercase tracking-wide">{t('products.gainers')}</span>
            </div>
            {gainers.length === 0 ? (
              <p className="text-xs text-slate-400 py-4 text-center">{t('products.noGainers')}</p>
            ) : (
              <div className="space-y-1.5">
                {gainers.map((p, i) => (
                  <div key={i} className="flex items-center justify-between px-2 py-1.5 rounded-md bg-green-50/50 hover:bg-green-50">
                    <div className="min-w-0 flex-1 mr-3">
                      <p className="text-xs font-medium text-slate-700 truncate" title={p.productName}>
                        {p.productName}
                      </p>
                      <p className="text-[10px] text-slate-400">
                        {formatCurrency(p.currentRevenue)} ({formatNumber(p.currentQty)} qty)
                      </p>
                    </div>
                    <span className="text-xs font-semibold text-green-600 whitespace-nowrap">
                      +{p.growthPct.toFixed(0)}%
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Losers */}
          <div>
            <div className="flex items-center gap-1.5 mb-2">
              <span className="w-2 h-2 rounded-full bg-red-500" />
              <span className="text-xs font-medium text-red-700 uppercase tracking-wide">{t('products.losers')}</span>
            </div>
            {losers.length === 0 ? (
              <p className="text-xs text-slate-400 py-4 text-center">{t('products.noLosers')}</p>
            ) : (
              <div className="space-y-1.5">
                {losers.map((p, i) => (
                  <div key={i} className="flex items-center justify-between px-2 py-1.5 rounded-md bg-red-50/50 hover:bg-red-50">
                    <div className="min-w-0 flex-1 mr-3">
                      <p className="text-xs font-medium text-slate-700 truncate" title={p.productName}>
                        {p.productName}
                      </p>
                      <p className="text-[10px] text-slate-400">
                        {formatCurrency(p.currentRevenue)} ({formatNumber(p.currentQty)} qty)
                      </p>
                    </div>
                    <span className="text-xs font-semibold text-red-600 whitespace-nowrap">
                      {p.growthPct.toFixed(0)}%
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
})
