import { memo, useState, useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { useProductPairs } from '../../hooks/useApi'
import { formatNumber } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'

type SortKey = 'coOccurrence' | 'lift' | 'confidenceAtoB' | 'confidenceBtoA'

export const FrequentlyBoughtTogether = memo(function FrequentlyBoughtTogether() {
  const { t } = useTranslation()
  const [selectedProductId, setSelectedProductId] = useState<number | null>(null)
  const [sortKey, setSortKey] = useState<SortKey>('coOccurrence')
  const { data: pairs, isLoading } = useProductPairs(selectedProductId)

  const handleProductClick = useCallback((productId: number | null) => {
    setSelectedProductId((prev) => (prev === productId ? null : productId))
  }, [])

  const sortedPairs = pairs
    ? [...pairs].sort((a, b) => {
        if (sortKey === 'coOccurrence') return b.coOccurrence - a.coOccurrence
        if (sortKey === 'lift') return b.lift - a.lift
        if (sortKey === 'confidenceAtoB') return b.confidenceAtoB - a.confidenceAtoB
        return b.confidenceBtoA - a.confidenceBtoA
      })
    : []

  const SortButton = ({ label, field }: { label: string; field: SortKey }) => (
    <button
      onClick={() => setSortKey(field)}
      className={`text-xs px-2 py-1 rounded-md transition-colors ${
        sortKey === field
          ? 'bg-purple-100 text-purple-700 font-medium'
          : 'text-slate-500 hover:bg-slate-100'
      }`}
    >
      {label}
    </button>
  )

  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm">
      <div className="px-4 py-3 border-b border-slate-100 flex flex-wrap items-center justify-between gap-2">
        <div className="flex items-center gap-1.5">
          <h3 className="text-sm font-semibold text-slate-800">{t('products.fbtTitle')}</h3>
          <InfoPopover title={t('products.fbtTitle')}>
            <p className="text-xs text-slate-300 mb-2">
              {t('products.fbtDesc')}
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">{t('products.coPurchases')}:</strong> {t('products.fbtCoDesc')}
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">{t('products.support')}:</strong> {t('products.fbtSupportDesc')}
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">{t('products.confAB')}:</strong> {t('products.fbtConfABDesc')}
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">{t('products.confBA')}:</strong> {t('products.fbtConfBADesc')}
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-purple-400">{t('products.lift')}:</strong> {t('products.fbtLiftDesc')}
            </p>
          </InfoPopover>
          {selectedProductId && (
            <button
              onClick={() => setSelectedProductId(null)}
              className="text-xs bg-purple-100 text-purple-700 px-2 py-0.5 rounded-full hover:bg-purple-200"
            >
              {t('products.clearFilter')}
            </button>
          )}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-xs text-slate-400 mr-1">{t('products.sort')}</span>
          <SortButton label={t('products.count')} field="coOccurrence" />
          <SortButton label={t('products.lift')} field="lift" />
          <SortButton label={t('products.confAB')} field="confidenceAtoB" />
          <SortButton label={t('products.confBA')} field="confidenceBtoA" />
        </div>
      </div>

      {isLoading ? (
        <div className="p-8 text-center">
          <div className="w-6 h-6 border-2 border-purple-500 border-t-transparent rounded-full animate-spin mx-auto" />
        </div>
      ) : !sortedPairs.length ? (
        <div className="p-8 text-center text-sm text-slate-400">{t('products.noPairsFound')}</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 border-b border-slate-100">
                <th className="text-left px-4 py-2 font-medium">{t('products.productA')}</th>
                <th className="text-left px-4 py-2 font-medium">{t('products.productB')}</th>
                <th className="text-right px-4 py-2 font-medium">{t('products.coPurchases')}</th>
                <th className="text-right px-4 py-2 font-medium hidden sm:table-cell">{t('products.support')}</th>
                <th className="text-right px-4 py-2 font-medium hidden md:table-cell">{t('products.confAB')}</th>
                <th className="text-right px-4 py-2 font-medium hidden md:table-cell">{t('products.confBA')}</th>
                <th className="text-right px-4 py-2 font-medium">{t('products.lift')}</th>
              </tr>
            </thead>
            <tbody>
              {sortedPairs.map((pair, i) => (
                <tr key={i} className="border-b border-slate-50 hover:bg-slate-50/50">
                  <td className="px-4 py-2">
                    <button
                      onClick={() => pair.productA.id && handleProductClick(pair.productA.id)}
                      className="text-left text-purple-600 hover:text-purple-800 hover:underline truncate max-w-[200px] block"
                      title={pair.productA.name}
                    >
                      {pair.productA.name}
                    </button>
                  </td>
                  <td className="px-4 py-2">
                    <button
                      onClick={() => pair.productB.id && handleProductClick(pair.productB.id)}
                      className="text-left text-purple-600 hover:text-purple-800 hover:underline truncate max-w-[200px] block"
                      title={pair.productB.name}
                    >
                      {pair.productB.name}
                    </button>
                  </td>
                  <td className="px-4 py-2 text-right font-medium">{formatNumber(pair.coOccurrence)}</td>
                  <td className="px-4 py-2 text-right text-slate-500 hidden sm:table-cell">
                    {(pair.support * 100).toFixed(2)}%
                  </td>
                  <td className="px-4 py-2 text-right text-slate-500 hidden md:table-cell">
                    {(pair.confidenceAtoB * 100).toFixed(1)}%
                  </td>
                  <td className="px-4 py-2 text-right text-slate-500 hidden md:table-cell">
                    {(pair.confidenceBtoA * 100).toFixed(1)}%
                  </td>
                  <td className="px-4 py-2 text-right">
                    <span
                      className={`inline-flex px-1.5 py-0.5 rounded text-xs font-medium ${
                        pair.lift >= 5
                          ? 'bg-green-100 text-green-700'
                          : pair.lift >= 2
                            ? 'bg-blue-100 text-blue-700'
                            : 'bg-slate-100 text-slate-600'
                      }`}
                    >
                      {pair.lift.toFixed(1)}x
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
})
