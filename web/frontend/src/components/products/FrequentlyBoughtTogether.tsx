import { memo, useState, useCallback } from 'react'
import { useProductPairs } from '../../hooks/useApi'
import { formatNumber } from '../../utils/formatters'
import { InfoPopover } from '../ui/InfoPopover'

type SortKey = 'coOccurrence' | 'lift' | 'confidenceAtoB' | 'confidenceBtoA'

export const FrequentlyBoughtTogether = memo(function FrequentlyBoughtTogether() {
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
          <h3 className="text-sm font-semibold text-slate-800">Frequently Bought Together</h3>
          <InfoPopover title="Frequently Bought Together">
            <p className="text-xs text-slate-300 mb-2">
              Products often purchased in the same order.
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">Co-purchases:</strong> Number of orders containing both products.
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">Support:</strong> Percentage of all orders that contain this pair. Higher = more common.
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">Conf A→B:</strong> If a customer buys Product A, the probability they also buy Product B.
            </p>
            <p className="text-xs text-slate-300 mb-2">
              <strong className="text-purple-400">Conf B→A:</strong> If a customer buys Product B, the probability they also buy Product A.
            </p>
            <p className="text-xs text-slate-300">
              <strong className="text-purple-400">Lift:</strong> How much more likely these products are bought together vs. by chance. Lift &gt; 1 means positive association; higher = stronger link.
            </p>
          </InfoPopover>
          {selectedProductId && (
            <button
              onClick={() => setSelectedProductId(null)}
              className="text-xs bg-purple-100 text-purple-700 px-2 py-0.5 rounded-full hover:bg-purple-200"
            >
              Clear filter
            </button>
          )}
        </div>
        <div className="flex items-center gap-1">
          <span className="text-xs text-slate-400 mr-1">Sort:</span>
          <SortButton label="Count" field="coOccurrence" />
          <SortButton label="Lift" field="lift" />
          <SortButton label="Conf A→B" field="confidenceAtoB" />
          <SortButton label="Conf B→A" field="confidenceBtoA" />
        </div>
      </div>

      {isLoading ? (
        <div className="p-8 text-center">
          <div className="w-6 h-6 border-2 border-purple-500 border-t-transparent rounded-full animate-spin mx-auto" />
        </div>
      ) : !sortedPairs.length ? (
        <div className="p-8 text-center text-sm text-slate-400">No product pairs found</div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-xs text-slate-500 border-b border-slate-100">
                <th className="text-left px-4 py-2 font-medium">Product A</th>
                <th className="text-left px-4 py-2 font-medium">Product B</th>
                <th className="text-right px-4 py-2 font-medium">Co-purchases</th>
                <th className="text-right px-4 py-2 font-medium hidden sm:table-cell">Support</th>
                <th className="text-right px-4 py-2 font-medium hidden md:table-cell">Conf A→B</th>
                <th className="text-right px-4 py-2 font-medium hidden md:table-cell">Conf B→A</th>
                <th className="text-right px-4 py-2 font-medium">Lift</th>
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
