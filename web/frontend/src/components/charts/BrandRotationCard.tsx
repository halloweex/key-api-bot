import { memo } from 'react'
import { ChartContainer } from './ChartContainer'
import { InfoPopover } from '../ui/InfoPopover'
import { useBrandRotation } from '../../hooks'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import type { BrandRotationItem } from '../../types/api'

const HEALTH_STYLE: Record<BrandRotationItem['health'], { row: string; chip: string; label: string }> = {
  great:    { row: 'bg-emerald-50',  chip: 'bg-emerald-100 text-emerald-800',  label: '< 60d' },
  ok:       { row: 'bg-emerald-50/60', chip: 'bg-emerald-50 text-emerald-700', label: '60-120d' },
  warning:  { row: 'bg-amber-50',    chip: 'bg-amber-100 text-amber-800',     label: '120-200d' },
  poor:     { row: 'bg-orange-50',   chip: 'bg-orange-100 text-orange-800',   label: '200-365d' },
  critical: { row: 'bg-red-50',      chip: 'bg-red-100 text-red-800',         label: '> 365d' },
}

function BrandRotationCardComponent() {
  const { data, isLoading, error } = useBrandRotation()

  return (
    <ChartContainer
      title="Brand rotation scorecard"
      titleExtra={
        <InfoPopover title="Brand rotation">
          <div className="space-y-2 text-xs text-slate-300">
            <p>
              Скорость оборота капитала по бренду. Rotation days = cost basis / (COGS in 90d / 90).
              Цель — &lt; 120 дней. Бренды с &gt; 365 — кандидаты на стоп-закупки.
            </p>
            <p>
              <strong className="text-emerald-400">GMROI</strong> = annualized gross profit / cost basis.
              Бенчмарк для cosmetics: 200–400%. Ниже 100% = убыточный бренд.
            </p>
            <p>
              <strong className="text-red-400">Frozen %</strong> — доля SKU бренда с DOS &gt; 180 дней.
            </p>
          </div>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error}
      ariaLabel="Brand rotation scorecard"
    >
      {data && data.length > 0 && (
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-[11px] uppercase tracking-wide text-slate-500 border-b border-slate-200">
                <th className="text-left py-2 px-1 font-medium">Brand</th>
                <th className="text-right py-2 px-1 font-medium">SKU</th>
                <th className="text-right py-2 px-1 font-medium">Frozen</th>
                <th className="text-right py-2 px-1 font-medium">Cost basis</th>
                <th className="text-right py-2 px-1 font-medium">90d revenue</th>
                <th className="text-right py-2 px-1 font-medium">GMROI</th>
                <th className="text-right py-2 px-1 font-medium">Rotation</th>
              </tr>
            </thead>
            <tbody>
              {data.slice(0, 20).map((b) => {
                const style = HEALTH_STYLE[b.health]
                const gmroiPct = b.gmroi != null ? Math.round(b.gmroi * 100) : null
                const gmroiColor =
                  gmroiPct == null ? 'text-slate-400' :
                  gmroiPct < 100 ? 'text-red-600' :
                  gmroiPct < 200 ? 'text-amber-600' :
                  'text-emerald-600'
                const frozenPct = Math.round(b.frozenShare * 100)

                return (
                  <tr key={b.brand} className={`${style.row} border-b border-white`}>
                    <td className="py-1.5 px-1 font-medium text-slate-700 truncate max-w-[180px]" title={b.brand}>
                      {b.brand}
                    </td>
                    <td className="py-1.5 px-1 text-right text-slate-600 tabular-nums">
                      {b.skuCount}
                    </td>
                    <td className="py-1.5 px-1 text-right tabular-nums">
                      <span className={frozenPct > 50 ? 'text-red-600 font-medium' : frozenPct > 25 ? 'text-amber-600' : 'text-slate-500'}>
                        {b.frozenSkus}/{b.skuCount} ({frozenPct}%)
                      </span>
                    </td>
                    <td className="py-1.5 px-1 text-right text-slate-700 tabular-nums">
                      {formatCurrency(b.costBasis)}
                    </td>
                    <td className="py-1.5 px-1 text-right text-slate-600 tabular-nums">
                      {formatCurrency(b.revenue90d)}
                      <div className="text-[10px] text-slate-400">{formatNumber(b.qtySold90d)} units</div>
                    </td>
                    <td className={`py-1.5 px-1 text-right font-medium tabular-nums ${gmroiColor}`}>
                      {gmroiPct != null ? `${gmroiPct}%` : '—'}
                    </td>
                    <td className="py-1.5 px-1 text-right">
                      <span className={`inline-block px-2 py-0.5 rounded text-[11px] font-medium tabular-nums ${style.chip}`}>
                        {b.rotationDays != null ? `${b.rotationDays}d` : '—'}
                      </span>
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
          {data.length > 20 && (
            <div className="text-[11px] text-slate-400 mt-2 text-center">
              Показаны топ-20 из {data.length} брендов
            </div>
          )}
        </div>
      )}
      {data && data.length === 0 && (
        <div className="text-center text-slate-500 py-8 text-sm">
          Нет данных по брендам
        </div>
      )}
    </ChartContainer>
  )
}

export const BrandRotationCard = memo(BrandRotationCardComponent)
