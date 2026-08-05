import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import type { SmsComparison } from '../types/api'

// ─── SmsLiftInterval ─────────────────────────────────────────────────────────
//
// The one visual that decides whether a campaign worked.
//
// A bare "+4.2pp lift" number invites the reader to believe it. What actually
// matters is whether the 95% interval clears zero: with a 10% holdout the
// control group is small, so a healthy-looking gap is often indistinguishable
// from noise. So the interval is the mark, zero is the axis, and an interval
// touching zero is drawn muted — you cannot read a win off it by accident.

interface SmsLiftIntervalProps {
  comparison: SmsComparison
}

/** Half-width of the plotted range, in percentage points. */
function plotRange(c: SmsComparison): number {
  const reach = Math.max(Math.abs(c.ci95Pp[0]), Math.abs(c.ci95Pp[1]), 1)
  return reach * 1.15
}

/** Map a pp value onto 0–100% of the plot width, with zero at the centre. */
function toPct(value: number, range: number): number {
  return ((value + range) / (2 * range)) * 100
}

export const SmsLiftInterval = memo(function SmsLiftInterval({
  comparison,
}: SmsLiftIntervalProps) {
  const { t } = useTranslation()
  const range = plotRange(comparison)
  const [lo, hi] = comparison.ci95Pp

  const left = toPct(lo, range)
  const width = Math.max(toPct(hi, range) - left, 0.5)
  const point = toPct(comparison.liftPp, range)

  const proven = comparison.significant
  const barClass = proven ? 'bg-emerald-500/25' : 'bg-slate-300/50'
  const edgeClass = proven ? 'bg-emerald-600' : 'bg-slate-400'

  return (
    <div className="space-y-1.5">
      <div className="relative h-8">
        {/* Zero line — the whole point of reference */}
        <div className="absolute inset-y-0 left-1/2 w-px bg-slate-400" aria-hidden="true" />

        {/* Confidence interval */}
        <div
          className={`absolute top-2.5 h-3 rounded-sm ${barClass}`}
          style={{ left: `${left}%`, width: `${width}%` }}
        />
        {/* Interval ends */}
        <div
          className={`absolute top-1.5 h-5 w-0.5 ${edgeClass}`}
          style={{ left: `${left}%` }}
        />
        <div
          className={`absolute top-1.5 h-5 w-0.5 ${edgeClass}`}
          style={{ left: `${toPct(hi, range)}%` }}
        />
        {/* Point estimate */}
        <div
          className={`absolute top-1 h-6 w-1.5 -ml-0.5 rounded-sm ${edgeClass}`}
          style={{ left: `${point}%` }}
        />
      </div>

      <div className="flex items-center justify-between text-[11px] text-slate-500 tabular-nums">
        <span>{lo.toFixed(1)}</span>
        <span className="text-slate-400">{t('sms.zeroNoEffect')}</span>
        <span>+{hi.toFixed(1)}</span>
      </div>
    </div>
  )
})
