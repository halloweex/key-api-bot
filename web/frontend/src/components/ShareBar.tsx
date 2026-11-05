import { memo } from 'react'
import { formatPercent } from '../utils/formatters'

// ─── Share bar ────────────────────────────────────────────────────────────────
//
// A share of a whole, drawn from the left edge of its track with the share
// written beside it. The bar is anchored: square where it starts, rounded
// where the share ends, so every row starts on one line. That is how the
// funnel's bars are drawn too.
//
// A share above zero is never thinner than 3 px. Inside a track rounded at
// both ends, 0.5 % of a 78 px track was a 0.4 px sliver, which the rounded
// end clipped to a dot at its tip. The dot read as if it had slipped off the
// bar (the owner's report, 2026-11-05). The number beside the bar is the
// value; the bar only shows it at a glance.

export interface ShareBarProps {
  /** Share in percent, 0–100; drawn clamped, written as given. */
  pct: number
  /** The series' own colour, the one its dot carries. */
  color: string
}

export const ShareBar = memo(function ShareBar({ pct, color }: ShareBarProps) {
  const drawn = Math.min(100, Math.max(0, pct))
  return (
    <div className="flex items-center gap-2 min-w-[120px]">
      <div className="relative h-2 flex-1 overflow-hidden rounded-r bg-slate-100" aria-hidden="true">
        {drawn > 0 && (
          <div
            className="absolute inset-y-0 left-0 min-w-[3px] rounded-r"
            style={{ width: `${drawn}%`, backgroundColor: color }}
          />
        )}
      </div>
      <span className="w-12 text-right tabular-nums text-slate-600">{formatPercent(pct)}</span>
    </div>
  )
})
