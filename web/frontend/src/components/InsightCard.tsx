import { memo } from 'react'
import type { IconComponent } from './icons'

// ─── InsightCard ─────────────────────────────────────────────────────────────
//
// Tinted "finding" tile: icon + title + headline value + explanation, with an
// optional muted subtext line. Used for narrative insights (retention trends,
// revenue opportunities) where a MetricCard's bare number is not enough.
// Visual is owned here; consumers express severity via `tone` only.

type InsightTone = 'green' | 'red' | 'amber' | 'neutral'

interface InsightCardProps {
  /** Leading icon. Sized and coloured by the card itself. */
  icon: IconComponent
  title: string
  value: string
  description: string
  subtext?: string
  tone?: InsightTone
}

const toneStyles: Record<InsightTone, { bg: string; icon: string; value: string }> = {
  green: {
    bg: 'bg-emerald-50 border-emerald-200',
    icon: 'text-emerald-600',
    value: 'text-emerald-700',
  },
  red: {
    bg: 'bg-red-50 border-red-200',
    icon: 'text-red-600',
    value: 'text-red-700',
  },
  amber: {
    bg: 'bg-amber-50 border-amber-200',
    icon: 'text-amber-600',
    value: 'text-amber-700',
  },
  neutral: {
    bg: 'bg-slate-50 border-slate-200',
    icon: 'text-slate-500',
    value: 'text-slate-700',
  },
}

export const InsightCard = memo(function InsightCard({
  icon: Icon,
  title,
  value,
  description,
  subtext,
  tone = 'neutral',
}: InsightCardProps) {
  const styles = toneStyles[tone]
  return (
    <div className={`${styles.bg} border rounded-lg p-3 flex gap-3`}>
      <div className={`${styles.icon} mt-0.5 shrink-0`}>
        <Icon className="w-[18px] h-[18px]" aria-hidden />
      </div>
      <div className="min-w-0">
        <p className="text-xs font-medium text-slate-500">{title}</p>
        <p className={`text-sm font-bold ${styles.value}`}>{value}</p>
        <p className="text-xs text-slate-600 mt-0.5">{description}</p>
        {subtext && (
          <p className="text-xs text-slate-400 mt-0.5">{subtext}</p>
        )}
      </div>
    </div>
  )
})
