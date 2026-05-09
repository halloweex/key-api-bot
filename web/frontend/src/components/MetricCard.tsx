import { memo, type ReactNode } from 'react'

// ─── MetricCard ──────────────────────────────────────────────────────────────
//
// Compact "label + value" tile used to summarise a single metric. Surfaces
// cover every tile pattern in the design system; tone selects the value
// (and, on tinted surfaces, the background) colour. There are no
// className/style escapes — every visual lives inside.
//
//   surface="card"        — primary white card with shadow (top-level summaries)
//   surface="tile"        — soft grey tile (secondary summaries inside a card)
//   surface="tile-dark"   — translucent dark tile (metrics over a darker chart)
//   surface="tile-tinted" — flat colour-tinted tile, bg shaded by tone
//
//   tone="neutral"        — default text colour for the surface
//   tone="green|red|blue|purple|orange|indigo|cyan" — semantic accent
//
// Optional `icon` renders a tone-coloured tile on the left and switches the
// internal layout to icon+text row.

type MetricSurface = 'card' | 'tile' | 'tile-dark' | 'tile-tinted' | 'tile-gradient'
type MetricTone =
  | 'neutral'
  | 'green'
  | 'red'
  | 'blue'
  | 'purple'
  | 'orange'
  | 'indigo'
  | 'cyan'
  | 'rose'
  | 'teal'

interface MetricCardProps {
  label: string
  value: string
  surface?: MetricSurface
  tone?: MetricTone
  /** Optional secondary line beneath the value (small, muted). */
  sub?: string
  /** Optional icon shown in a tone-coloured tile to the left of label/value. */
  icon?: ReactNode
}

const surfaceClass: Record<MetricSurface, string> = {
  card: 'bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] p-4',
  tile: 'bg-slate-50 rounded-lg p-3 sm:p-4 border border-slate-100',
  'tile-dark': 'bg-slate-700/50 rounded-xl border border-slate-600/40 shadow-[var(--shadow-card)] py-2 px-3',
  'tile-tinted': 'rounded-lg p-3',
  'tile-gradient': 'rounded-xl p-4 border',
}

const labelClass: Record<MetricSurface, string> = {
  card: 'text-xs text-slate-500 font-medium mb-1',
  tile: 'text-xs sm:text-sm text-slate-500 mb-1',
  'tile-dark': 'text-xs text-slate-300 font-medium',
  'tile-tinted': 'text-xs text-slate-600 font-medium',
  'tile-gradient': 'text-xs text-slate-600 font-medium',
}

const valueToneLight: Record<MetricTone, string> = {
  neutral: 'text-slate-900',
  green: 'text-green-600',
  red: 'text-red-600',
  blue: 'text-blue-600',
  purple: 'text-purple-600',
  orange: 'text-orange-600',
  indigo: 'text-indigo-600',
  cyan: 'text-cyan-600',
  rose: 'text-rose-600',
  teal: 'text-teal-600',
}

const valueToneDark: Record<MetricTone, string> = {
  neutral: 'text-slate-100',
  green: 'text-green-400',
  red: 'text-red-400',
  blue: 'text-blue-400',
  purple: 'text-purple-400',
  orange: 'text-orange-400',
  indigo: 'text-indigo-400',
  cyan: 'text-cyan-400',
  rose: 'text-rose-400',
  teal: 'text-teal-400',
}

const tintedBgClass: Record<MetricTone, string> = {
  neutral: 'bg-slate-50',
  green: 'bg-emerald-50',
  red: 'bg-red-50',
  blue: 'bg-blue-50',
  purple: 'bg-purple-50',
  orange: 'bg-amber-50',
  indigo: 'bg-indigo-50',
  cyan: 'bg-cyan-50',
  rose: 'bg-rose-50',
  teal: 'bg-teal-50',
}

const valueToneTinted: Record<MetricTone, string> = {
  neutral: 'text-slate-700',
  green: 'text-emerald-600',
  red: 'text-red-600',
  blue: 'text-blue-600',
  purple: 'text-purple-700',
  orange: 'text-amber-600',
  indigo: 'text-indigo-600',
  cyan: 'text-cyan-600',
  rose: 'text-rose-600',
  teal: 'text-teal-600',
}

const iconBgClass: Record<MetricTone, string> = {
  neutral: 'bg-slate-200/60',
  green: 'bg-emerald-200/60',
  red: 'bg-red-200/60',
  blue: 'bg-blue-200/60',
  purple: 'bg-purple-200/60',
  orange: 'bg-amber-200/60',
  indigo: 'bg-indigo-200/60',
  cyan: 'bg-cyan-200/60',
  rose: 'bg-rose-200/60',
  teal: 'bg-teal-200/60',
}

// Gradient surface — tone-coloured gradient + matching border
const gradientBgClass: Record<MetricTone, string> = {
  neutral: 'bg-gradient-to-br from-slate-100 to-slate-50 border-slate-200',
  green: 'bg-gradient-to-br from-green-100 to-green-50 border-green-200',
  red: 'bg-gradient-to-br from-red-100 to-red-50 border-red-200',
  blue: 'bg-gradient-to-br from-blue-100 to-blue-50 border-blue-200',
  purple: 'bg-gradient-to-br from-purple-100 to-purple-50 border-purple-200',
  orange: 'bg-gradient-to-br from-orange-100 to-orange-50 border-orange-200',
  indigo: 'bg-gradient-to-br from-indigo-100 to-indigo-50 border-indigo-200',
  cyan: 'bg-gradient-to-br from-cyan-100 to-cyan-50 border-cyan-200',
  rose: 'bg-gradient-to-br from-rose-100 to-rose-50 border-rose-200',
  teal: 'bg-gradient-to-br from-teal-100 to-teal-50 border-teal-200',
}

export const MetricCard = memo(function MetricCard({
  label,
  value,
  surface = 'card',
  tone = 'neutral',
  sub,
  icon,
}: MetricCardProps) {
  const valueClass =
    surface === 'tile-dark' ? valueToneDark[tone]
    : surface === 'tile-tinted' ? valueToneTinted[tone]
    : valueToneLight[tone]

  const containerClass =
    surface === 'tile-tinted'
      ? `${surfaceClass[surface]} ${tintedBgClass[tone]} ${icon ? '' : 'text-center'}`
      : surface === 'tile-gradient'
      ? `${surfaceClass[surface]} ${gradientBgClass[tone]}`
      : surfaceClass[surface]

  if (icon) {
    return (
      <div className={containerClass}>
        <div className="flex items-start gap-3">
          <div className={`p-2 rounded-lg ${iconBgClass[tone]} ${valueClass} [&_svg]:w-5 [&_svg]:h-5 text-lg flex-shrink-0`}>
            {icon}
          </div>
          <div className="flex-1 min-w-0">
            <p className={labelClass[surface]}>{label}</p>
            <p className={`text-xl font-bold tracking-tight truncate ${valueClass}`}>{value}</p>
            {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className={containerClass}>
      <p className={labelClass[surface]}>{label}</p>
      <p className={`text-xl font-bold tracking-tight ${valueClass}`}>{value}</p>
      {sub && <p className="text-xs text-slate-400 mt-0.5">{sub}</p>}
    </div>
  )
})
