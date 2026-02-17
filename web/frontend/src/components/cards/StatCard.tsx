import { memo } from 'react'
import { AnimatedNumber } from '../ui/AnimatedNumber'

// ─── Types ───────────────────────────────────────────────────────────────────

export type StatCardVariant = 'blue' | 'green' | 'purple' | 'orange' | 'red' | 'cyan'

export interface StatCardProps {
  /** Card label displayed above the value */
  label: string
  /** Numeric value to display */
  value: number
  /** Function to format the value for display */
  formatter: (value: number) => string
  /** Optional icon to display on the left */
  icon?: React.ReactNode
  /** Optional subtitle below the value */
  subtitle?: string
  /** Color variant for the value */
  variant?: StatCardVariant
  /** Optional trend indicator (-1 = down, 0 = neutral, 1 = up) */
  trend?: -1 | 0 | 1
  /** Trend percentage value */
  trendValue?: number
  /** Animation duration in ms */
  animationDuration?: number
  /** Accessible description for screen readers */
  ariaLabel?: string
  /** Show clickable indicator */
  clickable?: boolean
  /** Optional click handler */
  onClick?: () => void
}

// ─── Styling ─────────────────────────────────────────────────────────────────

const variantStyles: Record<StatCardVariant, string> = {
  blue: 'text-blue-600',
  green: 'text-green-600',
  purple: 'text-purple-600',
  orange: 'text-orange-600',
  red: 'text-red-600',
  cyan: 'text-cyan-600',
}

// Card background gradients (pastel)
const cardBgStyles: Record<StatCardVariant, string> = {
  blue: 'bg-gradient-to-br from-blue-100 to-blue-50 border-blue-200',
  green: 'bg-gradient-to-br from-green-100 to-green-50 border-green-200',
  purple: 'bg-gradient-to-br from-purple-100 to-purple-50 border-purple-200',
  orange: 'bg-gradient-to-br from-orange-100 to-orange-50 border-orange-200',
  red: 'bg-gradient-to-br from-red-100 to-red-50 border-red-200',
  cyan: 'bg-gradient-to-br from-cyan-100 to-cyan-50 border-cyan-200',
}

const trendStyles = {
  up: 'text-green-600',
  down: 'text-red-600',
  neutral: 'text-slate-500',
} as const

const TrendIcon = {
  up: (
    <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20" aria-hidden="true">
      <path fillRule="evenodd" d="M5.293 9.707a1 1 0 010-1.414l4-4a1 1 0 011.414 0l4 4a1 1 0 01-1.414 1.414L11 7.414V15a1 1 0 11-2 0V7.414L6.707 9.707a1 1 0 01-1.414 0z" clipRule="evenodd" />
    </svg>
  ),
  down: (
    <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20" aria-hidden="true">
      <path fillRule="evenodd" d="M14.707 10.293a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 111.414-1.414L9 12.586V5a1 1 0 012 0v7.586l2.293-2.293a1 1 0 011.414 0z" clipRule="evenodd" />
    </svg>
  ),
  neutral: (
    <svg className="w-3 h-3" fill="currentColor" viewBox="0 0 20 20" aria-hidden="true">
      <path fillRule="evenodd" d="M5 10a1 1 0 011-1h8a1 1 0 110 2H6a1 1 0 01-1-1z" clipRule="evenodd" />
    </svg>
  ),
} as const

// ─── Icon Background Styles ───────────────────────────────────────────────────

const iconBgStyles: Record<StatCardVariant, string> = {
  blue: 'bg-blue-200/60',
  green: 'bg-green-200/60',
  purple: 'bg-purple-200/60',
  orange: 'bg-orange-200/60',
  red: 'bg-red-200/60',
  cyan: 'bg-cyan-200/60',
}

// ─── Component ───────────────────────────────────────────────────────────────

export const StatCard = memo(function StatCard({
  label,
  value,
  formatter,
  icon,
  subtitle,
  variant = 'blue',
  trend,
  trendValue,
  animationDuration = 500,
  ariaLabel,
  clickable = false,
  onClick,
}: StatCardProps) {
  const trendKey = trend === 1 ? 'up' : trend === -1 ? 'down' : 'neutral'
  const showTrend = trend !== undefined && trend !== 0 && trendValue !== undefined

  return (
    <div
      className={`rounded-xl p-4 border ${cardBgStyles[variant]} transition-all duration-200 hover:shadow-lg hover:-translate-y-0.5 ${clickable ? 'relative cursor-pointer' : ''}`}
      onClick={onClick}
      role={onClick ? 'button' : undefined}
      tabIndex={onClick ? 0 : undefined}
      onKeyDown={onClick ? (e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onClick() } } : undefined}
    >
        {/* Clickable indicator */}
        {clickable && (
          <div className="absolute top-2 right-2 flex items-center gap-1 group/hint cursor-pointer">
            <span className="bg-orange-500 hover:bg-orange-600 text-white text-xs px-2 py-0.5 rounded-full shadow-sm transition-all duration-200 hover:scale-105 animate-pulse hover:animate-none">
              Click me
            </span>
            <svg className="w-4 h-4 text-orange-400 transition-transform duration-200 group-hover/hint:translate-y-0.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
            </svg>
          </div>
        )}
        <article
          aria-label={ariaLabel || `${label}: ${formatter(value)}`}
          className={icon ? "flex items-start gap-3" : "text-center"}
        >
          {/* Icon */}
          {icon && (
            <div className={`p-2 lg:p-3 rounded-lg ${iconBgStyles[variant]} ${variantStyles[variant]} [&_svg]:w-5 [&_svg]:h-5 lg:[&_svg]:w-6 lg:[&_svg]:h-6`}>
              {icon}
            </div>
          )}

          <div className={icon ? "flex-1 min-w-0" : ""}>
            {/* Label */}
            <h3 className="text-xs lg:text-sm text-slate-600 font-medium mb-1">{label}</h3>

            {/* Value with animation */}
            <div className={`flex items-baseline gap-2 ${icon ? "" : "justify-center"} min-w-0`}>
              <AnimatedNumber
                value={value}
                formatter={formatter}
                duration={animationDuration}
                className={`text-lg sm:text-xl lg:text-2xl font-bold tracking-tight truncate ${variantStyles[variant]}`}
              />

              {/* Trend indicator */}
              {showTrend && (
                <span
                  className={`flex items-center gap-0.5 text-xs font-medium ${trendStyles[trendKey]}`}
                  aria-label={`${trendKey === 'up' ? 'Increased' : 'Decreased'} by ${trendValue}%`}
                >
                  {TrendIcon[trendKey]}
                  <span>{Math.abs(trendValue!).toFixed(1)}%</span>
                </span>
              )}
            </div>

            {/* Subtitle */}
            {subtitle && (
              <p className="text-xs text-slate-500 mt-1">{subtitle}</p>
            )}

          </div>
        </article>
    </div>
  )
})

// ─── Skeleton ────────────────────────────────────────────────────────────────

export function StatCardSkeleton() {
  return (
    <div className="rounded-xl p-4 border bg-gradient-to-br from-slate-100 to-slate-50 border-slate-200">
      <div className="animate-pulse text-center">
        <div className="h-3 w-16 bg-slate-200 rounded mx-auto mb-2" />
        <div className="h-7 w-24 bg-slate-200 rounded mx-auto" />
      </div>
    </div>
  )
}
