import { memo, type ReactNode } from 'react'
import { Card, CardContent } from '../ui'
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
  /** Optional subtitle below the value */
  subtitle?: string
  /** Color variant for the value */
  variant?: StatCardVariant
  /** Optional icon to display */
  icon?: ReactNode
  /** Optional trend indicator (-1 = down, 0 = neutral, 1 = up) */
  trend?: -1 | 0 | 1
  /** Trend percentage value */
  trendValue?: number
  /** Animation duration in ms */
  animationDuration?: number
  /** Accessible description for screen readers */
  ariaLabel?: string
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

// Gradient backgrounds for icon containers
const iconBgStyles: Record<StatCardVariant, string> = {
  blue: 'bg-gradient-to-br from-blue-500 to-blue-600 text-white',
  green: 'bg-gradient-to-br from-green-500 to-green-600 text-white',
  purple: 'bg-gradient-to-br from-purple-500 to-purple-600 text-white',
  orange: 'bg-gradient-to-br from-orange-400 to-orange-500 text-white',
  red: 'bg-gradient-to-br from-red-500 to-red-600 text-white',
  cyan: 'bg-gradient-to-br from-cyan-500 to-cyan-600 text-white',
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

// ─── Component ───────────────────────────────────────────────────────────────

export const StatCard = memo(function StatCard({
  label,
  value,
  formatter,
  subtitle,
  variant = 'blue',
  icon,
  trend,
  trendValue,
  animationDuration = 500,
  ariaLabel,
}: StatCardProps) {
  const trendKey = trend === 1 ? 'up' : trend === -1 ? 'down' : 'neutral'
  const showTrend = trend !== undefined && trend !== 0 && trendValue !== undefined

  return (
    <Card interactive className="group">
      <CardContent className="py-4">
        <article
          aria-label={ariaLabel || `${label}: ${formatter(value)}`}
          className={icon ? "flex items-start gap-4" : "text-center"}
        >
          {/* Icon with gradient background */}
          {icon && (
            <div
              className={`
                flex-shrink-0 w-10 h-10 rounded-lg flex items-center justify-center
                ${iconBgStyles[variant]}
                shadow-sm group-hover:shadow-md transition-shadow duration-200
              `}
              aria-hidden="true"
            >
              {icon}
            </div>
          )}

          {/* Content */}
          <div className={icon ? "flex-1 min-w-0" : ""}>
            {/* Label */}
            <h3 className="text-xs sm:text-sm text-slate-500 font-medium mb-0.5">{label}</h3>

            {/* Value with animation */}
            <div className={icon ? "flex items-baseline gap-2" : "flex items-baseline justify-center gap-2"}>
              <AnimatedNumber
                value={value}
                formatter={formatter}
                duration={animationDuration}
                className={`text-xl sm:text-2xl font-bold tracking-tight ${variantStyles[variant]}`}
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
              <p className="text-[10px] sm:text-xs text-slate-400 mt-1">{subtitle}</p>
            )}
          </div>
        </article>
      </CardContent>
    </Card>
  )
})

// ─── Skeleton ────────────────────────────────────────────────────────────────

export function StatCardSkeleton() {
  return (
    <Card>
      <CardContent className="py-4">
        <div className="animate-pulse flex items-start gap-4">
          <div className="w-10 h-10 bg-slate-200 rounded-lg flex-shrink-0" />
          <div className="flex-1">
            <div className="h-4 w-20 bg-slate-200 rounded mb-2" />
            <div className="h-7 w-28 bg-slate-200 rounded" />
          </div>
        </div>
      </CardContent>
    </Card>
  )
}
