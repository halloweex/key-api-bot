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
  blue: 'text-blue-400',
  green: 'text-green-400',
  purple: 'text-purple-400',
  orange: 'text-orange-400',
  red: 'text-red-400',
  cyan: 'text-cyan-400',
}

const trendStyles = {
  up: 'text-green-400',
  down: 'text-red-400',
  neutral: 'text-slate-400',
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
    <Card>
      <CardContent className="py-3">
        <article
          aria-label={ariaLabel || `${label}: ${formatter(value)}`}
          className="flex flex-col"
        >
          {/* Header with label and optional icon */}
          <div className="flex items-center justify-between mb-1">
            <h3 className="text-sm text-slate-400 font-medium">{label}</h3>
            {icon && (
              <span className={`${variantStyles[variant]} opacity-60`} aria-hidden="true">
                {icon}
              </span>
            )}
          </div>

          {/* Value with animation */}
          <div className="flex items-baseline gap-2">
            <AnimatedNumber
              value={value}
              formatter={formatter}
              duration={animationDuration}
              className={`text-2xl font-bold ${variantStyles[variant]}`}
            />

            {/* Trend indicator */}
            {showTrend && (
              <span
                className={`flex items-center gap-0.5 text-xs ${trendStyles[trendKey]}`}
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
        </article>
      </CardContent>
    </Card>
  )
})

// ─── Skeleton ────────────────────────────────────────────────────────────────

export function StatCardSkeleton() {
  return (
    <Card>
      <CardContent className="py-3">
        <div className="animate-pulse">
          <div className="h-4 w-20 bg-slate-700 rounded mb-2" />
          <div className="h-8 w-28 bg-slate-700 rounded" />
        </div>
      </CardContent>
    </Card>
  )
}
