import { memo, useLayoutEffect, useRef, useState, type RefObject } from 'react'
import { TrendingUp, TrendingDown, Minus, ChevronDown } from 'lucide-react'
import { useAnimatedNumber } from '../hooks/useAnimatedNumber'
import type { IconComponent } from './icons'

// ─── Types ───────────────────────────────────────────────────────────────────

export type StatCardVariant = 'blue' | 'green' | 'purple' | 'orange' | 'red' | 'cyan'

export interface StatCardProps {
  /** Card label displayed above the value */
  label: string
  /** Numeric value to display */
  value: number
  /** Function to format the value for display */
  formatter: (value: number) => string
  /** The value in fewer characters (`formatCurrencyShort`), shown only when
   *  the full figure does not fit the card — the whole history is nine
   *  digits of hryvnia. The full figure stays in the tooltip and the label. */
  shortFormatter?: (value: number) => string
  /** Optional icon to display on the left. Sized by the card itself. */
  icon?: IconComponent
  /** Optional subtitle below the value */
  subtitle?: string
  /** Optional second line under the subtitle, for a figure that should not
   *  wrap into the first one mid-phrase */
  detail?: string
  /** Color variant for the value */
  variant?: StatCardVariant
  /** Optional trend indicator (-1 = down, 0 = neutral, 1 = up) */
  trend?: -1 | 0 | 1
  /** Trend percentage value */
  trendValue?: number
  /** Animation duration in ms */
  animationDuration?: number
  /** Optional extra element rendered after the label (e.g. info tooltip) */
  labelExtra?: React.ReactNode
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
  up: <TrendingUp className="w-3 h-3" aria-hidden="true" />,
  down: <TrendingDown className="w-3 h-3" aria-hidden="true" />,
  neutral: <Minus className="w-3 h-3" aria-hidden="true" />,
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

const VALUE_TEXT = 'text-lg sm:text-xl lg:text-2xl font-bold tracking-tight'

// Whether the full figure fits its row beside the trend. Measured on a hidden
// copy of the final figure, never on the animated one (it grows from the last
// value), and again whenever the card is resized. Where nothing can be
// measured — no ResizeObserver, no layout — it fits.
function useFullFigureFits(
  row: RefObject<HTMLDivElement | null>,
  full: RefObject<HTMLSpanElement | null>,
  trend: RefObject<HTMLSpanElement | null>,
  text: string,
  enabled: boolean,
): boolean {
  const [fits, setFits] = useState(true)
  useLayoutEffect(() => {
    const rowEl = row.current
    const fullEl = full.current
    if (!enabled || !rowEl || !fullEl) return
    const measure = () => {
      const beside = trend.current ? trend.current.offsetWidth + 8 : 0  // gap-2
      setFits(fullEl.offsetWidth + beside <= rowEl.clientWidth)
    }
    measure()
    if (typeof ResizeObserver === 'undefined') return
    const observer = new ResizeObserver(measure)
    observer.observe(rowEl)
    return () => observer.disconnect()
  }, [row, full, trend, text, enabled])
  return !enabled || fits
}

// ─── Component ───────────────────────────────────────────────────────────────

export const StatCard = memo(function StatCard({
  label,
  value,
  formatter,
  shortFormatter,
  icon: Icon,
  subtitle,
  detail,
  variant = 'blue',
  trend,
  trendValue,
  animationDuration = 500,
  labelExtra,
  ariaLabel,
  clickable = false,
  onClick,
}: StatCardProps) {
  const trendKey = trend === 1 ? 'up' : trend === -1 ? 'down' : 'neutral'
  const showTrend = trend !== undefined && trend !== 0 && trendValue !== undefined
  const rowRef = useRef<HTMLDivElement>(null)
  const fullRef = useRef<HTMLSpanElement>(null)
  const trendRef = useRef<HTMLSpanElement>(null)
  const fullText = formatter(value)
  const fits = useFullFigureFits(rowRef, fullRef, trendRef, `${fullText}|${showTrend}`, !!shortFormatter)
  const shortened = !fits && !!shortFormatter
  const animatedDisplay = useAnimatedNumber(
    value, shortened ? shortFormatter : formatter, animationDuration)

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
            <ChevronDown className="w-4 h-4 text-orange-400 transition-transform duration-200 group-hover/hint:translate-y-0.5" />
          </div>
        )}
        <article
          aria-label={ariaLabel || `${label}: ${formatter(value)}`}
          className={Icon ? "flex items-start gap-3" : "text-center"}
        >
          {/* Icon */}
          {Icon && (
            <div className={`p-2 lg:p-3 rounded-lg ${iconBgStyles[variant]} ${variantStyles[variant]}`}>
              <Icon className="w-5 h-5 lg:w-6 lg:h-6" aria-hidden />
            </div>
          )}

          <div className={Icon ? "flex-1 min-w-0" : ""}>
            {/* Label */}
            <h3 className="text-xs lg:text-sm text-slate-600 font-medium mb-1 flex items-center gap-1">{label}{labelExtra}</h3>

            {/* Value with animation */}
            <div
              ref={rowRef}
              className={`relative overflow-hidden flex items-baseline gap-2 ${Icon ? "" : "justify-center"} min-w-0`}
            >
              <span
                aria-live="polite"
                title={shortened ? fullText : undefined}
                className={`${VALUE_TEXT} truncate ${variantStyles[variant]}`}
              >
                {animatedDisplay}
              </span>
              {shortFormatter && (
                <span
                  ref={fullRef}
                  aria-hidden="true"
                  className={`${VALUE_TEXT} invisible absolute left-0 top-0 whitespace-nowrap pointer-events-none`}
                >
                  {fullText}
                </span>
              )}

              {/* Trend indicator */}
              {showTrend && (
                <span
                  ref={trendRef}
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
            {detail && (
              <p className={`text-xs text-slate-500 ${subtitle ? '' : 'mt-1'}`}>{detail}</p>
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
