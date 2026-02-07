interface SkeletonProps {
  className?: string
  /** Use pulse animation instead of shimmer */
  pulse?: boolean
  /** Rounded corners variant */
  rounded?: 'sm' | 'md' | 'lg' | 'full'
  /** Optional inline styles */
  style?: React.CSSProperties
}

const roundedStyles = {
  sm: 'rounded',
  md: 'rounded-lg',
  lg: 'rounded-xl',
  full: 'rounded-full',
}

export function Skeleton({ className = '', pulse = false, rounded = 'md', style }: SkeletonProps) {
  return (
    <div
      className={`
        ${pulse ? 'animate-skeleton bg-slate-200' : 'animate-shimmer'}
        ${roundedStyles[rounded]}
        ${className}
      `}
      style={style}
    />
  )
}

export function SkeletonCard() {
  return (
    <div className="bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] p-5 animate-stagger">
      <div className="flex items-start gap-4">
        <Skeleton className="h-10 w-10 flex-shrink-0" rounded="lg" />
        <div className="flex-1 space-y-2">
          <Skeleton className="h-4 w-24" />
          <Skeleton className="h-7 w-32" />
        </div>
      </div>
    </div>
  )
}

export function SkeletonChart() {
  return (
    <div className="bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)]">
      <div className="px-5 py-4 border-b border-slate-100">
        <Skeleton className="h-5 w-40" />
      </div>
      <div className="p-5">
        <div className="space-y-3 animate-stagger">
          {/* Chart bars skeleton */}
          <div className="flex items-end gap-2 h-48">
            {[...Array(7)].map((_, i) => (
              <Skeleton
                key={i}
                className="flex-1"
                style={{ height: `${30 + Math.random() * 60}%` }}
                rounded="sm"
              />
            ))}
          </div>
          {/* X-axis labels */}
          <div className="flex justify-between pt-2">
            {[...Array(7)].map((_, i) => (
              <Skeleton key={i} className="h-3 w-8" rounded="sm" />
            ))}
          </div>
        </div>
      </div>
    </div>
  )
}

export function SkeletonText({ lines = 3, className = '' }: { lines?: number; className?: string }) {
  return (
    <div className={`space-y-2 animate-stagger ${className}`}>
      {[...Array(lines)].map((_, i) => (
        <Skeleton
          key={i}
          className={`h-4 ${i === lines - 1 ? 'w-3/4' : 'w-full'}`}
          rounded="sm"
        />
      ))}
    </div>
  )
}

export function SkeletonAvatar({ size = 'md' }: { size?: 'sm' | 'md' | 'lg' }) {
  const sizeClasses = {
    sm: 'h-8 w-8',
    md: 'h-10 w-10',
    lg: 'h-12 w-12',
  }

  return <Skeleton className={sizeClasses[size]} rounded="full" />
}

export function SkeletonButton({ size = 'md' }: { size?: 'sm' | 'md' | 'lg' }) {
  const sizeClasses = {
    sm: 'h-8 w-20',
    md: 'h-10 w-24',
    lg: 'h-12 w-32',
  }

  return <Skeleton className={sizeClasses[size]} rounded="lg" />
}
