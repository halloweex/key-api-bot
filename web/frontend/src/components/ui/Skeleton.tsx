import { useId } from 'react'

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

/** SVG shimmer gradient defs — reusable across all SVG skeletons */
function SvgShimmerDefs({ id }: { id: string }) {
  return (
    <defs>
      <linearGradient id={id}>
        <stop offset="0%" stopColor="#e2e8f0" />
        <stop offset="50%" stopColor="#f1f5f9" />
        <stop offset="100%" stopColor="#e2e8f0" />
        <animateTransform
          attributeName="gradientTransform"
          type="translate"
          from="-2 0"
          to="2 0"
          dur="1.5s"
          repeatCount="indefinite"
        />
      </linearGradient>
    </defs>
  )
}

/** Horizontal bar chart skeleton (BrandAffinity, CategoryCombos) */
export function SkeletonHorizontalBars() {
  const gid = useId().replace(/:/g, '')
  const barWidths = [210, 175, 145, 120, 95, 70]

  return (
    <svg viewBox="0 0 400 280" className="w-full h-[280px]" preserveAspectRatio="xMidYMid meet">
      <SvgShimmerDefs id={gid} />
      {barWidths.map((w, i) => {
        const y = 18 + i * 43
        return (
          <g key={i}>
            <rect x="12" y={y + 4} width="78" height="12" rx="3" fill={`url(#${gid})`} />
            <rect x="100" y={y} width={w} height="20" rx="4" fill={`url(#${gid})`} />
          </g>
        )
      })}
    </svg>
  )
}

/** Table skeleton (FrequentlyBoughtTogether) */
export function SkeletonTable() {
  const gid = useId().replace(/:/g, '')
  const colX = [16, 160, 320, 410, 500]
  const colW = [120, 120, 60, 50, 50]

  return (
    <svg viewBox="0 0 580 230" className="w-full h-[230px]" preserveAspectRatio="xMidYMid meet">
      <SvgShimmerDefs id={gid} />
      {/* Header */}
      <line x1="0" y1="34" x2="580" y2="34" stroke="#f1f5f9" strokeWidth="1" />
      {colX.map((x, j) => (
        <rect key={`h${j}`} x={x} y="12" width={colW[j] * 0.7} height="10" rx="3" fill={`url(#${gid})`} />
      ))}
      {/* Data rows */}
      {[...Array(5)].map((_, i) => {
        const ry = 46 + i * 38
        return (
          <g key={i}>
            <line x1="0" y1={ry + 30} x2="580" y2={ry + 30} stroke="#f8fafc" strokeWidth="1" />
            {colX.map((x, j) => (
              <rect key={j} x={x} y={ry + 6} width={colW[j]} height="12" rx="3" fill={`url(#${gid})`} />
            ))}
          </g>
        )
      })}
    </svg>
  )
}

/** Momentum two-column skeleton (ProductMomentum) */
export function SkeletonMomentum() {
  const gid = useId().replace(/:/g, '')

  const renderColumn = (offsetX: number) => (
    <g>
      {/* Column title */}
      <rect x={offsetX} y="8" width="60" height="10" rx="3" fill={`url(#${gid})`} />
      {/* 3 list items */}
      {[0, 1, 2].map((i) => {
        const iy = 30 + i * 56
        return (
          <g key={i}>
            <rect x={offsetX} y={iy} width="200" height="44" rx="6" fill="#f8fafc" />
            <rect x={offsetX + 8} y={iy + 8} width="140" height="10" rx="3" fill={`url(#${gid})`} />
            <rect x={offsetX + 8} y={iy + 26} width="90" height="8" rx="3" fill={`url(#${gid})`} />
            <rect x={offsetX + 160} y={iy + 14} width="32" height="14" rx="3" fill={`url(#${gid})`} />
          </g>
        )
      })}
    </g>
  )

  return (
    <svg viewBox="0 0 440 200" className="w-full h-[200px]" preserveAspectRatio="xMidYMid meet">
      <SvgShimmerDefs id={gid} />
      {renderColumn(8)}
      {renderColumn(228)}
    </svg>
  )
}

/** Retention matrix heatmap skeleton */
export function SkeletonRetentionMatrix() {
  const gid = useId().replace(/:/g, '')
  const rows = 6
  const cols = 7 // cohort label + size + M0-M4

  return (
    <svg viewBox="0 0 520 230" className="w-full h-[230px]" preserveAspectRatio="xMidYMid meet">
      <SvgShimmerDefs id={gid} />
      {/* Header row */}
      <rect x="12" y="8" width="60" height="10" rx="3" fill={`url(#${gid})`} />
      <rect x="82" y="8" width="32" height="10" rx="3" fill={`url(#${gid})`} />
      {[...Array(5)].map((_, j) => (
        <rect key={`h${j}`} x={128 + j * 76} y="8" width="28" height="10" rx="3" fill={`url(#${gid})`} />
      ))}
      <line x1="0" y1="26" x2="520" y2="26" stroke="#f1f5f9" strokeWidth="1" />
      {/* Data rows */}
      {[...Array(rows)].map((_, i) => {
        const ry = 34 + i * 32
        return (
          <g key={i}>
            <rect x="12" y={ry + 4} width="54" height="12" rx="3" fill={`url(#${gid})`} />
            <rect x="82" y={ry + 4} width="28" height="12" rx="3" fill={`url(#${gid})`} />
            {[...Array(cols - 2)].map((_, j) => (
              <rect key={j} x={124 + j * 76} y={ry} width="64" height="20" rx="4" fill={`url(#${gid})`} />
            ))}
            <line x1="0" y1={ry + 28} x2="520" y2={ry + 28} stroke="#f8fafc" strokeWidth="1" />
          </g>
        )
      })}
    </svg>
  )
}

/** Vertical bar chart skeleton (BasketDistribution) */
export function SkeletonVerticalBars() {
  const gid = useId().replace(/:/g, '')
  const barHeights = [150, 195, 115, 165, 90, 65]

  return (
    <svg viewBox="0 0 400 280" className="w-full h-[280px]" preserveAspectRatio="xMidYMid meet">
      <SvgShimmerDefs id={gid} />
      {/* Bars */}
      {barHeights.map((h, i) => {
        const x = 28 + i * 60
        return (
          <g key={i}>
            <rect x={x} y={240 - h} width="40" height={h} rx="4" fill={`url(#${gid})`} />
            <rect x={x + 6} y="252" width="28" height="10" rx="3" fill={`url(#${gid})`} />
          </g>
        )
      })}
      {/* Axis line */}
      <line x1="20" y1="242" x2="388" y2="242" stroke="#e2e8f0" strokeWidth="1" />
    </svg>
  )
}
