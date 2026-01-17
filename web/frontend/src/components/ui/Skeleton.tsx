interface SkeletonProps {
  className?: string
}

export function Skeleton({ className = '' }: SkeletonProps) {
  return (
    <div
      className={`animate-pulse bg-slate-200/70 rounded-lg ${className}`}
    />
  )
}

export function SkeletonCard() {
  return (
    <div className="bg-white rounded-xl border border-slate-200/60 shadow-[var(--shadow-card)] p-5">
      <div className="flex items-start gap-4">
        <Skeleton className="h-10 w-10 rounded-lg flex-shrink-0" />
        <div className="flex-1">
          <Skeleton className="h-4 w-24 mb-2" />
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
        <Skeleton className="h-64 w-full rounded-lg" />
      </div>
    </div>
  )
}
