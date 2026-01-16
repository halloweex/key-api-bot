interface SkeletonProps {
  className?: string
}

export function Skeleton({ className = '' }: SkeletonProps) {
  return (
    <div
      className={`animate-pulse bg-slate-200 rounded ${className}`}
    />
  )
}

export function SkeletonCard() {
  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
      <Skeleton className="h-4 w-24 mb-2" />
      <Skeleton className="h-8 w-32" />
    </div>
  )
}

export function SkeletonChart() {
  return (
    <div className="bg-white rounded-lg border border-slate-200 shadow-sm p-4">
      <Skeleton className="h-5 w-40 mb-4" />
      <Skeleton className="h-64 w-full" />
    </div>
  )
}
