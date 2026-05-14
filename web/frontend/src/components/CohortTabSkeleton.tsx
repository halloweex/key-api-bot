import { SkeletonCard, SkeletonRetentionMatrix, SkeletonVerticalBars, SkeletonTable } from './Skeleton'
import type { TabId } from './cohortTabsConfig'

export function CohortTabSkeleton({ tab }: { tab: TabId }) {
  if (tab === 'retention' || tab === 'revenue') {
    return (
      <>
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 mb-6">
          {[...Array(4)].map((_, i) => <SkeletonCard key={i} />)}
        </div>
        <SkeletonRetentionMatrix />
      </>
    )
  }
  if (tab === 'timing' || tab === 'ltv') {
    return (
      <>
        <div className="grid grid-cols-3 gap-4 mb-6">
          {[...Array(3)].map((_, i) => <SkeletonCard key={i} />)}
        </div>
        <SkeletonVerticalBars />
      </>
    )
  }
  // at-risk
  return (
    <>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
        {[...Array(4)].map((_, i) => <SkeletonCard key={i} />)}
      </div>
      <SkeletonTable />
    </>
  )
}
