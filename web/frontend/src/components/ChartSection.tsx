import { Suspense, type ReactNode } from 'react'
import { SkeletonChart } from './Skeleton'

// ─── ChartSection ────────────────────────────────────────────────────────────
//
// One page section holding a (usually lazy) chart panel: renders the chart
// skeleton until the code and data arrive. Every dashboard-style page wraps
// its chart blocks in this instead of hand-rolling <section> + <Suspense>.

export function ChartSection({ children }: { children: ReactNode }) {
  return (
    <section>
      <Suspense fallback={<SkeletonChart />}>{children}</Suspense>
    </section>
  )
}
