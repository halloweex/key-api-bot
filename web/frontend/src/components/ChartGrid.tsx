import { type ReactNode } from 'react'

// ─── ChartGrid ───────────────────────────────────────────────────────────────
//
// Side-by-side chart panels: one column on mobile, two from lg up. Pure
// layout, so density is the only knob — it must match the vertical rhythm of
// the page the grid sits in:
//
//   comfortable — PageShell "feature" pages (gap-4 sm:gap-6)
//   dense       — the dashboard's tight rhythm (gap-1.5 sm:gap-2)

type Density = 'comfortable' | 'dense'

const gapClass: Record<Density, string> = {
  comfortable: 'gap-4 sm:gap-6',
  dense: 'gap-1.5 sm:gap-2',
}

interface ChartGridProps {
  children: ReactNode
  density?: Density
}

export function ChartGrid({ children, density = 'comfortable' }: ChartGridProps) {
  return (
    <section className={`grid grid-cols-1 lg:grid-cols-2 ${gapClass[density]}`}>
      {children}
    </section>
  )
}
