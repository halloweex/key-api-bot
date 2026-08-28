import { type ReactNode } from 'react'

// ─── ChartGrid ───────────────────────────────────────────────────────────────
//
// Side-by-side chart panels: one column on mobile, two from lg up. The gap is
// the page rhythm (16px → 24px on sm+) — the same value PageShell keeps
// between stacked sections, so a grid row reads as part of the page, not as a
// tighter cluster. There is deliberately no density knob: every tab spaces
// its panels identically.

interface ChartGridProps {
  children: ReactNode
}

export function ChartGrid({ children }: ChartGridProps) {
  return (
    <section className="grid grid-cols-1 lg:grid-cols-2 gap-4 sm:gap-6">
      {children}
    </section>
  )
}
