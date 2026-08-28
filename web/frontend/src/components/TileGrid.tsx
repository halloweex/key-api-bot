import { type ReactNode } from 'react'

// ─── TileGrid ────────────────────────────────────────────────────────────────
//
// The grid for metric tiles: summary cards at the top of a page, KPI rows
// inside a chart panel, insight cards. One gap everywhere — 12px → 16px on
// sm+ — a half-step tighter than the page rhythm, so a row of tiles reads as
// one block while the blocks themselves stay clearly separated. Before this
// existed every summary row picked its own gap (gap-2/gap-3/gap-4 in
// different mixes per tab), which is exactly the inconsistency it removes.
//
// `columns` is the count at desktop; the mobile arrangement is decided here:
//   2 — one column, pairs from md up (wide tiles: insight cards, breakdowns)
//   3 — always three-up (compact KPI triplets)
//   4 — pairs on mobile, four-up from lg
//   5 — pairs on mobile, five-up from lg

type Columns = 2 | 3 | 4 | 5

// `mobile-single-col` (index.css) drops the paired presets to one column on
// ultra-narrow screens (<360px) — a tile cannot halve a 320px viewport.
const columnsClass: Record<Columns, string> = {
  2: 'grid-cols-1 md:grid-cols-2',
  3: 'grid-cols-3',
  4: 'grid-cols-2 lg:grid-cols-4 mobile-single-col',
  5: 'grid-cols-2 lg:grid-cols-5 mobile-single-col',
}

interface TileGridProps {
  children: ReactNode
  columns?: Columns
}

export function TileGrid({ children, columns = 4 }: TileGridProps) {
  return (
    <div className={`grid ${columnsClass[columns]} gap-3 sm:gap-4`}>
      {children}
    </div>
  )
}
