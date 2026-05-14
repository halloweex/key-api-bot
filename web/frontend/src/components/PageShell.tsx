import { type ReactNode } from 'react'

// ─── PageShell ───────────────────────────────────────────────────────────────
//
// The <main> wrapper for top-level pages. Two variants:
//   feature  — page inside the AppShell sidebar layout (Traffic, Reports,
//              Marketing, Inventory, ProductIntel, Margin). Fills the flex
//              parent, scrolls internally, centred content column with
//              vertical rhythm between sections.
//   admin    — standalone admin page (no AppShell). Centred narrower max
//              width, generous padding, no internal rhythm.
//
// Page-level layout/visual chrome is owned entirely by this primitive. Pages
// just compose: <PageShell><Heading/><Card/>...</PageShell>. The `feature`
// variant wraps children in a centred content column with consistent
// section spacing; consumers don't repeat that wrapper.

type Variant = 'feature' | 'admin'

interface PageShellProps {
  variant?: Variant
  children: ReactNode
  ariaLabel?: string
}

const outerClass: Record<Variant, string> = {
  feature: 'flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto',
  admin: 'p-3 sm:p-6 lg:p-8 max-w-[1400px] mx-auto',
}

const innerClass: Record<Variant, string> = {
  feature: 'max-w-[1800px] mx-auto space-y-4 sm:space-y-6',
  admin: '',
}

export function PageShell({ variant = 'feature', children, ariaLabel }: PageShellProps) {
  const inner = innerClass[variant]
  return (
    <main className={outerClass[variant]} aria-label={ariaLabel}>
      {inner ? <div className={inner}>{children}</div> : children}
    </main>
  )
}
