import { type ReactNode } from 'react'

// ─── PageShell ───────────────────────────────────────────────────────────────
//
// The <main> wrapper for top-level pages. Three variants:
//   feature   — page inside the AppShell sidebar layout (Traffic, Reports,
//               Marketing, Inventory, ProductIntel, Margin). Fills the flex
//               parent, scrolls internally, centred content column with
//               vertical rhythm between sections.
//   dashboard — the main dashboard's tighter rhythm: minimal padding, dense
//               spacing between chart sections.
//   admin     — standalone admin page (no AppShell). Centred narrower max
//               width, generous padding, no internal rhythm.
//
// Page-level layout/visual chrome is owned entirely by this primitive. Pages
// just compose: <PageShell><Heading/><Card/>...</PageShell>. The `feature`
// and `dashboard` variants wrap children in a content column with consistent
// section spacing; consumers don't repeat that wrapper.

type Variant = 'feature' | 'dashboard' | 'admin'

interface PageShellProps {
  variant?: Variant
  children: ReactNode
  ariaLabel?: string
}

const outerClass: Record<Variant, string> = {
  feature: 'flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto',
  dashboard: 'py-1.5 px-1 sm:py-2 sm:px-1.5 lg:py-3 lg:px-2',
  admin: 'p-3 sm:p-6 lg:p-8 max-w-[1400px] mx-auto',
}

const innerClass: Record<Variant, string> = {
  feature: 'max-w-[1800px] mx-auto space-y-4 sm:space-y-6',
  dashboard: 'max-w-[1800px] space-y-1.5 sm:space-y-2',
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
