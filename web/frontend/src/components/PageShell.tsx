import { type ReactNode } from 'react'

// ─── PageShell ───────────────────────────────────────────────────────────────
//
// The <main> wrapper for top-level pages. Two variants:
//   feature  — page inside the AppShell sidebar layout (Traffic, Reports,
//              Marketing, Inventory, ProductIntel). Fills the flex parent,
//              scrolls internally, edge-to-edge padding scaling with width.
//   admin    — standalone admin page (no AppShell). Centered, narrower max
//              width, generous padding.
//
// Page-level layout/visual chrome is owned entirely by this primitive. Pages
// just compose: <PageShell><Heading/><Card/>...</PageShell>.

type Variant = 'feature' | 'admin'

interface PageShellProps {
  variant?: Variant
  children: ReactNode
  ariaLabel?: string
}

const variantClass: Record<Variant, string> = {
  feature: 'flex-1 py-3 px-1 sm:py-4 sm:px-1.5 lg:py-6 lg:px-2 overflow-auto',
  admin: 'p-3 sm:p-6 lg:p-8 max-w-[1400px] mx-auto',
}

export function PageShell({ variant = 'feature', children, ariaLabel }: PageShellProps) {
  return (
    <main className={variantClass[variant]} aria-label={ariaLabel}>
      {children}
    </main>
  )
}
