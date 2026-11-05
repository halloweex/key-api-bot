import { memo, type MouseEvent } from 'react'
import { navigate } from '../hooks/useRouter'

// ─── SectionNav ──────────────────────────────────────────────────────────────
//
// The sections of one page, each a path of its own. Links, not tabs: a
// section is somewhere a link can send somebody, and the header's filters
// follow the path (`utils/pageFilters`). So this is `<nav>` with
// `aria-current`, where `TabBar` is a tablist switching state in place.
//
// Wide screens get the segmented look in a row; a phone gets the same
// segments as equal columns with their short labels, so every section stays
// on screen without scrolling the bar.

export interface SectionNavItem {
  href: string
  label: string
  /** What a phone shows when `label` would not fit a third of the width. */
  shortLabel?: string
}

interface SectionNavProps {
  items: readonly SectionNavItem[]
  /** The href of the section on screen. */
  active: string
  ariaLabel: string
}

export const SectionNav = memo(function SectionNav({ items, active, ariaLabel }: SectionNavProps) {
  const follow = (href: string) => (e: MouseEvent<HTMLAnchorElement>) => {
    // A new tab or window is the browser's to open.
    if (e.button !== 0 || e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return
    e.preventDefault()
    navigate(href)
  }

  return (
    <nav aria-label={ariaLabel}>
      <ul
        className="grid gap-1 rounded-xl bg-slate-100 p-1 sm:inline-flex"
        style={{ gridTemplateColumns: `repeat(${items.length}, minmax(0, 1fr))` }}
      >
        {items.map((item) => {
          const current = item.href === active
          return (
            <li key={item.href} className="flex">
              <a
                href={item.href}
                onClick={follow(item.href)}
                aria-current={current ? 'page' : undefined}
                className={`flex min-h-[44px] w-full items-center justify-center rounded-lg px-3 text-sm
                            transition-colors sm:min-h-0 sm:px-4 sm:py-2 sm:whitespace-nowrap ${
                  current
                    ? 'bg-white font-semibold text-slate-900 shadow-sm'
                    : 'font-medium text-slate-600 hover:text-slate-900'
                }`}
              >
                <span className="sm:hidden">{item.shortLabel ?? item.label}</span>
                <span className="hidden sm:inline">{item.label}</span>
              </a>
            </li>
          )
        })}
      </ul>
    </nav>
  )
})
