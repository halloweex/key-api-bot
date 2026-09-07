import type { Permissions, TabFeature } from '../types/api'

// ─── Tab access ──────────────────────────────────────────────────────────────
//
// The two pure answers the access UI needs, kept out of the components that
// ask them: one for the admin reading a row, one for the router deciding where
// to send somebody who cannot open the page they asked for.

/** Which page each tab lives at, in the sidebar's order. */
export const TAB_PATHS: ReadonlyArray<{ feature: keyof Permissions; path: string }> = [
  { feature: 'dashboard', path: '/' },
  { feature: 'products', path: '/products' },
  { feature: 'traffic', path: '/traffic' },
  { feature: 'inventory', path: '/inventory' },
  { feature: 'reports', path: '/reports' },
  { feature: 'marketing', path: '/marketing' },
  { feature: 'margin', path: '/margin' },
  { feature: 'sms', path: '/sms' },
]

/** The first page these permissions open, or null when they open none. */
export function firstAllowedPath(permissions: Permissions | null): string | null {
  if (!permissions) return null
  const hit = TAB_PATHS.find(({ feature }) => permissions[feature]?.view)
  return hit ? hit.path : null
}

/**
 * One person's tab set as a sentence.
 *
 * Three states, and the middle one is the one that gets lost if this is
 * written inline: `null` is "as the role", `[]` is "none of them" — a real
 * choice that locks the account out of every page — and anything else is the
 * list itself.
 */
export function tabSummary(
  value: TabFeature[] | null,
  label: (tab: TabFeature) => string,
  asRole: string,
  none: string,
): string {
  if (value === null) return asRole
  if (value.length === 0) return none
  return value.map(label).join(', ')
}
