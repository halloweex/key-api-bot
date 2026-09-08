import { TAB_FEATURES, type Permissions, type TabFeature } from '../types/api'

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
  roleFeatures: readonly TabFeature[] = [],
): string {
  // "As the role" on its own answers the wrong question: an admin reading a
  // row wants to know which tabs this person opens, not where the answer comes
  // from. So it names them, and keeps the prefix that says nobody decided it
  // for this person specifically.
  if (value === null) {
    return roleFeatures.length
      ? `${asRole}: ${roleFeatures.map(label).join(', ')}`
      : asRole
  }
  if (value.length === 0) return none
  return value.map(label).join(', ')
}

/** The tabs a role opens, in sidebar order, out of the permissions matrix.
 *
 * Ordered by `TAB_FEATURES` and not by `TAB_PATHS`: the latter lists the eight
 * that are pages, and `expenses` is a grantable tab without one — driving this
 * from it would have hidden a granted tab from the sentence.
 */
export function roleTabs(
  matrix: Record<string, { view: boolean }> | undefined,
): TabFeature[] {
  if (!matrix) return []
  return TAB_FEATURES.filter((feature) => matrix[feature]?.view)
}
