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

/**
 * The tabs a level opens when nobody has set any — **as the server says**.
 *
 * This used to be derived from the permissions matrix, taking every feature
 * with `view`. That was right while the matrix carried areas. It became "all
 * nine tabs, for every level" the day the matrix became uniform depth, and the
 * admin page then lit nine chips on an inheriting row instead of six — where
 * one click would write an explicit eight-tab set and hand somebody margin,
 * expenses and the SMS roster by touching an unrelated tab.
 *
 * So it is read, not computed. `null` from the server means the level is not
 * narrowed at all, which is every tab.
 */
export function defaultTabsFor(
  defaults: Record<string, TabFeature[] | null> | undefined,
  role: string | undefined,
): readonly TabFeature[] {
  if (!defaults || !role || !(role in defaults)) return []
  const tabs = defaults[role]
  return tabs === null ? TAB_FEATURES : tabs
}
