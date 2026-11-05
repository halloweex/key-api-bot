/**
 * The /traffic tab's sections, each a path of its own.
 *
 * The tab held nine blocks on one page, and on 2026-11-04 the owner found the
 * site funnel lost among them and the header's filters unexplained: source and
 * sales type change nothing in a funnel read from Google Analytics. So the
 * tab is three sections, one question each — where the site loses buyers
 * (Google Analytics), which channels bring orders (CRM), which campaigns and
 * whether the ads pay (CRM and typed-in spend).
 *
 * A section is a path, not a state, for two reasons. A link opens it. And the
 * header shows only the filters that section's blocks read
 * (`utils/pageFilters`), which is decided by the path.
 */

export type TrafficTab = 'funnel' | 'sales' | 'campaigns'

export const TRAFFIC_TABS: ReadonlyArray<{ tab: TrafficTab; path: string }> = [
  { tab: 'funnel', path: '/traffic' },
  { tab: 'sales', path: '/traffic/sales' },
  { tab: 'campaigns', path: '/traffic/campaigns' },
]

/** The section a path opens — `/v2/…` and a trailing slash included — or null
 *  when the path is not one of them. */
export function trafficTabFor(path: string): TrafficTab | null {
  const bare = path.startsWith('/v2/') ? path.slice(3) : path
  const trimmed = bare.length > 1 ? bare.replace(/\/+$/, '') : bare
  return TRAFFIC_TABS.find((t) => t.path === trimmed)?.tab ?? null
}

/** The path a section lives at. */
export function trafficPath(tab: TrafficTab): string {
  return TRAFFIC_TABS.find((t) => t.tab === tab)?.path ?? '/traffic'
}
