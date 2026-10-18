/**
 * A name the wizard offers for a new campaign, so the field opens filled in.
 *
 * The name is also the campaign's key: freezing a second roster under a name
 * already taken is refused, and the control group is drawn from (buyer, name).
 * So the suggestion is never one the list already holds — compared without
 * case, since two names that differ only in case read as the same campaign to
 * whoever opens the results.
 *
 * A new campaign is named after the day it is built (`sms-2026-10-17`), which
 * sorts and needs no thought; a follow-up after the campaign it follows
 * (`aug-summer-sale-push`), which is the one thing worth saying about it.
 * Either way it is a suggestion: the field stays editable.
 */

/** The longest name the server accepts (`_CAMPAIGN_PATTERN`). */
export const CAMPAIGN_NAME_MAX = 40

function pad(n: number): string {
  return String(n).padStart(2, '0')
}

/** The day in the reader's own calendar — the day they are building it on. */
function dayStamp(now: Date): string {
  return `${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}`
}

/** `base`, or `base-2`, `base-3`… — the first one `taken` does not hold.
 *  The base is cut, never the suffix, so every candidate fits the limit. */
function firstFree(base: string, taken: Set<string>): string {
  for (let n = 1; ; n++) {
    const suffix = n === 1 ? '' : `-${n}`
    const name = base.slice(0, CAMPAIGN_NAME_MAX - suffix.length).replace(/[-_]+$/, '') + suffix
    if (!taken.has(name.toLowerCase())) return name
  }
}

export function suggestCampaignName(
  existing: Iterable<string>,
  opts: { now?: Date; followUpOf?: string | null } = {},
): string {
  const taken = new Set(Array.from(existing, (name) => name.toLowerCase()))
  const base = opts.followUpOf
    ? `${opts.followUpOf}-push`
    : `sms-${dayStamp(opts.now ?? new Date())}`
  return firstFree(base, taken)
}
