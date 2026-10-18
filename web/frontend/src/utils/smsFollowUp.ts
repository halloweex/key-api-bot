/**
 * Which campaigns can be followed up, and the query a follow-up travels as.
 *
 * A follow-up is a second message to the people a sent campaign reached, read
 * on that campaign's control. The server decides who is in it and refuses
 * anything else beside `follow_up_of` — so the wizard sends nothing else, and
 * the preview and the freeze are one query string.
 */
import type { SmsCampaignSummary } from '../types/api'

/** How far back the wizard offers a campaign to follow up. A follow-up is a
 *  same-day or next-days push; anything older is a new campaign. */
export const FOLLOW_UP_WINDOW_DAYS = 7

/** Sent campaigns of their own from the last week, newest first. A follow-up
 *  of a follow-up is refused by the server — both are measured on the root. */
export function followUpCandidates(
  campaigns: SmsCampaignSummary[], now: Date = new Date(),
): SmsCampaignSummary[] {
  const since = now.getTime() - FOLLOW_UP_WINDOW_DAYS * 24 * 60 * 60 * 1000
  return campaigns
    .filter((c) => c.sentAt && !c.followUpOf)
    .filter((c) => {
      const sent = new Date(c.sentAt as string).getTime()
      return !Number.isNaN(sent) && sent >= since
    })
    .sort((a, b) =>
      new Date(b.sentAt as string).getTime() - new Date(a.sentAt as string).getTime())
}

/** The query a follow-up is previewed and frozen with. */
export function followUpParams(
  root: string, extra: { campaign?: string; promocode?: string } = {},
): string {
  const p = new URLSearchParams({ follow_up_of: root })
  if (extra.campaign) p.set('campaign', extra.campaign)
  if (extra.promocode) p.set('promocode', extra.promocode)
  return p.toString()
}
