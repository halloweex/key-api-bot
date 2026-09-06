/**
 * The campaign being built, kept for the length of the session.
 *
 * Closing the wizard used to throw the audience away — every filter, the name,
 * the text — so a glance at the campaign list below cost the whole thing. An
 * audience is ten decisions; losing it to a misclick is the kind of thing
 * people stop trusting a page over.
 *
 * sessionStorage, not localStorage: a draft should survive a closed wizard and
 * a reloaded page, not reappear next week as a half-built campaign nobody
 * remembers starting. It is cleared the moment the roster is frozen, because
 * from then on the campaign exists on the server and the draft would be a
 * stale copy of it.
 */
import { audienceFromPreset, emptyAudience } from './smsAudience'
import type { SmsAudienceCriteria } from '../types/api'

const KEY = 'sms.campaignDraft.v1'

export interface SmsCampaignDraft {
  audience: SmsAudienceCriteria
  campaign: string
  promocode: string
  text: string
  presetName: string
}

export function emptyDraft(): SmsCampaignDraft {
  return {
    audience: emptyAudience(),
    campaign: '',
    promocode: '',
    text: '',
    presetName: '',
  }
}

export function loadDraft(): SmsCampaignDraft {
  const blank = emptyDraft()
  try {
    const raw = sessionStorage.getItem(KEY)
    if (!raw) return blank
    const stored = JSON.parse(raw) as Partial<SmsCampaignDraft>
    return {
      // Through the same forgiving reader a preset goes through: a draft
      // written by an older version of this page must still open.
      audience: audienceFromPreset(stored.audience),
      campaign: typeof stored.campaign === 'string' ? stored.campaign : '',
      promocode: typeof stored.promocode === 'string' ? stored.promocode : '',
      text: typeof stored.text === 'string' ? stored.text : '',
      presetName: typeof stored.presetName === 'string' ? stored.presetName : '',
    }
  } catch {
    // A corrupt or unavailable store must cost a draft, never the page.
    return blank
  }
}

export function saveDraft(draft: SmsCampaignDraft): void {
  try {
    sessionStorage.setItem(KEY, JSON.stringify(draft))
  } catch {
    /* Private mode, quota, whatever — the wizard still works without it. */
  }
}

export function clearDraft(): void {
  try {
    sessionStorage.removeItem(KEY)
  } catch {
    /* as above */
  }
}
