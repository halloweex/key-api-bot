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
 *
 * v2 holds the audience as conditions (`smsConditions.ts`), in the same shape
 * a preset stores — the server's terms beside the conditions — so dates stay
 * dates across a reload. A v1 draft, written before conditions, is read once
 * through the same mapping a preset from then goes through.
 */
import {
  definitionFromPreset, definitionToPreset, emptyDefinition,
  type AudienceDefinition,
} from './smsConditions'

const KEY = 'sms.campaignDraft.v2'
const LEGACY_KEY = 'sms.campaignDraft.v1'

export interface SmsCampaignDraft {
  audience: AudienceDefinition
  campaign: string
  promocode: string
  text: string
  presetName: string
  /** Building a follow-up rather than a new audience. Kept with the draft so
   *  a reload cannot quietly turn a follow-up back into a fresh campaign. */
  followUp: boolean
  /** The campaign being followed up; '' until one is picked. */
  followUpOf: string
}

export function emptyDraft(): SmsCampaignDraft {
  return {
    audience: emptyDefinition(),
    campaign: '',
    promocode: '',
    text: '',
    presetName: '',
    followUp: false,
    followUpOf: '',
  }
}

function read(key: string): Partial<Record<keyof SmsCampaignDraft, unknown>> | null {
  const raw = sessionStorage.getItem(key)
  if (!raw) return null
  const parsed = JSON.parse(raw)
  return parsed && typeof parsed === 'object' ? parsed : null
}

export function loadDraft(): SmsCampaignDraft {
  const blank = emptyDraft()
  try {
    const stored = read(KEY) ?? read(LEGACY_KEY)
    if (!stored) return blank
    const str = (v: unknown) => (typeof v === 'string' ? v : '')
    return {
      // Through the same forgiving reader a preset goes through: a draft
      // written by an older version of this page must still open, and a v1
      // draft's audience is the server shape a v1 preset had.
      audience: definitionFromPreset(stored.audience),
      campaign: str(stored.campaign),
      promocode: str(stored.promocode),
      text: str(stored.text),
      presetName: str(stored.presetName),
      followUp: stored.followUp === true,
      followUpOf: str(stored.followUpOf),
    }
  } catch {
    // A corrupt or unavailable store must cost a draft, never the page.
    return blank
  }
}

export function saveDraft(draft: SmsCampaignDraft): void {
  try {
    sessionStorage.setItem(KEY, JSON.stringify({
      ...draft, audience: definitionToPreset(draft.audience),
    }))
    // Once a v2 draft exists the v1 one is history; leaving it would bring
    // the old audience back the moment the v2 one was cleared.
    sessionStorage.removeItem(LEGACY_KEY)
  } catch {
    /* Private mode, quota, whatever — the wizard still works without it. */
  }
}

export function clearDraft(): void {
  try {
    sessionStorage.removeItem(KEY)
    sessionStorage.removeItem(LEGACY_KEY)
  } catch {
    /* as above */
  }
}
