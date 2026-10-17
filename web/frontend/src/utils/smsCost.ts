// ─── smsCost ─────────────────────────────────────────────────────────────────
//
// What a message will be billed as, computed while it is being typed.
//
// A mirror of `count_segments` in core/turbosms.py. It is duplicated rather
// than fetched because the number has to move with the keystroke: the cost
// cliff is the thing worth seeing before the text is finished, not after. The
// server stays the authority — its figure comes back with every send.

/** GSM 03.38, the 7-bit alphabet operators bill at 160 characters. */
const GSM7_BASIC = new Set(
  "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?" +
  '¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà',
)

/** Sent as an escape plus a code, so each costs two GSM-7 characters. */
const GSM7_EXTENDED = new Set('^{}\\[~]|€')

export interface SmsCost {
  encoding: 'gsm7' | 'ucs2'
  /** Billable units, not string length — GSM-7 escapes count twice. */
  characters: number
  parts: number
}

export function smsCost(text: string): SmsCost {
  const chars = Array.from(text)
  const unicodeNeeded = chars.some((c) => !GSM7_BASIC.has(c) && !GSM7_EXTENDED.has(c))

  let characters: number
  let single: number
  let concatenated: number

  if (unicodeNeeded) {
    // UCS-2 is billed in 16-bit units, not characters. A JS string is already
    // UTF-16, so its length is the billed figure — while Array.from would
    // collapse an emoji surrogate pair into one and understate the message.
    characters = text.length
    single = 70
    concatenated = 67
  } else {
    characters = chars.reduce((n, c) => n + (GSM7_EXTENDED.has(c) ? 2 : 1), 0)
    single = 160
    concatenated = 153
  }

  let parts: number
  if (characters === 0) {
    parts = 0
  } else if (characters <= single) {
    parts = 1
  } else {
    // Concatenation spends part of every segment on the joining header.
    parts = Math.ceil(characters / concatenated)
  }

  return { encoding: unicodeNeeded ? 'ucs2' : 'gsm7', characters, parts }
}

/**
 * What the TurboSMS balance must hold before a campaign will send.
 *
 * The server refuses a send whose balance does not cover recipients × parts ×
 * tariff plus 2% — the tariff below a 10 000 ₴ top-up is 1.29 ₴ where config
 * says 1.28, and the gateway bills batches it then fails to dispatch. Mirrored
 * here only so the dialog can say so before the press; the server's figure is
 * the one that decides, and comes back with any refusal.
 */
export const SMS_BALANCE_MARGIN = 1.02

export interface SmsSendRequirement {
  /** At the configured tariff: what the send is expected to cost. */
  cost: number
  /** What the balance has to cover for the server to let it go. */
  required: number
}

/** The balance as the page last read it. */
export type SmsBalanceReading =
  | { state: 'loading' }
  /** The gateway gave no figure, or could not be reached. */
  | { state: 'unknown' }
  | { state: 'known'; amount: number }

/** Whether a balance known to the page falls short of what the send needs.
 *  Loading and unknown are not short: the server reads it again and decides. */
export function balanceIsShort(
  balance: SmsBalanceReading, recipients: number, parts: number,
  pricePerPart: number | null,
): boolean {
  if (balance.state !== 'known' || pricePerPart == null) return false
  const { required } = smsSendRequirement(recipients, Math.max(parts, 1), pricePerPart)
  return balance.amount < required
}

export function smsSendRequirement(
  recipients: number, parts: number, pricePerPart: number,
): SmsSendRequirement {
  const cost = recipients * parts * pricePerPart
  // Rounded to the kopiyka the way the server rounds it, so a balance equal
  // to the figure on screen is never refused for a fraction it cannot see.
  return {
    cost: Math.round(cost * 100) / 100,
    required: Math.round(cost * SMS_BALANCE_MARGIN * 100) / 100,
  }
}
