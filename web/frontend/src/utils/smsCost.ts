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
    characters = chars.length
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
