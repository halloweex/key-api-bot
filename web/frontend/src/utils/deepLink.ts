/**
 * Links into a section of a page: `/marketing#promotions`.
 *
 * The Monday question's [Додати] and the forecast badge's «+ акція» both land
 * on the promotions calendar. Inside the Telegram Mini App the hash is not
 * ours alone: Telegram passes its launch parameters there, after our own
 * anchor when the link carries one (`#promotions?tgWebAppData=…`, the form
 * `telegram-web-app.js` itself reads as `_path` + parameters) and as the
 * whole hash when it does not (`#tgWebAppData=…`). So the anchor is the
 * hash's first segment, before any `?` or `&`, and a segment that is itself
 * a parameter (`name=value`) names no anchor.
 */

/** The anchor of the promotions calendar on /marketing. */
export const PROMOTIONS_ANCHOR = 'promotions'

/** Where «+ акція» and the bot's [Додати] go. */
export const PROMOTIONS_HREF = `/marketing#${PROMOTIONS_ANCHOR}`

/** The anchor a location hash names, or null: `#promotions?tgWebAppData=…` → `promotions`. */
export function hashTarget(hash: string): string | null {
  const body = hash.replace(/^#/, '')
  if (!body) return null
  const first = body.split(/[?&]/, 1)[0]
  if (!first || first.includes('=')) return null
  try {
    return decodeURIComponent(first)
  } catch {
    return first
  }
}
