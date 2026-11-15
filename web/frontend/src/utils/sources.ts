/**
 * What the dashboard calls a KeyCRM source.
 *
 * Instagram, Telegram and Shopify are proper nouns and are shown as they are,
 * wherever the name comes from. The offline store (source 7, a category of its
 * own by the owner's decision of 2026-11-15) is a phrase, and is said in the
 * reader's language: the server names it in Ukrainian, as it does the
 * exhibition, and that name is what a page shows only when it has no words of
 * its own.
 */
export const OFFLINE_STORE_SOURCE_ID = 7

export function sourceName(
  id: number | null | undefined,
  name: string,
  t: (key: string) => string,
): string {
  return id === OFFLINE_STORE_SOURCE_ID ? t('source.offlineStore') : name
}
