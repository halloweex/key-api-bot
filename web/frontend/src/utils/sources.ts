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

/**
 * What the server calls the offline store wherever it names it: Silver's
 * `source_name`, `/reports` and the doughnut by source
 * (`_REPORT_SOURCE_NAMES` in core/repositories/revenue.py, which
 * tests/unit/test_offline_store.py holds to this line).
 */
export const OFFLINE_STORE_SERVER_NAME = 'Офлайн-магазин'

export function sourceName(
  id: number | null | undefined,
  name: string,
  t: (key: string) => string,
): string {
  return id === OFFLINE_STORE_SOURCE_ID ? t('source.offlineStore') : name
}

/**
 * The same, for an answer that names its sources and carries no ids: the
 * doughnut by source (`/api/sales/by-source`), whose shape the recorded
 * answers in tests/integration/golden/dashboard.json pin.
 */
export function sourceLabel(name: string, t: (key: string) => string): string {
  return name === OFFLINE_STORE_SERVER_NAME ? t('source.offlineStore') : name
}
