import type { TFunction } from 'i18next'

/**
 * The bucket of goods with no category, as the API spells it: `Other` on the
 * dashboard's category chart, `Uncategorized` on /margin and /inventory. The
 * API keeps its spellings — the dashboard's drill-down asks for its key — and
 * the screen says one name for the bucket, in the reader's language
 * (2026-11-27: the tabs showed two English words for it, and /inventory's
 * translation never showed, because the API had already filled the name in).
 */
const NO_CATEGORY = new Set(['Other', 'Uncategorized'])

export function isNoCategory(name: string | null | undefined): boolean {
  return !name || NO_CATEGORY.has(name)
}

/** A category's name for the screen: the no-category bucket in the reader's
 * words, every other name as the catalogue spells it. */
export function categoryLabel(name: string | null | undefined, t: TFunction): string {
  return isNoCategory(name) ? t('category.none') : (name as string)
}
