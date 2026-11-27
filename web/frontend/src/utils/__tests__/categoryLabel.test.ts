import { afterEach, describe, expect, it } from 'vitest'
import type { TFunction } from 'i18next'
import i18n from '../../lib/i18n'
import { categoryLabel, isNoCategory } from '../categoryLabel'

const t = i18n.t.bind(i18n) as TFunction

afterEach(async () => {
  await i18n.changeLanguage('en')
})

describe('the bucket of goods with no category', () => {
  it('is one name on every tab, whichever spelling the API used', async () => {
    await i18n.changeLanguage('uk')
    // The dashboard's chart says `Other`, /margin and /inventory `Uncategorized`.
    expect(categoryLabel('Other', t)).toBe('Без категорії')
    expect(categoryLabel('Uncategorized', t)).toBe('Без категорії')
    expect(categoryLabel(null, t)).toBe('Без категорії')
    expect(categoryLabel('', t)).toBe('Без категорії')
  })

  it('is said in the reader’s language', async () => {
    await i18n.changeLanguage('ru')
    expect(categoryLabel('Other', t)).toBe('Без категории')
    await i18n.changeLanguage('en')
    expect(categoryLabel('Uncategorized', t)).toBe('Uncategorized')
  })

  it('leaves every real category as the catalogue spells it', () => {
    expect(categoryLabel('Face', t)).toBe('Face')
    expect(categoryLabel('Догляд за обличчям', t)).toBe('Догляд за обличчям')
    expect(isNoCategory('Face')).toBe(false)
    expect(isNoCategory('Other')).toBe(true)
  })
})
