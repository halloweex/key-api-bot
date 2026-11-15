import { describe, expect, it } from 'vitest'
import en from '../../locales/en.json'
import uk from '../../locales/uk.json'
import ru from '../../locales/ru.json'
import { SOURCES } from '../smsConditions'
import {
  OFFLINE_STORE_SERVER_NAME, OFFLINE_STORE_SOURCE_ID, sourceLabel, sourceName,
} from '../sources'

const t = (key: string) => `t:${key}`

describe('sourceName', () => {
  it('says the offline store in the reader’s words, whatever the server called it', () => {
    expect(sourceName(OFFLINE_STORE_SOURCE_ID, 'Офлайн-магазин', t)).toBe('t:source.offlineStore')
  })

  it('leaves the proper nouns as they came', () => {
    expect(sourceName(1, 'Instagram', t)).toBe('Instagram')
    expect(sourceName(4, 'Shopify', t)).toBe('Shopify')
    expect(sourceName(null, 'All', t)).toBe('All')
  })

  it('is the source the server calls the offline store', () => {
    // core.duckdb_constants.OFFLINE_STORE_SOURCE_ID
    expect(OFFLINE_STORE_SOURCE_ID).toBe(7)
  })

  it('says it in the reader’s words where the answer carries only names', () => {
    // The doughnut by source: names, no ids (tests/unit/test_offline_store.py
    // holds the constant to the server's name).
    expect(OFFLINE_STORE_SERVER_NAME).toBe('Офлайн-магазин')
    expect(sourceLabel(OFFLINE_STORE_SERVER_NAME, t)).toBe('t:source.offlineStore')
    expect(sourceLabel('Instagram', t)).toBe('Instagram')
    expect(sourceLabel('Shopify', t)).toBe('Shopify')
  })

  it('has the words in every language', () => {
    for (const dict of [en, uk, ru] as Record<string, string>[]) {
      expect(dict['source.offlineStore']).toBeTruthy()
      expect(dict['filter.offline']).toBeTruthy()
    }
  })
})

describe('the SMS source condition', () => {
  // Offered, and under this page's retail-only audiences it matches nobody,
  // as the exhibition never has (the comment on SOURCES says why).
  it('offers the offline store, with a label in every language', () => {
    expect(SOURCES).toContain(OFFLINE_STORE_SOURCE_ID)
    for (const dict of [en, uk, ru] as Record<string, string>[]) {
      for (const id of SOURCES) expect(dict[`sms.source.${id}`]).toBeTruthy()
    }
  })
})
