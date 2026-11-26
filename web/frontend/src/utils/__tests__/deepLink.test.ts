import { describe, expect, it } from 'vitest'
import { hashTarget, PROMOTIONS_HREF } from '../deepLink'

// The Monday question's [Додати] and the badge's «+ акція» land on
// /marketing#promotions. Inside the Telegram Mini App the hash also carries
// Telegram's launch parameters, after our anchor or alone.

describe('the anchor a hash names', () => {
  it('reads our own anchor', () => {
    expect(hashTarget('#promotions')).toBe('promotions')
    expect(PROMOTIONS_HREF).toBe('/marketing#promotions')
  })

  it('reads it with Telegram’s launch parameters after it', () => {
    expect(hashTarget('#promotions?tgWebAppData=query_id%3DAA&tgWebAppVersion=8.0')).toBe('promotions')
    expect(hashTarget('#promotions&tgWebAppPlatform=ios')).toBe('promotions')
  })

  it('finds no anchor in the launch parameters alone, or in no hash', () => {
    expect(hashTarget('#tgWebAppData=query_id%3DAA&tgWebAppVersion=8.0')).toBeNull()
    expect(hashTarget('')).toBeNull()
    expect(hashTarget('#')).toBeNull()
  })

  it('reads another anchor as itself', () => {
    expect(hashTarget('#other')).toBe('other')
  })
})
