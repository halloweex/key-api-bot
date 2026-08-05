import { describe, it, expect } from 'vitest'
import { smsCost } from '../smsCost'

// This mirrors tests/unit/test_turbosms.py::TestCountSegments. The two
// implementations have to agree, or the counter under the textarea contradicts
// the figure the send comes back with.

describe('smsCost', () => {
  it('fits a full GSM-7 segment at 160 characters', () => {
    expect(smsCost('A'.repeat(160))).toMatchObject({ encoding: 'gsm7', parts: 1 })
  })

  it('splits one character past the limit', () => {
    expect(smsCost('A'.repeat(161)).parts).toBe(2)
  })

  it('lets a single Cyrillic character halve the whole message', () => {
    expect(smsCost('A'.repeat(100)).parts).toBe(1)
    expect(smsCost('A'.repeat(99) + 'я')).toMatchObject({ encoding: 'ucs2', parts: 2 })
  })

  it('fits seventy characters of Ukrainian', () => {
    expect(smsCost('я'.repeat(70)).parts).toBe(1)
    expect(smsCost('я'.repeat(71)).parts).toBe(2)
  })

  it('charges two septets for an extended character', () => {
    expect(smsCost('A'.repeat(159) + '€')).toMatchObject({
      encoding: 'gsm7', characters: 161, parts: 2,
    })
  })

  it('shrinks later parts by the concatenation header', () => {
    expect(smsCost('A'.repeat(306)).parts).toBe(2)
    expect(smsCost('A'.repeat(307)).parts).toBe(3)
  })

  it('costs nothing for an empty text', () => {
    expect(smsCost('').parts).toBe(0)
  })

  it('keeps a line break inside GSM-7', () => {
    expect(smsCost('Line one\nLine two').encoding).toBe('gsm7')
  })

  it('counts an emoji as the unit the operator bills, not as two', () => {
    // Surrogate pairs are one code point; splitting on UTF-16 units would
    // double-count and overstate the price.
    expect(smsCost('🎉').characters).toBe(1)
  })
})
