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

  it('counts an emoji as the two units the operator bills', () => {
    // UCS-2 is billed in 16-bit units. A pictographic emoji is a surrogate
    // pair, so it takes two of the 70 — counting it as one code point
    // understates the message and can hide an extra segment.
    expect(smsCost('🎉').characters).toBe(2)
  })

  it('does not let an emoji sneak a message into another segment', () => {
    expect(smsCost('я'.repeat(69) + '🎉').characters).toBe(71)
    expect(smsCost('я'.repeat(69) + '🎉').parts).toBe(2)
  })

  it('still counts a BMP symbol as one unit', () => {
    expect(smsCost('❤').characters).toBe(1)
  })
})
