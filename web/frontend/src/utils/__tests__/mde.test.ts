import { describe, it, expect } from 'vitest'
import { alphaFor, mdePercentagePoints, verdictFor } from '../mde'

describe('mdePercentagePoints', () => {
  it('matches the figures the campaign was designed against', () => {
    // August, as sent: 5 550 messaged against a 646 control.
    expect(mdePercentagePoints(5550, 646)).toBeCloseTo(1.92, 1)
    // The VIP tier of that same campaign — it measured +3.21 and could not
    // have proved it.
    expect(mdePercentagePoints(1189, 162)).toBeCloseTo(5.56, 1)
  })

  it('improves with the holdout, not with the size of the send', () => {
    const tenth = mdePercentagePoints(1350, 150)!
    const third = mdePercentagePoints(1050, 450)!

    // Same 1 500 people: a bigger control is a sharper measurement, even
    // though fewer messages go out.
    expect(third).toBeLessThan(tenth)
    expect(tenth / third).toBeGreaterThan(1.8)
  })

  it('is stricter once several arms are compared at once', () => {
    const plain = mdePercentagePoints(4100, 1758)!
    const corrected = mdePercentagePoints(4100, 1758, { alpha: alphaFor(3) })!

    expect(corrected).toBeGreaterThan(plain)
  })

  it('says nothing rather than something invented on an empty arm', () => {
    expect(mdePercentagePoints(1000, 0)).toBeNull()
    expect(mdePercentagePoints(0, 200)).toBeNull()
  })
})

describe('verdictFor', () => {
  it('calls a campaign hopeless when it cannot see a plausible effect', () => {
    // The measured effect of the only campaign there has been was ~2 pp.
    expect(verdictFor(5.56)).toBe('hopeless')
    expect(verdictFor(2.6)).toBe('tight')
    expect(verdictFor(1.07)).toBe('good')
    expect(verdictFor(null)).toBeNull()
  })
})
