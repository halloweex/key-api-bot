import { describe, expect, it } from 'vitest'
import {
  FOLDED_DEVICES, OVERDISPERSION, biggestLeak, buyersMoved, canCompare, conclude, conversionChange,
  countChange, enoughToClaim, foldDevices, funnelSteps, groupFigures, judge, mixShift, noiseZ,
  perBase, roughly, rowsGap,
} from '../funnel'
import {
  BY_CHANNEL, BY_DEVICE, BY_TYPE, FUNNEL, NOW, PREVIOUS, STEADY, YEAR_AGO, c,
} from '../../test/funnelFixtures'

/** The plain two-proportion z, to hold `noiseZ` to. */
function plainZ(k1: number, n1: number, k2: number, n2: number): number {
  const p = (k1 + k2) / (n1 + n2)
  return (k1 / n1 - k2 / n2) / Math.sqrt(p * (1 - p) * (1 / n1 + 1 / n2))
}

describe('telling a change from noise', () => {
  it('widens the binomial spread by the measured overdispersion', () => {
    const z = noiseZ(690, 34000, 700, 30000)
    expect(z).not.toBeNull()
    expect(z!).toBeCloseTo(plainZ(690, 34000, 700, 30000) / Math.sqrt(OVERDISPERSION), 10)
  })

  it('calls a plain z of 2.5 ordinary noise, and 3 a change', () => {
    // 2.5 / 1.4 = 1.79 is inside the cut; 3 / 1.4 = 2.14 is past it.
    expect(judge(2.5 / Math.sqrt(OVERDISPERSION))).toBe('same')
    expect(judge(-3 / Math.sqrt(OVERDISPERSION))).toBe('worse')
    expect(judge(3 / Math.sqrt(OVERDISPERSION))).toBe('better')
  })

  it('judges nothing from too few people, or from counts that cannot be', () => {
    expect(noiseZ(6, 20, 9, 25)).toBeNull()
    expect(noiseZ(1, 400, 2, 400)).toBeNull()
    expect(noiseZ(50, 40, 10, 100)).toBeNull()
    expect(judge(null)).toBeNull()
  })

  it('claims nothing about a day of ~1 100 people', () => {
    expect(enoughToClaim(c(1100, 600, 80, 35, 23))).toBe(false)
    expect(enoughToClaim(NOW)).toBe(true)
  })

  it('reads the month against the one before as within noise, and against a year ago as up', () => {
    expect(conversionChange(NOW, PREVIOUS).change).toBe('same')
    expect(conversionChange(NOW, YEAR_AGO).change).toBe('better')
  })
})

describe('the steps', () => {
  it('gives each step its share of the one before and of everyone, and who left', () => {
    const steps = funnelSteps(NOW, PREVIOUS, true)
    expect(steps.map((s) => s.people)).toEqual([34000, 18700, 2420, 1015, 690])
    expect(steps[2].ofBefore).toBeCloseTo(2420 / 18700, 10)
    expect(steps[2].ofEntered).toBeCloseTo(2420 / 34000, 10)
    expect(steps.map((s) => s.left)).toEqual([null, 15300, 16280, 1405, 325])
    expect(steps.map((s) => s.change)).toEqual([null, 'same', 'same', 'worse', 'same'])
  })

  it('judges no step when the windows are too small to', () => {
    expect(funnelSteps(NOW, PREVIOUS, false).every((s) => s.change === null)).toBe(true)
  })

  it('finds where the most people leave, the earlier step on a tie', () => {
    expect(biggestLeak(NOW)).toBe('added_to_cart')
    expect(biggestLeak(c(100, 50, 0, 0, 0))).toBe('viewed_product')
  })

  it('prices a step by the buyers it moved, the rest of the funnel held as it is now', () => {
    // (1015/2420 − 1050/2200) × 2420 carts × 690/1015 who went on to buy.
    expect(buyersMoved(NOW, PREVIOUS, 'began_checkout')).toBeCloseTo((1015 / 2420 - 1050 / 2200) * 2420 * (690 / 1015), 8)
    expect(buyersMoved(NOW, PREVIOUS, 'purchased')).toBeCloseTo((690 / 1015 - 700 / 1050) * 1015, 8)
  })

  it('rounds the way a count is said', () => {
    expect([roughly(83.7), roughly(-95.2), roughly(239.6), roughly(1234), roughly(5.3), roughly(0.4)])
      .toEqual([80, 100, 240, 1200, 5, 0])
  })

  it('says a rate as so many of a base that gives at least ten', () => {
    expect(perBase(0.0202)).toEqual({ base: 1000, count: 20 })
    expect(perBase(0.15)).toEqual({ base: 100, count: 15 })
    expect(perBase(0.003)).toEqual({ base: 10000, count: 30 })
  })

  it('calls a count within 5 % the same', () => {
    expect(countChange(690, 700).move).toBe('same')
    expect(countChange(34000, 30000)).toEqual({ move: 'more', ratio: 34000 / 30000 - 1 })
    expect(countChange(5, 0)).toEqual({ move: 'more', ratio: null })
  })
})

describe('what the period can be set beside', () => {
  it('compares with a window GA4 still holds whole', () => {
    expect(canCompare(FUNNEL, 'previous')).toBe(true)
    expect(canCompare(FUNNEL, 'year_ago')).toBe(true)
    const old = { ...FUNNEL, periods: { ...FUNNEL.periods, year_ago: { start: '2025-03-01', end: '2025-03-31' } } }
    expect(canCompare(old, 'year_ago')).toBe(false)
    const empty = { ...FUNNEL, total: { ...FUNNEL.total, previous: c(0, 0, 0, 0, 0) } }
    expect(canCompare(empty, 'previous')).toBe(false)
  })
})

describe('the conclusion', () => {
  it('starts from the counts when the conversion held and one step fell', () => {
    const verdict = conclude(NOW, PREVIOUS, BY_TYPE, 'previous')
    expect(verdict.headline).toEqual({ kind: 'counts' })
    expect(verdict.buyers.move).toBe('same')
    expect(verdict.entered.move).toBe('more')
    expect(verdict.moves.map((m) => [m.step, m.change, roughly(m.buyers ?? 0)])).toEqual([['began_checkout', 'worse', 100]])
    expect(verdict.mix).toBeNull()
  })

  it('names the step that moved the most buyers, not the surest one', () => {
    const verdict = conclude(NOW, YEAR_AGO, BY_TYPE, 'year_ago')
    expect(verdict.headline).toMatchObject({ kind: 'conversion', change: 'better' })
    // arrival → product is the surer change; product → cart moved ~240 buyers.
    expect(verdict.strongest?.step).toBe('added_to_cart')
    expect(roughly(verdict.strongest?.buyers ?? 0)).toBe(240)
    expect(verdict.moves).toEqual([])
  })

  it('says the mix carried part of the rise only when it pushed the same way and by a fifth', () => {
    expect(conclude(NOW, YEAR_AGO, BY_TYPE, 'year_ago').mix).toEqual({
      change: 'better', type: 'warm', more: true, shareNow: 19300 / 34000, shareWas: 20000 / 60000,
    })
    expect(mixShift(BY_TYPE, 'previous')).toBeNull()
  })

  it('names the share of the site the type moved to, which can rise while its people fall', () => {
    // The shape of October 2026 against 2025: fewer warm visitors than a year
    // ago (19 300 against 20 000), a much larger share of the site. A sentence
    // about the counts said the opposite of the table.
    const mix = conclude(NOW, YEAR_AGO, BY_TYPE, 'year_ago').mix
    const warm = BY_TYPE.find((r) => r.key === 'warm')!
    expect(warm.current.entered).toBeLessThan(warm.year_ago.entered)
    expect(mix?.shareNow).toBeGreaterThan(mix?.shareWas ?? 1)
  })

  it('has nothing beyond noise to say about a steady month', () => {
    const verdict = conclude(NOW, STEADY, BY_TYPE, 'previous')
    expect(verdict.headline).toEqual({ kind: 'steady' })
    expect(verdict.moves).toEqual([])
    expect(verdict.mix).toBeNull()
  })
})

describe('the breakdown', () => {
  it('marks a step clearly lower than on the rest of the site, and a group worse than before', () => {
    const groups = groupFigures(BY_TYPE, NOW, 'previous', true, true)
    const lower = Object.fromEntries(groups.map((g) => [
      g.row.key, Object.entries(g.steps).filter(([, v]) => v.lower).map(([k]) => k),
    ]))
    expect(lower).toEqual({ cold: ['added_to_cart'], warm: ['viewed_product'], other: ['viewed_product'] })
    expect(groups.map((g) => g.change)).toEqual(['same', 'worse', 'same'])
  })

  it('shows a group under 200 people and judges nothing about it', () => {
    const email = groupFigures(BY_CHANNEL, NOW, 'previous', true, true).find((g) => g.row.key === 'Email')!
    expect(email.small).toBe(true)
    expect(Object.values(email.steps).some((s) => s.lower)).toBe(false)
    expect(email.change).toBeNull()
  })

  it('judges the rest of the site with this period alone, and then only with both', () => {
    const groups = groupFigures(BY_TYPE, NOW, 'previous', true, false)
    expect(groups.some((g) => Object.values(g.steps).some((s) => s.lower))).toBe(true)
    expect(groups.every((g) => g.change === null)).toBe(true)
    expect(groupFigures(BY_TYPE, NOW, 'previous', false, false).every((g) => !Object.values(g.steps).some((s) => s.lower))).toBe(true)
  })

  it('folds devices under 1 % of visitors into one row, never judged, when there are two', () => {
    const { rows, folded } = foldDevices(BY_DEVICE, NOW)
    expect(folded).toEqual(['smart tv', 'tablet'])
    const other = rows.find((r) => r.key === FOLDED_DEVICES)!
    expect(other.current).toEqual(c(440, 340, 12, 6, 3))
    expect(groupFigures(rows, NOW, 'previous', true, true).find((g) => g.row.key === FOLDED_DEVICES)?.unjudged).toBe(true)
    expect(foldDevices(BY_DEVICE.slice(0, 3), NOW).folded).toEqual([])
  })

  it('leaves out a row nobody was in, then or now', () => {
    const rows = [...BY_DEVICE, { key: 'nobody', current: c(0, 0, 0, 0, 0), previous: c(0, 0, 0, 0, 0), year_ago: c(0, 0, 0, 0, 0) }]
    expect(groupFigures(rows, NOW, 'previous', true, true).map((g) => g.row.key)).not.toContain('nobody')
  })

  it('says how far the rows fall short of the site', () => {
    expect(rowsGap(BY_TYPE, NOW)).toBe(200)
    expect(rowsGap(BY_CHANNEL, NOW)).toBe(200)
  })
})
