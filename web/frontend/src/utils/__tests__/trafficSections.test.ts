import { describe, expect, it } from 'vitest'
import { filtersForPath } from '../pageFilters'
import { hasComparison, periodDates } from '../periodDates'
import { trafficPath, trafficTabFor, TRAFFIC_TABS } from '../trafficTabs'
import { isUnder } from '../../hooks/useRouter'

describe('the /traffic sections', () => {
  it('opens each section at its own path, /v2 and a trailing slash included', () => {
    expect(trafficTabFor('/traffic')).toBe('funnel')
    expect(trafficTabFor('/traffic/sales')).toBe('sales')
    expect(trafficTabFor('/traffic/campaigns/')).toBe('campaigns')
    expect(trafficTabFor('/v2/traffic/sales')).toBe('sales')
    expect(trafficTabFor('/v2/traffic')).toBe('funnel')
  })

  it('opens nothing at a path that is not one of them', () => {
    expect(trafficTabFor('/traffic/roas')).toBeNull()
    expect(trafficTabFor('/trafficking')).toBeNull()
    expect(trafficTabFor('/')).toBeNull()
  })

  it('puts every section at the path that opens it', () => {
    for (const { tab, path } of TRAFFIC_TABS) {
      expect(trafficPath(tab)).toBe(path)
      expect(trafficTabFor(path)).toBe(tab)
    }
  })

  it('keeps the sidebar link lit under its section paths, and only there', () => {
    expect(isUnder('/traffic/sales', '/traffic')).toBe(true)
    expect(isUnder('/traffic', '/traffic')).toBe(true)
    expect(isUnder('/trafficking', '/traffic')).toBe(false)
    expect(isUnder('/traffic', '/')).toBe(false)
  })
})

describe('the filters each section applies', () => {
  it('gives the funnel the period alone: Google Analytics sees nothing of a CRM order', () => {
    expect([...filtersForPath('/traffic')]).toEqual(['period'])
    expect([...filtersForPath('/v2/traffic')]).toEqual(['period'])
  })

  it('gives the CRM sections the period, sales type and source', () => {
    for (const path of ['/traffic/sales', '/traffic/campaigns']) {
      expect([...filtersForPath(path)]).toEqual(['period', 'salesType', 'sourceId'])
    }
  })

  it('leaves every other page every filter', () => {
    expect(filtersForPath('/products').size).toBe(6)
    expect(filtersForPath('/').has('salesType')).toBe(true)
  })
})

describe('the period in dates, as the server counts it', () => {
  // Wednesday 4 November 2026; a week starts on Monday.
  const today = '2026-11-04'

  it('resolves every named period', () => {
    expect(periodDates('today', null, null, today)).toEqual({ start: '2026-11-04', end: '2026-11-04' })
    expect(periodDates('yesterday', null, null, today)).toEqual({ start: '2026-11-03', end: '2026-11-03' })
    expect(periodDates('week', null, null, today)).toEqual({ start: '2026-11-02', end: '2026-11-04' })
    expect(periodDates('last_week', null, null, today)).toEqual({ start: '2026-10-26', end: '2026-11-01' })
    expect(periodDates('month', null, null, today)).toEqual({ start: '2026-11-01', end: '2026-11-04' })
    expect(periodDates('last_month', null, null, today)).toEqual({ start: '2026-10-01', end: '2026-10-31' })
    expect(periodDates('last_7_days', null, null, today)).toEqual({ start: '2026-10-29', end: '2026-11-04' })
    expect(periodDates('last_28_days', null, null, today)).toEqual({ start: '2026-10-08', end: '2026-11-04' })
    expect(periodDates('all_time', null, null, today)).toEqual({ start: '2023-12-02', end: '2026-11-04' })
  })

  it('compares every period but the whole history', () => {
    expect(hasComparison('all_time')).toBe(false)
    expect(hasComparison('month')).toBe(true)
    expect(hasComparison('custom')).toBe(true)
  })

  it('starts the week on Monday when today is Sunday, and last month across a new year', () => {
    expect(periodDates('week', null, null, '2026-11-08')).toEqual({ start: '2026-11-02', end: '2026-11-08' })
    expect(periodDates('last_month', null, null, '2027-01-15')).toEqual({ start: '2026-12-01', end: '2026-12-31' })
  })

  it('takes custom dates only as a real pair', () => {
    expect(periodDates('custom', '2026-10-01', '2026-10-15', today)).toEqual({ start: '2026-10-01', end: '2026-10-15' })
    expect(periodDates('custom', '2026-10-01', null, today)).toBeNull()
    expect(periodDates('custom', '2026-02-30', '2026-03-01', today)).toBeNull()
  })
})
