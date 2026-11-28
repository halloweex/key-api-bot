import { describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
import { ApiError } from '../../api/client'
import { FUNNEL, c } from '../../test/funnelFixtures'
import type { TrafficFunnelResponse } from '../../types/api'

// Which state the funnel chose, read through the i18n key it asked for and the
// figures put into it, never through the sentence. The mock appends the
// interpolation values to the key.
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => (opts ? `${key}${JSON.stringify(opts)}` : key),
    i18n: { language: 'en', changeLanguage: () => Promise.resolve() },
  }),
  initReactI18next: { type: '3rdParty', init: () => {} },
}))

const state = vi.hoisted(() => ({
  funnel: { data: undefined as unknown, isLoading: false, error: null as unknown },
}))

vi.mock('../../hooks/useApi', () => ({
  useTrafficFunnel: () => ({ ...state.funnel, refetch: vi.fn() }),
  useSiteOrders: () => ({ data: { totalOrders: 760 } }),
}))

import { SiteFunnel } from '../SiteFunnel'

const shown = () => (document.body.textContent ?? '').replace(/[  ]/g, ' ')

function withData(data: TrafficFunnelResponse) {
  state.funnel = { data, isLoading: false, error: null }
}

describe('the state the site funnel chooses', () => {
  it('claims nothing about a day of few people, and passes both counts', () => {
    const day = c(1100, 600, 80, 35, 23)
    withData({ ...FUNNEL, total: { current: day, previous: c(1050, 580, 75, 33, 22), year_ago: c(0, 0, 0, 0, 0) } })
    render(<SiteFunnel />)
    const text = shown()
    expect(text).toMatch(/traffic\.funnel\.state\.few\{"entered":"1\D?100","bought":"23"\}/)
    expect(text).not.toContain('traffic.funnel.hero.verdict')
  })

  it('says Google Analytics is not connected when the server has no key', () => {
    state.funnel = { data: undefined, isLoading: false, error: new ApiError(503, 'GA4 is not configured') }
    render(<SiteFunnel />)
    expect(shown()).toContain('traffic.funnel.state.notConfiguredTitle')
    expect(shown()).not.toContain('traffic.funnel.state.unavailableTitle')
  })

  it('says it is unavailable, not unconfigured, for any other 503', () => {
    state.funnel = { data: undefined, isLoading: false, error: new ApiError(503, 'upstream timeout') }
    render(<SiteFunnel />)
    expect(shown()).toContain('traffic.funnel.state.unavailableTitle')
    expect(shown()).not.toContain('traffic.funnel.state.notConfiguredTitle')
  })

  it('says a backwards period is one, rather than that nobody came', () => {
    withData({ ...FUNNEL, periods: { ...FUNNEL.periods, current: { start: '2026-10-31', end: '2026-10-01' } } })
    render(<SiteFunnel />)
    expect(shown()).toContain('traffic.funnel.state.backwards')
    expect(shown()).not.toContain('traffic.funnel.state.empty')
  })

  it('says when the figures are from, while Google Analytics does not answer', () => {
    withData({ ...FUNNEL, stale: true })
    render(<SiteFunnel />)
    expect(shown()).toMatch(/traffic\.funnel\.state\.stale\{"time":"[^"]*10:42[^"]*"\}/)
  })
})
