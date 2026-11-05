import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import i18n from '../../lib/i18n'
import { ApiError } from '../../api/client'
import { FUNNEL, c } from '../../test/funnelFixtures'
import type { TrafficFunnelResponse } from '../../types/api'

// The setup answers every `t` with its key; this block is read for what it
// says, so it gets the real dictionaries.
vi.unmock('react-i18next')

const state = vi.hoisted(() => ({
  funnel: { data: undefined as unknown, isLoading: false, error: null as unknown },
}))

vi.mock('../../hooks/useApi', () => ({
  useTrafficFunnel: () => ({ ...state.funnel, refetch: vi.fn() }),
  useSiteOrders: () => ({ data: { totalOrders: 760 } }),
}))

import { SiteFunnel } from '../SiteFunnel'

// Intl puts a no-break space before % and between thousands in Russian.
const plain = (s: string | null | undefined) => (s ?? '').replace(/[\u00a0\u202f]/g, ' ')
const shown = () => plain(document.body.textContent)

function withData(data: TrafficFunnelResponse) {
  state.funnel = { data, isLoading: false, error: null }
}

beforeEach(async () => { await i18n.changeLanguage('ru') })
afterEach(async () => { await i18n.changeLanguage('en') })

describe('the site funnel', () => {
  it('opens on the conversion and what it says, then the five steps and their total', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    const text = shown()
    expect(text).toContain('2,03 %')
    expect(text).toContain('из 1 000 зашедших купили 20')
    expect(text).toContain('было 2,33 % — в пределах обычных колебаний')
    expect(text).toContain('Покупателей почти столько же, а зашедших на 13 % больше')
    expect(text).toContain('Просел один шаг:')
    for (const step of ['Зашли на сайт', 'Открыли товар', 'Положили в корзину', 'Начали оформление', 'Купили']) {
      expect(text).toContain(step)
    }
    expect(text).toContain('здесь уходит больше всего людей')
    expect(text).toContain('хуже обычного: было 48 %')
    expect(text).toContain('купили 2,03 % зашедших — 20 из 1 000')
  })

  it('says the funnel is not every order, beside the CRM count for the same days', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).toContain('690 покупателей — это не все заказы сайта')
    expect(shown()).toContain('Заказов с сайта в CRM за эти дни: 760.')
  })

  it('says what it compares with, in dates', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).toContain('с 31 августа по 30 сентября 2026 — тоже 31 день')
  })

  it('folds the smallest devices into one row and says which', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    await userEvent.click(screen.getAllByRole('radio', { name: 'устройства' })[0])
    expect(shown()).toContain('Другие устройства')
    expect(shown()).toContain('«Другие устройства» — телевизор, планшет')
  })

  it('claims nothing about a day of few people, and says so', () => {
    const day = c(1100, 600, 80, 35, 23)
    withData({ ...FUNNEL, total: { current: day, previous: c(1050, 580, 75, 33, 22), year_ago: c(0, 0, 0, 0, 0) } })
    render(<SiteFunnel />)
    expect(shown()).toContain('Зашли 1 100 человек — этого мало, чтобы отличить изменение от случайности.')
    expect(shown()).not.toContain('в пределах обычных колебаний')
    expect(shown()).not.toContain('хуже обычного')
  })

  it('says Google Analytics is not connected, when the server has no key', () => {
    state.funnel = { data: undefined, isLoading: false, error: new ApiError(503, 'GA4 is not configured') }
    render(<SiteFunnel />)
    expect(shown()).toContain('Google Analytics ещё не подключён')
    expect(shown()).toContain('Остальные разделы «Трафика» работают.')
  })

  it('says a backwards period is one, rather than that nobody came', () => {
    withData({ ...FUNNEL, periods: { ...FUNNEL.periods, current: { start: '2026-10-31', end: '2026-10-01' } } })
    render(<SiteFunnel />)
    expect(shown()).toContain('Начало периода позже его конца — выберите даты заново.')
  })

  it('says when the figures are from, while Google Analytics does not answer', () => {
    withData({ ...FUNNEL, stale: true })
    render(<SiteFunnel />)
    expect(shown()).toMatch(/Показаны данные на (\d+ \S+, )?\d{2}:\d{2}/)
  })
})
