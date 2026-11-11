import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
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
  it('opens on the conversion and its verdict, then what moved, the five steps and their total', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    const text = shown()
    expect(text).toContain('2,03 %')
    expect(text).toContain('из 1 000 зашедших купили 20')
    expect(text).toContain('было 2,33 %')
    expect(text).toContain('в пределах колебаний')
    expect(text).toContain('Покупателей почти столько же, а зашедших на 13 % больше.')
    expect(text).toContain('Просел один шаг: корзина → оформление — минус около 100 покупателей.')
    for (const step of ['Зашли на сайт', 'Открыли товар', 'Положили в корзину', 'Начали оформление', 'Купили']) {
      expect(text).toContain(step)
    }
    expect(text).toContain('дошли 55 % · ушли 15 300')
    expect(text).toContain('дошли 12,9 % · ушли 16 280 · больше всего')
    expect(text).toContain('больше всего')
    expect(text).toContain('хуже обычного: было 48 %')
    expect(text).toContain('Итог')
    expect(text).toContain('купили 2,03 % зашедших')
  })

  it('says the verdict once, beside the number, and nowhere as a sentence', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown().match(/в пределах колебаний/g)).toHaveLength(1)
    expect(shown()).not.toMatch(/Конверсия (выросла|упала)|Всё в пределах/)
  })

  it('sets "было" beside the first and the last step only', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).toContain('было 30 000')
    expect(shown()).toContain('было 700')
    for (const middle of ['16 500', '2 200', '1 050']) expect(shown()).not.toContain(`было ${middle}`)
  })

  it('sets the CRM count beside the funnel in a line, and why they differ a tap away', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).toContain('В CRM за эти дни 760 заказов с сайта')
    expect(shown()).not.toContain('без возвратов')
    await userEvent.click(screen.getByRole('button', { name: 'почему не совпадает с воронкой' }))
    const sheet = screen.getByRole('dialog', { name: 'Почему не совпадает с CRM' })
    expect(plain(sheet.textContent)).toContain('В воронке 690 покупателей, в CRM — 760 заказов с сайта за те же дни, без возвратов.')
  })

  it('keeps how to read it under the "?", off the screen', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).not.toContain('Шаги — события Google Analytics 4')
    await userEvent.click(screen.getByRole('button', { name: 'Как читать воронку' }))
    const help = screen.getByRole('dialog', { name: 'Как читать воронку' })
    expect(plain(help.textContent)).toContain('Шаги — события Google Analytics 4 на сайте')
  })

  it('says what it compares with, in dates', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).toContain('31 августа – 30 сентября 2026, тоже 31 день')
  })

  it('says the mix as the share of the site, against a year ago', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    await userEvent.click(screen.getAllByRole('radio', { name: 'с прошлым годом' })[0])
    expect(shown()).toContain('было 1,13 %')
    expect(shown()).toContain('лучше обычного')
    expect(shown()).toContain('Сильнее всего вырос шаг «товар → корзина» — плюс около 240 покупателей.')
    // Fewer warm visitors than a year ago (19 300 against 20 000), a larger
    // share of the site: «тёплых стало больше» said the opposite.
    expect(shown()).toContain('доля тёплых выросла с 33 % до 57 %')
    expect(shown()).not.toContain('тёплых стало больше')
  })

  it('folds the smallest devices into one row and says which', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    await userEvent.click(screen.getAllByRole('radio', { name: 'устройства' })[0])
    expect(shown()).toContain('Другие устройства')
    expect(shown()).toContain('«Другие устройства» — телевизор, планшет')
  })

  it('says under each GA4 channel what it is, in our words, and its type', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    await userEvent.click(screen.getAllByRole('radio', { name: 'каналы GA4' })[0])
    expect(shown()).toContain('Organic Social')
    expect(shown()).toContain('наши соцсети · тёплые')
    expect(shown()).toContain('реклама в соцсетях · холодные')
  })

  it('says in words where a group falls short of the rest of the site', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    expect(shown()).toContain('в корзину 9,5 % — ниже остального сайта')
    expect(shown()).toContain('открыли товар 49 % — ниже остального сайта')
    expect(shown()).toContain('▼ хуже обычного')
  })

  it('claims nothing about a day of few people, and says so with both counts', () => {
    const day = c(1100, 600, 80, 35, 23)
    withData({ ...FUNNEL, total: { current: day, previous: c(1050, 580, 75, 33, 22), year_ago: c(0, 0, 0, 0, 0) } })
    render(<SiteFunnel />)
    expect(shown()).toContain('Мало данных для выводов: зашли 1 100, купили 23. Показаны только цифры.')
    expect(shown()).not.toContain('в пределах колебаний')
    expect(shown()).not.toContain('хуже обычного')
    expect(shown()).not.toContain('лучше обычного')
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
    expect(shown()).toMatch(/Google Analytics не отвечает — показаны данные на (\d+ \S+, )?\d{2}:\d{2}/)
  })
})

describe('the site funnel on a phone', () => {
  const original = window.matchMedia
  beforeEach(() => {
    window.matchMedia = ((query: string) => ({
      matches: query.includes('max-width'), media: query, onchange: null,
      addEventListener: () => {}, removeEventListener: () => {},
      addListener: () => {}, removeListener: () => {}, dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia
  })
  afterEach(() => { window.matchMedia = original })

  it('opens how to read it as a sheet that a button closes', async () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    await userEvent.click(screen.getByRole('button', { name: 'Как читать воронку' }))
    const sheet = screen.getByRole('dialog', { name: 'Как читать воронку' })
    expect(plain(sheet.textContent)).toContain('Синяя полоса — дошли до шага')
    await userEvent.click(within(sheet).getByRole('button', { name: 'Закрыть' }))
    expect(screen.queryByRole('dialog', { name: 'Как читать воронку' })).toBeNull()
  })

  it('gives the "?" a target a thumb can hit', () => {
    withData(FUNNEL)
    render(<SiteFunnel />)
    const help = screen.getByRole('button', { name: 'Как читать воронку' })
    expect(help.className).toMatch(/\bh-11\b/)
    expect(help.className).toMatch(/\bw-11\b/)
  })
})
