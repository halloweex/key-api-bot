import { afterEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render } from '@testing-library/react'
import i18n from '../../lib/i18n'
import { RevenueTrendActions } from '../RevenueTrendActions'
import { formatDayRange, formatShortRange } from '../revenueTrendHelpers'
import { kyivToday } from '../../utils/smsDates'
import type {
  PlannedPromotion,
  RevenueForecast,
  RevenueForecastBlackFriday,
  RevenueForecastRange,
} from '../../types/api'

// The badge is read for what it says, so it gets the real dictionaries.
vi.unmock('react-i18next')

// "(≈ ₴4.5M)" is held together by a no-break space.
const plain = (s: string | null | undefined) => (s ?? '').replace(/[\u00a0\u202f]/g, ' ')

const FORECAST: RevenueForecast = {
  actual_to_date: 1_850_000,
  predicted_remaining: 1_400_000,
  predicted_total: 3_250_000,
  daily_predictions: [],
  model_metrics: { mae: 0, mape: 0 },
  last_trained: '2026-11-16',
  month_start: '2026-11-01',
  month_end: '2026-11-30',
}
const RANGE: RevenueForecastRange = { lo: 2_940_000, hi: 3_610_000, coverage: 0.8, days_left: 13 }
const BLACK_FRIDAY: RevenueForecastBlackFriday = {
  extra: 1_250_000, total: 4_500_000, size_days: 11.9, normal_day: 105_000,
  last_year_start: '2025-11-27', last_year_end: '2025-11-29', kind: 'black_friday',
}

function badge(props: {
  forecast?: RevenueForecast
  monthRange?: RevenueForecastRange | null
  blackFriday?: RevenueForecastBlackFriday | null
}, language = 'ru') {
  const t = i18n.getFixedT(language) as (key: string, o?: Record<string, string | number>) => string
  const { container } = render(
    <RevenueTrendActions
      forecast={props.forecast}
      monthRange={props.monthRange}
      blackFriday={props.blackFriday}
      growthData={undefined}
      isLoading={false}
      compareType="year_ago"
      canCompare={false}
      onCompareTypeChange={() => {}}
      t={t}
    />,
  )
  return container
}

describe('the month forecast badge', () => {
  it('says the point alone without a range', () => {
    const text = badge({ forecast: FORECAST }).textContent
    expect(text).toContain('₴3.3M')
    expect(text).not.toContain('скорее всего')
  })

  it('says the 80% range beside the point', () => {
    const container = badge({ forecast: FORECAST, monthRange: RANGE })
    expect(container.textContent).toContain('скорее всего ₴2.9–3.6M')
    expect(container.querySelector('[title]')?.getAttribute('title')).toBe(
      'Без большой акции итог месяца попадает в такой диапазон 8 раз из 10.')
  })

  it('does not say a range the point has left', () => {
    const past = { ...FORECAST, predicted_total: 3_700_000 }
    expect(badge({ forecast: past, monthRange: RANGE }).textContent).not.toContain('скорее всего')
  })

  it('says Black Friday on its own line, beside the point and not in it', () => {
    const container = badge({ forecast: FORECAST, monthRange: RANGE, blackFriday: BLACK_FRIDAY })
    const line = container.querySelector('p')
    expect(plain(line?.textContent)).toBe('Если Чёрная пятница как в прошлом году: +₴1.3M (≈ ₴4.5M)')
    expect(line?.textContent).toContain('(≈\u00a0₴4.5M)')
    expect(line?.getAttribute('title')).toContain('27.11–29.11.2025')
    // The point and the range are what they were without it.
    expect(container.textContent).toContain('₴3.3M')
    expect(container.textContent).toContain('скорее всего ₴2.9–3.6M')
  })

  it("keeps the morning's total while today's sales move the point", () => {
    // On a day of the event the point counts its sales as they come, and
    // last year's day is in `extra`: the point plus `extra` would count them twice.
    const later = { ...FORECAST, predicted_total: 3_450_000 }
    expect(plain(badge({ forecast: later, blackFriday: BLACK_FRIDAY }).querySelector('p')?.textContent))
      .toBe('Если Чёрная пятница как в прошлом году: +₴1.3M (≈ ₴4.5M)')
  })

  it('says no line the point has reached', () => {
    const past = { ...FORECAST, predicted_total: 4_500_000 }
    expect(badge({ forecast: past, blackFriday: BLACK_FRIDAY }).querySelector('p')).toBeNull()
  })

  it('names a big promotion of another month for what it is', () => {
    const march: RevenueForecastBlackFriday = {
      ...BLACK_FRIDAY, last_year_start: '2026-03-07', last_year_end: '2026-03-08', kind: 'big_promotion',
    }
    expect(plain(badge({ forecast: FORECAST, blackFriday: march }, 'uk').querySelector('p')?.textContent))
      .toBe('Якщо велика акція як торік: +₴1.3M (≈ ₴4.5M)')
  })

  it('says nothing of a forecast that is not there', () => {
    const text = badge({ monthRange: RANGE, blackFriday: BLACK_FRIDAY }).textContent
    expect(text).toBe('')
  })
})

describe('a range in the badge units', () => {
  it('takes one decimal of millions, as the point does', () => {
    expect(formatShortRange(2_940_000, 3_610_000)).toBe('₴2.9–3.6M')
  })

  it('takes a second where one would print both ends the same', () => {
    expect(formatShortRange(3_520_000, 3_540_000)).toBe('₴3.52–3.54M')
  })

  it('says thousands under a million', () => {
    expect(formatShortRange(950_000, 990_000)).toBe('₴950–990K')
  })

  it('writes days as the dashboard does', () => {
    expect(formatDayRange('2025-11-27', '2025-11-29')).toBe('27.11–29.11.2025')
    expect(formatDayRange('2025-11-28', '2025-11-28')).toBe('28.11.2025')
  })
})

// ─── The promotions planned for the rest of the month ──────────────────────
//
// A line per planned promotion under the Black Friday line: what a typical
// past promotion of two days or more added, which runs high, and nothing for
// Black Friday, whose own line says it. «+ акція» goes to the calendar.

describe('the promotions planned for the month', () => {
  const year = kyivToday().slice(0, 4)
  const SPRING: PlannedPromotion = {
    id: 12, name: 'Весняний розпродаж', starts_on: `${year}-11-26`, ends_on: `${year}-11-28`,
    kind: 'other', extra: 463_000,
  }
  const FRIDAY: PlannedPromotion = {
    id: 13, name: 'Чорна п’ятниця', starts_on: `${year}-11-27`, ends_on: `${year}-11-30`,
    kind: 'black_friday', extra: null,
  }

  afterEach(async () => {
    await i18n.changeLanguage('en')
    window.history.replaceState(null, '', '/')
  })

  async function month(props: {
    forecast?: RevenueForecast
    monthRange?: RevenueForecastRange | null
    blackFriday?: RevenueForecastBlackFriday | null
    plannedPromotions?: PlannedPromotion[] | null
    canAddPromotion?: boolean
  }) {
    // The dates are said by Intl in the global language, as on the page.
    await i18n.changeLanguage('uk')
    const t = i18n.getFixedT('uk') as (key: string, o?: Record<string, string | number>) => string
    const { container } = render(
      <RevenueTrendActions
        forecast={props.forecast}
        monthRange={props.monthRange}
        blackFriday={props.blackFriday}
        plannedPromotions={props.plannedPromotions}
        canAddPromotion={props.canAddPromotion}
        growthData={undefined}
        isLoading={false}
        compareType="year_ago"
        canCompare={false}
        onCompareTypeChange={() => {}}
        t={t}
      />,
    )
    return container
  }

  const lines = (c: HTMLElement) => [...c.querySelectorAll('p')].map((p) => plain(p.textContent))

  it('says a line per planned promotion, with what a past one typically added', async () => {
    const c = await month({ forecast: FORECAST, plannedPromotions: [SPRING, FRIDAY] })
    expect(lines(c)).toEqual([
      'Заплановано: «Весняний розпродаж», 26–28 листопада. Якщо як минулі помітні акції: +₴463K',
      'Заплановано: «Чорна п’ятниця», 27–30 листопада',
    ])
  })

  it('says why the figure runs high, and only beside a figure', async () => {
    const [spring, friday] = (await month({ forecast: FORECAST, plannedPromotions: [SPRING, FRIDAY] }))
      .querySelectorAll('p')
    expect(spring.getAttribute('title')).toBe(
      'Оцінка за минулими акціями на 2+ дні, які помітно підняли виручку. Акції без ефекту '
      + 'в неї не потрапили, тож вона завищена. Основне число й діапазон акція не змінює.')
    expect(friday.hasAttribute('title')).toBe(false)
  })

  it('leaves the point as it was', async () => {
    expect((await month({ forecast: FORECAST, plannedPromotions: [SPRING] })).textContent).toContain('₴3.3M')
  })

  it('says nothing of them without a forecast, or with nothing planned', async () => {
    expect((await month({ plannedPromotions: [SPRING], canAddPromotion: true })).textContent).toBe('')
    expect(lines(await month({ forecast: FORECAST, plannedPromotions: [] }))).toEqual([])
    expect(lines(await month({ forecast: FORECAST, plannedPromotions: null }))).toEqual([])
  })

  it('says the rest of the badge, and nothing of promotions, to a reader without marketing', async () => {
    // The planned promotions are /marketing's: `/api/revenue/forecast`
    // answers `planned_promotions: null` to an account that may not open it.
    const c = await month({
      forecast: FORECAST, monthRange: RANGE, blackFriday: BLACK_FRIDAY, plannedPromotions: null,
    })
    expect(lines(c)).toEqual(["Якщо Чорна п'ятниця як торік: +₴1.3M (≈ ₴4.5M)"])
    expect(plain(c.textContent)).toContain('найімовірніше ₴2.9–3.6M')
    expect(c.textContent).not.toContain('Заплановано')
    expect(c.querySelector('a')).toBeNull()
  })

  it('offers «+ акція» only to whoever may add, and it opens the calendar in the page', async () => {
    expect((await month({ forecast: FORECAST })).querySelector('a')).toBeNull()
    const link = (await month({ forecast: FORECAST, canAddPromotion: true })).querySelector('a')
    expect(link?.textContent).toBe('+ акція')
    expect(link?.getAttribute('href')).toBe('/marketing#promotions')
    fireEvent.click(link!)
    expect(window.location.pathname).toBe('/marketing')
    expect(window.location.hash).toBe('#promotions')
  })
})
