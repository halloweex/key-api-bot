import { describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
import i18n from '../../lib/i18n'
import { RevenueTrendActions } from '../RevenueTrendActions'
import { formatDayRange, formatShortRange } from '../revenueTrendHelpers'
import type {
  RevenueForecast,
  RevenueForecastBlackFriday,
  RevenueForecastRange,
} from '../../types/api'

// The badge is read for what it says, so it gets the real dictionaries.
vi.unmock('react-i18next')

// "(≈ ₴4.5M)" is held together by a no-break space.
const plain = (s: string | null | undefined) => (s ?? '').replace(/\u00a0/g, ' ')

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
  last_year_start: '2025-11-27', last_year_end: '2025-11-29',
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
      'Итог месяца попадает в такой диапазон 8 раз из 10.')
  })

  it('does not say a range the point has left', () => {
    const past = { ...FORECAST, predicted_total: 3_700_000 }
    expect(badge({ forecast: past, monthRange: RANGE }).textContent).not.toContain('скорее всего')
  })

  it('says Black Friday on its own line, added to the point it stands beside', () => {
    const container = badge({ forecast: FORECAST, monthRange: RANGE, blackFriday: BLACK_FRIDAY })
    const line = container.querySelector('p')
    expect(plain(line?.textContent)).toBe('Если Чёрная пятница как в прошлом году: +₴1.3M (≈ ₴4.5M)')
    expect(line?.textContent).toContain('(≈\u00a0₴4.5M)')
    expect(line?.getAttribute('title')).toContain('27.11–29.11.2025')
    // The point and the range are what they were without it.
    expect(container.textContent).toContain('скорее всего ₴2.9–3.6M')
  })

  it('names a big promotion of another month for what it is', () => {
    const march = { ...BLACK_FRIDAY, last_year_start: '2026-03-07', last_year_end: '2026-03-08' }
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
