import type { Meta, StoryObj } from '@storybook/react-vite'
import { useLayoutEffect, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { ChartContainer } from './ChartContainer'
import { RevenueTrendActions } from './RevenueTrendActions'
import type { CompareType } from './revenueTrendTypes'
import { kyivToday } from '../utils/smsDates'
import type {
  PlannedPromotion,
  RevenueForecast,
  RevenueForecastBlackFriday,
  RevenueForecastRange,
} from '../types/api'

/**
 * The revenue chart's header: the forecast badge, the growth badge and the
 * comparison. For the month the badge says its 80% range beside the point —
 * the month's total lands in a range like it 8 times out of 10 — and, in a
 * month whose last year had Black Friday ahead, what that would add, on a
 * line of its own: never inside the point or the range.
 *
 * Illustrative numbers in a real November's proportions, not production's.
 */

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
  extra: 1_250_000,
  total: 4_500_000,
  size_days: 11.9,
  normal_day: 105_000,
  last_year_start: '2025-11-27',
  last_year_end: '2025-11-29',
  kind: 'black_friday',
}

const GROWTH = { current: 1_850_000, previous: 1_700_000, growth_percent: 8.8 }

// This year's November, so the days are said without a year as on the page.
const YEAR = kyivToday().slice(0, 4)
const PLANNED: PlannedPromotion[] = [
  { id: 12, name: 'Тиждень Anua', starts_on: `${YEAR}-11-24`, ends_on: `${YEAR}-11-26`, kind: 'other', extra: 463_000 },
  { id: 13, name: 'Чорна п’ятниця', starts_on: `${YEAR}-11-27`, ends_on: `${YEAR}-11-30`, kind: 'black_friday', extra: null },
]

/** Renders in Ukrainian and puts the reader's language back when it unmounts. */
function InUkrainian({ children }: { children: ReactNode }) {
  const { i18n } = useTranslation()
  useLayoutEffect(() => {
    const before = i18n.language
    void i18n.changeLanguage('uk')
    return () => { void i18n.changeLanguage(before) }
  }, [i18n])
  return <>{children}</>
}

function Header(props: {
  forecast?: RevenueForecast
  monthRange?: RevenueForecastRange | null
  blackFriday?: RevenueForecastBlackFriday | null
  plannedPromotions?: PlannedPromotion[] | null
  canAddPromotion?: boolean
}) {
  const { t } = useTranslation()
  const [compareType, setCompareType] = useState<CompareType>('year_ago')
  // In the chart's own header, so the stories show how the row wraps.
  return (
    <ChartContainer
      title={t('chart.revenueTrend')}
      height="sm"
      action={
        <RevenueTrendActions
          forecast={props.forecast}
          monthRange={props.monthRange}
          blackFriday={props.blackFriday}
          plannedPromotions={props.plannedPromotions}
          canAddPromotion={props.canAddPromotion}
          growthData={GROWTH}
          isLoading={false}
          compareType={compareType}
          onCompareTypeChange={setCompareType}
          t={t}
        />
      }
    >
      <div className="h-24 rounded bg-blue-50" />
    </ChartContainer>
  )
}

const meta = {
  title: 'Charts/RevenueTrendActions',
  component: Header,
} satisfies Meta<typeof Header>

export default meta
type Story = StoryObj<typeof meta>

const PHONE = { viewport: { value: 'mobile2', isRotated: false } }

/** The week, or a month whose range could not be said: the point alone. */
export const NoRange: Story = { args: { forecast: FORECAST } }

/** The month: the point and, beside it, its 80% range. */
export const WithRange: Story = { args: { forecast: FORECAST, monthRange: RANGE } }

/** A November with last year's Black Friday still ahead: its own line. */
export const WithRangeAndBlackFriday: Story = {
  args: { forecast: FORECAST, monthRange: RANGE, blackFriday: BLACK_FRIDAY },
}

/** The same header on a phone: the row wraps, the line keeps to itself. */
export const WithRangeAndBlackFridayOnAPhone: Story = {
  args: { forecast: FORECAST, monthRange: RANGE, blackFriday: BLACK_FRIDAY },
  globals: PHONE,
}

/** Last year's big event of this month was not Black Friday. */
export const BigPromotionInAnotherMonth: Story = {
  args: {
    forecast: { ...FORECAST, month_start: '2027-03-01', month_end: '2027-03-31' },
    monthRange: RANGE,
    blackFriday: {
      ...BLACK_FRIDAY,
      last_year_start: '2026-03-07',
      last_year_end: '2026-03-08',
      kind: 'big_promotion',
    },
  },
}

/** A big day has carried the point past the morning's range: the range is
 *  not said beside it. */
export const PointPastTheRange: Story = {
  args: { forecast: { ...FORECAST, predicted_total: 3_700_000 }, monthRange: RANGE },
}

/**
 * Promotions planned for the rest of the month, a line each under the Black
 * Friday line: what a typical past promotion of two days or more added (the
 * hint says why it runs high), nothing for the Black Friday the line above
 * already says. «+ акція» goes to the calendar, for `marketing` edit.
 */
export const WithPlannedPromotions: Story = {
  args: {
    forecast: FORECAST, monthRange: RANGE, blackFriday: BLACK_FRIDAY,
    plannedPromotions: PLANNED, canAddPromotion: true,
  },
  render: (args) => <InUkrainian><Header {...args} /></InUkrainian>,
}

/** The same on a phone. */
export const WithPlannedPromotionsOnAPhone: Story = {
  ...WithPlannedPromotions,
  globals: PHONE,
}

/** Nothing planned yet: the link alone, under the badge. */
export const AddPromotionLinkOnly: Story = {
  args: { forecast: FORECAST, monthRange: RANGE, plannedPromotions: [], canAddPromotion: true },
  render: (args) => <InUkrainian><Header {...args} /></InUkrainian>,
}
