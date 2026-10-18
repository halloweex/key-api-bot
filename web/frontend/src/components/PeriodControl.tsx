import { useTranslation } from 'react-i18next'
import { SegmentedControl } from './SegmentedControl'
import { NumberWithUnit } from './NumberWithUnit'
import { DateRangeField } from './DateRangeField'
import { FilterChip } from './FilterChip'
import { Wrapper } from './Wrapper'
import { addDays, daysBetween, isIsoDate, kyivToday, lastDays, rangeEnds } from '../utils/smsDates'
import { DAY_CHIPS, MAX_DAYS, type PeriodValue } from '../utils/smsConditions'
import { plural } from '../utils/smsConditionText'

// ─── PeriodControl ───────────────────────────────────────────────────────────
//
// When something is counted, as one control with three ways of saying it:
// «За всё время | Последние … дней | С … по …».
//
// It replaced five fields that each said part of this and interacted out of
// sight — a base window, a period start, a last-order date pair, the same
// pair again in days, and a first-order pair — which is how a period starting
// on 1 January was quietly cut by a 270-day window nobody had set.
//
// "The last N days" says which dates it is, under the number: the server
// counts an order N days old as inside the period (`today − date <= N`), so
// the first day shown is today − N, both ends included — N + 1 calendar days
// (`lastDays`, the same arithmetic the query is built with). A relative
// period is kept as N and resolved each time it is used; a range is kept as
// dates.
//
// Moving between the modes carries the same days over, so switching from
// "last 90 days" to dates shows those 90 days as dates rather than starting
// from nothing.

export function PeriodControl({
  value,
  onChange,
  kinds = ['all', 'last', 'range'],
  unavailable = 'disable',
  ariaLabel,
  today = kyivToday(),
}: {
  value: PeriodValue
  onChange: (next: PeriodValue) => void
  /** Which modes this period may take right now. */
  kinds?: Array<PeriodValue['kind']>
  /** A mode outside `kinds`: shown disabled (taken by a sibling for now), or
   *  not shown (something this period can never be). */
  unavailable?: 'disable' | 'hide'
  ariaLabel: string
  today?: string
}) {
  const { t } = useTranslation()

  function pick(kind: PeriodValue['kind']) {
    if (kind === value.kind) return
    if (kind === 'all') return onChange({ kind: 'all' })
    if (kind === 'last') {
      // "The last N days" always ends today, so the start is what carries
      // over: 1 Jan – 14 Oct becomes the last N days that start on 1 Jan.
      const days = value.kind === 'range' && isIsoDate(value.from)
        ? Math.max(1, daysBetween(value.from, today))
        : 90
      return onChange({ kind: 'last', days: Math.min(MAX_DAYS, days) })
    }
    const days = value.kind === 'last' && value.days ? value.days : 90
    onChange({ kind: 'range', from: addDays(today, -days), to: today })
  }

  return (
    <Wrapper dir="column" gap="sm">
      <SegmentedControl
        ariaLabel={ariaLabel}
        size="sm"
        value={value.kind}
        onChange={pick}
        options={(['all', 'last', 'range'] as const)
          .filter((k) => unavailable === 'disable' || kinds.includes(k))
          .map((k) => ({
            value: k, label: t(`periodControl.${k}`), disabled: !kinds.includes(k),
          }))}
      />

      {value.kind === 'last' && (
        <Wrapper dir="column" gap="sm">
          <Wrapper dir="row" gap="sm" align="center" wrap>
            <NumberWithUnit
              ariaLabel={t('periodControl.days')}
              value={value.days}
              onChange={(days) => onChange({ kind: 'last', days })}
              min={1}
              max={MAX_DAYS}
              unit={plural(t, 'word.days', value.days ?? 5)}
            />
            <Wrapper dir="row" gap="xs" wrap>
              {DAY_CHIPS.map((d) => (
                <FilterChip
                  key={d}
                  size="md"
                  active={value.days === d}
                  onClick={() => onChange({ kind: 'last', days: d })}
                >
                  {d}
                </FilterChip>
              ))}
            </Wrapper>
          </Wrapper>
          {typeof value.days === 'number' && value.days >= 1 && value.days <= MAX_DAYS && (
            <p className="text-xs text-slate-500 tabular-nums">
              {t('periodControl.resolved', (() => {
                const { from, to } = lastDays(value.days, today)
                return rangeEnds(from, to)
              })())}
            </p>
          )}
        </Wrapper>
      )}

      {value.kind === 'range' && (
        <DateRangeField
          from={value.from}
          to={value.to}
          onChange={({ from, to }) => onChange({ kind: 'range', from, to })}
          fromLabel={t('periodControl.from')}
          toLabel={t('periodControl.to')}
          allowOpen
          describe={(days) => t('periodControl.length', {
            n: days, word: plural(t, 'word.days', days),
          })}
          swappedNote={t('periodControl.swapped')}
        />
      )}
    </Wrapper>
  )
}
