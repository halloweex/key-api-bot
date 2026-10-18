import { useTranslation } from 'react-i18next'
import { DateRangeField } from './DateRangeField'
import { FilterChip } from './FilterChip'
import { Input } from './Input'
import { MultiPick, type PickOption } from './MultiPick'
import { NumberWithUnit } from './NumberWithUnit'
import { PeriodControl } from './PeriodControl'
import { Select } from './Select'
import { Wrapper } from './Wrapper'
import {
  allowedPeriodKinds, DAY_CHIPS, MAX_DAYS, MAX_ORDERS, SOURCES,
  type Condition, type CountOp, type OrdersCondition, type WhenValue,
} from '../utils/smsConditions'
import { plural, sourceLabel, type T } from '../utils/smsConditionText'
import { daysAgoRange, formatDay, kyivToday, lastDays, rangeEnds } from '../utils/smsDates'

// ─── SmsConditionEditor ──────────────────────────────────────────────────────
//
// The inputs of one condition: what the blanks of its sentence are made of.
// Drawn under the sentence on desktop and inside a bottom sheet on a phone —
// the same inputs either way, stacked, each with the native control that fits
// it: a number pad with the unit beside it, the phone's own date picker, a
// segmented choice, a searchable list.
//
// Every edit is passed up as it is made. The caller decides when it counts:
// live on desktop, when the sheet closes on a phone.

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <Wrapper dir="column" gap="xs">
      <span className="text-xs font-medium text-slate-500">{label}</span>
      {children}
    </Wrapper>
  )
}

const COUNT_OPS: CountOp[] = ['atLeast', 'exactly', 'atMost', 'between', 'none']

function OrdersEditor({
  c, siblings, onChange,
}: { c: OrdersCondition; siblings: Condition[]; onChange: (c: Condition) => void }) {
  const { t } = useTranslation()
  const kinds = allowedPeriodKinds(c, siblings)
  // "None at all, ever" is nobody — every customer here has ordered — so it
  // is offered only for a period.
  const ops = c.period.kind === 'all' ? COUNT_OPS.filter((op) => op !== 'none') : COUNT_OPS

  function setOp(op: CountOp) {
    onChange({
      ...c, op,
      count: op === 'none' ? null : c.count ?? 1,
      countMax: op === 'between' ? Math.max(c.countMax ?? 0, (c.count ?? 1) + 1) : null,
    })
  }

  return (
    <Wrapper dir="column" gap="md">
      <Field label={t('sms.cond.edit.howMany')}>
        <Wrapper dir="row" gap="sm" align="center" wrap>
          <Select
            aria-label={t('sms.cond.edit.op')}
            variant="compact"
            allowEmpty={false}
            value={c.op}
            onChange={(v) => setOp((v as CountOp) || 'atLeast')}
            options={ops.map((op) => ({ value: op, label: t(`sms.cond.op.${op}`) }))}
          />
          {c.op !== 'none' && (
            <NumberWithUnit
              ariaLabel={t('sms.cond.edit.count')}
              stepper
              min={1}
              max={MAX_ORDERS}
              value={c.count}
              onChange={(count) => onChange({ ...c, count })}
              unit={c.op === 'between' ? undefined
                : plural(t, c.op === 'atMost' ? 'word.ordersGen' : 'word.orders', c.count ?? 5)}
              decreaseLabel={t('sms.cond.edit.less')}
              increaseLabel={t('sms.cond.edit.more')}
            />
          )}
          {c.op === 'between' && (
            <NumberWithUnit
              ariaLabel={t('sms.cond.edit.countMax')}
              stepper
              min={1}
              max={MAX_ORDERS}
              value={c.countMax}
              onChange={(countMax) => onChange({ ...c, countMax })}
              unit={plural(t, 'word.ordersGen', c.countMax ?? 5)}
              decreaseLabel={t('sms.cond.edit.less')}
              increaseLabel={t('sms.cond.edit.more')}
            />
          )}
        </Wrapper>
      </Field>
      <Field label={t('sms.cond.edit.when')}>
        <PeriodControl
          ariaLabel={t('sms.cond.edit.when')}
          kinds={kinds}
          value={c.period}
          onChange={(period) => onChange({
            ...c, period,
            ...(period.kind === 'all' && c.op === 'none' ? { op: 'atLeast', count: 1 } : {}),
          })}
        />
        {kinds.length < 3 && (
          <p className="text-xs text-slate-500">{t('sms.cond.edit.periodTaken')}</p>
        )}
      </Field>
    </Wrapper>
  )
}

type WhenKind = WhenValue['kind']

const isDays = (n: number | null | undefined, min = 1): n is number =>
  typeof n === 'number' && Number.isInteger(n) && n >= min && n <= MAX_DAYS

/**
 * Which dates a relative "when" is today, in the words under a period's
 * number — computed with `lastDays`/`daysAgoRange`, the arithmetic the
 * conditions are sent with, so the line cannot say one edge and the query
 * count another. Null while the number is not one the query would take.
 */
function resolvedWhen(value: WhenValue, today: string, t: T) {
  if (value.kind === 'within' && isDays(value.days)) {
    const { from, to } = lastDays(value.days, today)
    return t('periodControl.resolved', rangeEnds(from, to))
  }
  if (value.kind === 'before' && isDays(value.days)) {
    const { to } = daysAgoRange(value.days, null, today)
    return t('periodControl.resolvedUpTo', { to: formatDay(to!, { month: 'long' }) })
  }
  if (value.kind === 'ago') {
    const lo = isDays(value.min, 0) ? value.min : null
    const hi = isDays(value.max, 0) ? value.max : null
    if (lo == null && hi == null) return null
    if (lo != null && hi != null && lo > hi) return null
    const { from, to } = daysAgoRange(lo, hi, today)
    if (from && to) return t('periodControl.resolved', rangeEnds(from, to))
    if (to) return t('periodControl.resolvedUpTo', { to: formatDay(to, { month: 'long' }) })
    return t('periodControl.resolved', rangeEnds(from!, today))
  }
  return null
}

function WhenEditor({
  value, onChange,
}: { value: WhenValue; onChange: (w: WhenValue) => void }) {
  const { t } = useTranslation()
  const today = kyivToday()

  function setKind(kind: WhenKind) {
    if (kind === value.kind) return
    const days = value.kind === 'within' || value.kind === 'before' ? value.days
      : value.kind === 'ago' ? value.max ?? value.min : null
    if (kind === 'within' || kind === 'before') onChange({ kind, days: days ?? 90 })
    else if (kind === 'ago') onChange({ kind, min: 90, max: 180 })
    else onChange({ kind, from: '', to: today })
  }

  const dayChips = (current: number | null, set: (d: number) => void) => (
    <Wrapper dir="row" gap="xs" wrap>
      {DAY_CHIPS.map((d) => (
        <FilterChip key={d} size="md" active={current === d} onClick={() => set(d)}>{d}</FilterChip>
      ))}
    </Wrapper>
  )

  return (
    <Wrapper dir="column" gap="sm">
      <Select
        aria-label={t('sms.cond.edit.whenMode')}
        allowEmpty={false}
        value={value.kind}
        onChange={(v) => setKind((v as WhenKind) || 'within')}
        options={(['within', 'before', 'ago', 'dates'] as const).map((k) => ({
          value: k, label: t(`sms.cond.mode.${k}`),
        }))}
      />
      {(value.kind === 'within' || value.kind === 'before') && (
        <Wrapper dir="row" gap="sm" align="center" wrap>
          <NumberWithUnit
            ariaLabel={t('sms.cond.edit.days')}
            min={1}
            max={MAX_DAYS}
            value={value.days}
            onChange={(days) => onChange({ kind: value.kind, days })}
            unit={value.kind === 'before'
              ? t('sms.cond.edit.daysAgoOrMore', { word: plural(t, 'word.days', value.days ?? 5) })
              : plural(t, 'word.days', value.days ?? 5)}
          />
          {dayChips(value.days, (days) => onChange({ kind: value.kind, days }))}
        </Wrapper>
      )}
      {value.kind === 'ago' && (
        <Wrapper dir="row" gap="sm" align="center" wrap>
          <span className="text-sm text-slate-600">{t('sms.cond.edit.from')}</span>
          <NumberWithUnit
            ariaLabel={t('sms.cond.edit.daysMin')}
            min={0}
            max={MAX_DAYS}
            value={value.min}
            onChange={(min) => onChange({ ...value, min })}
          />
          <span className="text-sm text-slate-600">{t('sms.cond.edit.to')}</span>
          <NumberWithUnit
            ariaLabel={t('sms.cond.edit.daysMax')}
            min={0}
            max={MAX_DAYS}
            value={value.max}
            onChange={(max) => onChange({ ...value, max })}
            unit={t('sms.cond.edit.daysAgo', { word: plural(t, 'word.days', value.max ?? 5) })}
          />
        </Wrapper>
      )}
      {resolvedWhen(value, today, t) && (
        <p className="text-xs text-slate-500 tabular-nums">{resolvedWhen(value, today, t)}</p>
      )}
      {value.kind === 'dates' && (
        <DateRangeField
          from={value.from}
          to={value.to}
          onChange={({ from, to }) => onChange({ kind: 'dates', from, to })}
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

function MoneyEditor({
  min, max, onChange,
}: { min: number | null; max: number | null; onChange: (v: { min: number | null; max: number | null }) => void }) {
  const { t } = useTranslation()
  return (
    <Wrapper dir="row" gap="sm" align="center" wrap>
      <span className="text-sm text-slate-600">{t('sms.cond.edit.from')}</span>
      <NumberWithUnit
        ariaLabel={t('sms.cond.edit.moneyMin')}
        prefix="₴" step={100} placeholder="5000"
        value={min}
        onChange={(v) => onChange({ min: v, max })}
      />
      <span className="text-sm text-slate-600">{t('sms.cond.edit.to')}</span>
      <NumberWithUnit
        ariaLabel={t('sms.cond.edit.moneyMax')}
        prefix="₴" step={100} placeholder="50000"
        value={max}
        onChange={(v) => onChange({ min, max: v })}
      />
    </Wrapper>
  )
}

export function SmsConditionEditor({
  condition: c,
  siblings,
  onChange,
  brandOptions,
  categoryOptions,
}: {
  condition: Condition
  /** The other conditions of the group — which period an orders count may take. */
  siblings: Condition[]
  onChange: (next: Condition) => void
  brandOptions: PickOption[]
  categoryOptions: PickOption[]
}) {
  const { t } = useTranslation()
  const pickProps = {
    emptyText: t('sms.cond.edit.nothingFound'),
    removeLabel: (label: string) => t('sms.cond.edit.remove', { name: label }),
  }

  switch (c.type) {
    case 'orders':
      return <OrdersEditor c={c} siblings={siblings} onChange={onChange} />
    case 'lastOrder':
    case 'firstOrder':
      return <WhenEditor value={c.when} onChange={(when) => onChange({ ...c, when })} />
    case 'spent':
    case 'aov':
      return (
        <Wrapper dir="column" gap="xs">
          <MoneyEditor min={c.min} max={c.max} onChange={(v) => onChange({ ...c, ...v })} />
          <p className="text-xs text-slate-500">{t(`sms.cond.edit.${c.type}Hint`)}</p>
        </Wrapper>
      )
    case 'bought':
      return (
        <Wrapper dir="column" gap="md">
          <Field label={t('sms.cond.edit.brands')}>
            <MultiPick
              ariaLabel={t('sms.cond.edit.brands')}
              placeholder={t('sms.cond.edit.searchBrand')}
              options={brandOptions}
              values={c.brands}
              onChange={(brands) => onChange({ ...c, brands })}
              {...pickProps}
            />
          </Field>
          <Field label={t('sms.cond.edit.categories')}>
            <MultiPick
              ariaLabel={t('sms.cond.edit.categories')}
              placeholder={t('sms.cond.edit.searchCategory')}
              options={categoryOptions}
              values={c.categoryIds.map(String)}
              onChange={(ids) => onChange({ ...c, categoryIds: ids.map(Number) })}
              {...pickProps}
            />
          </Field>
          <Field label={t('sms.cond.edit.promocode')}>
            <Input
              type="text" width="full" placeholder="KS-AUG" maxLength={40}
              aria-label={t('sms.cond.edit.promocode')}
              value={c.promocode}
              onChange={(promocode) => onChange({ ...c, promocode })}
            />
          </Field>
          <Field label={t('sms.cond.edit.when')}>
            <PeriodControl
              ariaLabel={t('sms.cond.edit.when')}
              kinds={['all', 'last']}
              unavailable="hide"
              value={c.withinDays == null ? { kind: 'all' } : { kind: 'last', days: c.withinDays }}
              onChange={(p) => onChange({
                ...c, withinDays: p.kind === 'last' ? p.days : null,
              })}
            />
          </Field>
          <p className="text-xs text-slate-500">{t('sms.cond.edit.boughtHint')}</p>
        </Wrapper>
      )
    case 'source':
      return (
        <Wrapper dir="column" gap="sm">
          <div role="group" aria-label={t('sms.cond.label.source')}>
            <Wrapper dir="row" gap="xs" wrap>
              {SOURCES.map((id) => (
                <FilterChip
                  key={id} size="md"
                  active={c.sourceIds.includes(id)}
                  onClick={() => onChange({
                    ...c,
                    sourceIds: c.sourceIds.includes(id)
                      ? c.sourceIds.filter((x) => x !== id)
                      : [...c.sourceIds, id],
                  })}
                >
                  {sourceLabel(id, t)}
                </FilterChip>
              ))}
            </Wrapper>
          </div>
          {siblings.some((x) => x.type === 'bought') && (
            <p className="text-xs text-slate-500">{t('sms.cond.edit.sourceWithBought')}</p>
          )}
        </Wrapper>
      )
    case 'city':
      return (
        <MultiPick
          ariaLabel={t('sms.cond.label.city')}
          placeholder={t('sms.cond.edit.typeCity')}
          options={[]}
          values={c.cities}
          onChange={(cities) => onChange({ ...c, cities })}
          allowCustom
          addLabel={(v) => t('sms.cond.edit.addValue', { value: v })}
          {...pickProps}
          emptyText={t('sms.cond.edit.typeCityHint')}
        />
      )
    case 'gender':
      return (
        <Wrapper dir="column" gap="md">
          <div role="group" aria-label={t('sms.cond.label.gender')}>
            <Wrapper dir="row" gap="xs" wrap>
              {(['f', 'm'] as const).map((g) => (
                <FilterChip
                  key={g} size="md"
                  active={c.genders.includes(g)}
                  onClick={() => onChange({
                    ...c,
                    genders: c.genders.includes(g)
                      ? c.genders.filter((x) => x !== g)
                      : [...c.genders, g],
                  })}
                >
                  {t(g === 'f' ? 'sms.genderFemale' : 'sms.genderMale')}
                </FilterChip>
              ))}
            </Wrapper>
          </div>
          <Field label={t('sms.filterGenderConfidence')}>
            {/* No floor and `medium` select the same people once a gender is
                picked (a NULL verdict never matches it), so both read as "any
                decided verdict"; a stored `medium` is shown as itself. */}
            <Select
              aria-label={t('sms.filterGenderConfidence')}
              allowEmpty={false}
              value={c.confidence ?? 'medium'}
              onChange={(v) => onChange({
                ...c, confidence: v === 'certain' || v === 'high' || v === 'medium' ? v : null,
              })}
              options={[
                { value: 'medium', label: t('sms.genderConfAny') },
                { value: 'high', label: t('sms.genderConfHigh') },
                { value: 'certain', label: t('sms.genderConfCertain') },
              ]}
            />
          </Field>
          <p className="text-xs text-slate-500">{t('sms.genderNullNote')}</p>
        </Wrapper>
      )
  }
}
