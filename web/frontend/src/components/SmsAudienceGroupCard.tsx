import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Banknote, CalendarCheck, CalendarClock, CalendarRange, Clock, Layers, MapPin,
  Receipt, ShieldCheck, ShoppingBag, Store, Tag, Ticket, Trash2, UserPlus, Users,
  VenusAndMars,
} from 'lucide-react'
import { Button } from './Button'
import { Input } from './Input'
import { Select } from './Select'
import { Wrapper } from './Wrapper'
import {
  SmsChoiceChip, SmsFilterFamily, SmsFilterField, SmsMultiPicker, SmsNumberRange,
} from './SmsAudienceControls'
import { useBrands, useCategories } from '../hooks/useApi'
import {
  countFilters, dateToDaysAgo, daysAgoToDate, MAX_WINDOW_DAYS, MIN_WINDOW_DAYS,
  periodReachDays,
} from '../utils/smsAudience'
import { formatNumber } from '../utils/formatters'
import type {
  SmsAudienceFilters, SmsAudienceGroup, SmsAudienceGroupShare,
} from '../types/api'

// ─── SmsAudienceGroupCard ────────────────────────────────────────────────────
//
// One group of an audience: its own window and its own filters, every one of
// which must hold. An audience is one or more of these, and a customer gets
// the message if they match ANY group — which is how "two purchases this
// year, or anyone who bought in the last three months" is said at all.
//
// The filters are the questions actually asked when a promotion is planned:
// what they bought, how much they are worth, how long since they last came
// back, and where they are. Every field states its own example under it,
// because the page is used a few times a month by somebody who does not hold
// this model in their head.
//
// Things that belong to the whole campaign — the value level, the ranking
// basis, the split, the control — are not here: a group decides who, the
// campaign decides how they are measured.

const SOURCES: Array<{ id: number; label: string }> = [
  // The active sources, as Silver classifies them. Opencart (3) is deprecated
  // and never reaches revenue, so offering it would be offering an empty set.
  { id: 1, label: 'Instagram' },
  { id: 2, label: 'Telegram' },
  { id: 4, label: 'Shopify' },
  { id: 5, label: 'Виставка' },
]

type PeriodMode = 'since' | 'within'

function clampWindow(days: number): number {
  return Math.min(MAX_WINDOW_DAYS, Math.max(MIN_WINDOW_DAYS, Math.round(days)))
}

export const SmsAudienceGroupCard = memo(function SmsAudienceGroupCard({
  index,
  group,
  onChange,
  onRemove,
  share,
}: {
  /** 0-based position; the card is titled with it, counted from one. */
  index: number
  group: SmsAudienceGroup
  onChange: (next: SmsAudienceGroup) => void
  /** Absent for a group that cannot be removed — the first one. */
  onRemove?: () => void
  /** This group's part of the audience, once a preview has run with groups. */
  share?: SmsAudienceGroupShare
}) {
  const { t } = useTranslation()
  const { data: brands } = useBrands()
  const { data: categories } = useCategories()

  const filters = group.filters
  // Which way the period is said. Held here rather than derived alone,
  // because "in the last N days" picked before N is typed sets nothing, and
  // deriving it from the filters would flip the control back under the
  // cursor.
  const [periodMode, setPeriodMode] = useState<PeriodMode>(
    filters.periodDays != null ? 'within' : 'since',
  )
  // What is being typed into the window, until it is a window. Clamping on
  // every keystroke turned "365" into 3 → 30 → 306 → 730: the box can only
  // hold a number in range once the typing is done.
  const [windowDraft, setWindowDraft] = useState<string | null>(null)

  function setFilter<K extends keyof SmsAudienceFilters>(
    key: K, value: SmsAudienceFilters[K],
  ) {
    const next = { ...filters }
    if (value === undefined || value === '' || value === null
        || (Array.isArray(value) && !value.length)) {
      delete next[key]
    } else {
      next[key] = value
    }
    onChange({ ...group, filters: next })
  }

  function pickPeriodMode(mode: PeriodMode) {
    setPeriodMode(mode)
    // One period, said one way: the server refuses both at once.
    const next = { ...filters }
    delete next[mode === 'since' ? 'periodDays' : 'periodFrom']
    onChange({ ...group, filters: next })
  }

  const brandOptions = useMemo(
    () => (brands ?? []).map((b) => ({ value: b.name, label: b.name })),
    [brands],
  )
  const categoryOptions = useMemo(
    () => (categories ?? []).map((c) => ({ value: String(c.id), label: c.name })),
    [categories],
  )

  const reach = periodReachDays(filters)
  const active = countFilters(filters)

  return (
    <section
      className="rounded-xl border border-slate-200 bg-white p-4"
      aria-label={t('sms.groupTitle', { n: index + 1 })}
    >
      <Wrapper dir="column" gap="md">
        {/* ── Which group, and what it brings ─────────────────────────── */}
        <Wrapper dir="row" gap="sm" align="center" justify="between" wrap>
          <Wrapper dir="row" gap="sm" align="baseline" wrap>
            <h3 className="text-sm font-semibold text-slate-800">
              {t('sms.groupTitle', { n: index + 1 })}
            </h3>
            {share && share.matched === 0 && (
              <span className="text-xs text-amber-700">{t('sms.groupMatchesNobody')}</span>
            )}
            {share && share.matched > 0 && share.recipients === 0 && (
              <span className="text-xs text-amber-700">
                {t('sms.groupUnreachable', { matched: formatNumber(share.matched) })}
              </span>
            )}
            {share && share.recipients > 0 && (
              <span className="text-xs text-slate-500 tabular-nums">
                {t('sms.groupShare', {
                  recipients: formatNumber(share.recipients),
                  only: formatNumber(share.onlyHere),
                })}
              </span>
            )}
          </Wrapper>
          {onRemove && (
            <Button variant="secondary" size="sm" onClick={onRemove}>
              <Trash2 className="w-3.5 h-3.5" /> {t('sms.groupRemove')}
            </Button>
          )}
        </Wrapper>

        {/* ── When they bought ─────────────────────────────────────────── */}
        <SmsFilterFamily
          icon={Clock}
          title={t('sms.filterGroupWhen')}
          hint={t('sms.filterGroupWhenHint')}
        >
          <SmsFilterField
            icon={CalendarRange}
            label={t('sms.filterWindow')}
            example={t('sms.filterWindowHint')}
          >
            <div className="flex items-center gap-2">
              <Input
                type="number" width="narrow"
                min={MIN_WINDOW_DAYS} max={MAX_WINDOW_DAYS}
                aria-label={t('sms.filterWindow')}
                value={windowDraft ?? group.maxRecencyDays}
                onChange={(raw) => {
                  setWindowDraft(raw)
                  const days = Number(raw)
                  if (raw !== '' && Number.isInteger(days)
                      && days >= MIN_WINDOW_DAYS && days <= MAX_WINDOW_DAYS) {
                    onChange({ ...group, maxRecencyDays: days })
                  }
                }}
                onBlur={() => {
                  if (windowDraft === null) return
                  const days = Number(windowDraft)
                  // Clamped rather than refused: an out-of-range window is
                  // a 422 from the server and an empty screen here.
                  if (windowDraft !== '' && Number.isFinite(days)) {
                    onChange({ ...group, maxRecencyDays: clampWindow(days) })
                  }
                  setWindowDraft(null)
                }}
              />
              <span className="text-xs text-slate-500">{t('sms.unitDays')}</span>
            </div>
          </SmsFilterField>

          <SmsFilterField
            icon={CalendarCheck}
            label={t('sms.filterPeriod')}
            example={t('sms.filterPeriodHint')}
          >
            <Wrapper dir="column" gap="sm">
              <div className="flex items-center gap-2">
                <span className="text-xs text-slate-600">{t('sms.periodAtLeast')}</span>
                <Input
                  type="number" width="narrow" min={1} placeholder="1"
                  aria-label={`${t('sms.filterPeriod')} ${t('sms.periodAtLeast')}`}
                  value={filters.periodOrdersMin ?? ''}
                  onChange={(raw) =>
                    setFilter('periodOrdersMin', raw === '' ? undefined : Number(raw))}
                />
                <span className="text-xs text-slate-500">{t('sms.unitOrders')}</span>
              </div>
              {/* The way the period is said on a line of its own: beside the
                  value, in a third of the row, it was cut to "since a dat". */}
              <Select
                options={[
                  { value: 'since', label: t('sms.periodModeSince') },
                  { value: 'within', label: t('sms.periodModeWithin') },
                ]}
                value={periodMode}
                onChange={(v) => pickPeriodMode((v as PeriodMode) || 'since')}
                allowEmpty={false}
                aria-label={t('sms.filterPeriod')}
              />
              {periodMode === 'since' ? (
                <Input
                  type="date" width="full"
                  aria-label={t('sms.periodModeSince')}
                  value={filters.periodFrom ?? ''}
                  onChange={(v) => setFilter('periodFrom', v)}
                />
              ) : (
                <div className="flex items-center gap-2">
                  <Input
                    type="number" width="narrow" min={1} placeholder="92"
                    aria-label={t('sms.periodModeWithin')}
                    value={filters.periodDays ?? ''}
                    onChange={(raw) =>
                      setFilter('periodDays', raw === '' ? undefined : Number(raw))}
                  />
                  <span className="text-xs text-slate-500">{t('sms.unitDays')}</span>
                </div>
              )}
            </Wrapper>
          </SmsFilterField>

          <SmsFilterField
            icon={CalendarClock}
            label={t('sms.filterLastOrder')}
            example={
              filters.recencyMin != null || filters.recencyMax != null
                ? t('sms.filterLastOrderAsDays', {
                    from: filters.recencyMin ?? 0,
                    to: filters.recencyMax ?? '∞',
                  })
                : t('sms.filterLastOrderHint')
            }
          >
            <div className="flex items-center gap-2">
              <Input
                type="date" width="full"
                aria-label={t('sms.filterLastOrderFrom')}
                value={daysAgoToDate(filters.recencyMax)}
                onChange={(v) => setFilter('recencyMax', dateToDaysAgo(v))}
              />
              <span className="text-slate-400 text-sm">—</span>
              <Input
                type="date" width="full"
                aria-label={t('sms.filterLastOrderTo')}
                value={daysAgoToDate(filters.recencyMin)}
                onChange={(v) => setFilter('recencyMin', dateToDaysAgo(v))}
              />
            </div>
          </SmsFilterField>

          <SmsFilterField
            icon={Clock}
            label={t('sms.filterRecency')}
            example={t('sms.filterRecencyHint')}
          >
            <SmsNumberRange
              label={t('sms.filterRecency')}
              unit={t('sms.unitDays')}
              fromPlaceholder="90" toPlaceholder="270"
              from={filters.recencyMin} to={filters.recencyMax}
              onFrom={(v) => setFilter('recencyMin', v)}
              onTo={(v) => setFilter('recencyMax', v)}
            />
          </SmsFilterField>

          <SmsFilterField
            icon={UserPlus}
            label={t('sms.filterFirstOrder')}
            example={t('sms.exFirstOrder')}
          >
            <div className="flex items-center gap-2">
              <Input
                type="date" width="full"
                aria-label={t('sms.filterFirstOrderFrom')}
                value={filters.firstOrderFrom ?? ''}
                onChange={(v) => setFilter('firstOrderFrom', v)}
              />
              <span className="text-slate-400 text-sm">—</span>
              <Input
                type="date" width="full"
                aria-label={t('sms.filterFirstOrderTo')}
                value={filters.firstOrderTo ?? ''}
                onChange={(v) => setFilter('firstOrderTo', v)}
              />
            </div>
          </SmsFilterField>
        </SmsFilterFamily>

        {/* The two contradictions a group's time rules can produce. Asking
            for people quieter than the window returns nobody, and a period
            reaching past the window is quietly cut by it — "two orders since
            January" inside 270 days loses everybody last seen in January. */}
        {filters.recencyMin != null && filters.recencyMin >= group.maxRecencyDays && (
          <p className="text-sm text-amber-800 bg-amber-50 border border-amber-100
                        rounded-lg px-3 py-2">
            {t('sms.windowConflict', {
              window: group.maxRecencyDays, min: filters.recencyMin,
            })}
          </p>
        )}
        {reach != null && reach > group.maxRecencyDays && (
          <p className="text-sm text-amber-800 bg-amber-50 border border-amber-100
                        rounded-lg px-3 py-2">
            {t('sms.periodWindowConflict', { reach, window: group.maxRecencyDays })}
          </p>
        )}

        {/* ── How much they bought ─────────────────────────────────────── */}
        <SmsFilterFamily
          icon={Banknote}
          title={t('sms.filterGroupHowMuch')}
          hint={t('sms.filterGroupHowMuchHint')}
        >
          <SmsFilterField icon={ShoppingBag} label={t('sms.filterOrders')}
                          example={t('sms.exOrders')}>
            <SmsNumberRange
              label={t('sms.filterOrders')}
              unit={t('sms.unitOrders')}
              fromPlaceholder="2" toPlaceholder="10"
              from={filters.ordersMin} to={filters.ordersMax}
              onFrom={(v) => setFilter('ordersMin', v)}
              onTo={(v) => setFilter('ordersMax', v)}
            />
          </SmsFilterField>

          <SmsFilterField icon={Banknote} label={t('sms.filterLtv')}
                          example={t('sms.exLtv')}>
            <SmsNumberRange
              label={t('sms.filterLtv')}
              unit="₴"
              fromPlaceholder="5000" toPlaceholder="50000"
              from={filters.ltvMin} to={filters.ltvMax}
              onFrom={(v) => setFilter('ltvMin', v)}
              onTo={(v) => setFilter('ltvMax', v)}
              step={100}
            />
          </SmsFilterField>

          <SmsFilterField icon={Receipt} label={t('sms.filterAov')}
                          example={t('sms.exAov')}>
            <SmsNumberRange
              label={t('sms.filterAov')}
              unit="₴"
              fromPlaceholder="1000" toPlaceholder="5000"
              from={filters.aovMin} to={filters.aovMax}
              onFrom={(v) => setFilter('aovMin', v)}
              onTo={(v) => setFilter('aovMax', v)}
              step={100}
            />
          </SmsFilterField>
        </SmsFilterFamily>

        {/* ── What they bought ─────────────────────────────────────────── */}
        <SmsFilterFamily
          icon={ShoppingBag}
          title={t('sms.filterGroupWhat')}
          hint={t('sms.filterGroupWhatHint')}
        >
          <SmsFilterField icon={Tag} label={t('sms.filterBrand')}
                          example={t('sms.exBrand')}>
            <SmsMultiPicker
              label={t('sms.filterBrand')}
              options={brandOptions}
              values={filters.brands ?? []}
              onChange={(v) => setFilter('brands', v)}
              placeholder={t('sms.filterBrandPlaceholder')}
            />
          </SmsFilterField>

          <SmsFilterField icon={Layers} label={t('sms.filterCategory')}
                          example={t('sms.exCategory')}>
            <SmsMultiPicker
              label={t('sms.filterCategory')}
              options={categoryOptions}
              values={(filters.categoryIds ?? []).map(String)}
              onChange={(v) => setFilter('categoryIds', v.map(Number))}
              placeholder={t('sms.filterCategoryPlaceholder')}
            />
          </SmsFilterField>

          <SmsFilterField icon={Store} label={t('sms.filterSource')}
                          example={t('sms.exSource')}>
            <div className="flex flex-wrap gap-2">
              {SOURCES.map((s) => (
                <SmsChoiceChip
                  key={s.id}
                  active={(filters.sourceIds ?? []).includes(s.id)}
                  onClick={() => {
                    const current = filters.sourceIds ?? []
                    setFilter(
                      'sourceIds',
                      current.includes(s.id)
                        ? current.filter((x) => x !== s.id)
                        : [...current, s.id],
                    )
                  }}
                >
                  {s.label}
                </SmsChoiceChip>
              ))}
            </div>
          </SmsFilterField>

          <SmsFilterField icon={Ticket} label={t('sms.filterPromocode')}
                          example={t('sms.exPromocode')}>
            <Input
              type="text" width="full" placeholder="KS-AUG" maxLength={40}
              aria-label={t('sms.filterPromocode')}
              value={filters.promocodeUsed ?? ''}
              onChange={(v) => setFilter('promocodeUsed', v)}
            />
          </SmsFilterField>

          {/* Sits inside this family because it qualifies only this family:
              it is "bought that, recently", not a rule of its own. */}
          <SmsFilterField icon={CalendarClock} label={t('sms.filterBoughtWithin')}
                          example={t('sms.filterBoughtWithinHint')}>
            <div className="flex items-center gap-2">
              <Input
                type="number" width="narrow" placeholder="180"
                aria-label={t('sms.filterBoughtWithin')}
                value={filters.boughtWithinDays ?? ''}
                onChange={(raw) =>
                  setFilter('boughtWithinDays', raw === '' ? undefined : Number(raw))}
              />
              <span className="text-xs text-slate-500">{t('sms.unitDays')}</span>
            </div>
          </SmsFilterField>
        </SmsFilterFamily>

        {/* ── Who they are ─────────────────────────────────────────────── */}
        <SmsFilterFamily
          icon={Users}
          title={t('sms.filterGroupWho')}
          hint={t('sms.filterGroupWhoHint')}
        >
          <SmsFilterField icon={VenusAndMars} label={t('sms.filterGender')}
                          example={t('sms.exGender')}>
            <div className="flex flex-wrap gap-2" role="group"
                 aria-label={t('sms.filterGender')}>
              {(['f', 'm'] as const).map((g) => (
                <SmsChoiceChip
                  key={g}
                  active={(filters.genders ?? []).includes(g)}
                  onClick={() => {
                    const on = filters.genders ?? []
                    setFilter('genders', on.includes(g) ? on.filter((x) => x !== g) : [...on, g])
                  }}
                >
                  {t(g === 'f' ? 'sms.genderFemale' : 'sms.genderMale')}
                </SmsChoiceChip>
              ))}
            </div>
            {(filters.genders ?? []).length > 0 && (
              <p className="mt-1.5 text-xs text-slate-500">{t('sms.genderNullNote')}</p>
            )}
          </SmsFilterField>

          <SmsFilterField icon={ShieldCheck} label={t('sms.filterGenderConfidence')}
                          example={t('sms.exGenderConfidence')}>
            <Select
              options={[
                { value: 'medium', label: t('sms.genderConfAny') },
                { value: 'high', label: t('sms.genderConfHigh') },
                { value: 'certain', label: t('sms.genderConfCertain') },
              ]}
              value={filters.genderMinConfidence ?? ''}
              onChange={(v) =>
                setFilter('genderMinConfidence', (v as 'certain' | 'high' | 'medium') || null)}
              aria-label={t('sms.filterGenderConfidence')}
            />
          </SmsFilterField>

          <SmsFilterField icon={MapPin} label={t('sms.filterCity')}
                          example={t('sms.exCity')}>
            <Input
              type="text" width="full"
              placeholder={t('sms.filterCityPlaceholder')}
              aria-label={t('sms.filterCity')}
              value={(filters.cities ?? []).join(', ')}
              onChange={(v) =>
                setFilter('cities', v.split(',').map((c) => c.trim()).filter(Boolean))}
            />
          </SmsFilterField>
        </SmsFilterFamily>

        {active > 0 && (
          <div>
            <Button
              variant="secondary" size="sm"
              onClick={() => onChange({ ...group, filters: {} })}
            >
              {t('sms.clearFilters')}
            </Button>
          </div>
        )}
      </Wrapper>
    </section>
  )
})
