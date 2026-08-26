import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Banknote, CalendarClock, CalendarRange, ChevronDown, ChevronUp, Clock,
  Layers, MapPin, Receipt, Ruler, ShoppingBag, Sparkles, Store, Tag, Ticket, UserPlus,
  Users, X,
} from 'lucide-react'
import { Badge } from './Badge'
import { Button } from './Button'
import { Select } from './Select'
import { useBrands, useCategories } from '../hooks/useApi'
import {
  countFilters, DEFAULT_WINDOW_DAYS, MAX_WINDOW_DAYS, MIN_WINDOW_DAYS,
} from '../utils/smsAudience'
import { alphaFor, mdePercentagePoints, verdictFor, type MdeVerdict } from '../utils/mde'
import { formatNumber } from '../utils/formatters'
import type {
  SmsAudienceCriteria, SmsAudienceFilters, SmsGrouping, SmsLtvBasis, SmsSegment,
  SmsTier,
} from '../types/api'

// ─── SmsAudienceForm ─────────────────────────────────────────────────────────
//
// The controls that define who gets the message.
//
// Until this existed the audience was six numbers hardcoded in the API and one
// dropdown on screen, so every campaign went to the same cohort and "everyone
// who bought this brand since May" was not a thing anybody could ask for. The
// filters here are the questions actually asked when a promotion is planned:
// what they bought, how much they are worth, how long since they last came
// back, and where they are.
//
// Two families, and they read the customer differently. The aggregate ones
// (recency, orders, value) narrow who is in the audience. The content ones
// (brand, category, source, promocode) ask whether the customer ever bought a
// particular thing — optionally within a window, which is what makes "since
// May" expressible.
//
// Every field states its own example under it. The filters are used a few
// times a month by somebody who does not hold this model in their head, and
// "Days since last order: 90 — quiet for three months" is the difference
// between a control that is understood and one that is guessed at.

const SOURCES: Array<{ id: number; label: string }> = [
  // The active sources, as Silver classifies them. Opencart (3) is deprecated
  // and never reaches revenue, so offering it would be offering an empty set.
  { id: 1, label: 'Instagram' },
  { id: 2, label: 'Telegram' },
  { id: 4, label: 'Shopify' },
  { id: 5, label: 'Виставка' },
]

const TIERS: SmsTier[] = ['VIP', 'CORE', 'REACTIVATION']

const INPUT = `px-3 py-2 text-sm bg-white border border-slate-200 rounded-lg
               text-slate-800 placeholder:text-slate-300
               focus:outline-none focus:ring-2 focus:ring-purple-500/30
               focus:border-purple-400`

function Chip({
  active, onClick, children,
}: {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={`px-3.5 py-2 text-sm rounded-lg border transition-colors tabular-nums ${
        active
          ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
          : 'border-slate-200 text-slate-600 hover:border-slate-300 hover:bg-slate-50'
      }`}
    >
      {children}
    </button>
  )
}

/** One family of filters, under the question it answers. */
function Group({
  icon, title, hint, children,
}: {
  icon: React.ReactNode
  title: string
  hint: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-lg border border-slate-200 bg-slate-50/40 p-4">
      <div className="flex items-center gap-2">
        <span className="text-slate-400">{icon}</span>
        <h4 className="text-sm font-semibold text-slate-800">{title}</h4>
      </div>
      <p className="text-xs text-slate-500 mt-1 mb-3 leading-snug">{hint}</p>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">{children}</div>
    </section>
  )
}

/** One control, its icon, and the example that says what goes in it. */
function Field({
  icon, label, example, children,
}: {
  icon: React.ReactNode
  label: string
  example?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <div className="flex items-center gap-1.5 mb-1.5">
        <span className="text-slate-400">{icon}</span>
        <span className="text-sm font-medium text-slate-700">{label}</span>
      </div>
      {children}
      {example && (
        <p className="mt-1.5 text-xs text-slate-500 leading-snug">{example}</p>
      )}
    </div>
  )
}

/** A from–to pair. The unit sits beside the boxes, not inside the label. */
function NumberPair({
  label, unit, fromPlaceholder, toPlaceholder, from, to, onFrom, onTo, step,
}: {
  label: string
  unit?: string
  fromPlaceholder?: string
  toPlaceholder?: string
  from: number | null | undefined
  to: number | null | undefined
  onFrom: (v: number | undefined) => void
  onTo: (v: number | undefined) => void
  step?: number
}) {
  const parse = (raw: string) => (raw === '' ? undefined : Number(raw))

  return (
    <div className="flex items-center gap-2">
      <input
        type="number" step={step} className={`${INPUT} w-24 tabular-nums`}
        aria-label={`${label} min`} placeholder={fromPlaceholder}
        value={from ?? ''} onChange={(e) => onFrom(parse(e.target.value))}
      />
      <span className="text-slate-400 text-sm">—</span>
      <input
        type="number" step={step} className={`${INPUT} w-24 tabular-nums`}
        aria-label={`${label} max`} placeholder={toPlaceholder}
        value={to ?? ''} onChange={(e) => onTo(parse(e.target.value))}
      />
      {unit && <span className="text-xs text-slate-500">{unit}</span>}
    </div>
  )
}

/** A picker that turns a list into removable chips. */
function MultiPicker({
  options, values, onChange, placeholder, label,
}: {
  options: Array<{ value: string; label: string }>
  values: string[]
  onChange: (next: string[]) => void
  placeholder: string
  label: string
}) {
  const remaining = options.filter((o) => !values.includes(o.value))

  return (
    <>
      <Select
        options={remaining}
        value=""
        onChange={(v) => v && onChange([...values, v])}
        placeholder={placeholder}
        aria-label={label}
      />
      {values.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {values.map((v) => (
            <span
              key={v}
              className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-md
                         bg-purple-50 text-purple-800 border border-purple-100"
            >
              {options.find((o) => o.value === v)?.label ?? v}
              <button
                type="button"
                aria-label={`remove ${v}`}
                onClick={() => onChange(values.filter((x) => x !== v))}
                className="text-purple-300 hover:text-purple-800"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </span>
          ))}
        </div>
      )}
    </>
  )
}

export const SmsAudienceForm = memo(function SmsAudienceForm({
  audience,
  onChange,
  segments,
}: {
  audience: SmsAudienceCriteria
  onChange: (next: SmsAudienceCriteria) => void
  /** The arms of the audience as it currently stands, for the tier sizes. */
  segments?: SmsSegment[]
}) {
  const { t } = useTranslation()
  // Open. The filters are the reason this page was rebuilt; behind a chevron
  // they read as an advanced corner, and the first person to use the wizard
  // reported there were no date filters at all.
  const [open, setOpen] = useState(true)
  const { data: brands } = useBrands()
  const { data: categories } = useCategories()

  const filters = audience.filters
  const active = countFilters(filters)

  function setFilter<K extends keyof SmsAudienceFilters>(
    key: K, value: SmsAudienceFilters[K],
  ) {
    const next = { ...filters }
    if (value === undefined || value === '' || (Array.isArray(value) && !value.length)) {
      delete next[key]
    } else {
      next[key] = value
    }
    onChange({ ...audience, filters: next })
  }

  const rules = audience.tierRules
  // The thresholds the server would apply if these are left blank. They differ
  // by basis, so the placeholders have to follow the basis rather than state a
  // constant that is wrong half the time.
  const defaults = audience.ltvBasis === 'margin'
    ? { vip: 5500, core: 2750 }
    : { vip: 10000, core: 5000 }

  function setRule(key: keyof typeof rules, raw: string) {
    const next = { ...rules }
    if (raw === '') delete next[key]
    else next[key] = Number(raw)
    onChange({ ...audience, tierRules: next })
  }

  // What this split can prove. Precision comes from the smaller arm, so a
  // three-way split of a small audience measures nothing — and the only place
  // to say that is where the split is chosen.
  const sensitivity = useMemo(() => {
    const arms = (segments ?? []).filter(
      (s) => audience.grouping !== 'rfm' || audience.tiers.length === 0
        || audience.tiers.includes(s.tier),
    )
    const alpha = alphaFor(arms.length)
    return arms
      .map((arm) => {
        const mde = mdePercentagePoints(arm.target, arm.holdout, { alpha })
        return mde == null ? null : { tier: arm.tier, mde, verdict: verdictFor(mde)! }
      })
      .filter((x): x is { tier: SmsTier; mde: number; verdict: MdeVerdict } => x !== null)
  }, [segments, audience.grouping, audience.tiers])

  const worst: MdeVerdict = sensitivity.some((a) => a.verdict === 'hopeless')
    ? 'hopeless'
    : sensitivity.some((a) => a.verdict === 'tight') ? 'tight' : 'good'

  const brandOptions = useMemo(
    () => (brands ?? []).map((b) => ({ value: b.name, label: b.name })),
    [brands],
  )
  const categoryOptions = useMemo(
    () => (categories ?? []).map((c) => ({ value: String(c.id), label: c.name })),
    [categories],
  )

  const icon = 'w-4 h-4'

  return (
    <div className="space-y-4">
      {/* ── The filters themselves ───────────────────────────────────── */}
      <div>
        <button
          type="button"
          onClick={() => setOpen(!open)}
          aria-expanded={open}
          className="flex items-center gap-1.5 text-sm font-medium text-slate-700
                     hover:text-slate-900"
        >
          {open ? <ChevronUp className={icon} /> : <ChevronDown className={icon} />}
          {t('sms.filtersTitle')}
          {active > 0 && <Badge tone="purple">{active}</Badge>}
        </button>
        <p className="mt-1 text-xs text-slate-500">{t('sms.filtersHint')}</p>

        {open && (
          <div className="mt-3 space-y-3">
            {/* ── When they bought ─────────────────────────────────── */}
            <Group
              icon={<Clock className={icon} />}
              title={t('sms.filterGroupWhen')}
              hint={t('sms.filterGroupWhenHint')}
            >
              <Field
                icon={<CalendarRange className={icon} />}
                label={t('sms.filterWindow')}
                example={t('sms.filterWindowHint')}
              >
                <div className="flex items-center gap-2">
                  <input
                    type="number"
                    min={MIN_WINDOW_DAYS}
                    max={MAX_WINDOW_DAYS}
                    className={`${INPUT} w-24 tabular-nums`}
                    aria-label={t('sms.filterWindow')}
                    value={audience.maxRecencyDays}
                    onChange={(e) => {
                      const raw = Number(e.target.value)
                      // Clamped rather than validated: an out-of-range window
                      // is a 422 from the server and an empty screen here.
                      const days = Number.isFinite(raw)
                        ? Math.min(MAX_WINDOW_DAYS, Math.max(MIN_WINDOW_DAYS, raw))
                        : DEFAULT_WINDOW_DAYS
                      onChange({ ...audience, maxRecencyDays: days })
                    }}
                  />
                  <span className="text-xs text-slate-500">{t('sms.unitDays')}</span>
                </div>
              </Field>

              <Field
                icon={<Clock className={icon} />}
                label={t('sms.filterRecency')}
                example={t('sms.filterRecencyHint')}
              >
                <NumberPair
                  label={t('sms.filterRecency')}
                  unit={t('sms.unitDays')}
                  fromPlaceholder="90" toPlaceholder="270"
                  from={filters.recencyMin} to={filters.recencyMax}
                  onFrom={(v) => setFilter('recencyMin', v)}
                  onTo={(v) => setFilter('recencyMax', v)}
                />
              </Field>

              <Field
                icon={<UserPlus className={icon} />}
                label={t('sms.filterFirstOrder')}
                example={t('sms.exFirstOrder')}
              >
                <div className="flex items-center gap-2">
                  <input
                    type="date" className={`${INPUT} w-full`}
                    aria-label={t('sms.filterFirstOrderFrom')}
                    value={filters.firstOrderFrom ?? ''}
                    onChange={(e) => setFilter('firstOrderFrom', e.target.value)}
                  />
                  <span className="text-slate-400 text-sm">—</span>
                  <input
                    type="date" className={`${INPUT} w-full`}
                    aria-label={t('sms.filterFirstOrderTo')}
                    value={filters.firstOrderTo ?? ''}
                    onChange={(e) => setFilter('firstOrderTo', e.target.value)}
                  />
                </div>
              </Field>
            </Group>

            {/* The one contradiction the two time rules can produce: asking for
                people quieter than the window itself returns nobody, and the
                funnel alone would not say why. */}
            {filters.recencyMin != null && filters.recencyMin >= audience.maxRecencyDays && (
              <p className="text-sm text-amber-800 bg-amber-50 border border-amber-100
                            rounded-lg px-3 py-2">
                {t('sms.windowConflict', {
                  window: audience.maxRecencyDays, min: filters.recencyMin,
                })}
              </p>
            )}

            {/* ── How much they bought ─────────────────────────────── */}
            <Group
              icon={<Banknote className={icon} />}
              title={t('sms.filterGroupHowMuch')}
              hint={t('sms.filterGroupHowMuchHint')}
            >
              <Field
                icon={<ShoppingBag className={icon} />}
                label={t('sms.filterOrders')}
                example={t('sms.exOrders')}
              >
                <NumberPair
                  label={t('sms.filterOrders')}
                  unit={t('sms.unitOrders')}
                  fromPlaceholder="2" toPlaceholder="10"
                  from={filters.ordersMin} to={filters.ordersMax}
                  onFrom={(v) => setFilter('ordersMin', v)}
                  onTo={(v) => setFilter('ordersMax', v)}
                />
              </Field>

              <Field
                icon={<Banknote className={icon} />}
                label={t('sms.filterLtv')}
                example={t('sms.exLtv')}
              >
                <NumberPair
                  label={t('sms.filterLtv')}
                  unit="₴"
                  fromPlaceholder="5000" toPlaceholder="50000"
                  from={filters.ltvMin} to={filters.ltvMax}
                  onFrom={(v) => setFilter('ltvMin', v)}
                  onTo={(v) => setFilter('ltvMax', v)}
                  step={100}
                />
              </Field>

              <Field
                icon={<Receipt className={icon} />}
                label={t('sms.filterAov')}
                example={t('sms.exAov')}
              >
                <NumberPair
                  label={t('sms.filterAov')}
                  unit="₴"
                  fromPlaceholder="1000" toPlaceholder="5000"
                  from={filters.aovMin} to={filters.aovMax}
                  onFrom={(v) => setFilter('aovMin', v)}
                  onTo={(v) => setFilter('aovMax', v)}
                  step={100}
                />
              </Field>

              <Field
                icon={<Sparkles className={icon} />}
                label={t('sms.levelLabel')}
                example={
                  audience.tiers.length === 0 ? t('sms.levelAllHint') : undefined
                }
              >
                <div className="flex flex-wrap gap-2" role="group" aria-label={t('sms.levelLabel')}>
                  {TIERS.map((tier) => {
                    const size = segments?.find((s) => s.tier === tier)?.target
                    return (
                      <Chip
                        key={tier}
                        active={audience.tiers.includes(tier)}
                        onClick={() =>
                          onChange({
                            ...audience,
                            tiers: audience.tiers.includes(tier)
                              ? audience.tiers.filter((x) => x !== tier)
                              : [...audience.tiers, tier],
                          })}
                      >
                        {t(`sms.tier.${tier}`)}
                        {size != null && ` · ${formatNumber(size)}`}
                      </Chip>
                    )
                  })}
                </div>
              </Field>

              <Field
                icon={<Banknote className={icon} />}
                label={t('sms.basisLabel')}
                example={t('sms.basisHint')}
              >
                <Select
                  options={[
                    { value: 'margin', label: t('sms.basisMargin') },
                    { value: 'revenue', label: t('sms.basisRevenue') },
                  ]}
                  value={audience.ltvBasis}
                  onChange={(v) =>
                    onChange({ ...audience, ltvBasis: (v as SmsLtvBasis) || 'margin' })}
                  allowEmpty={false}
                  aria-label={t('sms.basisLabel')}
                />
              </Field>

              <Field
                icon={<Banknote className={icon} />}
                label={t('sms.tierRulesLabel')}
                example={t('sms.tierRulesHint')}
              >
                <div className="space-y-2">
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-slate-600 w-24">{t('sms.tier.VIP')}</span>
                    <span className="text-xs text-slate-400">≥</span>
                    <input
                      type="number" step={100} className={`${INPUT} w-28 tabular-nums`}
                      aria-label={`${t('sms.tier.VIP')} ${t('sms.filterLtv')}`}
                      placeholder={String(defaults.vip)}
                      value={rules.vipLtv ?? ''}
                      onChange={(e) => setRule('vipLtv', e.target.value)}
                    />
                    <span className="text-xs text-slate-500">₴</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-slate-600 w-24">{t('sms.tier.CORE')}</span>
                    <span className="text-xs text-slate-400">≥</span>
                    <input
                      type="number" className={`${INPUT} w-16 tabular-nums`}
                      aria-label={`${t('sms.tier.CORE')} ${t('sms.filterOrders')}`}
                      placeholder="2"
                      value={rules.coreMinOrders ?? ''}
                      onChange={(e) => setRule('coreMinOrders', e.target.value)}
                    />
                    <span className="text-xs text-slate-500">{t('sms.orRule')}</span>
                    <input
                      type="number" step={100} className={`${INPUT} w-24 tabular-nums`}
                      aria-label={`${t('sms.tier.CORE')} ${t('sms.filterLtv')}`}
                      placeholder={String(defaults.core)}
                      value={rules.coreLtv ?? ''}
                      onChange={(e) => setRule('coreLtv', e.target.value)}
                    />
                    <span className="text-xs text-slate-500">₴</span>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-slate-600 w-24">
                      {t('sms.tier.REACTIVATION')}
                    </span>
                    <span className="text-xs text-slate-400">≤</span>
                    <input
                      type="number" className={`${INPUT} w-20 tabular-nums`}
                      aria-label={`${t('sms.tier.REACTIVATION')} ${t('sms.filterRecency')}`}
                      placeholder="120"
                      value={rules.reactivationMaxRecency ?? ''}
                      onChange={(e) => setRule('reactivationMaxRecency', e.target.value)}
                    />
                    <span className="text-xs text-slate-500">{t('sms.unitDays')}</span>
                  </div>
                </div>
              </Field>
            </Group>

            {/* ── What they bought ─────────────────────────────────── */}
            <Group
              icon={<ShoppingBag className={icon} />}
              title={t('sms.filterGroupWhat')}
              hint={t('sms.filterGroupWhatHint')}
            >
              <Field
                icon={<Tag className={icon} />}
                label={t('sms.filterBrand')}
                example={t('sms.exBrand')}
              >
                <MultiPicker
                  label={t('sms.filterBrand')}
                  options={brandOptions}
                  values={filters.brands ?? []}
                  onChange={(v) => setFilter('brands', v)}
                  placeholder={t('sms.filterBrandPlaceholder')}
                />
              </Field>

              <Field
                icon={<Layers className={icon} />}
                label={t('sms.filterCategory')}
                example={t('sms.exCategory')}
              >
                <MultiPicker
                  label={t('sms.filterCategory')}
                  options={categoryOptions}
                  values={(filters.categoryIds ?? []).map(String)}
                  onChange={(v) => setFilter('categoryIds', v.map(Number))}
                  placeholder={t('sms.filterCategoryPlaceholder')}
                />
              </Field>

              <Field
                icon={<Store className={icon} />}
                label={t('sms.filterSource')}
                example={t('sms.exSource')}
              >
                <div className="flex flex-wrap gap-2">
                  {SOURCES.map((s) => (
                    <Chip
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
                    </Chip>
                  ))}
                </div>
              </Field>

              <Field
                icon={<Ticket className={icon} />}
                label={t('sms.filterPromocode')}
                example={t('sms.exPromocode')}
              >
                <input
                  type="text"
                  className={`${INPUT} w-full`}
                  placeholder="KS-AUG"
                  maxLength={40}
                  aria-label={t('sms.filterPromocode')}
                  value={filters.promocodeUsed ?? ''}
                  onChange={(e) => setFilter('promocodeUsed', e.target.value)}
                />
              </Field>

              {/* Sits inside this group because it qualifies only this group:
                  it is "bought that, recently", not a rule of its own. */}
              <Field
                icon={<CalendarClock className={icon} />}
                label={t('sms.filterBoughtWithin')}
                example={t('sms.filterBoughtWithinHint')}
              >
                <div className="flex items-center gap-2">
                  <input
                    type="number"
                    className={`${INPUT} w-24 tabular-nums`}
                    placeholder="180"
                    aria-label={t('sms.filterBoughtWithin')}
                    value={filters.boughtWithinDays ?? ''}
                    onChange={(e) =>
                      setFilter(
                        'boughtWithinDays',
                        e.target.value === '' ? undefined : Number(e.target.value),
                      )}
                  />
                  <span className="text-xs text-slate-500">{t('sms.unitDays')}</span>
                </div>
              </Field>
            </Group>

            {/* ── Who they are ─────────────────────────────────────── */}
            <Group
              icon={<Users className={icon} />}
              title={t('sms.filterGroupWho')}
              hint={t('sms.filterGroupWhoHint')}
            >
              <Field
                icon={<MapPin className={icon} />}
                label={t('sms.filterCity')}
                example={t('sms.exCity')}
              >
                <input
                  type="text"
                  className={`${INPUT} w-full`}
                  placeholder={t('sms.filterCityPlaceholder')}
                  aria-label={t('sms.filterCity')}
                  value={(filters.cities ?? []).join(', ')}
                  onChange={(e) =>
                    setFilter(
                      'cities',
                      e.target.value.split(',').map((c) => c.trim()).filter(Boolean),
                    )}
                />
              </Field>
            </Group>

            {active > 0 && (
              <Button
                variant="secondary" size="sm"
                onClick={() => onChange({ ...audience, filters: {} })}
              >
                {t('sms.clearFilters')}
              </Button>
            )}
          </div>
        )}
      </div>

      {/* ── How the result is measured ──────────────────────────────── */}
      {/* Only the measurement question is left here. Choosing a value level is
          a filter — "send to VIP only" — and it moved up with the other money
          questions; splitting the result by level is a different decision with
          a price, stated below in the only currency that matters for it. */}
      <Group
        icon={<Sparkles className={icon} />}
        title={t('sms.splitTitle')}
        hint={t('sms.splitHint')}
      >
        <Field
          icon={<Layers className={icon} />}
          label={t('sms.groupingLabel')}
          example={t(`sms.groupingHint.${audience.grouping}`)}
        >
          <div className="flex gap-2" role="group" aria-label={t('sms.groupingLabel')}>
            {(['single', 'rfm'] as SmsGrouping[]).map((g) => (
              <Chip
                key={g}
                active={audience.grouping === g}
                onClick={() => onChange({ ...audience, grouping: g })}
              >
                {t(`sms.grouping.${g}`)}
              </Chip>
            ))}
          </div>
        </Field>

        {sensitivity.length > 0 && (
          <div className="sm:col-span-2">
            <div className="flex items-center gap-1.5 mb-1.5">
              <span className="text-slate-400"><Ruler className={icon} /></span>
              <span className="text-sm font-medium text-slate-700">
                {t('sms.mdeLabel')}
              </span>
            </div>
            <ul className="space-y-1">
              {sensitivity.map((arm) => (
                <li key={arm.tier} className="flex items-baseline gap-2 text-sm">
                  <span className="text-slate-600 w-28">
                    {t(`sms.tier.${arm.tier}`)}
                  </span>
                  <span
                    className={`tabular-nums font-medium ${
                      arm.verdict === 'good'
                        ? 'text-green-700'
                        : arm.verdict === 'tight' ? 'text-amber-700' : 'text-red-700'
                    }`}
                  >
                    {t('sms.mdeValue', { pp: arm.mde.toFixed(2) })}
                  </span>
                </li>
              ))}
            </ul>
            <p className="mt-1.5 text-xs text-slate-500 leading-snug">
              {t(worst === 'hopeless' ? 'sms.mdeHopeless'
                 : worst === 'tight' ? 'sms.mdeTight' : 'sms.mdeGood')}
            </p>
          </div>
        )}
      </Group>
    </div>
  )
})
