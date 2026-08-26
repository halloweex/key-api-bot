import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronUp, X } from 'lucide-react'
import { Badge } from './Badge'
import { Button } from './Button'
import { Select } from './Select'
import { useBrands, useCategories } from '../hooks/useApi'
import { countFilters } from '../utils/smsAudience'
import type {
  SmsAudienceCriteria, SmsAudienceFilters, SmsGrouping, SmsLtvBasis, SmsTier,
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

const SOURCES: Array<{ id: number; label: string }> = [
  // The active sources, as Silver classifies them. Opencart (3) is deprecated
  // and never reaches revenue, so offering it would be offering an empty set.
  { id: 1, label: 'Instagram' },
  { id: 2, label: 'Telegram' },
  { id: 4, label: 'Shopify' },
  { id: 5, label: 'Виставка' },
]

const TIERS: SmsTier[] = ['VIP', 'CORE', 'REACTIVATION']

function Chip({
  active, onClick, children, title,
}: {
  active: boolean
  onClick: () => void
  children: React.ReactNode
  title?: string
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      title={title}
      onClick={onClick}
      className={`px-3 py-1.5 text-xs rounded-md border transition-colors tabular-nums ${
        active
          ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
          : 'border-slate-200 text-slate-600 hover:border-slate-300'
      }`}
    >
      {children}
    </button>
  )
}

function NumberPair({
  label, hint, from, to, onFrom, onTo, step,
}: {
  label: string
  hint?: string
  from: number | null | undefined
  to: number | null | undefined
  onFrom: (v: number | undefined) => void
  onTo: (v: number | undefined) => void
  step?: number
}) {
  const parse = (raw: string) => (raw === '' ? undefined : Number(raw))
  const cls = `w-24 px-2 py-1.5 text-sm bg-white border border-slate-200 rounded-md
               text-slate-700 focus:outline-none focus:ring-2 focus:ring-purple-500/30
               focus:border-purple-400 tabular-nums`

  return (
    <div>
      <span className="text-xs text-slate-600">{label}</span>
      <div className="mt-1 flex items-center gap-2">
        <input
          type="number" step={step} className={cls} aria-label={`${label} min`}
          value={from ?? ''} onChange={(e) => onFrom(parse(e.target.value))}
        />
        <span className="text-slate-400 text-xs">—</span>
        <input
          type="number" step={step} className={cls} aria-label={`${label} max`}
          value={to ?? ''} onChange={(e) => onTo(parse(e.target.value))}
        />
      </div>
      {hint && <p className="mt-1 text-[11px] text-slate-500">{hint}</p>}
    </div>
  )
}

/** A picker that turns a list into removable chips. */
function MultiPicker({
  label, options, values, onChange, placeholder,
}: {
  label: string
  options: Array<{ value: string; label: string }>
  values: string[]
  onChange: (next: string[]) => void
  placeholder: string
}) {
  const remaining = options.filter((o) => !values.includes(o.value))

  return (
    <div>
      <span className="text-xs text-slate-600">{label}</span>
      <div className="mt-1">
        <Select
          options={remaining}
          value=""
          onChange={(v) => v && onChange([...values, v])}
          placeholder={placeholder}
          variant="compact"
          aria-label={label}
        />
      </div>
      {values.length > 0 && (
        <div className="mt-1.5 flex flex-wrap gap-1.5">
          {values.map((v) => (
            <span
              key={v}
              className="inline-flex items-center gap-1 px-2 py-0.5 text-[11px] rounded
                         bg-slate-100 text-slate-700"
            >
              {options.find((o) => o.value === v)?.label ?? v}
              <button
                type="button"
                aria-label={`remove ${v}`}
                onClick={() => onChange(values.filter((x) => x !== v))}
                className="text-slate-400 hover:text-slate-700"
              >
                <X className="w-3 h-3" />
              </button>
            </span>
          ))}
        </div>
      )}
    </div>
  )
}

export const SmsAudienceForm = memo(function SmsAudienceForm({
  audience,
  onChange,
}: {
  audience: SmsAudienceCriteria
  onChange: (next: SmsAudienceCriteria) => void
}) {
  const { t } = useTranslation()
  const [open, setOpen] = useState(false)
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

  const brandOptions = useMemo(
    () => (brands ?? []).map((b) => ({ value: b.name, label: b.name })),
    [brands],
  )
  const categoryOptions = useMemo(
    () => (categories ?? []).map((c) => ({ value: String(c.id), label: c.name })),
    [categories],
  )

  const text = `px-2 py-1.5 text-sm bg-white border border-slate-200 rounded-md
                text-slate-700 focus:outline-none focus:ring-2 focus:ring-purple-500/30
                focus:border-purple-400`

  return (
    <div className="space-y-3">
      {/* ── How the audience is split ────────────────────────────────── */}
      <div className="flex flex-wrap items-end gap-4">
        <div>
          <span className="text-xs text-slate-600">{t('sms.groupingLabel')}</span>
          <div className="mt-1 flex gap-2" role="group" aria-label={t('sms.groupingLabel')}>
            {(['rfm', 'single'] as SmsGrouping[]).map((g) => (
              <Chip
                key={g}
                active={audience.grouping === g}
                onClick={() => onChange({ ...audience, grouping: g })}
              >
                {t(`sms.grouping.${g}`)}
              </Chip>
            ))}
          </div>
        </div>

        <div>
          <span className="text-xs text-slate-600">{t('sms.basisLabel')}</span>
          <div className="mt-1">
            <Select
              options={[
                { value: 'margin', label: t('sms.basisMargin') },
                { value: 'revenue', label: t('sms.basisRevenue') },
              ]}
              value={audience.ltvBasis}
              onChange={(v) =>
                onChange({ ...audience, ltvBasis: (v as SmsLtvBasis) || 'margin' })}
              allowEmpty={false}
              variant="compact"
              aria-label={t('sms.basisLabel')}
            />
          </div>
        </div>
      </div>

      <p className="text-[11px] text-slate-500 leading-snug">
        {t(`sms.groupingHint.${audience.grouping}`)}
      </p>

      {/* ── Which tiers, when there are tiers ────────────────────────── */}
      {audience.grouping === 'rfm' && (
        <div>
          <span className="text-xs text-slate-600">{t('sms.exportTiers')}</span>
          <div className="mt-1 flex flex-wrap gap-2" role="group" aria-label={t('sms.exportTiers')}>
            {TIERS.map((tier) => (
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
              </Chip>
            ))}
          </div>
          {audience.tiers.length === 0 && (
            <p className="mt-1 text-[11px] text-slate-500">{t('sms.allTiersHint')}</p>
          )}
        </div>
      )}

      {/* ── The filters themselves ───────────────────────────────────── */}
      <div className="pt-2 border-t border-slate-100">
        <button
          type="button"
          onClick={() => setOpen(!open)}
          aria-expanded={open}
          className="flex items-center gap-1.5 text-xs text-slate-600 hover:text-slate-800"
        >
          {open ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
          {t('sms.filtersTitle')}
          {active > 0 && <Badge tone="purple">{active}</Badge>}
        </button>

        {open && (
          <div className="mt-3 grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
            <NumberPair
              label={t('sms.filterRecency')}
              hint={t('sms.filterRecencyHint')}
              from={filters.recencyMin} to={filters.recencyMax}
              onFrom={(v) => setFilter('recencyMin', v)}
              onTo={(v) => setFilter('recencyMax', v)}
            />
            <NumberPair
              label={t('sms.filterOrders')}
              from={filters.ordersMin} to={filters.ordersMax}
              onFrom={(v) => setFilter('ordersMin', v)}
              onTo={(v) => setFilter('ordersMax', v)}
            />
            <NumberPair
              label={t('sms.filterLtv')}
              hint={t('sms.filterLtvHint')}
              from={filters.ltvMin} to={filters.ltvMax}
              onFrom={(v) => setFilter('ltvMin', v)}
              onTo={(v) => setFilter('ltvMax', v)}
              step={100}
            />
            <NumberPair
              label={t('sms.filterAov')}
              from={filters.aovMin} to={filters.aovMax}
              onFrom={(v) => setFilter('aovMin', v)}
              onTo={(v) => setFilter('aovMax', v)}
              step={100}
            />

            <div>
              <span className="text-xs text-slate-600">{t('sms.filterFirstOrder')}</span>
              <div className="mt-1 flex items-center gap-2">
                <input
                  type="date" className={text} aria-label={t('sms.filterFirstOrderFrom')}
                  value={filters.firstOrderFrom ?? ''}
                  onChange={(e) => setFilter('firstOrderFrom', e.target.value)}
                />
                <span className="text-slate-400 text-xs">—</span>
                <input
                  type="date" className={text} aria-label={t('sms.filterFirstOrderTo')}
                  value={filters.firstOrderTo ?? ''}
                  onChange={(e) => setFilter('firstOrderTo', e.target.value)}
                />
              </div>
            </div>

            <div>
              <span className="text-xs text-slate-600">{t('sms.filterCity')}</span>
              <input
                type="text"
                className={`${text} mt-1 w-full`}
                placeholder={t('sms.filterCityPlaceholder')}
                aria-label={t('sms.filterCity')}
                value={(filters.cities ?? []).join(', ')}
                onChange={(e) =>
                  setFilter(
                    'cities',
                    e.target.value.split(',').map((c) => c.trim()).filter(Boolean),
                  )}
              />
            </div>

            <MultiPicker
              label={t('sms.filterBrand')}
              options={brandOptions}
              values={filters.brands ?? []}
              onChange={(v) => setFilter('brands', v)}
              placeholder={t('sms.filterBrandPlaceholder')}
            />

            <MultiPicker
              label={t('sms.filterCategory')}
              options={categoryOptions}
              values={(filters.categoryIds ?? []).map(String)}
              onChange={(v) => setFilter('categoryIds', v.map(Number))}
              placeholder={t('sms.filterCategoryPlaceholder')}
            />

            <div>
              <span className="text-xs text-slate-600">{t('sms.filterSource')}</span>
              <div className="mt-1 flex flex-wrap gap-2">
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
            </div>

            <div>
              <span className="text-xs text-slate-600">{t('sms.filterPromocode')}</span>
              <input
                type="text"
                className={`${text} mt-1 w-full`}
                placeholder="KS-AUG"
                maxLength={40}
                aria-label={t('sms.filterPromocode')}
                value={filters.promocodeUsed ?? ''}
                onChange={(e) => setFilter('promocodeUsed', e.target.value)}
              />
            </div>

            <div>
              <span className="text-xs text-slate-600">{t('sms.filterBoughtWithin')}</span>
              <input
                type="number"
                className={`${text} mt-1 w-24 tabular-nums`}
                aria-label={t('sms.filterBoughtWithin')}
                value={filters.boughtWithinDays ?? ''}
                onChange={(e) =>
                  setFilter(
                    'boughtWithinDays',
                    e.target.value === '' ? undefined : Number(e.target.value),
                  )}
              />
              <p className="mt-1 text-[11px] text-slate-500">
                {t('sms.filterBoughtWithinHint')}
              </p>
            </div>

            {active > 0 && (
              <div className="sm:col-span-2 lg:col-span-3">
                <Button
                  variant="secondary" size="sm"
                  onClick={() => onChange({ ...audience, filters: {} })}
                >
                  {t('sms.clearFilters')}
                </Button>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  )
})
