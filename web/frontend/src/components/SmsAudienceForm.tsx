import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import {
  Banknote, ChevronDown, ChevronUp, Plus, SlidersHorizontal, Sparkles,
} from 'lucide-react'
import { Badge } from './Badge'
import { Button } from './Button'
import { Select } from './Select'
import { Wrapper } from './Wrapper'
import { SmsAudienceGroupCard } from './SmsAudienceGroupCard'
import { SmsChoiceChip, SmsFilterFamily, SmsFilterField } from './SmsAudienceControls'
import {
  countAudienceFilters, emptyGroup, MAX_AUDIENCE_GROUPS,
} from '../utils/smsAudience'
import { alphaFor, mdePercentagePoints, verdictFor, type MdeVerdict } from '../utils/mde'
import { formatNumber } from '../utils/formatters'
import type {
  SmsAudienceCriteria, SmsAudienceGroup, SmsAudienceGroupShare, SmsLtvBasis,
  SmsSegment, SmsTier,
} from '../types/api'

// ─── SmsAudienceForm ─────────────────────────────────────────────────────────
//
// The controls that define who gets the message.
//
// Until this existed the audience was six numbers hardcoded in the API and one
// dropdown on screen, so every campaign went to the same cohort and "everyone
// who bought this brand since May" was not a thing anybody could ask for.
//
// Then the filters were one set, ANDed, and "two purchases this year OR a
// purchase in the last three months" could not be said either. So an audience
// is GROUPS now: each group is a set of filters that must all hold, with its
// own window (<SmsAudienceGroupCard>), and a customer gets the message if they
// match any group. "+ Add group" adds the next one.
//
// What is common to the campaign sits below the groups and applies to the
// union: the value level, the ranking basis, and — folded away — how the
// result is measured.

const TIERS: SmsTier[] = ['VIP', 'CORE', 'REACTIVATION']

const INPUT = `px-3 py-2 text-sm bg-white border border-slate-200 rounded-lg
               text-slate-800 placeholder:text-slate-300
               focus:outline-none focus:ring-2 focus:ring-purple-500/30
               focus:border-purple-400`

export const SmsAudienceForm = memo(function SmsAudienceForm({
  audience,
  onChange,
  segments,
  shares,
  recipients,
}: {
  audience: SmsAudienceCriteria
  onChange: (next: SmsAudienceCriteria) => void
  /** The arms of the audience as it currently stands, for the tier sizes. */
  segments?: SmsSegment[]
  /** Each group's part of the audience, from the same preview. */
  shares?: SmsAudienceGroupShare[]
  /** The de-duplicated total the groups add up to. */
  recipients?: number
}) {
  const { t } = useTranslation()
  // Open. The filters are the reason this page was rebuilt; behind a chevron
  // they read as an advanced corner, and the first person to use the wizard
  // reported there were no date filters at all.
  const [open, setOpen] = useState(true)
  const [measureOpen, setMeasureOpen] = useState(false)

  const active = countAudienceFilters(audience)
  const several = audience.groups.length > 1
  // Shares belong to the groups they were counted for. While a new preview is
  // on its way after a group was added or removed, the old ones would be
  // pinned to the wrong cards — and the old total, counted for one group,
  // would read as the union's — so both are shown only when the counts match.
  const shown = several && shares?.length === audience.groups.length ? shares : undefined

  function setGroup(index: number, next: SmsAudienceGroup) {
    onChange({
      ...audience,
      groups: audience.groups.map((g, i) => (i === index ? next : g)),
    })
  }

  function addGroup() {
    if (audience.groups.length >= MAX_AUDIENCE_GROUPS) return
    onChange({ ...audience, groups: [...audience.groups, emptyGroup()] })
  }

  function removeGroup(index: number) {
    onChange({ ...audience, groups: audience.groups.filter((_, i) => i !== index) })
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

  return (
    <Wrapper dir="column" gap="lg">
      {/* ── Who: the groups ──────────────────────────────────────────── */}
      <div>
        <button
          type="button"
          onClick={() => setOpen(!open)}
          aria-expanded={open}
          className="flex items-center gap-1.5 text-sm font-medium text-slate-700
                     hover:text-slate-900"
        >
          {open ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          {t('sms.filtersTitle')}
          {active > 0 && <Badge tone="purple">{active}</Badge>}
        </button>
        <p className="mt-1 text-xs text-slate-500">
          {t(several ? 'sms.groupsHint' : 'sms.filtersHint')}
        </p>

        {open && (
          <Wrapper dir="column" gap="md" marginTop="md">
            {audience.groups.map((group, i) => (
              <Wrapper key={i} dir="column" gap="md">
                {i > 0 && (
                  <Wrapper dir="row" gap="sm" align="center">
                    <span className="h-px flex-1 bg-slate-200" aria-hidden />
                    <span className="text-xs font-semibold uppercase tracking-wide
                                     text-purple-700">
                      {t('sms.groupsOr')}
                    </span>
                    <span className="h-px flex-1 bg-slate-200" aria-hidden />
                  </Wrapper>
                )}
                <SmsAudienceGroupCard
                  index={i}
                  group={group}
                  onChange={(next) => setGroup(i, next)}
                  onRemove={i > 0 ? () => removeGroup(i) : undefined}
                  share={shown?.[i]}
                />
              </Wrapper>
            ))}

            <Wrapper dir="row" gap="sm" align="center" wrap>
              <Button
                variant="secondary" size="sm" onClick={addGroup}
                disabled={audience.groups.length >= MAX_AUDIENCE_GROUPS}
              >
                <Plus className="w-3.5 h-3.5" /> {t('sms.groupAdd')}
              </Button>
              <span className="text-xs text-slate-500">
                {audience.groups.length >= MAX_AUDIENCE_GROUPS
                  ? t('sms.groupMax', { max: MAX_AUDIENCE_GROUPS })
                  : t('sms.groupAddHint')}
              </span>
            </Wrapper>

            {shown && recipients != null && (
              <p className="text-sm text-slate-700 bg-purple-50 border border-purple-100
                            rounded-lg px-3 py-2 tabular-nums">
                {t('sms.groupsTotal', { total: formatNumber(recipients) })}
              </p>
            )}
          </Wrapper>
        )}
      </div>

      {/* ── For the whole campaign ───────────────────────────────────── */}
      {/* The level and the basis read the union, not a group: "VIP only" is
          the VIPs among everyone the groups selected, ranked one way. */}
      <SmsFilterFamily
        icon={SlidersHorizontal}
        title={t('sms.commonTitle')}
        hint={t('sms.commonHint')}
      >
        <SmsFilterField
          icon={Sparkles}
          label={t('sms.levelLabel')}
          example={audience.tiers.length === 0 ? t('sms.levelAllHint') : undefined}
        >
          <div className="flex flex-wrap gap-2" role="group" aria-label={t('sms.levelLabel')}>
            {TIERS.map((tier) => {
              const size = segments?.find((s) => s.tier === tier)?.target
              return (
                <SmsChoiceChip
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
                </SmsChoiceChip>
              )
            })}
          </div>
        </SmsFilterField>

        <SmsFilterField
          icon={Banknote}
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
        </SmsFilterField>
      </SmsFilterFamily>

      {/* ── Measurement, folded away ─────────────────────────────────── */}
      {/* Off the manager's path on purpose. Building a campaign is: who gets
          it, what it says, send. Arms, thresholds and multiplicity corrections
          are an analyst's questions, and putting them in the middle of that
          path is what made the sequence unreadable — filters, then somehow
          three tiers. One switch, phrased as what it gets you and what it
          costs, and everything else stays folded. */}
      <div>
        <button
          type="button"
          onClick={() => setMeasureOpen(!measureOpen)}
          aria-expanded={measureOpen}
          className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700"
        >
          {measureOpen ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          {t('sms.measureTitle')}
          {audience.grouping === 'rfm' && <Badge tone="purple">{t('sms.measureOn')}</Badge>}
        </button>

        {measureOpen && (
          <div className="mt-3 rounded-lg border border-slate-200 bg-slate-50/70 p-4
                          flex flex-col gap-3">
            <SmsFilterField
              icon={Banknote}
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
            </SmsFilterField>
            <label className="flex items-start gap-2.5 cursor-pointer">
              <input
                type="checkbox"
                className="mt-0.5"
                checked={audience.grouping === 'rfm'}
                onChange={(e) =>
                  onChange({ ...audience, grouping: e.target.checked ? 'rfm' : 'single' })}
              />
              <span>
                <span className="text-sm font-medium text-slate-800">
                  {t('sms.measureSplit')}
                </span>
                <span className="block text-xs text-slate-500 leading-snug mt-0.5">
                  {t('sms.measureSplitHint')}
                </span>
              </span>
            </label>

            {sensitivity.length > 0 && (
              <div>
                <p className="text-xs font-medium text-slate-700">{t('sms.mdeLabel')}</p>
                <ul className="mt-1 space-y-1">
                  {sensitivity.map((arm) => (
                    <li key={arm.tier} className="flex items-baseline gap-2 text-sm">
                      <span className="text-slate-600 w-28">{t(`sms.tier.${arm.tier}`)}</span>
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
          </div>
        )}
      </div>
    </Wrapper>
  )
})
