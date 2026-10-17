import { useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { ChevronDown, ChevronUp } from 'lucide-react'
import { Checkbox } from './Checkbox'
import { NumberWithUnit } from './NumberWithUnit'
import { SegmentedControl } from './SegmentedControl'
import { Select } from './Select'
import { Wrapper } from './Wrapper'
import { alphaFor, mdePercentagePoints, verdictFor, type MdeVerdict } from '../utils/mde'
import { formatNumber } from '../utils/formatters'
import { tierRuleProblems, type AudienceDefinition } from '../utils/smsConditions'
import type { SmsLtvBasis, SmsSegment, SmsTier, SmsTierRules } from '../types/api'

// ─── SmsMeasurementSettings ──────────────────────────────────────────────────
//
// How the campaign will be measured: the control group, whether the result is
// read per value level, the cut-offs of those levels, and what "value" ranks.
//
// These sat in the audience step until 2026-10 and were half of why it was
// hard to read: "compare by value level" and three thresholds beside the
// conditions read as more conditions. They decide how the result is read, not
// who gets the message, so they live here, beside the holdout they share a
// trade-off with — and what the split can still prove is said where it is
// chosen.
//
// The value level as a FILTER ("VIPs only") stays in the audience step: it
// decides who is in the audience at all.

const HOLDOUT_CHOICES = ['10', '20', '30', '40'] as const
const TIERS: SmsTier[] = ['VIP', 'CORE', 'REACTIVATION']

export function SmsMeasurementSettings({
  audience,
  onChange,
  segments,
  target,
  holdout,
}: {
  audience: AudienceDefinition
  onChange: (next: AudienceDefinition) => void
  /** The arms as the preview last counted them. */
  segments?: SmsSegment[]
  target: number
  holdout: number
}) {
  const { t } = useTranslation()
  const [advanced, setAdvanced] = useState(false)
  const rules = audience.tierRules
  // A threshold the server would refuse is left out of the query, not sent —
  // and said so here, the one place it can be fixed.
  const ignored = tierRuleProblems(audience)
  // The thresholds the server applies to a blank one; they differ by basis.
  const defaults = audience.ltvBasis === 'margin'
    ? { vip: 5500, core: 2750 }
    : { vip: 10000, core: 5000 }

  function setRule(key: keyof SmsTierRules, value: number | null) {
    const next = { ...rules }
    if (value == null) delete next[key]
    else next[key] = value
    onChange({ ...audience, tierRules: next })
  }

  // What this split can prove. Precision comes from the smaller arm, so a
  // three-way split of a small audience measures nothing.
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
      <Wrapper dir="column" gap="sm">
        <p className="text-xs leading-snug text-slate-500">{t('sms.holdoutExplain')}</p>
        {/* A share outside the usual four (an old preset, a past campaign)
            is shown as one more choice rather than as nothing selected. */}
        <SegmentedControl
          ariaLabel={t('sms.holdoutLabel')}
          value={String(audience.holdoutPct)}
          onChange={(v) => onChange({ ...audience, holdoutPct: Number(v) })}
          options={[...new Set([...HOLDOUT_CHOICES, String(audience.holdoutPct)])]
            .sort((a, b) => Number(a) - Number(b))
            .map((pct) => ({ value: pct, label: `${pct}%` }))}
        />
        <p className="text-[11px] text-slate-500 tabular-nums">
          {t('sms.holdoutHint', { target: formatNumber(target), holdout: formatNumber(holdout) })}
        </p>
      </Wrapper>

      <label className="flex cursor-pointer items-start gap-2.5">
        <span className="mt-0.5">
          <Checkbox
            checked={audience.grouping === 'rfm'}
            onChange={(on) => onChange({ ...audience, grouping: on ? 'rfm' : 'single' })}
          />
        </span>
        <span>
          <span className="text-sm font-medium text-slate-800">{t('sms.measureSplit')}</span>
          <span className="mt-0.5 block text-xs leading-snug text-slate-500">
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
                <span className="w-28 text-slate-600">{t(`sms.tier.${arm.tier}`)}</span>
                <span className={`font-medium tabular-nums ${
                  arm.verdict === 'good' ? 'text-green-700'
                    : arm.verdict === 'tight' ? 'text-amber-700' : 'text-red-700'
                }`}>
                  {t('sms.mdeValue', { pp: arm.mde.toFixed(2) })}
                </span>
              </li>
            ))}
          </ul>
          <p className="mt-1.5 text-xs leading-snug text-slate-500">
            {t(worst === 'hopeless' ? 'sms.mdeHopeless'
               : worst === 'tight' ? 'sms.mdeTight' : 'sms.mdeGood')}
          </p>
        </div>
      )}

      <div>
        <button
          type="button"
          onClick={() => setAdvanced(!advanced)}
          aria-expanded={advanced}
          className="flex items-center gap-1.5 text-sm text-slate-500 hover:text-slate-700"
        >
          {advanced ? <ChevronUp className="h-4 w-4" aria-hidden />
                    : <ChevronDown className="h-4 w-4" aria-hidden />}
          {t('sms.measure.advanced')}
        </button>
        {advanced && (
          <div className="mt-3 flex flex-col gap-4 rounded-lg border border-slate-200
                          bg-slate-50/70 p-4">
            <Wrapper dir="column" gap="xs">
              <span className="text-sm font-medium text-slate-700">{t('sms.basisLabel')}</span>
              <Select
                aria-label={t('sms.basisLabel')}
                allowEmpty={false}
                value={audience.ltvBasis}
                onChange={(v) => onChange({ ...audience, ltvBasis: (v as SmsLtvBasis) || 'margin' })}
                options={[
                  { value: 'margin', label: t('sms.basisMargin') },
                  { value: 'revenue', label: t('sms.basisRevenue') },
                ]}
              />
              <p className="text-xs text-slate-500">{t('sms.basisHint')}</p>
            </Wrapper>

            <Wrapper dir="column" gap="sm">
              <span className="text-sm font-medium text-slate-700">{t('sms.tierRulesLabel')}</span>
              <p className="text-xs text-slate-500">{t('sms.tierRulesHint')}</p>
              {TIERS.map((tier) => (
                <Wrapper key={tier} dir="row" gap="sm" align="center" wrap>
                  <span className="w-28 text-xs text-slate-600">{t(`sms.tier.${tier}`)}</span>
                  {tier === 'VIP' && (
                    <NumberWithUnit
                      ariaLabel={`${t('sms.tier.VIP')} ${t('sms.filterLtv')}`}
                      prefix="₴" step={100} placeholder={String(defaults.vip)}
                      value={rules.vipLtv ?? null}
                      onChange={(v) => setRule('vipLtv', v)}
                    />
                  )}
                  {tier === 'CORE' && (
                    <>
                      <NumberWithUnit
                        ariaLabel={`${t('sms.tier.CORE')} ${t('sms.filterOrders')}`}
                        min={2} max={50} placeholder="2"
                        value={rules.coreMinOrders ?? null}
                        onChange={(v) => setRule('coreMinOrders', v)}
                        unit={t('sms.orRule')}
                      />
                      <NumberWithUnit
                        ariaLabel={`${t('sms.tier.CORE')} ${t('sms.filterLtv')}`}
                        prefix="₴" step={100} placeholder={String(defaults.core)}
                        value={rules.coreLtv ?? null}
                        onChange={(v) => setRule('coreLtv', v)}
                      />
                    </>
                  )}
                  {tier === 'REACTIVATION' && (
                    <NumberWithUnit
                      ariaLabel={`${t('sms.tier.REACTIVATION')} ${t('sms.filterRecency')}`}
                      min={7} max={730} placeholder="120"
                      value={rules.reactivationMaxRecency ?? null}
                      onChange={(v) => setRule('reactivationMaxRecency', v)}
                      unit={t('sms.unitDays')}
                    />
                  )}
                </Wrapper>
              ))}
              {ignored.length > 0 && (
                <p className="text-xs text-amber-800">
                  {t('sms.measure.ruleIgnored', {
                    rules: ignored.map((k) => t(`sms.measure.rule.${k}`)).join(', '),
                  })}
                </p>
              )}
            </Wrapper>
          </div>
        )}
      </div>
    </Wrapper>
  )
}
