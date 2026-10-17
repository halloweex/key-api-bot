import { memo, useMemo, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { Plus } from 'lucide-react'
import { AudienceGroupCard } from './AudienceGroupCard'
import { Button } from './Button'
import { FilterChip } from './FilterChip'
import { TemplateChips } from './TemplateChips'
import { Wrapper } from './Wrapper'
import { useBrands, useCategories } from '../hooks/useApi'
import { useIsPhone } from '../hooks/useMediaQuery'
import {
  applyTemplate, emptyConditionGroup, newId, TEMPLATE_KEYS,
  type AudienceDefinition, type ConditionGroup, type TemplateKey,
} from '../utils/smsConditions'
import { MAX_AUDIENCE_GROUPS } from '../utils/smsAudience'
import { formatNumber } from '../utils/formatters'
import type { SmsAudienceGroupShare, SmsSegment, SmsTier } from '../types/api'

// ─── SmsAudienceForm ─────────────────────────────────────────────────────────
//
// Step 1's body: who the campaign is for.
//
// It was a form with every field of every filter showing in every group —
// about fifteen inputs a group, three screens with two groups — and the owner
// got lost in it on the first real campaign. It is now built the way the
// segment builders people already know are (Klaviyo, Shopify, Braze):
//
// 1. «С чего начнём?» — a template, a past campaign or a saved audience,
//    each of which fills the groups with ordinary, editable conditions;
// 2. the groups: each one sentences joined by «и», and the groups joined by
//    «ИЛИ» — a customer is in the audience if they match any group;
// 3. what is common to the whole campaign: the value level as a filter.
//
// How the result is measured — the split by value level, its thresholds, the
// ranking basis — is step 2's, beside the control group.

const LEVELS: SmsTier[] = ['VIP', 'CORE', 'REACTIVATION']

function duplicate(group: ConditionGroup): ConditionGroup {
  return {
    id: newId('g'),
    conditions: group.conditions.map((c) => ({ ...c, id: newId() })),
  }
}

export const SmsAudienceForm = memo(function SmsAudienceForm({
  audience,
  onChange,
  shares,
  segments,
  library,
}: {
  audience: AudienceDefinition
  onChange: (next: AudienceDefinition) => void
  /** Each group's part of the audience, from the latest preview. */
  shares?: SmsAudienceGroupShare[]
  /** The arms of the latest preview, for the size beside each level. */
  segments?: SmsSegment[]
  /** "Like a past campaign" and the saved audiences, after the templates. */
  library?: ReactNode
}) {
  const { t } = useTranslation()
  const phone = useIsPhone()
  const { data: brands } = useBrands()
  const { data: categories } = useCategories()
  const [editingId, setEditingId] = useState<string | null>(null)

  const brandOptions = useMemo(
    () => (brands ?? []).map((b) => ({ value: b.name, label: b.name })),
    [brands],
  )
  const categoryOptions = useMemo(
    () => (categories ?? []).map((c) => ({ value: String(c.id), label: c.name })),
    [categories],
  )

  const groups = audience.groups
  // Shares belong to the groups they were counted for. While a new preview is
  // on its way after a group was added or removed they would be pinned to the
  // wrong cards, so they are shown only when the counts line up.
  const shown = shares?.length === groups.length ? shares : undefined

  function setGroup(index: number, next: ConditionGroup) {
    onChange({ ...audience, groups: groups.map((g, i) => (i === index ? next : g)) })
  }

  // A template replaces every group. One tap on the wrong chip must not cost
  // an audience that took ten decisions, so the one before stays a tap away.
  const [beforeTemplate, setBeforeTemplate] = useState<AudienceDefinition | null>(null)

  function pickTemplate(key: string) {
    const { definition, focus } = applyTemplate(key as TemplateKey, audience)
    const hadConditions = audience.groups.some((g) => g.conditions.length > 0)
    setBeforeTemplate(hadConditions ? (beforeTemplate ?? audience) : null)
    onChange(definition)
    setEditingId(focus ?? null)
  }

  const canAddGroup = groups.length < MAX_AUDIENCE_GROUPS
  const firstHasConditions = (groups[0]?.conditions.length ?? 0) > 0

  return (
    <Wrapper dir="column" gap="lg">
      <TemplateChips
        title={t('sms.tpl.title')}
        items={TEMPLATE_KEYS.map((key) => ({
          key, label: t(`sms.tpl.${key}`), hint: t(`sms.tpl.${key}Hint`),
        }))}
        onPick={pickTemplate}
        extra={(
          <Wrapper dir="column" gap="sm">
            {library}
            {beforeTemplate && (
              <Wrapper dir="row" gap="sm" align="center" wrap>
                <span className="text-xs text-slate-600">{t('sms.tpl.replaced')}</span>
                <Button size="sm" variant="ghost" onClick={() => {
                  onChange(beforeTemplate)
                  setBeforeTemplate(null)
                  setEditingId(null)
                }}>
                  {t('sms.tpl.undo')}
                </Button>
              </Wrapper>
            )}
          </Wrapper>
        )}
      />

      <Wrapper dir="column" gap="sm">
        {groups.map((group, i) => (
          <Wrapper key={group.id} dir="column" gap="sm">
            {i > 0 && (
              <div className="flex items-center gap-3" role="separator"
                   aria-label={t('sms.groupsOrLabel')}>
                <span className="h-px flex-1 bg-purple-200" aria-hidden />
                <span className="rounded-full bg-purple-100 px-3 py-0.5 text-xs font-bold
                                 uppercase tracking-wider text-purple-800">
                  {t('sms.groupsOr')}
                </span>
                <span className="h-px flex-1 bg-purple-200" aria-hidden />
              </div>
            )}
            <AudienceGroupCard
              index={i}
              group={group}
              onChange={(next) => setGroup(i, next)}
              onRemove={groups.length > 1
                ? () => onChange({ ...audience, groups: groups.filter((_, j) => j !== i) })
                : undefined}
              onDuplicate={canAddGroup && group.conditions.length > 0
                ? () => onChange({
                    ...audience,
                    groups: [...groups.slice(0, i + 1), duplicate(group), ...groups.slice(i + 1)],
                  })
                : undefined}
              share={shown?.[i]}
              single={groups.length === 1}
              editingId={editingId}
              onEditingChange={setEditingId}
              phone={phone}
              brandOptions={brandOptions}
              categoryOptions={categoryOptions}
            />
          </Wrapper>
        ))}

        {firstHasConditions && (
          <Wrapper dir="row" gap="sm" align="center" wrap>
            <Button
              variant="secondary" size="sm"
              disabled={!canAddGroup}
              onClick={() => onChange({ ...audience, groups: [...groups, emptyConditionGroup()] })}
            >
              <Plus className="h-3.5 w-3.5" aria-hidden /> {t('sms.groupAdd')}
            </Button>
            <span className="text-xs text-slate-500">
              {canAddGroup
                ? t('sms.groupAddHint')
                : t('sms.groupMax', { max: MAX_AUDIENCE_GROUPS })}
            </span>
          </Wrapper>
        )}
      </Wrapper>

      {/* Applied to everybody the groups selected, after one SMS per phone:
          "VIPs only" is the VIPs among the union, not a condition of one
          group. */}
      <section aria-label={t('sms.commonTitle')}
               className="rounded-xl border border-slate-200 bg-white p-3 sm:p-4">
        <Wrapper dir="column" gap="sm">
          <h3 className="text-sm font-semibold text-slate-800">
            {t('sms.commonTitle')}
            <span className="font-normal text-slate-500"> · {t('sms.levelLabel')}</span>
          </h3>
          <div role="group" aria-label={t('sms.levelLabel')}>
            <Wrapper dir="row" gap="xs" wrap>
              {LEVELS.map((tier) => {
                const size = segments?.find((s) => s.tier === tier)?.target
                return (
                  <FilterChip
                    key={tier}
                    size="md"
                    active={audience.tiers.includes(tier)}
                    onClick={() => onChange({
                      ...audience,
                      tiers: audience.tiers.includes(tier)
                        ? audience.tiers.filter((x) => x !== tier)
                        : [...audience.tiers, tier],
                    })}
                  >
                    {t(`sms.tier.${tier}`)}
                    {size != null && ` · ${formatNumber(size)}`}
                  </FilterChip>
                )
              })}
            </Wrapper>
          </div>
          <p className="text-xs text-slate-500">
            {audience.tiers.length === 0 ? t('sms.levelAllHint') : t('sms.levelHint')}
          </p>
        </Wrapper>
      </section>
    </Wrapper>
  )
})
