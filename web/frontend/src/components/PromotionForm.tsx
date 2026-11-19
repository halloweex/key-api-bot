import { useId, useMemo, useState, type FormEvent, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { FilterChip } from './FilterChip'
import { Input } from './Input'
import { MultiPick, type PickOption } from './MultiPick'
import { Select } from './Select'
import { Textarea } from './Textarea'
import { Wrapper } from './Wrapper'
import {
  draftFromPromotion,
  draftToPayload,
  emptyDraft,
  LIMITS,
  PROMOTION_CHANNELS,
  PROMOTION_KINDS,
  PROMOTION_SCOPES,
  validateDraft,
  type DraftError,
  type DraftField,
  type PromotionDraft,
} from '../utils/promotions'
import { formatDay, kyivToday } from '../utils/smsDates'
import type { Promotion, PromotionIn, PromotionScope, SmsCampaignSummary } from '../types/api'

// ─── PromotionForm ───────────────────────────────────────────────────────────
//
// «Додати акцію» and «Змінити»: the approved mockup's form. Only what is known
// before a promotion starts — its days, the hour when it does not open in the
// morning, its type, how deep the discount goes and on what, and where it is
// announced. Saving sends the whole promotion; the server writes it as a new
// version, so nothing typed here overwrites what was known before.
//
// The checks repeat the API's (`utils/promotions.validateDraft`) and are
// shown beside their fields once the person has tried to save; whatever the
// server still refuses comes back to the caller, which says it in a toast.

interface PromotionFormProps {
  /** The promotion to change; null adds one. */
  promotion: Promotion | null
  /**
   * The SMS campaigns to fill the form from — undefined when the reader may
   * not see SMS campaigns, and then there is no such choice at all.
   */
  smsCampaigns?: SmsCampaignSummary[]
  brandOptions: PickOption[]
  categoryOptions: PickOption[]
  saving: boolean
  onSubmit: (body: PromotionIn) => void
  onClose: () => void
}

function Field({
  id, label, error, hint, children,
}: {
  id: string
  label: string
  error?: string
  hint?: string
  children: ReactNode
}) {
  return (
    <Wrapper dir="column" gap="xs">
      <label htmlFor={id} className="text-xs font-medium text-slate-600">{label}</label>
      {children}
      {hint && !error && <p className="text-xs text-slate-500">{hint}</p>}
      {error && <p id={`${id}-error`} className="text-xs text-red-600">{error}</p>}
    </Wrapper>
  )
}

/** Two fields side by side from `sm` up, one under the other on a phone. */
function Pair({ children }: { children: ReactNode }) {
  return <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">{children}</div>
}

/** A campaign as the choice says it: its name, its promocode, when it went out. */
function campaignLabel(c: SmsCampaignSummary, t: (k: string, o?: Record<string, unknown>) => string): string {
  const code = c.promocode ? ` (${c.promocode})` : ''
  const when = c.sentAt
    ? t('promo.smsSent', { date: formatDay(kyivToday(new Date(c.sentAt))) })
    : t('promo.smsNotSent')
  return `${c.campaign}${code} · ${when}`
}

export function PromotionForm({
  promotion,
  smsCampaigns,
  brandOptions,
  categoryOptions,
  saving,
  onSubmit,
  onClose,
}: PromotionFormProps) {
  const { t } = useTranslation()
  const ids = useId()
  const id = (field: string) => `${ids}-${field}`
  const [draft, setDraft] = useState<PromotionDraft>(
    () => (promotion ? draftFromPromotion(promotion) : emptyDraft()))
  // Errors are said once a save was tried, and then as they are mended.
  const [tried, setTried] = useState(false)
  const errors = validateDraft(draft)
  const shown = (field: DraftField): string | undefined => {
    const e: DraftError | undefined = tried ? errors[field] : undefined
    return e ? t(e.key, e.params) : undefined
  }
  const invalid = (field: DraftField) => ({
    'aria-invalid': Boolean(shown(field)) || undefined,
    'aria-describedby': shown(field) ? `${id(field)}-error` : undefined,
  })

  const edit = (patch: Partial<PromotionDraft>) => setDraft((d) => ({ ...d, ...patch }))

  // Newest first; the one already chosen stays offered even when the list
  // no longer carries it.
  const campaignOptions = useMemo(() => {
    if (!smsCampaigns) return []
    const stamp = (c: SmsCampaignSummary) => c.sentAt ?? c.exportedAt ?? ''
    const options = [...smsCampaigns]
      .sort((a, b) => stamp(b).localeCompare(stamp(a)))
      .map((c) => ({ value: c.campaign, label: campaignLabel(c, t) }))
    if (draft.smsCampaign && !options.some((o) => o.value === draft.smsCampaign)) {
      options.unshift({ value: draft.smsCampaign, label: draft.smsCampaign })
    }
    return options
  }, [smsCampaigns, draft.smsCampaign, t])

  /** Picking a campaign names the promotion after it, unless the person has
   *  named it already, and announces it by SMS. */
  function pickCampaign(name: string | null) {
    setDraft((d) => {
      if (!name) return { ...d, smsCampaign: null, fromSms: false }
      const named = d.name.trim() !== '' && d.name !== d.smsCampaign
      return {
        ...d,
        smsCampaign: name,
        fromSms: true,
        name: named ? d.name : name,
        channels: d.channels.includes('sms') ? d.channels : [...d.channels, 'sms'],
      }
    })
  }

  function setScope(scope: PromotionScope) {
    // A brand is not a category: the names do not carry over.
    edit({ scope, scopeItems: scope === draft.scope ? draft.scopeItems : [] })
  }

  function submit(e: FormEvent) {
    e.preventDefault()
    // Enter in a field submits too: one save at a time, as the button says.
    if (saving) return
    setTried(true)
    const first = (Object.keys(errors) as DraftField[])[0]
    if (first) {
      document.getElementById(id(first))?.focus()
      return
    }
    onSubmit(draftToPayload(draft))
  }

  const title = promotion ? t('promo.formEdit', { name: promotion.name }) : t('promo.add')
  const itemsLabel = draft.scope === 'brands' ? t('promo.field.brands') : t('promo.field.categories')

  return (
    // As wide as the mockup's card at most: a field across a whole desktop
    // panel is read as a line, not a field.
    <form noValidate onSubmit={submit} aria-labelledby={id('title')} className="max-w-2xl">
      <Wrapper dir="column" gap="md">
        <h4 id={id('title')} className="text-sm font-semibold text-slate-800">{title}</h4>

        {smsCampaigns ? (
          <Field id={id('sms')} label={t('promo.field.fromSms')} hint={t('promo.field.fromSmsHint')}>
            <Select
              id={id('sms')}
              fullWidth
              options={campaignOptions}
              value={draft.smsCampaign}
              onChange={pickCampaign}
              emptyLabel={t('promo.field.fromSmsNone')}
            />
          </Field>
        ) : draft.smsCampaign ? (
          // The link stays as it was; only a reader of SMS campaigns may change it.
          <p className="text-xs text-slate-500">{t('promo.fromSms', { name: draft.smsCampaign })}</p>
        ) : null}

        <Field id={id('name')} label={t('promo.field.name')} error={shown('name')}>
          <Input
            id={id('name')}
            width="full"
            value={draft.name}
            maxLength={LIMITS.name}
            autoComplete="off"
            onChange={(name) => edit({ name })}
            {...invalid('name')}
          />
        </Field>

        <Pair>
          <Field id={id('startsOn')} label={t('promo.field.startsOn')} error={shown('startsOn')}>
            <Input
              id={id('startsOn')}
              type="date"
              width="full"
              value={draft.startsOn}
              onChange={(startsOn) => edit({
                startsOn,
                // A one-day promotion is the common case: its last day follows.
                endsOn: draft.endsOn && draft.endsOn >= startsOn ? draft.endsOn : startsOn,
              })}
              {...invalid('startsOn')}
            />
          </Field>
          <Field id={id('endsOn')} label={t('promo.field.endsOn')} error={shown('endsOn')}>
            <Input
              id={id('endsOn')}
              type="date"
              width="full"
              min={draft.startsOn || undefined}
              value={draft.endsOn}
              onChange={(endsOn) => edit({ endsOn })}
              {...invalid('endsOn')}
            />
          </Field>
        </Pair>

        <Pair>
          <Field id={id('startTime')} label={t('promo.field.startTime')} error={shown('startTime')}>
            <Input
              id={id('startTime')}
              type="time"
              width="full"
              step={60}
              value={draft.startTime}
              onChange={(startTime) => edit({ startTime })}
              {...invalid('startTime')}
            />
          </Field>
          <Field id={id('kind')} label={t('promo.field.kind')}>
            <Select
              id={id('kind')}
              fullWidth
              allowEmpty={false}
              options={PROMOTION_KINDS.map((k) => ({ value: k, label: t(`promo.kind.${k}`) }))}
              value={draft.kind}
              onChange={(kind) => kind && edit({ kind: kind as PromotionDraft['kind'] })}
            />
          </Field>
        </Pair>

        <Pair>
          <Field id={id('discount')} label={t('promo.field.discount')} error={shown('discount')}>
            <Input
              id={id('discount')}
              type="number"
              width="full"
              inputMode="decimal"
              min={0}
              max={LIMITS.discount}
              step="any"
              value={draft.discount}
              onChange={(discount) => edit({ discount })}
              {...invalid('discount')}
            />
          </Field>
          <Field id={id('scope')} label={t('promo.field.scope')}>
            <Select
              id={id('scope')}
              fullWidth
              allowEmpty={false}
              options={PROMOTION_SCOPES.map((s) => ({ value: s, label: t(`promo.scopeOption.${s}`) }))}
              value={draft.scope}
              onChange={(scope) => scope && setScope(scope as PromotionScope)}
            />
          </Field>
        </Pair>

        {draft.scope !== 'store' && (
          <Wrapper dir="column" gap="xs">
            {/* Focusable by the save that finds it empty, so the error is where
                the reader lands. */}
            <span id={id('scopeItems')} tabIndex={-1} className="text-xs font-medium text-slate-600 focus:outline-none">
              {itemsLabel}
            </span>
            <MultiPick
              ariaLabel={itemsLabel}
              placeholder={t(draft.scope === 'brands' ? 'promo.field.searchBrand' : 'promo.field.searchCategory')}
              options={draft.scope === 'brands' ? brandOptions : categoryOptions}
              values={draft.scopeItems}
              onChange={(scopeItems) => edit({ scopeItems })}
              allowCustom
              addLabel={(value) => t('promo.field.addItem', { value })}
              emptyText={t('promo.field.noMatch')}
              removeLabel={(value) => t('promo.field.removeItem', { value })}
            />
            {shown('scopeItems') && (
              <p className="text-xs text-red-600" role="alert">{shown('scopeItems')}</p>
            )}
          </Wrapper>
        )}

        <Wrapper dir="column" gap="xs">
          <span id={id('channels')} className="text-xs font-medium text-slate-600">
            {t('promo.field.channels')}
          </span>
          <div role="group" aria-labelledby={id('channels')}>
            <Wrapper dir="row" gap="sm" wrap>
              {PROMOTION_CHANNELS.map((c) => (
                <FilterChip
                  key={c}
                  size="md"
                  tone="blue"
                  active={draft.channels.includes(c)}
                  onClick={() => edit({
                    channels: draft.channels.includes(c)
                      ? draft.channels.filter((x) => x !== c)
                      : [...draft.channels, c],
                  })}
                >
                  {t(`promo.channel.${c}`)}
                </FilterChip>
              ))}
            </Wrapper>
          </div>
        </Wrapper>

        <Field id={id('note')} label={t('promo.field.note')} error={shown('note')}>
          <Textarea
            id={id('note')}
            size="sm"
            rows={2}
            fullWidth
            maxLength={LIMITS.note}
            value={draft.note}
            onChange={(note) => edit({ note })}
            {...invalid('note')}
          />
        </Field>

        {tried && Object.keys(errors).length > 0 && (
          <p className="text-xs font-medium text-red-600" role="alert">{t('promo.error.fixFields')}</p>
        )}

        <Wrapper dir="row" gap="sm" align="center" wrap>
          <Button type="submit" variant="primary" disabled={saving}>
            {saving ? t('promo.saving') : t('promo.save')}
          </Button>
          <Button type="button" variant="secondary" onClick={onClose}>{t('promo.close')}</Button>
        </Wrapper>
        <p className="text-xs text-slate-500">{t('promo.versionsNote')}</p>
      </Wrapper>
    </form>
  )
}
