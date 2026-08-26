import { memo, useEffect, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Bookmark, Check, X } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Button } from './Button'
import { Select } from './Select'
import { ApiErrorState } from './ApiErrorState'
import { SmsAudienceForm } from './SmsAudienceForm'
import { SmsAudiencePreview } from './SmsAudiencePreview'
import { SmsCostLine } from './SmsCostLine'
import { SmsSendDialog } from './SmsSendDialog'
import { SmsTestSendDialog } from './SmsTestSendDialog'
import { useToast } from './Toast'
import {
  useCreateSmsCampaign,
  useDeleteSmsAudiencePreset,
  useSaveSmsAudiencePreset,
  useSmsAudiencePresets,
  useSmsChannels,
  useSmsSegments,
} from '../hooks/useApi'
import {
  audienceFromPreset, audienceToParams, describeAudience,
} from '../utils/smsAudience'
import { clearDraft, loadDraft, saveDraft } from '../utils/smsDraft'
import { smsCost } from '../utils/smsCost'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsAudienceCriteria, SmsCampaignSummary } from '../types/api'

// ─── SmsCampaignWizard ───────────────────────────────────────────────────────
//
// One campaign, start to finish: who gets it, how much is withheld, what it
// says, a rehearsal, and the send.
//
// The order is not decoration. Every step here is a decision that cannot be
// revisited once the messages are away — the roster is frozen at creation
// because the eligible population shifts daily, and the control group recorded
// at that instant is the only one the results will ever have. Presenting the
// steps in the order they bind is the difference between a manager who knows
// what they just did and one who finds out when the report is unreadable.
//
// The audience is a live preview all the way through: the counter under the
// filters is the same query that will be frozen, so what is on screen at the
// moment of pressing "create" is what gets recorded.

const CAMPAIGN_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/
const SMS_LIMIT = 600
const HOLDOUT_CHOICES = [10, 20, 30, 40]

type StepId = 'audience' | 'control' | 'message' | 'launch'

const STEPS: StepId[] = ['audience', 'control', 'message', 'launch']

function StepHeader({
  step, index, current, onPick, done,
}: {
  step: StepId
  index: number
  current: StepId
  onPick: () => void
  done: boolean
}) {
  const { t } = useTranslation()
  const active = step === current

  return (
    <button
      type="button"
      onClick={onPick}
      aria-current={active ? 'step' : undefined}
      className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-xs transition-colors ${
        active
          ? 'bg-purple-50 text-purple-800 font-medium'
          : 'text-slate-500 hover:text-slate-700'
      }`}
    >
      <span
        className={`w-5 h-5 rounded-full grid place-items-center text-[11px] tabular-nums ${
          done ? 'bg-green-100 text-green-700' : 'bg-slate-100 text-slate-600'
        }`}
      >
        {done ? <Check className="w-3 h-3" /> : index + 1}
      </span>
      {t(`sms.wizard.${step}`)}
    </button>
  )
}

/** Every step ends the same way: back on the left, this step's action on the
 *  right, anything optional between them.
 *
 *  It was not always so, and the buttons moved from step to step — the one
 *  control a person aims for without reading has to be in the same place every
 *  time. */
function StepActions({
  onBack, secondary, primary,
}: {
  onBack?: () => void
  secondary?: React.ReactNode
  primary?: React.ReactNode
}) {
  const { t } = useTranslation()

  return (
    <div className="mt-4 pt-3 border-t border-slate-100 flex flex-wrap items-center gap-2">
      {onBack && (
        <Button variant="secondary" size="sm" onClick={onBack}>
          {t('sms.wizardBack')}
        </Button>
      )}
      {secondary}
      <div className="flex-1" />
      {primary}
    </div>
  )
}

export const SmsCampaignWizard = memo(function SmsCampaignWizard({
  onClose,
}: {
  onClose: () => void
}) {
  const { t } = useTranslation()
  const { addToast } = useToast()

  // Restored from the session, so closing the wizard — or reloading the page —
  // does not throw away an audience that took ten decisions to build.
  const restored = useMemo(() => loadDraft(), [])

  const [step, setStep] = useState<StepId>('audience')
  const [audience, setAudience] = useState<SmsAudienceCriteria>(restored.audience)
  const [campaign, setCampaign] = useState(restored.campaign)
  const [promocode, setPromocode] = useState(restored.promocode)
  const [text, setText] = useState(restored.text)
  const [presetName, setPresetName] = useState('')
  const [savingPreset, setSavingPreset] = useState(false)
  const [selectedPreset, setSelectedPreset] = useState(restored.presetName)
  const [testing, setTesting] = useState(false)
  const [created, setCreated] = useState<SmsCampaignSummary | null>(null)
  const [sending, setSending] = useState(false)

  // Every keystroke, because the point is surviving the misclick nobody plans.
  useEffect(() => {
    saveDraft({ audience, campaign, promocode, text, presetName: selectedPreset })
  }, [audience, campaign, promocode, text, selectedPreset])

  const { data: presetData } = useSmsAudiencePresets()
  const { data: channels } = useSmsChannels()
  const savePreset = useSaveSmsAudiencePreset()
  const deletePreset = useDeleteSmsAudiencePreset()
  const create = useCreateSmsCampaign()

  // One query, and it carries everything the campaign carries — including the
  // value level, which is a filter and so decides who is in the audience at
  // all. The preview and the freeze are built from the same parameters, so the
  // roster recorded is the one that was on screen.
  //
  // `campaign` is left out: it only seeds the holdout split, and a half-typed
  // name in the query key would refetch on every keystroke.
  const previewParams = useMemo(() => audienceToParams(audience), [audience])
  const { data, isLoading, error, refetch } = useSmsSegments(previewParams)

  const selectedSaved = (presetData?.presets ?? []).some(
    (p) => p.name === selectedPreset && !p.builtin,
  )

  const campaignValid = CAMPAIGN_PATTERN.test(campaign)
  const target = data?.totals.target ?? 0
  const holdout = data?.totals.holdout ?? 0
  const cost = useMemo(() => smsCost(text.trim()), [text])

  // What the gateway will bill: parts × recipients × the tariff it quotes.
  // The tariff comes from the server rather than a constant here — two numbers
  // for one price is how a page ends up promising one figure and charging
  // another. Without a text yet, one part is the floor, and the line says so.
  const estimate = useMemo(() => {
    const price = channels?.pricePerPart
    if (price == null || target === 0) return null
    return (cost.parts || 1) * target * price
  }, [channels?.pricePerPart, cost.parts, target])

  function handlePickPreset(picked: string | null) {
    const name = picked ?? ''
    setSelectedPreset(name)
    if (!name) return
    const preset = (presetData?.presets ?? []).find((p) => p.name === name)
    if (!preset) return
    // Picking an audience fills in every control it was built from — grouping,
    // basis, holdout, window and each filter — so the next campaign to the same
    // people is the same campaign, not one assembled from memory.
    const loaded = audienceFromPreset(preset.criteria)
    setAudience(loaded)
    // Says how many filters arrived, including none: two of the audiences that
    // ship with the page carry no filters at all, and a message claiming they
    // were "filled in" is how a working control gets reported as broken.
    addToast({
      type: 'success',
      title: t('sms.presetApplied', { name }),
      message: describeAudience(loaded, t),
    })
  }

  /** Editing the form means the list no longer describes what is on screen. */
  function editAudience(next: SmsAudienceCriteria) {
    setAudience(next)
    setSelectedPreset('')
  }

  function handleSavePreset() {
    const name = presetName.trim()
    if (!name) return
    savePreset.mutate(
      { name, criteria: audience },
      {
        onSuccess: () => {
          setPresetName('')
          setSavingPreset(false)
          setSelectedPreset(name)
          addToast({ type: 'success', title: t('sms.presetSaved', { name }) })
        },
        onError: (e: Error) =>
          addToast({ type: 'error', title: t('sms.presetSaveFailed'), message: e.message }),
      },
    )
  }

  function handleCreate() {
    const params = audienceToParams(audience, {
      campaign,
      promocode: promocode.trim() || undefined,
    })
    create.mutate(params, {
      onSuccess: (result) => {
        setCreated({
          campaign: result.campaign,
          ltvBasis: audience.ltvBasis,
          salesType: 'retail',
          holdoutPct: audience.holdoutPct,
          promocode: promocode.trim() || null,
          exportedAt: new Date().toISOString(),
          sentAt: null,
          notes: null,
          members: result.totals.customers,
          target: result.totals.target,
          holdout: result.totals.holdout,
        })
        // From here the campaign exists on the server; a draft of it would be
        // a stale copy that reopens as a half-built duplicate.
        clearDraft()
        addToast({ type: 'success', title: t('sms.campaignCreated', { campaign }) })
      },
      onError: (e: Error) =>
        addToast({ type: 'error', title: t('sms.campaignCreateFailed'), message: e.message }),
    })
  }

  function handleCsv() {
    const params = audienceToParams(audience, {
      campaign: campaignValid ? campaign : undefined,
    })
    window.open(`/api/customers/sms-segments/export/csv?${params}`, '_blank')
  }

  const input = `px-2 py-1.5 text-sm bg-white border border-slate-200 rounded-md
                 text-slate-700 focus:outline-none focus:ring-2 focus:ring-purple-500/30
                 focus:border-purple-400`

  const doneUpTo: Record<StepId, boolean> = {
    audience: target > 0,
    control: target > 0,
    message: text.trim().length > 0 && campaignValid,
    launch: created !== null,
  }

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <CardTitle>{t('sms.wizardTitle')}</CardTitle>
          <Button variant="secondary" size="sm" onClick={onClose}>
            <X className="w-3.5 h-3.5" /> {t('sms.wizardClose')}
          </Button>
        </div>
        <nav className="mt-2 flex flex-wrap gap-1" aria-label={t('sms.wizardTitle')}>
          {STEPS.map((s, i) => (
            <StepHeader
              key={s} step={s} index={i} current={step}
              done={doneUpTo[s] && step !== s}
              onPick={() => setStep(s)}
            />
          ))}
        </nav>
      </CardHeader>

      <CardContent>
        {/* Visible on every step, because "who is this going to and what will
            it cost" is the question being answered by all four of them. */}
        <div className="mb-4 grid gap-3 sm:grid-cols-3 rounded-lg border border-slate-200
                        bg-slate-50/60 p-3">
          <div className="sm:col-span-2">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              {t('sms.summaryWho')}
            </div>
            <div className="text-sm text-slate-800 mt-0.5">
              {campaign || t('sms.summaryUnnamed')}
            </div>
            <div className="text-xs text-slate-500 mt-0.5">
              {describeAudience(audience, t)}
            </div>
          </div>
          <div>
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              {t('sms.summaryHowMany')}
            </div>
            <div className="text-sm text-slate-800 mt-0.5 tabular-nums">
              {t('sms.summaryContacts', {
                target: formatNumber(target), holdout: formatNumber(holdout),
              })}
            </div>
            <div className="text-xs text-slate-500 mt-0.5 tabular-nums">
              {estimate == null
                ? t('sms.summaryNoPrice')
                : t(text.trim() ? 'sms.summaryCost' : 'sms.summaryCostFrom', {
                    cost: formatCurrency(estimate),
                    parts: cost.parts || 1,
                  })}
            </div>
          </div>
        </div>

        {error ? (
          <ApiErrorState error={error} onRetry={() => refetch()} />
        ) : (
          <>
            {/* ── 1. Audience ──────────────────────────────────────── */}
            {step === 'audience' && (
              <div className="space-y-4">
                {/* The name comes first because it is what everything after it
                    belongs to — and because a field at the foot of this step
                    was being read as the campaign's name anyway. */}
                <div>
                  <label className="text-xs text-slate-600">
                    <span className="block mb-1">{t('sms.campaignName')}</span>
                    <input
                      type="text" className={input} value={campaign}
                      placeholder="sep-brand" maxLength={40}
                      onChange={(e) => setCampaign(e.target.value)}
                    />
                  </label>
                  {campaign.length > 0 && !campaignValid && (
                    <p className="mt-1 text-xs text-amber-700 bg-amber-50 rounded-md px-2 py-1.5">
                      {t('sms.campaignRequired')}
                    </p>
                  )}
                </div>

                <SmsAudienceForm
                  audience={audience}
                  onChange={editAudience}
                  segments={data?.segments}
                />

                <div className="pt-3 border-t border-slate-100">
                  <SmsAudiencePreview data={data} isLoading={isLoading} />
                </div>

                <div>
                  <span className="text-xs text-slate-600">{t('sms.presetsLabel')}</span>
                  <p className="text-[11px] text-slate-500 mt-0.5 mb-1">
                    {t('sms.presetsHint')}
                  </p>
                  <div className="flex flex-wrap items-center gap-2">
                    <Select
                      options={(presetData?.presets ?? []).map((p) => ({
                        value: p.name,
                        label: p.builtin ? `${p.name} · ${t('sms.presetBuiltin')}` : p.name,
                      }))}
                      value={selectedPreset}
                      onChange={handlePickPreset}
                      placeholder={t('sms.presetsPlaceholder')}
                      variant="compact"
                      aria-label={t('sms.presetsLabel')}
                    />
                    {selectedSaved && (
                      <Button
                        variant="secondary" size="sm"
                        onClick={() => {
                          deletePreset.mutate(selectedPreset)
                          setSelectedPreset('')
                        }}
                      >
                        {t('sms.presetDeleteSelected')}
                      </Button>
                    )}
                    <Button
                      variant="secondary" size="sm"
                      onClick={() => setSavingPreset(!savingPreset)}
                    >
                      <Bookmark className="w-3.5 h-3.5" /> {t('sms.presetSaveToggle')}
                    </Button>
                  </div>
                  <p className="mt-1.5 text-[11px] text-slate-600 tabular-nums">
                    {describeAudience(audience, t)}
                  </p>

                  {savingPreset && (
                    <div className="mt-2 flex flex-wrap items-end gap-2">
                      <label className="text-xs text-slate-600">
                        <span className="block mb-1">{t('sms.presetSaveAs')}</span>
                        <input
                          type="text" className={input} maxLength={60}
                          value={presetName} placeholder={t('sms.presetNamePlaceholder')}
                          onChange={(e) => setPresetName(e.target.value)}
                        />
                      </label>
                      <Button
                        variant="secondary" size="sm"
                        onClick={handleSavePreset}
                        disabled={!presetName.trim() || savePreset.isPending}
                      >
                        {t('sms.presetSave')}
                      </Button>
                    </div>
                  )}
                </div>


                <StepActions
                  primary={
                    <Button
                      size="sm"
                      onClick={() => setStep('control')}
                      disabled={target === 0 || !campaignValid}
                    >
                      {t('sms.wizardNext')}
                    </Button>
                  }
                />
              </div>
            )}

            {/* ── 2. Control group ─────────────────────────────────── */}
            {step === 'control' && (
              <div className="space-y-3">
                <p className="text-xs text-slate-500 leading-snug">
                  {t('sms.holdoutExplain')}
                </p>
                <div className="flex flex-wrap gap-2" role="group" aria-label={t('sms.holdoutLabel')}>
                  {HOLDOUT_CHOICES.map((pct) => (
                    <button
                      key={pct}
                      type="button"
                      aria-pressed={audience.holdoutPct === pct}
                      onClick={() => editAudience({ ...audience, holdoutPct: pct })}
                      className={`px-3 py-1.5 text-xs rounded-md border transition-colors
                                  tabular-nums ${
                        audience.holdoutPct === pct
                          ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
                          : 'border-slate-200 text-slate-600 hover:border-slate-300'
                      }`}
                    >
                      {pct}%
                    </button>
                  ))}
                </div>
                <p className="text-[11px] text-slate-500 tabular-nums">
                  {t('sms.holdoutHint', {
                    target: formatNumber(target),
                    holdout: formatNumber(holdout),
                  })}
                </p>
                <StepActions
                  onBack={() => setStep('audience')}
                  primary={
                    <Button size="sm" onClick={() => setStep('message')}>
                      {t('sms.wizardNext')}
                    </Button>
                  }
                />
              </div>
            )}

            {/* ── 3. The message ───────────────────────────────────── */}
            {step === 'message' && (
              <div className="space-y-3">
                <div className="flex flex-wrap gap-3">
                  <label className="text-xs text-slate-600">
                    <span className="block mb-1">{t('sms.promocodeOptional')}</span>
                    <input
                      type="text" className={input} value={promocode}
                      placeholder="KS-SEP" maxLength={40}
                      onChange={(e) => setPromocode(e.target.value)}
                    />
                  </label>
                </div>

                <label className="block text-xs text-slate-600">
                  <span className="block mb-1">{t('sms.messageText')}</span>
                  <textarea
                    rows={3}
                    className={`${input} w-full`}
                    value={text}
                    onChange={(e) => setText(e.target.value)}
                  />
                </label>
                <SmsCostLine cost={cost} limit={SMS_LIMIT} recipients={target} />

                <StepActions
                  onBack={() => setStep('control')}
                  secondary={
                    <Button variant="secondary" size="sm" onClick={() => setTesting(true)}>
                      {t('sms.testSend')}
                    </Button>
                  }
                  primary={
                    <Button
                      size="sm"
                      onClick={() => setStep('launch')}
                      disabled={!campaignValid || text.trim().length === 0}
                    >
                      {t('sms.wizardNext')}
                    </Button>
                  }
                />
              </div>
            )}

            {/* ── 4. Launch ────────────────────────────────────────── */}
            {step === 'launch' && (
              <div className="space-y-3">
                <dl className="grid gap-2 sm:grid-cols-3 text-xs">
                  <div>
                    <dt className="text-slate-500">{t('sms.campaign')}</dt>
                    <dd className="text-slate-800 font-medium">{campaign || '—'}</dd>
                  </div>
                  <div>
                    <dt className="text-slate-500">{t('sms.toSend')}</dt>
                    <dd className="text-slate-800 font-medium tabular-nums">
                      {formatNumber(created?.target ?? target)}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-500">{t('sms.control')}</dt>
                    <dd className="text-slate-800 font-medium tabular-nums">
                      {formatNumber(created?.holdout ?? holdout)}
                    </dd>
                  </div>
                </dl>

                <p className="text-[11px] text-slate-500 leading-snug">
                  {created ? t('sms.rosterFrozen') : t('sms.rosterWillFreeze')}
                </p>

                {created ? (
                  <StepActions
                    secondary={
                      <>
                        <Button variant="secondary" size="sm" onClick={handleCsv}>
                          {t('sms.downloadCsv')}
                        </Button>
                        <Button variant="secondary" size="sm" onClick={onClose}>
                          {t('sms.wizardDone')}
                        </Button>
                      </>
                    }
                    primary={
                      <Button size="sm" onClick={() => setSending(true)}>
                        {t('sms.sendNow')}
                      </Button>
                    }
                  />
                ) : (
                  <StepActions
                    onBack={() => setStep('message')}
                    primary={
                      <Button
                        size="sm"
                        onClick={handleCreate}
                        disabled={!campaignValid || create.isPending || target === 0}
                      >
                        {create.isPending ? t('sms.creating') : t('sms.createCampaign')}
                      </Button>
                    }
                  />
                )}
              </div>
            )}
          </>
        )}
      </CardContent>

      {/* The rehearsal starts from the text just written, not from an empty
          box: a test of a different message is not a test. */}
      {testing && (
        <SmsTestSendDialog initialText={text} onClose={() => setTesting(false)} />
      )}
      {sending && created && (
        <SmsSendDialog campaign={created} onClose={() => setSending(false)} />
      )}
    </Card>
  )
})

export default SmsCampaignWizard
