import { memo, useMemo, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Check, X } from 'lucide-react'
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
  useSmsSegments,
} from '../hooks/useApi'
import {
  audienceFromPreset, audienceToParams, describeAudience, emptyAudience,
} from '../utils/smsAudience'
import { smsCost } from '../utils/smsCost'
import { formatNumber } from '../utils/formatters'
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

  const [step, setStep] = useState<StepId>('audience')
  const [audience, setAudience] = useState<SmsAudienceCriteria>(emptyAudience())
  const [campaign, setCampaign] = useState('')
  const [promocode, setPromocode] = useState('')
  const [text, setText] = useState('')
  const [presetName, setPresetName] = useState('')
  const [savingPreset, setSavingPreset] = useState(false)
  const [selectedPreset, setSelectedPreset] = useState('')
  const [testing, setTesting] = useState(false)
  const [created, setCreated] = useState<SmsCampaignSummary | null>(null)
  const [sending, setSending] = useState(false)

  const { data: presetData } = useSmsAudiencePresets()
  const savePreset = useSaveSmsAudiencePreset()
  const deletePreset = useDeleteSmsAudiencePreset()
  const create = useCreateSmsCampaign()

  // The preview and the freeze are built from the same parameters, so the
  // roster that gets recorded is the one on screen. `campaign` is left out of
  // the preview: it only labels the holdout split, and putting a half-typed
  // name in the query key would refetch on every keystroke.
  const previewParams = useMemo(() => audienceToParams(audience), [audience])
  const { data, isLoading, error, refetch } = useSmsSegments(previewParams)

  const selectedSaved = (presetData?.presets ?? []).some(
    (p) => p.name === selectedPreset && !p.builtin,
  )

  const campaignValid = CAMPAIGN_PATTERN.test(campaign)
  const target = data?.totals.target ?? 0
  const cost = useMemo(() => smsCost(text.trim()), [text])

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
                    <button
                      type="button"
                      onClick={() => setSavingPreset(!savingPreset)}
                      className="text-[11px] text-purple-700 hover:text-purple-900 underline"
                    >
                      {t('sms.presetSaveToggle')}
                    </button>
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

                <SmsAudienceForm audience={audience} onChange={editAudience} />

                <div className="pt-3 border-t border-slate-100">
                  <SmsAudiencePreview data={data} isLoading={isLoading} />
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
                    holdout: formatNumber(data?.totals.holdout ?? 0),
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
                      {formatNumber(created?.holdout ?? data?.totals.holdout ?? 0)}
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
