import { memo, useEffect, useMemo, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Check, Plus, X } from 'lucide-react'
import { Card, CardContent, CardHeader, CardTitle } from './Card'
import { Button } from './Button'
import { Badge } from './Badge'
import { Input } from './Input'
import { Textarea } from './Textarea'
import { Wrapper } from './Wrapper'
import { ApiErrorState } from './ApiErrorState'
import { AudienceSummaryBar } from './AudienceSummaryBar'
import { SmsAudienceForm } from './SmsAudienceForm'
import { SmsAudienceLibrary } from './SmsAudienceLibrary'
import { SmsAudiencePreview } from './SmsAudiencePreview'
import { SmsAudienceSource } from './SmsAudienceSource'
import { SmsFollowUpPicker } from './SmsFollowUpPicker'
import { SmsMeasurementSettings } from './SmsMeasurementSettings'
import { SmsSelectionCriteria } from './SmsSelectionCriteria'
import { SmsCostLine } from './SmsCostLine'
import { SmsSendDialog } from './SmsSendDialog'
import { SmsTestSendDialog } from './SmsTestSendDialog'
import { useToast } from './Toast'
import { useIsPhone } from '../hooks/useMediaQuery'
import {
  useTelegramBackButton, useTelegramMainButton, useTelegramWebApp,
} from '../hooks/useTelegramButtons'
import { closeTopLayer, useAnyLayerOpen } from '../utils/layerStack'
import {
  useCreateSmsCampaign,
  useDeleteSmsAudiencePreset,
  useSaveSmsAudiencePreset,
  useSmsAudiencePresets,
  useSmsCampaigns,
  useSmsChannels,
  useSmsSegments,
} from '../hooks/useApi'
import { audienceToParams } from '../utils/smsAudience'
import {
  daysSince, definitionFromFrozen, definitionFromPreset, definitionToCriteria,
  definitionToPreset, emptyExtraGroups, hasAbsoluteDates, shiftDates, unfinished,
  type AudienceDefinition,
} from '../utils/smsConditions'
import { audienceSentence, describeMeasurement, plural } from '../utils/smsConditionText'
import { clearDraft, loadDraft, saveDraft } from '../utils/smsDraft'
import { followUpCandidates, followUpParams } from '../utils/smsFollowUp'
import { suggestCampaignName } from '../utils/smsCampaignName'
import { smsCost } from '../utils/smsCost'
import { describeSmsError, refusalMessage } from '../utils/smsRefusal'
import { formatCurrency, formatNumber } from '../utils/formatters'
import type { SmsCampaignSummary } from '../types/api'

// ─── SmsCampaignWizard ───────────────────────────────────────────────────────
//
// One campaign, start to finish: who gets it, how it is measured, what it
// says, a rehearsal, and the send.
//
// The order is not decoration. Every step here is a decision that cannot be
// revisited once the messages are away — the roster is frozen at creation
// because the eligible population shifts daily, and the control group recorded
// at that instant is the only one the results will ever have. Presenting the
// steps in the order they bind is the difference between a manager who knows
// what they just did and one who finds out when the report is unreadable.
//
// The audience is a live preview all the way through: the count under the
// groups is the same query that will be frozen, so what is on screen at the
// moment of pressing "create" is what gets recorded. That includes the
// control group, which is drawn per (buyer, campaign NAME): the preview asks
// under the name being typed (once it is a valid one), not under the
// placeholder "default". On 2026-10-17 it asked under "default" and showed a
// control of 375 for a campaign frozen with 429.
//
// A follow-up is the other way to start: the people a sent campaign reached,
// messaged a second time. It has no audience to build and no control of its
// own — it is read on the first campaign's control — so it skips those steps
// and its preview and freeze are one `follow_up_of` query.
//
// Inside the Telegram Mini App the audience step's «Далее · N получателей» is
// Telegram's MainButton — the same label, enabled state and handler as the
// bar's — and the BackButton closes the sheet on top, else steps back. Both
// are hidden on every other step, when a sheet covers the step, and when the
// wizard closes. Outside Telegram nothing of this runs.

const CAMPAIGN_PATTERN = /^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/
const SMS_LIMIT = 600

type StepId = 'audience' | 'control' | 'message' | 'launch'

const STEPS: StepId[] = ['audience', 'control', 'message', 'launch']

function StepHeader({
  step, current, onPick, done,
}: {
  step: StepId
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
      {/* Named, not numbered. These four used to count 1–4 beside a page that
          was separately counting 1–3, so "Step 2" meant Control here and
          Frozen campaigns eight centimetres below. The page keeps the numbers;
          the stages inside one step keep their names. */}
      <span
        className={`w-5 h-5 rounded-full grid place-items-center ${
          done ? 'bg-green-100 text-green-700'
               : active ? 'bg-purple-100 text-purple-700' : 'bg-slate-100 text-slate-400'
        }`}
      >
        {done ? <Check className="w-3 h-3" />
              : <span className="w-1.5 h-1.5 rounded-full bg-current" />}
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
 *  time. Step 1 ends in the audience bar, whose action sits in the same place. */
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
  const phone = useIsPhone()

  // Restored from the session, so closing the wizard — or reloading the page —
  // does not throw away an audience that took ten decisions to build.
  const restored = useMemo(() => loadDraft(), [])

  const [step, setStep] = useState<StepId>('audience')
  const [audience, setAudience] = useState<AudienceDefinition>(restored.audience)
  const [campaign, setCampaign] = useState(restored.campaign)
  const [promocode, setPromocode] = useState(restored.promocode)
  const [text, setText] = useState(restored.text)
  const [selectedPreset, setSelectedPreset] = useState(restored.presetName)
  const [testing, setTesting] = useState(false)
  const [created, setCreated] = useState<SmsCampaignSummary | null>(null)
  const [sending, setSending] = useState(false)
  const [followUp, setFollowUp] = useState(restored.followUp)
  const [followUpOf, setFollowUpOf] = useState(restored.followUpOf)
  // A past campaign's audience opened with its own dates: how far to move
  // them to today, if the person wants that.
  const [shiftOffer, setShiftOffer] = useState<{ campaign: string; days: number } | null>(null)

  // Every keystroke, because the point is surviving the misclick nobody plans.
  useEffect(() => {
    saveDraft({
      audience, campaign, promocode, text, presetName: selectedPreset,
      followUp, followUpOf,
    })
  }, [audience, campaign, promocode, text, selectedPreset, followUp, followUpOf])

  const { data: presetData } = useSmsAudiencePresets()
  const { data: channels } = useSmsChannels()
  const savePreset = useSaveSmsAudiencePreset()
  const deletePreset = useDeleteSmsAudiencePreset()
  const create = useCreateSmsCampaign()
  const { data: campaignList } = useSmsCampaigns()
  const candidates = useMemo(
    () => followUpCandidates(campaignList?.campaigns ?? []),
    [campaignList],
  )
  // Past campaigns to start from: the ones with an audience of their own.
  const pastCampaigns = useMemo(
    () => (campaignList?.campaigns ?? []).filter((c) => !c.followUpOf && c.criteria),
    [campaignList],
  )
  const steps = followUp ? STEPS.filter((s) => s !== 'control') : STEPS

  // The name the field was last filled with by the wizard rather than typed.
  // Only an empty field or that very value is replaced: once somebody types,
  // the name is theirs. Waits for the campaign list, so the suggestion is
  // never a name already taken; follows a change of the campaign followed up.
  //
  // Keyed on the names as one string, not on the list object: a refetch that
  // hands back a new object with the same names must not refill a field
  // somebody has just emptied to type their own.
  const suggested = useRef<string | null>(null)
  const [suggestion, setSuggestion] = useState<string | null>(null)
  const takenNames = campaignList
    ? campaignList.campaigns.map((c) => c.campaign).join('\n')
    : null
  useEffect(() => {
    if (takenNames == null) return
    const next = suggestCampaignName(
      takenNames ? takenNames.split('\n') : [],
      { followUpOf: followUp ? followUpOf || null : null },
    )
    setCampaign((current) => {
      if (current !== '' && current !== suggested.current) return current
      suggested.current = next
      setSuggestion(next)
      return next
    })
  }, [takenNames, followUp, followUpOf])

  const campaignValid = CAMPAIGN_PATTERN.test(campaign)

  // One query, and it carries everything the campaign carries — including the
  // value level, which is a filter, and the NAME, which seeds the holdout
  // split. The preview and the freeze are built from the same parameters, so
  // the roster recorded — control group included — is the one on screen.
  // A name that is not valid yet is left out (the server would refuse it),
  // and the debounce below keeps a name being typed from asking per letter.
  const previewParams = useMemo(
    () => audienceToParams(definitionToCriteria(audience), {
      campaign: campaignValid ? campaign : undefined,
    }),
    [audience, campaign, campaignValid],
  )

  // Typing "237" is three renders, and the segmentation is capped per minute:
  // firing a request per keystroke spends the whole allowance on a number
  // nobody has finished writing, and the page answers "too many requests".
  // One request, once the typing stops.
  const [settledParams, setSettledParams] = useState(previewParams)
  useEffect(() => {
    const id = setTimeout(() => setSettledParams(previewParams), 500)
    return () => clearTimeout(id)
  }, [previewParams])

  // An unfinished condition is left out of the query — its row says so — and
  // holds the step: nothing is frozen with a condition the person believes is
  // applied and is not.
  const open = useMemo(() => unfinished(audience), [audience])

  // A follow-up's query changes only when a campaign is picked, so it is not
  // debounced; and it asks for nothing but the campaign it follows.
  const followUpQuery = followUpOf ? followUpParams(followUpOf) : ''
  const { data, isLoading, isPlaceholderData, error, refetch } = useSmsSegments(
    followUp ? followUpQuery : settledParams,
    followUp ? Boolean(followUpOf) : true,
  )
  // The numbers on screen are the previous audience's while an edit is being
  // debounced or asked: said as "recounting", and Next waits for the answer —
  // a total from before the last edit must not read as this audience's.
  const stale = !followUp && (previewParams !== settledParams || isPlaceholderData)
  const emptyGroups = useMemo(() => emptyExtraGroups(audience), [audience])

  const target = data?.totals.target ?? 0
  const holdout = data?.totals.holdout ?? 0
  const cost = useMemo(() => smsCost(text.trim()), [text])

  // What the gateway will bill: parts × recipients × the tariff it quotes.
  // The tariff comes from the server rather than a constant here — two numbers
  // for one price is how a page ends up promising one figure and charging
  // another. Without a text yet, one part is the floor, and the line says so.
  const estimate = useMemo(() => {
    const price = channels?.pricePerPart
    const recipients = created?.target ?? target
    if (price == null || recipients === 0) return null
    return (cost.parts || 1) * recipients * price
  }, [channels?.pricePerPart, cost.parts, target, created])

  /** Editing the audience means the list no longer describes what is on screen. */
  function editAudience(next: AudienceDefinition) {
    setAudience(next)
    setSelectedPreset('')
    setShiftOffer(null)
  }

  function handlePickPreset(picked: string | null) {
    const name = picked ?? ''
    setSelectedPreset(name)
    setShiftOffer(null)
    if (!name) return
    const preset = (presetData?.presets ?? []).find((p) => p.name === name)
    if (!preset) return
    // Picking an audience fills in every condition it was built from, and the
    // way it is measured, so the next campaign to the same people is the same
    // campaign, not one assembled from memory.
    const loaded = definitionFromPreset(preset.criteria)
    setAudience(loaded)
    addToast({
      type: 'success',
      title: t('sms.presetApplied', { name }),
      message: audienceSentence(loaded, t),
    })
  }

  function handlePickCampaign(c: SmsCampaignSummary) {
    const loaded = definitionFromFrozen(c.criteria)
    setAudience(loaded)
    setSelectedPreset('')
    const days = daysSince(c.exportedAt)
    setShiftOffer(hasAbsoluteDates(loaded) && days > 0 ? { campaign: c.campaign, days } : null)
    addToast({
      type: 'success',
      title: t('sms.library.loaded', { campaign: c.campaign }),
      message: audienceSentence(loaded, t),
    })
  }

  function handleSavePreset(name: string, done: () => void) {
    savePreset.mutate(
      { name, criteria: definitionToPreset(audience) },
      {
        onSuccess: () => {
          done()
          setSelectedPreset(name)
          addToast({ type: 'success', title: t('sms.presetSaved', { name }) })
        },
        onError: (e: Error) =>
          addToast({ type: 'error', title: t('sms.presetSaveFailed'), message: e.message }),
      },
    )
  }

  function handleCreate() {
    // The same query the preview ran, plus the code: the roster frozen is the
    // one that was on screen.
    const params = followUp
      ? followUpParams(followUpOf, {
          campaign, promocode: promocode.trim() || undefined,
        })
      : audienceToParams(definitionToCriteria(audience), {
          campaign,
          promocode: promocode.trim() || undefined,
        })
    create.mutate(params, {
      onSuccess: (result) => {
        setCreated({
          campaign: result.campaign,
          ltvBasis: data?.ltvBasis ?? audience.ltvBasis,
          salesType: 'retail',
          holdoutPct: followUp ? 0 : audience.holdoutPct,
          promocode: promocode.trim() || null,
          exportedAt: new Date().toISOString(),
          sentAt: null,
          notes: null,
          members: result.totals.customers,
          target: result.totals.target,
          holdout: result.totals.holdout,
          followUpOf: followUp ? followUpOf : null,
        })
        // From here the campaign exists on the server; a draft of it would be
        // a stale copy that reopens as a half-built duplicate.
        clearDraft()
        addToast({ type: 'success', title: t('sms.campaignCreated', { campaign }) })
      },
      onError: (e: Error) =>
        addToast({
          type: 'error', title: t('sms.campaignCreateFailed'), message: describeSmsError(e, t),
        }),
    })
  }

  function handleCsv() {
    const params = audienceToParams(definitionToCriteria(audience), {
      campaign: campaignValid ? campaign : undefined,
    })
    window.open(`/api/customers/sms-segments/export/csv?${params}`, '_blank')
  }

  const doneUpTo: Record<StepId, boolean> = {
    audience: target > 0,
    control: target > 0,
    message: text.trim().length > 0 && campaignValid,
    launch: created !== null,
  }

  // Once frozen, the numbers are the frozen ones — never the preview's — so
  // the window never shows two different control groups for one campaign.
  const shownTarget = created?.target ?? target
  const shownHoldout = created?.holdout ?? holdout

  // ── The audience bar ────────────────────────────────────────────────────
  const shares = data?.groups
  const sharesAligned = shares && shares.length === audience.groups.length
  const overlap = !followUp && sharesAligned && audience.groups.length > 1 && data
    ? Math.max(0, data.totals.customers - shares.reduce((n, s) => n + s.onlyHere, 0))
    : 0
  const sentence = followUp
    ? (followUpOf ? t('sms.followUpSummary', { root: followUpOf }) : t('sms.followUpPickFirst'))
    : audienceSentence(audience, t, data && !stale ? { count: data.totals.customers } : {})
  const openGroups = [...new Set(open.map((o) => o.group))]
  const blocked = !campaignValid
    ? t('sms.blocked.name')
    : !followUp && openGroups.length === 1
      ? t('sms.blocked.unfinished', { groups: String(openGroups[0]) })
      : !followUp && openGroups.length > 1
        ? t('sms.blocked.unfinishedMany', { groups: openGroups.join(', ') })
        : !followUp && emptyGroups.length > 0
          ? t('sms.blocked.emptyGroup', { groups: emptyGroups.join(', ') })
          : error
            ? t('sms.blocked.error')
            : data && !stale && target === 0
              ? t('sms.blocked.empty')
              : null
  const stats = (isLoading && !data) || stale
    ? t(data ? 'sms.bar.recounting' : 'sms.bar.counting')
    : data
      ? [
          !followUp && t('sms.bar.control', { n: formatNumber(holdout) }),
          overlap > 0 && t('sms.bar.overlap', {
            n: formatNumber(overlap), word: plural(t, 'word.fits', overlap),
          }),
        ].filter(Boolean).join(' · ')
      : null
  const nextLabel = target > 0
    ? t('sms.wizardNextCount', {
        n: formatNumber(target), word: plural(t, 'word.recipients', target),
      })
    : t('sms.wizardNext')
  const nextDisabled = target === 0 || !campaignValid || stale || Boolean(error)
    || (!followUp && (open.length > 0 || emptyGroups.length > 0))
  const goNext = () => setStep(followUp ? 'message' : 'control')

  // ── Telegram's own buttons, inside the Mini App ─────────────────────────
  const webApp = useTelegramWebApp()
  const layerOpen = useAnyLayerOpen()
  useTelegramMainButton(webApp, {
    visible: step === 'audience' && !layerOpen,
    text: nextLabel,
    enabled: !nextDisabled,
    onClick: goNext,
  })
  const stepIndex = steps.indexOf(step)
  useTelegramBackButton(webApp, {
    visible: layerOpen || stepIndex > 0,
    onClick: () => {
      if (closeTopLayer()) return
      if (stepIndex > 0) setStep(steps[stepIndex - 1])
    },
  })

  return (
    <Card>
      <CardHeader>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Badge tone="slate" shape="tag">{t('sms.step', { n: 1 })}</Badge>
            <CardTitle>{t('sms.wizardTitle')}</CardTitle>
          </div>
          <Button variant="secondary" size="sm" onClick={onClose}>
            <X className="w-3.5 h-3.5" /> {t('sms.wizardClose')}
          </Button>
        </div>
        <nav className="mt-2 flex flex-wrap gap-1" aria-label={t('sms.wizardTitle')}>
          {steps.map((s) => (
            <StepHeader
              key={s} step={s} current={step}
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
          <div className="sm:col-span-2 min-w-0">
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              {t('sms.summaryWho')}
            </div>
            <div className="text-sm text-slate-800 mt-0.5">
              {campaign || t('sms.summaryUnnamed')}
            </div>
            <div className="text-xs text-slate-500 mt-0.5">
              {followUp ? sentence : audienceSentence(audience, t)}
            </div>
            {!followUp && (
              <div className="text-[11px] text-slate-400 mt-0.5">
                {describeMeasurement(audience, t)}
              </div>
            )}
          </div>
          <div>
            <div className="text-[11px] uppercase tracking-wide text-slate-500">
              {created ? t('sms.summaryFrozen') : t('sms.summaryHowMany')}
            </div>
            <div className="text-sm text-slate-800 mt-0.5 tabular-nums">
              {t('sms.summaryContacts', {
                target: formatNumber(shownTarget), holdout: formatNumber(shownHoldout),
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

        {/* Above the step, never instead of it: the form is how a refused
            audience gets fixed, and the draft would reopen the same one. */}
        {error && (
          <Wrapper dir="column" marginBottom="md">
            <ApiErrorState
              error={error} onRetry={() => refetch()}
              title={t('sms.previewFailed')}
              message={refusalMessage(error, t) ?? undefined}
            />
          </Wrapper>
        )}
        <>
            {/* ── 1. Audience ──────────────────────────────────────── */}
            {step === 'audience' && (
              <Wrapper dir="column" gap="lg">
                {/* The name comes first because it is what everything after it
                    belongs to — and because it decides who is held back. */}
                <Wrapper dir="column" gap="xs">
                  <label className="text-xs text-slate-600">
                    <span className="block mb-1">{t('sms.campaignName')}</span>
                    <Input
                      type="text" size="sm" value={campaign}
                      placeholder="sep-brand" maxLength={40}
                      aria-label={t('sms.campaignName')}
                      onChange={setCampaign}
                    />
                  </label>
                  {suggestion != null && campaign === suggestion && (
                    <p className="text-[11px] text-slate-500">{t('sms.campaignNameSuggested')}</p>
                  )}
                  {campaign.length > 0 && !campaignValid && (
                    <p className="text-xs text-amber-700 bg-amber-50 rounded-md px-2 py-1.5">
                      {t('sms.campaignRequired')}
                    </p>
                  )}
                </Wrapper>

                <SmsAudienceSource followUp={followUp} onChange={setFollowUp} />

                {followUp ? (
                  <SmsFollowUpPicker
                    candidates={candidates}
                    value={followUpOf || null}
                    onChange={(c) => setFollowUpOf(c ?? '')}
                    recipients={followUpOf ? data?.totals.target : undefined}
                  />
                ) : (
                  <>
                    <SmsAudienceForm
                      audience={audience}
                      onChange={editAudience}
                      shares={sharesAligned && !isPlaceholderData ? shares : undefined}
                      settled={!stale}
                      segments={data?.segments}
                      library={(
                        <Wrapper dir="column" gap="sm">
                          <SmsAudienceLibrary
                            campaigns={pastCampaigns}
                            onPickCampaign={handlePickCampaign}
                            presets={presetData?.presets ?? []}
                            selectedPreset={selectedPreset}
                            onPickPreset={handlePickPreset}
                            onDeletePreset={(name) => {
                              deletePreset.mutate(name)
                              setSelectedPreset('')
                            }}
                            onSavePreset={handleSavePreset}
                            savePending={savePreset.isPending}
                            phone={phone}
                          />
                          {shiftOffer && (
                            <div className="rounded-lg border border-purple-100 bg-purple-50 px-3 py-2">
                              <Wrapper dir="row" gap="sm" align="center" wrap>
                                <span className="text-sm text-purple-900">
                                  {t('sms.library.shiftOffer', {
                                    campaign: shiftOffer.campaign,
                                    n: shiftOffer.days,
                                    word: plural(t, 'word.days', shiftOffer.days),
                                  })}
                                </span>
                                <Button size="sm" variant="secondary" onClick={() => {
                                  setAudience(shiftDates(audience, shiftOffer.days))
                                  setShiftOffer(null)
                                }}>
                                  {t('sms.library.shift')}
                                </Button>
                                <Button size="sm" variant="ghost" onClick={() => setShiftOffer(null)}>
                                  {t('sms.library.keepDates')}
                                </Button>
                              </Wrapper>
                            </div>
                          )}
                        </Wrapper>
                      )}
                    />
                    {data && <SmsSelectionCriteria data={data} />}
                  </>
                )}

                <AudienceSummaryBar
                  sentence={sentence}
                  stats={stats}
                  blocked={blocked}
                  action={{ label: nextLabel, onClick: goNext, disabled: nextDisabled }}
                  actionPlacement={webApp ? 'native' : 'inline'}
                />
              </Wrapper>
            )}

            {/* ── 2. How it is measured ────────────────────────────── */}
            {step === 'control' && (
              <div className="space-y-4">
                <SmsMeasurementSettings
                  audience={audience}
                  onChange={editAudience}
                  segments={data?.segments}
                  target={target}
                  holdout={holdout}
                />
                <SmsAudiencePreview data={data} isLoading={isLoading} />
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
                <label className="block text-xs text-slate-600">
                  <span className="block mb-1">{t('sms.promocodeOptional')}</span>
                  <Input
                    type="text" size="sm" value={promocode}
                    placeholder="KS-SEP" maxLength={40}
                    aria-label={t('sms.promocodeOptional')}
                    onChange={setPromocode}
                  />
                </label>

                <label className="block text-xs text-slate-600">
                  <span className="block mb-1">{t('sms.messageText')}</span>
                  <Textarea
                    rows={3} size="sm" fullWidth
                    aria-label={t('sms.messageText')}
                    value={text}
                    onChange={setText}
                  />
                </label>
                <SmsCostLine cost={cost} limit={SMS_LIMIT} recipients={target} />

                <StepActions
                  onBack={() => setStep(followUp ? 'audience' : 'control')}
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
                      {formatNumber(shownTarget)}
                    </dd>
                  </div>
                  <div>
                    <dt className="text-slate-500">{t('sms.control')}</dt>
                    <dd className="text-slate-800 font-medium tabular-nums">
                      {formatNumber(shownHoldout)}
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
                        {!followUp && (
                          <Button variant="secondary" size="sm" onClick={handleCsv}>
                            {t('sms.downloadCsv')}
                          </Button>
                        )}
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
                        disabled={!campaignValid || create.isPending || target === 0
                          || (!followUp && open.length > 0)}
                      >
                        {create.isPending ? t('sms.creating') : t('sms.createCampaign')}
                      </Button>
                    }
                  />
                )}
              </div>
            )}
        </>
      </CardContent>

      {/* The rehearsal starts from the text just written, not from an empty
          box: a test of a different message is not a test. */}
      {testing && (
        <SmsTestSendDialog initialText={text} onClose={() => setTesting(false)} />
      )}
      {sending && created && (
        <SmsSendDialog
          campaign={created} initialText={text} onClose={() => setSending(false)}
        />
      )}
    </Card>
  )
})

// ─── SmsWizardIntro ──────────────────────────────────────────────────────────
//
// Step 1's collapsed state: the same panel header the wizard opens with, so
// the page reads Step 1 → 2 → 3 whether or not a campaign is being built.
// The "New campaign" button lives here — in the header slot every panel keeps
// its action in — not floating between sections.

export const SmsWizardIntro = memo(function SmsWizardIntro({ onStart }: { onStart: () => void }) {
  const { t } = useTranslation()
  return (
    <Card>
      <CardHeader divider={false}>
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Badge tone="slate" shape="tag">{t('sms.step', { n: 1 })}</Badge>
            <CardTitle>{t('sms.wizardTitle')}</CardTitle>
          </div>
          <Button variant="primary" size="sm" onClick={onStart}>
            <Plus className="w-4 h-4" /> {t('sms.newCampaign')}
          </Button>
        </div>
        <p className="text-xs text-slate-500 mt-0.5">{t('sms.wizardIntro')}</p>
      </CardHeader>
    </Card>
  )
})

export default SmsCampaignWizard
