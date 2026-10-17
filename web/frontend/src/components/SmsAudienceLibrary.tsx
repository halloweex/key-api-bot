import { useCallback, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Bookmark, History } from 'lucide-react'
import { BottomSheet } from './BottomSheet'
import { Button } from './Button'
import { Input } from './Input'
import { Popover } from './Popover'
import { Select } from './Select'
import { Wrapper } from './Wrapper'
import { describeFrozenCriteria, plural } from '../utils/smsConditionText'
import { formatNumber } from '../utils/formatters'
import { formatDay, kyivToday } from '../utils/smsDates'
import type { SmsAudiencePreset, SmsCampaignSummary } from '../types/api'

// ─── SmsAudienceLibrary ──────────────────────────────────────────────────────
//
// The two ways into an audience besides the templates: "like a past
// campaign", and a saved audience.
//
// A past campaign is offered by its sentence and its size, and what it loads
// is how its people were CHOSEN — the definition frozen with it — not its
// roster: "the same as last month" is the same question asked today.
//
// Saved audiences stay a native select: on a phone that is the system's own
// picker, and it is a short list of names. Saving the audience on screen sits
// beside it, under the list it saves into.

export function SmsAudienceLibrary({
  campaigns,
  onPickCampaign,
  presets,
  selectedPreset,
  onPickPreset,
  onDeletePreset,
  onSavePreset,
  savePending,
  phone,
}: {
  /** Frozen campaigns with their own audience — follow-ups have none. */
  campaigns: SmsCampaignSummary[]
  onPickCampaign: (campaign: SmsCampaignSummary) => void
  presets: SmsAudiencePreset[]
  selectedPreset: string
  onPickPreset: (name: string | null) => void
  onDeletePreset: (name: string) => void
  /** Saves the audience on screen; resolves when it has been saved. */
  onSavePreset: (name: string, done: () => void) => void
  savePending: boolean
  phone: boolean
}) {
  const { t } = useTranslation()
  const [pastOpen, setPastOpen] = useState(false)
  const [saving, setSaving] = useState(false)
  const [name, setName] = useState('')
  const closePast = useCallback(() => setPastOpen(false), [])
  const today = kyivToday()

  const selectedSaved = presets.some((p) => p.name === selectedPreset && !p.builtin)

  const list = campaigns.length === 0 ? (
    <p className="px-2 py-3 text-sm text-slate-500">{t('sms.library.noPast')}</p>
  ) : (
    <ul className="flex flex-col">
      {campaigns.map((c) => {
        const lines = describeFrozenCriteria(c.criteria, t).slice(1)
        const when = c.exportedAt ? formatDay(kyivToday(new Date(c.exportedAt))) : null
        return (
          <li key={c.campaign}>
            <button
              type="button"
              onClick={() => { setPastOpen(false); onPickCampaign(c) }}
              className="flex w-full flex-col gap-0.5 rounded-lg px-2 py-2.5 text-left
                         hover:bg-slate-50 focus:outline-none focus-visible:ring-2
                         focus-visible:ring-purple-500/40"
            >
              <span className="text-sm font-medium text-slate-800">
                {c.campaign}
                <span className="font-normal text-slate-500 tabular-nums">
                  {' · '}{t('sms.library.recipients', {
                    n: formatNumber(c.target), word: plural(t, 'word.recipients', c.target),
                  })}
                  {when && when !== formatDay(today) ? ` · ${when}` : ''}
                </span>
              </span>
              {lines.map((line) => (
                <span key={line} className="text-xs text-slate-500">{line}</span>
              ))}
            </button>
          </li>
        )
      })}
    </ul>
  )

  const pastTrigger = (
    <Button variant="secondary" size="sm" onClick={() => setPastOpen(!pastOpen)}
            aria-haspopup="dialog" aria-expanded={pastOpen}>
      <History className="h-3.5 w-3.5" aria-hidden /> {t('sms.library.likePast')}
    </Button>
  )

  return (
    <Wrapper dir="column" gap="sm">
      <Wrapper dir="row" gap="sm" align="center" wrap>
        {phone ? (
          <>
            {pastTrigger}
            <BottomSheet open={pastOpen} title={t('sms.library.likePastTitle')}
                         onClose={closePast} closeLabel={t('sms.cond.close')}>
              {list}
            </BottomSheet>
          </>
        ) : (
          <Popover open={pastOpen} onClose={closePast} trigger={pastTrigger}
                   label={t('sms.library.likePastTitle')}>
            <div className="max-h-[60vh] overflow-y-auto p-1">{list}</div>
          </Popover>
        )}

        <Select
          options={presets.map((p) => ({
            value: p.name,
            label: p.builtin ? `${p.name} · ${t('sms.presetBuiltin')}` : p.name,
          }))}
          value={selectedPreset}
          onChange={onPickPreset}
          placeholder={t('sms.presetsPlaceholder')}
          variant="compact"
          aria-label={t('sms.presetsLabel')}
        />
        {selectedSaved && (
          <Button variant="ghost" size="sm" onClick={() => onDeletePreset(selectedPreset)}>
            {t('sms.presetDeleteSelected')}
          </Button>
        )}
        <Button variant="ghost" size="sm" onClick={() => setSaving(!saving)}
                aria-expanded={saving}>
          <Bookmark className="h-3.5 w-3.5" aria-hidden /> {t('sms.presetSaveToggle')}
        </Button>
      </Wrapper>

      {saving && (
        <Wrapper dir="row" gap="sm" align="center" wrap>
          <Input
            type="text" size="sm" maxLength={60}
            aria-label={t('sms.presetSaveAs')}
            placeholder={t('sms.presetNamePlaceholder')}
            value={name}
            onChange={setName}
          />
          <Button
            variant="secondary" size="sm"
            disabled={!name.trim() || savePending}
            onClick={() => onSavePreset(name.trim(), () => { setName(''); setSaving(false) })}
          >
            {t('sms.presetSave')}
          </Button>
        </Wrapper>
      )}
    </Wrapper>
  )
}
