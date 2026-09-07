import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { Button } from './Button'
import { FilterChip } from './FilterChip'
import { Wrapper } from './Wrapper'
import { TAB_FEATURES, type AccessPreset, type TabFeature } from '../types/api'

// ─── UserAccessEditor ────────────────────────────────────────────────────────
//
// One person's tabs: a chip per page, a row of ready-made bundles, and the way
// back to "as the role".
//
// The three states this has to keep apart, because they are three different
// decisions and only two of them are obvious:
//
//   null   — no override. Whatever the role shows, and what every account
//            meant before per-user access existed.
//   [...]  — exactly these tabs.
//   []     — none of them. A real choice, and the reason the empty array is
//            not folded into `null` anywhere between here and the column.
//
// Chips rather than checkboxes: the set is small, it reads as a sentence at a
// glance ("Traffic, Reports"), and the same control is what the bot draws on
// the admin's phone when they approve somebody.

interface UserAccessEditorProps {
  value: TabFeature[] | null
  /** The grantable tabs, from the server. Falls back to the compiled list. */
  tabs?: readonly TabFeature[]
  presets?: AccessPreset[]
  disabled?: boolean
  onChange: (features: TabFeature[] | null) => void
  onPreset: (preset: string) => void
}

export const UserAccessEditor = memo(function UserAccessEditor({
  value,
  tabs = TAB_FEATURES,
  presets = [],
  disabled = false,
  onChange,
  onPreset,
}: UserAccessEditorProps) {
  const { t } = useTranslation()

  // A tab is ticked when the override names it. With no override the chips
  // show the role's own answer as "nothing ticked yet" rather than guessing —
  // the first tick then writes an explicit set, which is what the admin means
  // by touching it at all.
  const granted = new Set(value ?? [])

  const toggle = (tab: TabFeature) => {
    const next = new Set(granted)
    if (next.has(tab)) next.delete(tab)
    else next.add(tab)
    onChange(tabs.filter((key) => next.has(key)))
  }

  return (
    <Wrapper dir="column" gap="sm" paddingY="sm">
      <Wrapper dir="row" gap="xs" wrap align="center">
        {tabs.map((tab) => (
          <FilterChip
            key={tab}
            active={granted.has(tab)}
            disabled={disabled}
            onClick={() => toggle(tab)}
          >
            {t(`access.tab.${tab}`)}
          </FilterChip>
        ))}
      </Wrapper>

      <Wrapper dir="row" gap="xs" wrap align="center">
        <span className="text-xs text-slate-400">{t('access.presetsLabel')}</span>
        {presets.map((preset) => (
          <Button
            key={preset.key}
            size="sm"
            variant="secondary"
            disabled={disabled}
            onClick={() => onPreset(preset.key)}
          >
            {t(`access.preset.${preset.key}`)}
          </Button>
        ))}
        <Button
          size="sm"
          variant="ghost"
          disabled={disabled || value === null}
          onClick={() => onChange(null)}
        >
          {t('access.asRole')}
        </Button>
      </Wrapper>

      <p className="text-xs text-slate-400">{t('access.editorHint')}</p>
    </Wrapper>
  )
})
