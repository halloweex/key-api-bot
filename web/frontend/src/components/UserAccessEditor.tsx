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
  /**
   * The tabs this account's *role* opens — what "as the role" actually means
   * for this person. Without it the editor could only say the words, and an
   * admin had to know the matrix by heart to translate `viewer` into six tab
   * names.
   */
  roleFeatures?: readonly TabFeature[]
  /** The role's name, for the sentence that explains the grey chips. */
  roleName?: string
  presets?: AccessPreset[]
  disabled?: boolean
  onChange: (features: TabFeature[] | null) => void
  onPreset: (preset: string) => void
}

export const UserAccessEditor = memo(function UserAccessEditor({
  value,
  tabs = TAB_FEATURES,
  roleFeatures = [],
  roleName,
  presets = [],
  disabled = false,
  onChange,
  onPreset,
}: UserAccessEditorProps) {
  const { t } = useTranslation()

  // With no override the account still sees something — whatever its role
  // opens — so those chips are lit, in a different tone. Showing them all grey
  // said "no access at all" about somebody with six tabs, which is the reading
  // an admin actually reported.
  const inherited = value === null
  const granted = new Set(inherited ? roleFeatures : (value ?? []))

  const toggle = (tab: TabFeature) => {
    // **The first click starts from what they have, not from nothing.** It
    // used to start from an empty set, so ticking one tab on an inheriting
    // account silently revoked every other tab it was already seeing.
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
            // Slate while the answer comes from the role, purple once somebody
            // has decided it for this person: two states that look different,
            // because they behave differently.
            tone={inherited ? 'slate' : 'purple'}
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

      <p className="text-xs text-slate-400">
        {inherited
          ? t('access.inheritedHint', { role: roleName ?? '' })
          : t('access.editorHint')}
      </p>
    </Wrapper>
  )
})
