import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { userEvent } from 'storybook/test'
import { SmsAudienceLibrary } from './SmsAudienceLibrary'
import { Wrapper } from './Wrapper'
import type { SmsAudiencePreset, SmsCampaignSummary } from '../types/api'

/**
 * The two ways into an audience besides the templates: "like a past
 * campaign", and a saved audience.
 *
 * A past campaign is offered by its sentence and its size, and loads how its
 * people were CHOSEN — the definition frozen with it — not its roster: "the
 * same as last month" is the same question asked today. Saved audiences stay
 * a native select; on a phone that is the system's own picker.
 */

// Frozen campaigns as the store records them — filter names in the store's
// snake_case, one entry per group — because that is what the page is handed
// and what `describeFrozenCriteria` has to read back into a sentence.
const CAMPAIGNS: SmsCampaignSummary[] = [
  {
    campaign: 'ks_oct_repeat',
    ltvBasis: 'margin', salesType: 'retail', holdoutPct: 10, promocode: 'KS-OCT',
    exportedAt: '2026-10-14T09:12:00Z', sentAt: '2026-10-14T10:00:00Z', notes: null,
    members: 4_101, target: 3_691, holdout: 410,
    criteria: {
      grouping: 'single', ltvBasis: 'margin', holdoutPct: 10,
      groups: [
        { maxRecencyDays: 3650,
          filters: { period_orders_min: 2, period_from: '2026-01-01', period_to: '2026-10-14' } },
        { maxRecencyDays: 3650,
          filters: { period_orders_min: 1, period_from: '2026-07-16', period_to: '2026-10-14' } },
      ],
    },
  },
  {
    campaign: 'ks_aug_vip',
    ltvBasis: 'margin', salesType: 'retail', holdoutPct: 10, promocode: 'KS-AUG',
    exportedAt: '2026-08-20T08:30:00Z', sentAt: '2026-08-20T09:00:00Z', notes: null,
    members: 1_186, target: 1_067, holdout: 119,
    criteria: {
      grouping: 'rfm', ltvBasis: 'margin', holdoutPct: 10,
      vipLtv: 5500, coreLtv: 2750, coreMinOrders: 2, reactivationMaxRecency: 120,
      maxRecencyDays: 270, filters: {},
    },
  },
  {
    campaign: 'ks_jul_quiet_cosrx',
    ltvBasis: 'margin', salesType: 'retail', holdoutPct: 20, promocode: null,
    exportedAt: '2026-07-03T07:45:00Z', sentAt: null, notes: null,
    members: 980, target: 784, holdout: 196,
    criteria: {
      grouping: 'single', ltvBasis: 'margin', holdoutPct: 20,
      maxRecencyDays: 180,
      filters: { recency_min_days: 90, brands: ['COSRX'] },
    },
  },
]

const PRESETS: SmsAudiencePreset[] = [
  {
    name: 'RFM tiers', builtin: true, createdBy: null, createdAt: null, updatedAt: null,
    criteria: { grouping: 'rfm' },
  },
  {
    name: 'Everyone', builtin: true, createdBy: null, createdAt: null, updatedAt: null,
    criteria: { grouping: 'single' },
  },
  {
    name: 'VIP, quiet for 3 months', builtin: false, createdBy: 1,
    createdAt: '2026-09-02T10:00:00Z', updatedAt: '2026-09-02T10:00:00Z',
    criteria: { grouping: 'single', tiers: ['VIP'] },
  },
]

function Live({
  campaigns = CAMPAIGNS, initialPreset = '', phone = false,
}: {
  campaigns?: SmsCampaignSummary[]
  initialPreset?: string
  phone?: boolean
}) {
  const [presets, setPresets] = useState(PRESETS)
  const [selected, setSelected] = useState(initialPreset)
  const [pending, setPending] = useState(false)
  const [last, setLast] = useState<string | null>(null)
  return (
    <Wrapper dir="column" gap="sm">
      <SmsAudienceLibrary
        campaigns={campaigns}
        onPickCampaign={(c) => setLast(`Loaded the audience of ${c.campaign}`)}
        presets={presets}
        selectedPreset={selected}
        onPickPreset={(name) => {
          setSelected(name ?? '')
          if (name) setLast(`Loaded “${name}”`)
        }}
        onDeletePreset={(name) => {
          setPresets(presets.filter((p) => p.name !== name))
          setSelected('')
          setLast(`Deleted “${name}”`)
        }}
        onSavePreset={(name, done) => {
          // A round trip, so the disabled button while saving can be seen.
          setPending(true)
          setTimeout(() => {
            setPresets([...presets.filter((p) => p.name !== name), {
              name, builtin: false, createdBy: 1, createdAt: null, updatedAt: null, criteria: {},
            }])
            setSelected(name)
            setPending(false)
            setLast(`Saved as “${name}”`)
            done()
          }, 700)
        }}
        savePending={pending}
        phone={phone}
      />
      <span className="text-xs text-slate-500">{last ?? 'Nothing loaded yet.'}</span>
    </Wrapper>
  )
}

// Found by what it is rather than by its words: the label is translated, and
// the story must open it in whichever language the reader picked.
const openPast: Story['play'] = async ({ canvasElement }) => {
  const trigger = canvasElement.querySelector<HTMLElement>('[aria-haspopup="dialog"]')
  if (trigger) await userEvent.click(trigger)
}

const meta = {
  title: 'SMS/SmsAudienceLibrary',
  component: SmsAudienceLibrary,
  // Required props; every story renders through `Live`.
  args: {
    campaigns: CAMPAIGNS,
    onPickCampaign: () => {},
    presets: PRESETS,
    selectedPreset: '',
    onPickPreset: () => {},
    onDeletePreset: () => {},
    onSavePreset: () => {},
    savePending: false,
    phone: false,
  },
} satisfies Meta<typeof SmsAudienceLibrary>

export default meta
type Story = StoryObj<typeof meta>

/** As the wizard draws it after the templates, closed. */
export const Default: Story = {
  render: () => <Live />,
}

/**
 * The past campaigns opened: each by its name, size and date, then how its
 * people were chosen — one line per group when there were several.
 */
export const PastCampaignsOpen: Story = {
  render: () => <Live />,
  play: openPast,
}

/** A saved audience picked: «Delete» appears for it, never for a built-in one. */
export const SavedAudienceSelected: Story = {
  render: () => <Live initialPreset="VIP, quiet for 3 months" />,
}

/** Before the first campaign has been frozen. */
export const NoPastCampaigns: Story = {
  render: () => <Live campaigns={[]} />,
  play: openPast,
}

/** A phone: the past campaigns open in a bottom sheet instead of a popover. */
export const OnAPhone: Story = {
  parameters: { layout: 'fullscreen' },
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => (
    <Wrapper padding="md">
      <Live phone />
    </Wrapper>
  ),
  play: openPast,
}
