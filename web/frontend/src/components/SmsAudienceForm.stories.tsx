import { useState, type ReactNode } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsAudienceForm } from './SmsAudienceForm'
import { SmsAudienceLibrary } from './SmsAudienceLibrary'
import {
  emptyDefinition, type AudienceDefinition, type ConditionGroup,
} from '../utils/smsConditions'
import type {
  Brand, Category, SmsAudienceGroupShare, SmsAudiencePreset, SmsCampaignSummary, SmsSegment,
} from '../types/api'

/**
 * The wizard's first screen: who the campaign is for, built the way the
 * segment builders people already know are (Klaviyo, Shopify, Braze).
 *
 * 1. «Where to start?» — a template, a past campaign or a saved audience, each
 *    of which fills the groups with ordinary, editable conditions;
 * 2. the groups — each one sentences joined by «and», the groups joined by
 *    OR: a customer is in the audience if they match any group;
 * 3. what is common to the whole campaign: the value level, as a filter over
 *    the union.
 *
 * It replaced a form that showed about fifteen inputs per group whether or
 * not they were used. The owner got lost in it on the first real campaign —
 * typed 92 days into an order count, put a period's end date into its only
 * date field — and each story below that names the owner is that campaign.
 *
 * How the result is measured (the split by level, its cut-offs, the ranking
 * basis) is step 2's: see `SmsMeasurementSettings`.
 */

// The form reads brands and categories itself, through `useBrands` and
// `useCategories`, so the stories seed the cache under those hooks' keys
// rather than passing lists in — a prop-driven variant would document an API
// the component does not have.
const BRANDS: Brand[] = [
  { name: 'COSRX' }, { name: 'Beauty of Joseon' }, { name: 'Anua' },
  { name: 'Round Lab' }, { name: 'SKIN1004' }, { name: 'Medicube' },
  { name: 'Torriden' }, { name: 'Isntree' }, { name: 'Purito' }, { name: 'Klairs' },
]

const CATEGORIES: Category[] = [
  { id: 1, name: 'Догляд за обличчям' },
  { id: 2, name: 'Очищення' },
  { id: 3, name: 'Сонцезахист' },
  { id: 4, name: 'Маски' },
]

// Arm sizes from a value-level preview, for the counts beside the level chips.
const SEGMENTS: SmsSegment[] = [
  {
    tier: 'VIP', total: 1_186, target: 1_067, holdout: 119,
    totalLtv: 4_312_880, avgLtv: 3_636, totalRevenue: 7_940_210,
    totalMargin: 4_312_880, marginPct: 54.3, avgOrders: 4.2, avgRecencyDays: 61,
  },
  {
    tier: 'CORE', total: 2_104, target: 1_894, holdout: 210,
    totalLtv: 1_018_400, avgLtv: 484, totalRevenue: 1_875_500,
    totalMargin: 1_018_400, marginPct: 54.3, avgOrders: 2.1, avgRecencyDays: 74,
  },
  {
    tier: 'REACTIVATION', total: 811, target: 730, holdout: 81,
    totalLtv: 181_020, avgLtv: 223, totalRevenue: 333_400,
    totalMargin: 181_020, marginPct: 54.3, avgOrders: 1.3, avgRecencyDays: 102,
  },
]

// ─── The owner's audience, 2026-10-17 ────────────────────────────────────────
//
// Two purchases or more this year, OR anybody who bought in the last three
// months — the sentence the old form could not say.

const GROUP_1: ConditionGroup = {
  id: 'g1',
  conditions: [{
    id: 'g1-orders', type: 'orders', op: 'atLeast', count: 2, countMax: null,
    period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
  }],
}

const GROUP_2: ConditionGroup = {
  id: 'g2',
  conditions: [{
    id: 'g2-orders', type: 'orders', op: 'atLeast', count: 1, countMax: null,
    period: { kind: 'range', from: '2026-07-16', to: '2026-10-14' },
  }],
}

const OWNER: AudienceDefinition = { ...emptyDefinition(), groups: [GROUP_1, GROUP_2] }

const OWNER_SHARES: SmsAudienceGroupShare[] = [
  { group: 1, matched: 2_800, recipients: 2_612, onlyHere: 1_025 },
  { group: 2, matched: 3_300, recipients: 3_076, onlyHere: 1_489 },
]

function client() {
  const qc = new QueryClient({
    defaultOptions: { queries: { retry: false, refetchInterval: false } },
  })
  qc.setQueryData(['brands'], BRANDS)
  qc.setQueryData(['categories'], CATEGORIES)
  return qc
}

/**
 * Stateful on purpose. The form is controlled and every control reads back
 * from `audience`, so a frozen prop would show conditions that cannot be
 * opened and templates that cannot be applied — a screenshot rather than the
 * component.
 *
 * The shares stay as given while the groups change, as they would between
 * two previews; the form shows them only while their count matches the
 * number of groups, so adding or removing a group hides them, as on the page.
 */
function Live({
  initial, shares, segments, library,
}: {
  initial: AudienceDefinition
  shares?: SmsAudienceGroupShare[]
  segments?: SmsSegment[]
  library?: ReactNode
}) {
  const [qc] = useState(client)
  const [audience, setAudience] = useState(initial)
  return (
    <QueryClientProvider client={qc}>
      <SmsAudienceForm
        audience={audience}
        onChange={setAudience}
        shares={shares}
        segments={segments}
        library={library}
      />
    </QueryClientProvider>
  )
}

const meta = {
  title: 'SMS/SmsAudienceForm',
  component: SmsAudienceForm,
  parameters: { layout: 'padded' },
  // Meta-level args because `audience` and `onChange` are REQUIRED props, and
  // every story below renders through `Live` rather than through args. Without
  // them `tsc -b` refuses each story — args are only optional per story when
  // the component has no required prop.
  //
  // The values are a placeholder the stories never use: `render` replaces the
  // whole tree. They exist to satisfy the type and to give the Controls panel
  // something honest to show.
  args: {
    audience: emptyDefinition(),
    onChange: () => {},
  },
} satisfies Meta<typeof SmsAudienceForm>

export default meta
type Story = StoryObj<typeof meta>

/**
 * How the screen opens: one empty group — every buyer — and the templates
 * above it. «Add group» appears only once the first group says something.
 */
export const Default: Story = {
  render: () => <Live initial={emptyDefinition()} />,
}

/**
 * The owner's campaign, and the reason groups exist: 2+ orders between
 * 1 January and 14 October, OR 1+ order between 16 July and 14 October.
 * Each card says what its group brings; 1 587 people are in both, so the
 * audience is 4 101, not 5 688.
 */
export const OwnersAudience: Story = {
  render: () => <Live initial={OWNER} shares={OWNER_SHARES} />,
}

/**
 * The slip the owner made: 92 typed into the first group's order count when
 * the 92 was days. That group matches nobody, its card says so, and the row
 * offers «It's 92 days», which turns it into 1+ order in the last 92 days.
 */
export const TheSlip92: Story = {
  render: () => (
    <Live
      initial={{
        ...OWNER,
        groups: [
          {
            id: 'g1',
            conditions: [{
              id: 'g1-orders', type: 'orders', op: 'atLeast', count: 92, countMax: null,
              period: { kind: 'range', from: '2026-01-01', to: '2026-10-14' },
            }],
          },
          GROUP_2,
        ],
      }}
      shares={[
        { group: 1, matched: 0, recipients: 0, onlyHere: 0 },
        { group: 2, matched: 3_300, recipients: 3_076, onlyHere: 3_076 },
      ]}
    />
  ),
}

/**
 * A condition added and not filled in: «Bought» with nothing picked. It is
 * left out of the preview and the row says so; the wizard holds the step
 * until it is filled in or removed.
 */
export const UnfinishedCondition: Story = {
  render: () => (
    <Live
      initial={{
        ...OWNER,
        groups: [
          GROUP_1,
          {
            id: 'g2',
            conditions: [
              ...GROUP_2.conditions,
              { id: 'g2-bought', type: 'bought', brands: [], categoryIds: [], promocode: '', withinDays: null },
            ],
          },
        ],
      }}
      shares={OWNER_SHARES}
    />
  ),
}

/**
 * "VIPs only" is a filter over everybody the groups selected, not a condition
 * of one group — so it sits below them, with each level's size from the
 * latest preview beside its chip.
 */
export const VipOnly: Story = {
  render: () => (
    <Live initial={{ ...OWNER, tiers: ['VIP'] }} shares={OWNER_SHARES} segments={SEGMENTS} />
  ),
}

/** Five groups, the most an audience may OR together: «Add group» is disabled and says why. */
export const AtTheGroupLimit: Story = {
  render: () => {
    const lastOrder = (id: string, days: number): ConditionGroup => ({
      id, conditions: [{ id: `${id}-last`, type: 'lastOrder', when: { kind: 'within', days } }],
    })
    return (
      <Live
        initial={{
          ...OWNER,
          groups: [GROUP_1, GROUP_2, lastOrder('g3', 30), lastOrder('g4', 60), lastOrder('g5', 90)],
        }}
      />
    )
  },
}

// ─── The library slot ────────────────────────────────────────────────────────

const PAST: SmsCampaignSummary[] = [{
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
}]

const PRESETS: SmsAudiencePreset[] = [
  {
    name: 'RFM tiers', builtin: true, createdBy: null, createdAt: null, updatedAt: null,
    criteria: { grouping: 'rfm' },
  },
  {
    name: 'VIP, quiet for 3 months', builtin: false, createdBy: 1,
    createdAt: '2026-09-02T10:00:00Z', updatedAt: '2026-09-02T10:00:00Z',
    criteria: { grouping: 'single', tiers: ['VIP'] },
  },
]

/**
 * With the page's `library` slot filled: «Like a past campaign…» and the
 * saved audiences, after the templates. The form only places the slot; what
 * a pick loads is the wizard's (see `SmsAudienceLibrary`).
 */
export const WithLibrary: Story = {
  render: () => (
    <Live
      initial={emptyDefinition()}
      library={
        <SmsAudienceLibrary
          campaigns={PAST}
          onPickCampaign={() => {}}
          presets={PRESETS}
          selectedPreset=""
          onPickPreset={() => {}}
          onDeletePreset={() => {}}
          onSavePreset={(_, done) => done()}
          savePending={false}
          phone={false}
        />
      }
    />
  ),
}

/**
 * A phone: one column, the templates scroll sideways, and a condition opens
 * in a bottom sheet — tap a blank to see it. The form decides this itself
 * from the viewport (`useIsPhone`), so the story sets the viewport rather
 * than a prop.
 */
export const OnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => <Live initial={OWNER} shares={OWNER_SHARES} />,
}
