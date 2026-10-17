import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { SmsAudienceForm } from './SmsAudienceForm'
import { emptyAudience } from '../utils/smsAudience'
import type {
  Brand, Category, SmsAudienceCriteria, SmsAudienceGroupShare, SmsSegment,
} from '../types/api'

/**
 * The wizard's first screen: who the campaign is for.
 *
 * An audience is one or more GROUPS — each a set of filters that must all
 * hold, with its own window — and a customer gets the message if they match
 * any group. Below the groups sits what is common to the whole campaign: the
 * value level and the ranking basis apply to the union, not to a group.
 *
 * Two more things are worth seeing in isolation, because both were decisions
 * rather than defaults.
 *
 * The filter panel opens EXPANDED. Behind a chevron it read as an advanced
 * corner, and the first person to use the wizard reported there were no date
 * filters at all — they were there, folded away.
 *
 * The measurement section stays FOLDED, and for the opposite reason. Building
 * a campaign is: who gets it, what it says, send. Arms, thresholds and
 * multiplicity corrections are an analyst's questions, and putting them in the
 * middle of that path is what made the sequence unreadable.
 */

// The form reads brands and categories itself, so the stories seed the cache
// rather than passing them in — the shape the page uses. A prop-driven variant
// would document an API the component does not have.
const BRANDS: Brand[] = [
  { name: "COSRX" }, { name: "Beauty of Joseon" }, { name: "Anua" },
  { name: "Round Lab" }, { name: "SKIN1004" }, { name: "Medicube" },
]

const CATEGORIES: Category[] = [
  { id: 1, name: 'Догляд за обличчям' },
  { id: 2, name: 'Очищення' },
  { id: 3, name: 'Сонцезахист' },
  { id: 4, name: 'Маски' },
]

// Sizes from a real run against the retail base, so the tier chips carry
// numbers somebody could recognise rather than round invented ones.
const SEGMENTS: SmsSegment[] = [
  {
    tier: 'VIP', total: 1_186, target: 1_067, holdout: 119,
    totalLtv: 4_312_880, avgLtv: 3_636, totalRevenue: 7_940_210,
    totalMargin: 4_312_880, marginPct: 54.3, avgOrders: 4.2,
    avgRecencyDays: 61,
  },
  {
    tier: 'CORE', total: 3_204, target: 2_884, holdout: 320,
    totalLtv: 3_118_400, avgLtv: 973, totalRevenue: 5_742_900,
    totalMargin: 3_118_400, marginPct: 54.3, avgOrders: 2.1,
    avgRecencyDays: 94,
  },
  {
    tier: 'REACTIVATION', total: 4_200, target: 3_780, holdout: 420,
    totalLtv: 1_940_100, avgLtv: 462, totalRevenue: 3_573_300,
    totalMargin: 1_940_100, marginPct: 54.3, avgOrders: 1.2,
    avgRecencyDays: 188,
  },
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
 * Stateful on purpose. The form is a controlled component and every control
 * reads back from `audience`, so a story with a frozen prop would show chips
 * that cannot be pressed and ranges that cannot be typed into — a screenshot
 * rather than the component.
 */
function Live({
  initial, segments, shares, recipients,
}: {
  initial: SmsAudienceCriteria
  segments?: SmsSegment[]
  shares?: SmsAudienceGroupShare[]
  recipients?: number
}) {
  const [audience, setAudience] = useState(initial)
  return (
    <QueryClientProvider client={client()}>
      <SmsAudienceForm
        audience={audience}
        onChange={setAudience}
        segments={segments}
        shares={shares}
        recipients={recipients}
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
  // the component has no required prop, which is why the neighbouring
  // `AccessRequestsCard` story needs none.
  //
  // The values are a placeholder the stories never use: `render` replaces the
  // whole tree. They exist to satisfy the type and to give the Controls panel
  // something honest to show.
  args: {
    audience: emptyAudience(),
    onChange: () => {},
  },
} satisfies Meta<typeof SmsAudienceForm>

export default meta
type Story = StoryObj<typeof meta>

/** How the screen opens: one arm, no filters, nothing removed unasked. */
export const Default: Story = {
  render: () => <Live initial={emptyAudience()} />,
}

/**
 * With the arm sizes the page passes once a preview has run. The tier chips
 * carry their counts here and are blank in `Default` — that difference is the
 * whole reason `segments` is optional.
 */
export const WithArmSizes: Story = {
  render: () => (
    <Live
      initial={{ ...emptyAudience(), grouping: 'rfm', tiers: [] }}
      segments={SEGMENTS}
    />
  ),
}

/**
 * The gender filter, which is why this component was last touched.
 *
 * Two controls and not one. The chips pick who; the select says how far to
 * trust an inference. That second control is not decoration — measured on the
 * classifier's gold set, every false male comes from one layer, and that layer
 * is the only one emitting `medium`. On the real base: 313 men at any
 * confidence, 294 at `high` or better, 43 from patronymics alone.
 *
 * Customers whose gender could not be decided — 1.4% — can never enter a
 * gendered audience, and the note under the chips says so as soon as one is
 * picked.
 */
export const FilteredByGender: Story = {
  render: () => (
    <Live
      initial={{
        ...emptyAudience(),
        groups: [{ maxRecencyDays: 270,
                   filters: { genders: ['m'], genderMinConfidence: 'high' } }],
      }}
      segments={SEGMENTS}
    />
  ),
}

/**
 * Several filters at once, which is what the badge on the collapsed panel
 * counts. Worth looking at for the ordering: the groups read when → how much →
 * what → who, and gender sits in "who" because it describes the person rather
 * than anything they bought.
 */
export const HeavilyFiltered: Story = {
  render: () => (
    <Live
      initial={{
        ...emptyAudience(),
        groups: [{
          maxRecencyDays: 270,
          filters: {
            recencyMax: 90,
            ordersMin: 2,
            ltvMin: 1_500,
            brands: ['COSRX', 'Anua'],
            genders: ['f'],
            boughtWithinDays: 180,
          },
        }],
      }}
      segments={SEGMENTS}
    />
  ),
}

/**
 * Three arms instead of one. The cascade drops whoever matches none of its
 * conditions — mostly one-order buyers — which is why `single` is the default
 * and this is a deliberate switch rather than a setting somebody inherits.
 */
export const SplitIntoTiers: Story = {
  render: () => (
    <Live
      initial={{
        ...emptyAudience(),
        grouping: 'rfm',
        tiers: ['VIP', 'CORE'],
        tierRules: { vipLtv: 5_500, coreLtv: 2_750, coreMinOrders: 2 },
      }}
      segments={SEGMENTS}
    />
  ),
}

/**
 * The owner's campaign, and the reason groups exist: two or more purchases
 * since 1 January, OR anybody who bought in the last 92 days. Each group keeps
 * its own window, so the second group's 92 days do not cut the first one
 * down. Each card says what its group brings, and the line under them says
 * what they add up to once a number in both is counted once. Every figure is
 * the retail base as measured on production on 2026-10-17.
 */
export const TwoGroupsORed: Story = {
  render: () => (
    <Live
      initial={{
        ...emptyAudience(),
        groups: [
          { maxRecencyDays: 365,
            filters: { periodOrdersMin: 2, periodFrom: '2026-01-01' } },
          { maxRecencyDays: 92, filters: {} },
        ],
      }}
      shares={[
        { group: 1, matched: 2_739, recipients: 2_626, onlyHere: 1_035 },
        { group: 2, matched: 3_237, recipients: 3_103, onlyHere: 1_512 },
      ]}
      recipients={4_138}
    />
  ),
}

/**
 * A second group that selects nobody says so on its own card — an empty
 * audience must still be able to say which rule emptied it.
 */
export const AGroupMatchingNobody: Story = {
  render: () => (
    <Live
      initial={{
        ...emptyAudience(),
        groups: [
          { maxRecencyDays: 92, filters: {} },
          { maxRecencyDays: 270, filters: { ordersMin: 40 } },
        ],
      }}
      shares={[
        { group: 1, matched: 3_237, recipients: 3_103, onlyHere: 3_103 },
        { group: 2, matched: 0, recipients: 0, onlyHere: 0 },
      ]}
      recipients={3_103}
    />
  ),
}
