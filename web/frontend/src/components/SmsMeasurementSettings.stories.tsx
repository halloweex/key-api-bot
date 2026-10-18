import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { userEvent } from 'storybook/test'
import { SmsMeasurementSettings } from './SmsMeasurementSettings'
import { emptyDefinition, type AudienceDefinition } from '../utils/smsConditions'
import type { SmsSegment } from '../types/api'

/**
 * Step 2: how the campaign will be measured — the control group, whether the
 * result is read per value level, those levels' cut-offs, and what "value"
 * ranks.
 *
 * These sat in the audience step until 2026-10 and read as more conditions
 * there. They decide how the result is read, not who gets the message, so
 * they live beside the holdout they share a trade-off with — and what the
 * chosen split can still prove is said where it is chosen.
 */

// The owner's audience as one arm: 4 101 people, 10% withheld.
const ONE_ARM: SmsSegment[] = [{
  tier: 'ALL', total: 4_101, target: 3_691, holdout: 410,
  totalLtv: 5_512_300, avgLtv: 1_344, totalRevenue: 10_150_400,
  totalMargin: 5_512_300, marginPct: 54.3, avgOrders: 2.6, avgRecencyDays: 71,
}]

// The same people split by value level: each arm is smaller, so each can see
// less — which is what the sensitivity lines under the checkbox say.
const THREE_ARMS: SmsSegment[] = [
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

function Live({
  initial, segments,
}: { initial: AudienceDefinition; segments?: SmsSegment[] }) {
  const [audience, setAudience] = useState(initial)
  // What the next preview would count back: each arm's control re-drawn at
  // the chosen share, so picking 30% moves both the line under the choice and
  // what each arm can prove — the trade-off this screen exists to show.
  const counted = segments?.map((s) => {
    const withheld = Math.round((s.total * audience.holdoutPct) / 100)
    return { ...s, target: s.total - withheld, holdout: withheld }
  })
  const target = (counted ?? []).reduce((n, s) => n + s.target, 0)
  const holdout = (counted ?? []).reduce((n, s) => n + s.holdout, 0)
  return (
    <SmsMeasurementSettings
      audience={audience}
      onChange={setAudience}
      segments={counted}
      target={target}
      holdout={holdout}
    />
  )
}

const meta = {
  title: 'SMS/SmsMeasurementSettings',
  component: SmsMeasurementSettings,
  // Required props; every story renders through `Live`.
  args: {
    audience: emptyDefinition(),
    onChange: () => {},
    target: 0,
    holdout: 0,
  },
} satisfies Meta<typeof SmsMeasurementSettings>

export default meta
type Story = StoryObj<typeof meta>

/** One arm, a 10% control: the default, and the most sensitive measurement. */
export const OneArm: Story = {
  render: () => <Live initial={emptyDefinition()} segments={ONE_ARM} />,
}

/**
 * Split by value level: three arms, each measured on its own, with a
 * multiplicity correction — and each sees a smaller effect worse. The verdict
 * under the lines says whether that is still worth sending.
 */
export const SplitByLevel: Story = {
  render: () => (
    <Live initial={{ ...emptyDefinition(), grouping: 'rfm' }} segments={THREE_ARMS} />
  ),
}

/** A larger control buys precision: compare the lines with `SplitByLevel`. */
export const SplitWithALargerControl: Story = {
  render: () => (
    <Live
      initial={{ ...emptyDefinition(), grouping: 'rfm', holdoutPct: 30 }}
      segments={THREE_ARMS}
    />
  ),
}

/**
 * The folded corner opened: the ranking basis and the levels' cut-offs, with
 * the server's defaults for the chosen basis as placeholders. A blank cut-off
 * is the default, not zero.
 */
export const AdvancedOpen: Story = {
  render: () => (
    <Live
      initial={{
        ...emptyDefinition(), grouping: 'rfm',
        tierRules: { vipLtv: 6_000, coreMinOrders: 3 },
      }}
      segments={THREE_ARMS}
    />
  ),
  // The fold's toggle is the one control here with `aria-expanded`; found by
  // that, because its label is translated.
  play: async ({ canvasElement }) => {
    const toggle = canvasElement.querySelector<HTMLElement>('button[aria-expanded]')
    if (toggle) await userEvent.click(toggle)
  },
}

/** Before a preview has counted anything: no sensitivity lines, nothing guessed. */
export const NoPreviewYet: Story = {
  render: () => <Live initial={emptyDefinition()} />,
}
