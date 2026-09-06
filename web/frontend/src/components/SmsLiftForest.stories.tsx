import type { Meta, StoryObj } from '@storybook/react-vite'
import { SmsLiftForest, type ForestRow } from './SmsLiftForest'
import type { SmsComparison } from '../types/api'

const cmp = (
  liftPp: number,
  ci: [number, number],
  significant: boolean,
  verdictReady = true,
): SmsComparison => ({
  conversionTarget: 6.2,
  conversionHoldout: 6.2 - liftPp,
  liftPp,
  liftRelativePct: liftPp > 0 ? (liftPp / (6.2 - liftPp)) * 100 : null,
  ci95Pp: ci,
  pValue: significant ? 0.01 : 0.4,
  significant,
  verdictReady,
  eventsTarget: 96,
  eventsHoldout: 22,
  minEvents: 10,
  incrementalRevenuePerContact: 18.4,
  incrementalMarginPerContact: 7.2,
  incrementalRevenueTotal: 99_654,
  incrementalMarginTotal: 38_995,
})

const ROWS: ForestRow[] = [
  { label: 'Campaign overall', comparison: cmp(2.05, [0.4, 3.7], true), contacts: 5416, emphasis: true },
  { label: 'VIP', comparison: cmp(3.1, [-0.2, 6.4], false), contacts: 820 },
  { label: 'CORE', comparison: cmp(1.8, [0.1, 3.5], true), contacts: 3100 },
  { label: 'REACTIVATION', comparison: cmp(0.6, [-1.9, 3.1], false, false), contacts: 1496 },
]

const meta = {
  title: 'Domain/SmsLiftForest',
  component: SmsLiftForest,
  args: { rows: ROWS },
} satisfies Meta<typeof SmsLiftForest>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const SingleArm: Story = {
  args: { rows: [ROWS[0]] },
}
