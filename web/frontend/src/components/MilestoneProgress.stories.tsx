import type { Meta, StoryObj } from '@storybook/react-vite'
import { useState, type ReactNode } from 'react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { MilestoneProgress } from './MilestoneProgress'
import { queryKeys } from '../hooks/useApi'
import { useFilterStore } from '../store/filterStore'
import type { SmartGoalsResponse } from '../types/api'

// Illustrative numbers in production's proportions for November 2026, not
// production's export: retail's goal built from its own history, and the
// offline store, which has none of its own yet.

const METADATA = { overallYoY: 0.5019, monthlyYoY: 0.2911, calculatedAt: '2026-11-18T23:43:52+02:00' }

function period(amount: number | null) {
  return {
    amount,
    isCustom: false,
    suggestedAmount: amount,
    basedOnAverage: 146_600,
    trend: 29.11,
    confidence: 'medium' as const,
  }
}

const RETAIL: SmartGoalsResponse = {
  daily: period(160_000),
  weekly: {
    ...period(1_100_000),
    weeklyBreakdown: { 1: 1_010_000, 2: 1_070_000, 3: 1_090_000, 4: 1_150_000, 5: 380_000 },
  },
  monthly: {
    ...period(4_700_000),
    lastYearRevenue: 4_957_900,
    recent3MonthAvg: 3_545_400,
    yoyGoal: 6_401_200,
    recentGoal: 4_806_200,
    mlForecastGoal: 3_266_600,
    growthRate: 0.2911,
    seasonalityIndex: 1.3556,
    calculationMethod: 'yoy_growth',
    blendWeights: { yoy: 0.3, recent: 0.35, ml: 0.35 },
  },
  metadata: METADATA,
}

// No month a year earlier, no full month of sales, no forecast: no goal in
// any period (the owner's decision of 2026-11-19).
const NO_HISTORY: SmartGoalsResponse = {
  daily: period(null),
  weekly: { ...period(null), weeklyBreakdown: {} },
  monthly: {
    ...period(null),
    lastYearRevenue: 0,
    recent3MonthAvg: 0,
    yoyGoal: 0,
    recentGoal: 0,
    mlForecastGoal: 0,
    growthRate: 0.2911,
    seasonalityIndex: 1.3556,
    calculationMethod: 'no_history',
    blendWeights: null,
  },
  metadata: METADATA,
}

// The card reads its goal itself, so the story seeds the cache under the key
// the hook will ask for — the filter store's sales type, the month in
// progress — and fetches nothing (the trend beside it is read only for the
// last 28 days).
function Seeded({ goals, children }: { goals: SmartGoalsResponse; children: ReactNode }) {
  const salesType = useFilterStore((s) => s.salesType)
  const [client] = useState(() => {
    const c = new QueryClient({
      defaultOptions: { queries: { retry: false, staleTime: Infinity, enabled: false } },
    })
    c.setQueryData(queryKeys.smartGoals(salesType), goals)
    return c
  })
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}

const meta = {
  title: 'Dashboard/MilestoneProgress',
  component: MilestoneProgress,
  args: { revenue: 2_150_000 },
} satisfies Meta<typeof MilestoneProgress>

export default meta
type Story = StoryObj<typeof meta>

/** The month's goal, its weeks as milestones on the way. */
export const Default: Story = {
  render: (args) => (
    <Seeded goals={RETAIL}>
      <MilestoneProgress {...args} />
    </Seeded>
  ),
}

/** A sales type with no history of its own — the offline store before its
 *  first full month, the exhibition — has no goal, and the card says so in
 *  a line, with the rule behind the "?". It used to show retail's November
 *  average × growth: ₴5.7M for a shop open a week. */
export const NoHistoryOfItsOwn: Story = {
  args: { revenue: 21_140 },
  render: (args) => (
    <Seeded goals={NO_HISTORY}>
      <MilestoneProgress {...args} />
    </Seeded>
  ),
}
