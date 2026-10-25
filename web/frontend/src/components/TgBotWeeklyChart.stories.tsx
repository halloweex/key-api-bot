import type { Meta, StoryObj } from '@storybook/react-vite'
import { TgBotWeeklyChart } from './TgBotWeeklyChart'
import type { TgBotActivityWeek } from '../types/api'

// Illustrative: a wave of newcomers in one week, most of whom come back.
const WEEKLY: TgBotActivityWeek[] = [
  { week: '2026-09-21', active: 9, new: 3, returning: 6 },
  { week: '2026-09-28', active: 12, new: 4, returning: 8 },
  { week: '2026-10-05', active: 61, new: 48, returning: 13 },
  { week: '2026-10-12', active: 44, new: 9, returning: 35 },
  { week: '2026-10-19', active: 37, new: 6, returning: 31 },
  { week: '2026-10-26', active: 8, new: 1, returning: 7 },
]

const meta = {
  title: 'Telegram/TgBotWeeklyChart',
  component: TgBotWeeklyChart,
  args: { weekly: WEEKLY },
} satisfies Meta<typeof TgBotWeeklyChart>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Empty: Story = { args: { weekly: [] } }
