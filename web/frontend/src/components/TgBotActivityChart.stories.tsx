import type { Meta, StoryObj } from '@storybook/react-vite'
import { TgBotActivityChart } from './TgBotActivityChart'
import type { TgBotActivityDay } from '../types/api'

// Illustrative numbers in the bot's proportions: a quiet start, one wave of
// registrations from an announcement, a tail of returning people.
const DAILY: TgBotActivityDay[] = Array.from({ length: 30 }, (_, i) => {
  const day = new Date(Date.UTC(2026, 8, 26 + i)).toISOString().slice(0, 10)
  const wave = i === 10 ? 25 : i === 11 ? 9 : 0
  const registered = wave || (i % 4 === 0 ? 1 : 0)
  return { day, active: 4 + (i % 5) + wave * 2, new: registered + (i % 3 === 0 ? 1 : 0), registered }
})

const meta = {
  title: 'Telegram/TgBotActivityChart',
  component: TgBotActivityChart,
  args: { daily: DAILY, logStarts: null },
} satisfies Meta<typeof TgBotActivityChart>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

/** The log's first day inside the window is marked: before it, nothing was recorded. */
export const LogStartsInsideTheWindow: Story = {
  args: { logStarts: DAILY[5].day },
}

export const Loading: Story = { args: { daily: [], isLoading: true } }

export const BotUnreachable: Story = {
  args: { daily: [], error: new Error('bot unreachable: ConnectError'), onRetry: () => {} },
}
