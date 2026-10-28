import type { Meta, StoryObj } from '@storybook/react-vite'
import { TgBotEntryFunnel } from './TgBotEntryFunnel'

// Illustrative, in the proportions the bot showed on 2026-10-25.
const meta = {
  title: 'Telegram/TgBotEntryFunnel',
  component: TgBotEntryFunnel,
  args: {
    steps: [
      { key: 'started', people: 162 },
      { key: 'touched_share', people: 119 },
      { key: 'registered', people: 110 },
      { key: 'acted', people: 87 },
      { key: 'came_back', people: 12 },
    ],
  },
} satisfies Meta<typeof TgBotEntryFunnel>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const NobodyNew: Story = { args: { steps: [] } }
