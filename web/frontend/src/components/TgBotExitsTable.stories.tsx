import type { Meta, StoryObj } from '@storybook/react-vite'
import { TgBotExitsTable } from './TgBotExitsTable'

// Illustrative rows: what the table looks like a week into recording.
const meta = {
  title: 'Telegram/TgBotExitsTable',
  component: TgBotExitsTable,
  args: {
    tapsSince: '2026-10-25',
    silenceDays: 7,
    sessionGapMinutes: 30,
    leftTotal: 14,
    rows: [
      { what: 'menu:open_orders', label: '📦 Замовлення', presses: 64, people: 38, session_exits: 21, left_after: 6 },
      { what: 'ord:open', label: 'Замовлення ##.##', presses: 41, people: 22, session_exits: 17, left_after: 4 },
      { what: 'text', label: '✍️ Текстове повідомлення', presses: 19, people: 11, session_exits: 9, left_after: 2 },
      { what: 'menu:open_info', label: 'ℹ️ Довідка', presses: 12, people: 9, session_exits: 6, left_after: 2 },
      { what: 'key:favourites', label: '⭐ Улюблені', presses: 30, people: 17, session_exits: 5, left_after: 0 },
    ],
  },
} satisfies Meta<typeof TgBotExitsTable>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const BeforeAnyPress: Story = { args: { rows: [], tapsSince: null, leftTotal: 0 } }
