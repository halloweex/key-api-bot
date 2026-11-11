import type { Meta, StoryObj } from '@storybook/react-vite'
import { DataTable, Tr, Th, Td } from './DataTable'
import { ShareBar } from './ShareBar'
import { Wrapper } from './Wrapper'

/**
 * A share of a whole as a bar from the left edge of its track, with the share
 * written beside it: the organic channels and the "pixel only" buyers on
 * /traffic/sales.
 *
 * The bar is square where it starts and rounded where the share ends, so
 * every row starts on one line, as the funnel's bars do. A share above zero
 * is never thinner than 3 px: a fraction of a pixel inside a track rounded at
 * both ends read as a dot that had slipped off the bar.
 *
 * Illustrative numbers.
 */
const meta = {
  title: 'Data display/ShareBar',
  component: ShareBar,
  args: { pct: 21.5, color: '#EC4899' },
} satisfies Meta<typeof ShareBar>

export default meta
type Story = StoryObj<typeof meta>

/** One share. */
export const Default: Story = {}

/**
 * The case that was reported: a 0.4 % share above an empty one. The small
 * share is a stub where the empty track starts, not a dot at its tip.
 */
export const TinyBesideZero: Story = {
  render: () => (
    <Wrapper dir="column" gap="sm">
      <ShareBar pct={0.4} color="#A855F7" />
      <ShareBar pct={0} color="#93C5FD" />
    </Wrapper>
  ),
}

const ROWS = [
  { name: 'Channel A', pct: 64.0, color: '#DB2777' },
  { name: 'Channel B', pct: 21.5, color: '#EC4899' },
  { name: 'Channel C', pct: 5.0, color: '#0EA5E9' },
  { name: 'Channel D', pct: 1.2, color: '#059669' },
  { name: 'Channel E', pct: 0.4, color: '#A855F7' },
  { name: 'Channel F', pct: 0, color: '#93C5FD' },
]

/** In a table column, as on the page: every bar starts on one line. */
export const InTable: Story = {
  render: () => (
    <DataTable variant="feature">
      <thead>
        <Tr header>
          <Th>Channel</Th>
          <Th>Share</Th>
        </Tr>
      </thead>
      <tbody>
        {ROWS.map((r) => (
          <Tr key={r.name}>
            <Td bold>{r.name}</Td>
            <Td><ShareBar pct={r.pct} color={r.color} /></Td>
          </Tr>
        ))}
      </tbody>
    </DataTable>
  ),
}

/** The whole: the bar fills its track and ends rounded, like the track. */
export const Whole: Story = { args: { pct: 100, color: '#10B981' } }
