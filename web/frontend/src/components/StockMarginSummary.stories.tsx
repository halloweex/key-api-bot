import type { Meta, StoryObj } from '@storybook/react-vite'
import { StockMarginSummary } from './StockMarginSummary'
import { CLEAN_SHELF, stockMarginFixture } from '../test/stockMarginFixtures'

// Illustrative figures (test/stockMarginFixtures.ts), not production's.
const meta = {
  title: 'Margin/StockMarginSummary',
  component: StockMarginSummary,
  args: {
    data: stockMarginFixture(),
    bucket: null,
    onBucket: () => {},
    onShow: () => {},
  },
} satisfies Meta<typeof StockMarginSummary>

export default meta
type Story = StoryObj<typeof meta>

/** The four tiles, the line saying what they cover, and the margin distribution. */
export const Default: Story = {}

/** A bar picked: the table below is narrowed to it. */
export const BucketPicked: Story = { args: { bucket: 5 } }

/** Nothing to check: the fourth tile says so instead of listing problems. */
export const NothingToCheck: Story = { args: { data: stockMarginFixture(CLEAN_SHELF) } }

/** A phone: the tiles two by two, each line cut to one fact. */
export const Phone: Story = { globals: { viewport: { value: 'mobile2', isRotated: false } } }
