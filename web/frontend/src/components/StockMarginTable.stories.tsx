import { useState, type ComponentProps } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { StockMarginTable } from './StockMarginTable'
import { stockMarginFixture } from '../test/stockMarginFixtures'
import { NO_FILTERS, type StockMarginFilters } from '../utils/stockMargin'

// Illustrative figures (test/stockMarginFixtures.ts), not production's. The
// filters live in the story so the chips, search and brand work as on the page.
function Live(props: ComponentProps<typeof StockMarginTable>) {
  const [filters, setFilters] = useState<StockMarginFilters>(props.filters)
  return <StockMarginTable {...props} filters={filters} onFilters={setFilters} />
}

const meta = {
  title: 'Margin/StockMarginTable',
  component: StockMarginTable,
  args: {
    data: stockMarginFixture(),
    filters: NO_FILTERS,
    onFilters: () => {},
    onExport: () => {},
  },
  render: (args) => <Live {...args} />,
} satisfies Meta<typeof StockMarginTable>

export default meta
type Story = StoryObj<typeof meta>

/** Every good in stock, the most profit on the shelf first. */
export const Default: Story = {}

/** What to check: no price or cost, a mistyped cost, sold below cost. */
export const ToCheck: Story = { args: { filters: { ...NO_FILTERS, view: 'problems' } } }

/** More than a year of stock, or no sale in 90 days. */
export const Slow: Story = { args: { filters: { ...NO_FILTERS, view: 'slow' } } }

/** Not goods: the certificate and the giveaways, with no margin of their own. */
export const CertificatesAndGiveaways: Story = { args: { filters: { ...NO_FILTERS, view: 'other' } } }

/** A search nothing matches, and the way back. */
export const NothingMatches: Story = { args: { filters: { ...NO_FILTERS, query: 'no such item' } } }

/** A phone: a card per offer, sorted by a select, the record in a bottom sheet. */
export const Phone: Story = { globals: { viewport: { value: 'mobile2', isRotated: false } } }
