import type { Meta, StoryObj } from '@storybook/react-vite'
import { ShoppingCart, CircleDollarSign, Calculator } from 'lucide-react'
import { StatCard, StatCardSkeleton } from './StatCard'

const formatCurrency = (v: number) => `₴${v.toLocaleString('uk-UA')}`
const formatNumber = (v: number) => v.toLocaleString('uk-UA')

const meta = {
  title: 'Data display/StatCard',
  component: StatCard,
  args: {
    label: 'Total Revenue',
    value: 1834807,
    formatter: formatCurrency,
  },
} satisfies Meta<typeof StatCard>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: { variant: 'green', icon: CircleDollarSign },
}

export const Variants: Story = {
  render: () => (
    <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
      <StatCard label="Orders" value={1284} formatter={formatNumber} variant="blue" icon={ShoppingCart} />
      <StatCard label="Revenue" value={1834807} formatter={formatCurrency} variant="green" icon={CircleDollarSign} />
      <StatCard label="Avg check" value={1429} formatter={formatCurrency} variant="purple" icon={Calculator} />
    </div>
  ),
}

export const WithTrend: Story = {
  args: {
    variant: 'green',
    icon: CircleDollarSign,
    trend: 1,
    trendValue: 12.4,
    subtitle: 'vs last week',
  },
}

/** Two figures under the value, each on its own line: a narrow tile wraps
 *  a joined line between a number and its noun. */
export const WithDetail: Story = {
  args: {
    variant: 'blue',
    icon: ShoppingCart,
    subtitle: '612 orders',
    detail: '214 new customers',
  },
}

export const Clickable: Story = {
  args: { variant: 'orange', clickable: true, onClick: () => {} },
}

export const NoIcon: Story = {
  name: 'Without icon (centred)',
  args: { variant: 'cyan' },
}

export const Skeleton: Story = {
  render: () => <StatCardSkeleton />,
}

/** The whole history is nine digits of hryvnia. With `shortFormatter` a
 *  card too narrow for the full figure says it in millions and keeps the
 *  full one in its tooltip; a wide card keeps it whole. */
export const ShortenedWhenNarrow: Story = {
  name: 'Shortened when narrow',
  render: () => (
    <div className="grid grid-cols-1 gap-3">
      {[160, 220, 320].map((width) => (
        <div key={width} style={{ width }}>
          <StatCard
            label={`${width} px`}
            value={108007791}
            formatter={formatCurrency}
            shortFormatter={(v) => `₴${(v / 1e6).toLocaleString('uk-UA', { maximumFractionDigits: 1 })} млн`}
            variant="green"
            icon={CircleDollarSign}
          />
        </div>
      ))}
    </div>
  ),
}
