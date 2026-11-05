import type { Meta, StoryObj } from '@storybook/react-vite'
import { SectionNav } from './SectionNav'

const ITEMS = [
  { href: '/traffic', label: 'Воронка', shortLabel: 'Воронка' },
  { href: '/traffic/sales', label: 'Продажи по каналам', shortLabel: 'Продажи' },
  { href: '/traffic/campaigns', label: 'Кампании и реклама', shortLabel: 'Кампании' },
]

const meta = {
  title: 'Navigation/SectionNav',
  component: SectionNav,
  args: { items: ITEMS, active: '/traffic', ariaLabel: 'Разделы трафика' },
} satisfies Meta<typeof SectionNav>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const SecondActive: Story = { args: { active: '/traffic/sales' } }

/** Equal columns with the short labels: every section stays on screen. */
export const OnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}
