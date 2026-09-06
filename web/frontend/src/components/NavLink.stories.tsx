import type { Meta, StoryObj } from '@storybook/react-vite'
import { BarChart3, CircleDollarSign, Rocket } from 'lucide-react'
import { NavLink } from './NavLink'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Navigation/NavLink',
  component: NavLink,
} satisfies Meta<typeof NavLink>

export default meta
type Story = StoryObj<typeof meta>

export const SidebarGroup: Story = {
  args: { href: '#', children: null },
  render: () => (
    <div className="w-64 bg-slate-50 rounded-xl p-3">
      <Wrapper gap="xs">
        <NavLink href="/" icon={BarChart3}>Sales Dashboard</NavLink>
        <NavLink href="#marketing" icon={Rocket}>Marketing</NavLink>
        <NavLink href="#financial" icon={CircleDollarSign} disabled>Financial</NavLink>
      </Wrapper>
    </div>
  ),
}
