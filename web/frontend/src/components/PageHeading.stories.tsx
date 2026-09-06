import type { Meta, StoryObj } from '@storybook/react-vite'
import { ShieldCheck, ArrowLeft } from 'lucide-react'
import { PageHeading } from './PageHeading'
import { PageHeaderLink } from './PageHeaderLink'

const meta = {
  title: 'Navigation/PageHeading',
  component: PageHeading,
  args: { title: 'User Management' },
} satisfies Meta<typeof PageHeading>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const WithSubtitle: Story = {
  args: { subtitle: 'Approve dashboard access and assign roles' },
}

export const WithActions: Story = {
  args: {
    subtitle: 'Approve dashboard access and assign roles',
    actions: (
      <>
        <PageHeaderLink href="#" icon={ShieldCheck}>Permissions</PageHeaderLink>
        <PageHeaderLink href="#" icon={ArrowLeft}>Dashboard</PageHeaderLink>
      </>
    ),
  },
}
