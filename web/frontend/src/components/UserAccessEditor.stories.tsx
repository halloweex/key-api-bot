import type { Meta, StoryObj } from '@storybook/react-vite'
import { UserAccessEditor } from './UserAccessEditor'
import type { AccessPreset } from '../types/api'

const presets: AccessPreset[] = [
  { key: 'full', features: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing', 'margin', 'expenses', 'sms'] },
  { key: 'standard', features: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing'] },
  { key: 'traffic_only', features: ['traffic'] },
  { key: 'marketing', features: ['marketing', 'traffic', 'reports'] },
]

const meta = {
  title: 'Admin/UserAccessEditor',
  component: UserAccessEditor,
  args: {
    presets,
    onChange: () => {},
    onPreset: () => {},
  },
} satisfies Meta<typeof UserAccessEditor>

export default meta
type Story = StoryObj<typeof meta>

/** No override: the account sees whatever its role shows — and the chips say
 *  which tabs that is, in a different tone, rather than sitting grey and
 *  reading as "no access at all". */
export const AsTheRole: Story = {
  args: {
    value: null,
    roleName: 'Viewer',
    roleFeatures: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing'],
  },
}

/** The case this whole feature exists for. */
export const TrafficOnly: Story = {
  args: { value: ['traffic'] },
}

export const SeveralTabs: Story = {
  args: { value: ['dashboard', 'traffic', 'reports'] },
}

/** An empty set is a decision, not a missing value — and it locks the account
 *  out of every page, so it has to be visibly different from "as the role". */
export const NothingTicked: Story = {
  args: { value: [] },
}

export const WhileSaving: Story = {
  args: { value: ['traffic'], disabled: true },
}
