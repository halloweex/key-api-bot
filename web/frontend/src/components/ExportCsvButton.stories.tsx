import type { Meta, StoryObj } from '@storybook/react-vite'
import { ExportCsvButton } from './ExportCsvButton'

const meta = {
  title: 'Feedback/ExportCsvButton',
  component: ExportCsvButton,
  args: { onClick: () => {}, children: 'Export CSV' },
} satisfies Meta<typeof ExportCsvButton>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {}

export const Disabled: Story = {
  args: { disabled: true },
}
