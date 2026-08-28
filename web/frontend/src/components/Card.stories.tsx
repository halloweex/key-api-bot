import type { Meta, StoryObj } from '@storybook/react-vite'
import { Card, CardHeader, CardTitle, CardContent } from './Card'

const meta = {
  title: 'Layout/Card',
  component: Card,
} satisfies Meta<typeof Card>

export default meta
type Story = StoryObj<typeof meta>

export const Default: Story = {
  args: { children: null },
  render: () => (
    <Card>
      <CardHeader>
        <CardTitle>Revenue Trend</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-slate-600">Card body content sits here.</p>
      </CardContent>
    </Card>
  ),
}

export const Dark: Story = {
  args: { children: null },
  render: () => (
    <Card variant="dark">
      <CardHeader>
        <CardTitle>Dark surface</CardTitle>
      </CardHeader>
      <CardContent>
        <p className="text-sm text-slate-300">Used for panels drawn over dark chart areas.</p>
      </CardContent>
    </Card>
  ),
}

export const CompactPadding: Story = {
  args: { children: null },
  render: () => (
    <Card>
      <CardHeader>
        <CardTitle>Compact content</CardTitle>
      </CardHeader>
      <CardContent padding="compact">
        <p className="text-sm text-slate-600">Tighter body padding for dense panels.</p>
      </CardContent>
    </Card>
  ),
}

export const TablePadding: Story = {
  args: { children: null },
  render: () => (
    <Card>
      <CardHeader>
        <CardTitle>Table content</CardTitle>
      </CardHeader>
      <CardContent padding="table">
        <table className="w-full text-sm">
          <tbody>
            <tr className="border-b border-slate-100">
              <td className="py-2 px-5">Row flush with the card edge</td>
            </tr>
            <tr>
              <td className="py-2 px-5">Second row</td>
            </tr>
          </tbody>
        </table>
      </CardContent>
    </Card>
  ),
}
