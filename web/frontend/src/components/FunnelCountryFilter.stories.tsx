import type { Meta, StoryObj } from '@storybook/react-vite'
import { FunnelCountryFilter } from './FunnelCountryFilter'

/**
 * Whose visits the site funnel counts: Ukraine, with the visitors Google
 * Analytics could not place, or every country, bots included. Two segments on
 * a wide screen; on a phone a chip whose sheet says what each choice means.
 * It reads and sets the filter store, so the choice holds while the page is
 * open. On the page it sits beside the period, in `TrafficSectionNav`.
 */
const meta = {
  title: 'Traffic/FunnelCountryFilter',
  component: FunnelCountryFilter,
} satisfies Meta<typeof FunnelCountryFilter>

export default meta
type Story = StoryObj<typeof meta>

export const Wide: Story = {}

export const OnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}
