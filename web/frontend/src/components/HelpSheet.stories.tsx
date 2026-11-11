import type { Meta, StoryObj } from '@storybook/react-vite'
import { HelpSheet } from './HelpSheet'
import { Wrapper } from './Wrapper'

/**
 * An explanation one tap away, for text a block no longer says every time it
 * is opened: a light panel under its control on a wide screen, a bottom sheet
 * on a phone. The control is a round "?" with a 44 px target, or, with
 * `link`, a line saying what it opens.
 */

const meta = {
  title: 'Overlays/HelpSheet',
  component: HelpSheet,
  args: {
    title: 'How to read the funnel',
    label: 'How to read the funnel',
    children: null,
  },
} satisfies Meta<typeof HelpSheet>

export default meta
type Story = StoryObj<typeof meta>

const BODY = (
  <>
    <p>
      The steps are Google Analytics 4 events on the website. A person counts at a step once, and only after
      every step before it.
    </p>
    <p>The blue bar is the people who reached a step; the grey tail, those at the step before who did not.</p>
  </>
)

/** The round "?" beside a heading. */
export const Icon: Story = {
  render: (args) => (
    <Wrapper dir="row" gap="sm" align="center">
      <span className="text-base font-semibold text-slate-900">Site funnel</span>
      <HelpSheet {...args}>{BODY}</HelpSheet>
    </Wrapper>
  ),
}

/** A line that says what it opens, under a figure it explains. */
export const Link: Story = {
  args: { link: true, title: 'Why it differs from the CRM', label: 'why it differs from the funnel' },
  render: (args) => (
    <Wrapper dir="column" gap="xs">
      <span className="text-[13px] text-slate-600">The CRM has 760 website orders for these days</span>
      <HelpSheet {...args}>
        <p>The funnel counts people, not orders, and does not see a purchase made past the cart.</p>
      </HelpSheet>
    </Wrapper>
  ),
}

/** On a phone the same control opens a bottom sheet, closed by its button,
 *  a tap outside or Telegram's BackButton. */
export const OnAPhone: Story = {
  ...Icon,
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}
