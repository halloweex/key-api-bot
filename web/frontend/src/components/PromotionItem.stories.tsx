import type { Meta, StoryObj } from '@storybook/react-vite'
import { useLayoutEffect, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { userEvent, within } from 'storybook/test'
import { Card, CardContent } from './Card'
import { PromotionItem } from './PromotionItem'
import { currentOf, installPromotionsApi, promotionHistories } from '../test/promotionFixtures'
import { kyivToday } from '../utils/smsDates'

/**
 * One promotion in the calendar's lists: its name and type, then one line of
 * what was recorded. `marketing` edit adds «Змінити», «Скасувати» and
 * «Відновити»; a promotion saved more than once says its history in words.
 *
 * In Ukrainian, on the illustrative calendar of `test/promotionFixtures`,
 * dated from today; the history is read from its in-memory API.
 */

const TODAY = kyivToday()
const HISTORIES = promotionHistories(TODAY)
const byId = (id: number) => currentOf(HISTORIES[id - 1])

function Harness({ children }: { children: ReactNode }) {
  const { i18n } = useTranslation()
  // Before the first render, where the history's query is made.
  const [client] = useState(() => {
    installPromotionsApi(HISTORIES)
    return new QueryClient({ defaultOptions: { queries: { retry: false } } })
  })
  useLayoutEffect(() => {
    const before = i18n.language
    void i18n.changeLanguage('uk')
    return () => { void i18n.changeLanguage(before) }
  }, [i18n])
  return (
    <QueryClientProvider client={client}>
      <Card>
        <CardContent>
          <ul>{children}</ul>
        </CardContent>
      </Card>
    </QueryClientProvider>
  )
}

const meta = {
  title: 'Marketing/PromotionItem',
  component: PromotionItem,
  decorators: [(Story) => <Harness><Story /></Harness>],
  args: {
    promotion: byId(2),
    today: TODAY,
    canEdit: true,
    onEdit: () => {},
    onCancel: () => {},
    onRestore: () => {},
  },
} satisfies Meta<typeof PromotionItem>

export default meta
type Story = StoryObj<typeof meta>

/** A Black Friday saved three times, for somebody who may change it. */
export const BlackFriday: Story = {}

/** «Історія змін» open: moved with a deeper discount, then the site banner. */
export const HistoryOpen: Story = {
  play: async ({ canvasElement }) => {
    await userEvent.click(within(canvasElement).getByRole('button', { name: /Історія змін/ }))
  },
}

/** Running today, to somebody who may only read it. */
export const RunningReadOnly: Story = { args: { promotion: byId(1), canEdit: false } }

/** Started from an SMS campaign: two categories, from 18:00, a note. */
export const FromSmsCampaign: Story = { args: { promotion: byId(3) } }

/** Cancelled: struck through, and «Відновити» instead of «Змінити». */
export const Cancelled: Story = { args: { promotion: byId(6) } }

/** While its cancel is on its way. */
export const Busy: Story = { args: { busy: true } }
