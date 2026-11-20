import type { Meta, StoryObj } from '@storybook/react-vite'
import { useLayoutEffect, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { PromotionCalendar } from './PromotionCalendar'
import { ToastProvider } from './Toast'
import { authQueryKey } from '../hooks/useAuth'
import { installPromotionsApi, promotionHistories, promotionsSession } from '../test/promotionFixtures'
import type { CurrentUserResponse, PromotionVersion } from '../types/api'

/**
 * «Календар акцій» on /marketing: what is planned and what ran in the last
 * 90 days, for every reader of the tab; adding, changing, cancelling and
 * restoring for `marketing` edit. `/marketing#promotions` — the bot's Monday
 * [Додати] and the badge's «+ акція» — opens the form.
 *
 * The stories run on an in-memory `/api/promotions` that keeps every save as
 * a version (`test/promotionFixtures`), so they can be used: add one, change
 * the Black Friday and open its history, cancel and restore. A promotion
 * named «помилка» is refused, to show the API's refusal in its toast. In
 * Ukrainian; made-up promotions dated from today.
 */

function Harness({
  session, histories, link = false, children,
}: {
  session: CurrentUserResponse
  histories?: PromotionVersion[][]
  /** Arrive as the link does: `#promotions` in the address. */
  link?: boolean
  children: ReactNode
}) {
  const { i18n } = useTranslation()
  // Before the first render: the section reads the hash and the API at
  // mount. Every story sets the hash it wants, with or without the anchor:
  // the next story renders before the last one's cleanup would run.
  const [client] = useState(() => {
    installPromotionsApi(histories ?? promotionHistories(), session)
    const { pathname, search } = window.location
    window.history.replaceState(null, '', `${pathname}${search}${link ? '#promotions' : ''}`)
    const c = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    c.setQueryData(authQueryKey, session)
    return c
  })
  useLayoutEffect(() => {
    const before = i18n.language
    void i18n.changeLanguage('uk')
    return () => { void i18n.changeLanguage(before) }
  }, [i18n])
  return (
    <QueryClientProvider client={client}>
      <ToastProvider>{children}</ToastProvider>
    </QueryClientProvider>
  )
}

const meta = {
  title: 'Marketing/PromotionCalendar',
  component: PromotionCalendar,
} satisfies Meta<typeof PromotionCalendar>

export default meta
type Story = StoryObj<typeof meta>

const EDITOR = promotionsSession('edit', true)

/** `marketing` edit and the SMS tab: everything, an SMS campaign to start from included. */
export const Editor: Story = {
  render: () => <Harness session={EDITOR}><PromotionCalendar /></Harness>,
}

/** The tab alone: the lists, nothing that writes. */
export const ReadOnly: Story = {
  render: () => <Harness session={promotionsSession('view')}><PromotionCalendar /></Harness>,
}

/** Nothing recorded yet. */
export const Empty: Story = {
  render: () => <Harness session={EDITOR} histories={[]}><PromotionCalendar /></Harness>,
}

/** Arrived by `/marketing#promotions`: the form is open. */
export const ArrivedByLink: Story = {
  render: () => <Harness session={EDITOR} link><PromotionCalendar /></Harness>,
}

/** A phone, as the link opens it inside Telegram. */
export const OnAPhone: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => <Harness session={EDITOR} link><PromotionCalendar /></Harness>,
}

/** A phone, the lists only. */
export const OnAPhoneReadOnly: Story = {
  globals: { viewport: { value: 'mobile2', isRotated: false } },
  render: () => <Harness session={promotionsSession('view')}><PromotionCalendar /></Harness>,
}
