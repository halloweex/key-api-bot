import type { Meta, StoryObj } from '@storybook/react-vite'
import { useLayoutEffect, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { PromotionHistory } from './PromotionHistory'
import type { PromotionVersion } from '../types/api'

/**
 * «Історія змін»: a promotion's versions, newest first, each said as what
 * changed since the one before it. Who saved a version is never said — only
 * «ви» when it was the reader.
 *
 * In Ukrainian; illustrative versions of one spring promotion.
 */

function InUkrainian({ children }: { children: ReactNode }) {
  const { i18n } = useTranslation()
  useLayoutEffect(() => {
    const before = i18n.language
    void i18n.changeLanguage('uk')
    return () => { void i18n.changeLanguage(before) }
  }, [i18n])
  return <>{children}</>
}

const V1: PromotionVersion = {
  id: 12, version: 1, name: 'Весняний розпродаж', starts_on: '2027-03-05', ends_on: '2027-03-08',
  start_time: null, kind: 'other', discount_pct: 20, scope: 'store', scope_items: [],
  channels: ['sms', 'instagram'], sms_campaign: null, note: null, status: 'planned', source: 'form',
  recorded_at: '2026-11-19T10:12:03+02:00', recorded_by_me: false,
}
const V2: PromotionVersion = {
  ...V1, version: 2, starts_on: '2027-03-06', ends_on: '2027-03-09', discount_pct: 25,
  scope: 'brands', scope_items: ['COSRX', 'Anua'],
  recorded_at: '2026-11-20T09:01:44+02:00', recorded_by_me: true,
}
const V3: PromotionVersion = {
  ...V2, version: 3, channels: ['sms', 'instagram', 'site'], start_time: '18:00',
  recorded_at: '2026-11-24T16:40:00+02:00', recorded_by_me: true,
}
const V4: PromotionVersion = {
  ...V3, version: 4, status: 'cancelled', recorded_at: '2026-12-01T11:05:00+02:00', recorded_by_me: false,
}

const meta = {
  title: 'Marketing/PromotionHistory',
  component: PromotionHistory,
  decorators: [(Story) => <InUkrainian><Story /></InUkrainian>],
  args: { versions: [V4, V3, V2, V1], isLoading: false, error: null, onRetry: () => {} },
} satisfies Meta<typeof PromotionHistory>

export default meta
type Story = StoryObj<typeof meta>

/** Created, moved with a deeper discount on two brands, announced on the
 *  site from 18:00, then cancelled. */
export const FourVersions: Story = {}

/** One save that moved nothing but the days. */
export const Moved: Story = { args: { versions: [{ ...V1, version: 2, starts_on: '2027-03-06', ends_on: '2027-03-09' }, V1] } }

export const Loading: Story = { args: { versions: undefined, isLoading: true } }

export const Failed: Story = { args: { versions: undefined, error: new Error('503') } }
