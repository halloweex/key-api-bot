import type { Meta, StoryObj } from '@storybook/react-vite'
import { useLayoutEffect, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { userEvent, within } from 'storybook/test'
import { PromotionForm } from './PromotionForm'
import type { Promotion, SmsCampaignSummary } from '../types/api'

/**
 * «Додати акцію» and «Змінити» on /marketing: the approved mockup's form.
 * Only what is known before a promotion starts; saving sends the whole
 * promotion, which the server keeps as a new version. The checks are the
 * API's, said beside their fields once a save was tried.
 *
 * In Ukrainian, as the people who fill it read it. Illustrative names and
 * dates, not production's.
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

const BRANDS = ['Anua', 'Beauty of Joseon', 'COSRX', 'Isntree', 'Medicube', 'Round Lab', 'Skin1004']
  .map((name) => ({ value: name, label: name }))
const CATEGORIES = ['Догляд за обличчям', 'Сонцезахист', 'Догляд за тілом', 'Макіяж']
  .map((name) => ({ value: name, label: name }))

const CAMPAIGNS = [
  { campaign: 'nov-black-friday', promocode: 'BF26', sentAt: '2026-11-21T08:00:00Z', exportedAt: '2026-11-20T15:00:00Z' },
  { campaign: 'nov-skin1004', promocode: 'SKIN15', sentAt: null, exportedAt: '2026-11-18T12:00:00Z' },
  { campaign: 'oct-anua', promocode: null, sentAt: '2026-10-09T09:30:00Z', exportedAt: '2026-10-08T17:00:00Z' },
] as SmsCampaignSummary[]

const SPRING: Promotion = {
  id: 12, version: 3, name: 'Весняний розпродаж',
  starts_on: '2027-03-05', ends_on: '2027-03-08', start_time: '18:00',
  kind: 'other', discount_pct: 20, scope: 'brands', scope_items: ['COSRX', 'Anua'],
  channels: ['sms', 'instagram'], sms_campaign: null, note: 'Промокод SPRING у сторіс',
  status: 'planned', created_at: '2026-11-19T10:12:03+02:00',
  updated_at: '2026-11-20T09:01:44+02:00', source: 'form',
}

const meta = {
  title: 'Marketing/PromotionForm',
  component: PromotionForm,
  decorators: [(Story) => <InUkrainian><Story /></InUkrainian>],
  args: {
    promotion: null,
    brandOptions: BRANDS,
    categoryOptions: CATEGORIES,
    saving: false,
    onSubmit: () => {},
    onClose: () => {},
  },
} satisfies Meta<typeof PromotionForm>

export default meta
type Story = StoryObj<typeof meta>

/** A new promotion, from a reader who may not see SMS campaigns. */
export const Add: Story = {}

/** A reader of SMS campaigns may start from one: its name and the SMS
 *  channel are filled in, and the version says where it came from. */
export const AddFromSmsCampaign: Story = { args: { smsCampaigns: CAMPAIGNS } }

/** «Змінити»: the promotion as it stands, to be saved as its next version. */
export const Edit: Story = { args: { promotion: SPRING, smsCampaigns: CAMPAIGNS } }

/** A save tried on an empty form: each field says what it wants. */
export const RefusedWhenEmpty: Story = {
  play: async ({ canvasElement }) => {
    await userEvent.click(within(canvasElement).getByRole('button', { name: 'Зберегти' }))
  },
}

/** While the server answers. */
export const Saving: Story = { args: { promotion: SPRING, saving: true } }

/** On a phone the pairs stack, one field a row. */
export const OnAPhone: Story = {
  args: { promotion: SPRING, smsCampaigns: CAMPAIGNS },
  globals: { viewport: { value: 'mobile2', isRotated: false } },
}
