import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { PromotionForm } from '../PromotionForm'
import type { Promotion, SmsCampaignSummary } from '../../types/api'

// `t` is the setup's: every string is its key, so the assertions read which
// sentence is said, not how it is translated.

const CAMPAIGNS = [
  { campaign: 'nov-bf', promocode: 'BF26', sentAt: '2026-11-21T10:00:00Z', exportedAt: null },
  { campaign: 'oct-sale', promocode: null, sentAt: null, exportedAt: '2026-10-02T10:00:00Z' },
] as SmsCampaignSummary[]

const BRANDS = [{ value: 'COSRX', label: 'COSRX' }, { value: 'Anua', label: 'Anua' }]

function form(props: Partial<Parameters<typeof PromotionForm>[0]> = {}) {
  const onSubmit = vi.fn()
  const onClose = vi.fn()
  render(
    <PromotionForm
      promotion={null}
      brandOptions={BRANDS}
      categoryOptions={[]}
      saving={false}
      onSubmit={onSubmit}
      onClose={onClose}
      {...props}
    />,
  )
  return { onSubmit, onClose }
}

const field = (label: string) => screen.getByLabelText(label) as HTMLInputElement
const setDate = (label: string, value: string) => fireEvent.change(field(label), { target: { value } })
const save = () => userEvent.click(screen.getByRole('button', { name: 'promo.save' }))

describe('the promotion form refuses what the API refuses', () => {
  it('sends nothing empty, and says what is missing beside each field', async () => {
    const { onSubmit } = form()
    // Nothing is said before a save is tried.
    expect(screen.queryByText('promo.error.nameRequired')).not.toBeInTheDocument()

    await save()
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText('promo.error.nameRequired')).toBeInTheDocument()
    expect(screen.getAllByText('promo.error.dateRequired')).toHaveLength(2)
    expect(screen.getByText('promo.error.fixFields')).toBeInTheDocument()
    expect(field('promo.field.name')).toHaveAttribute('aria-invalid', 'true')
    // The reader lands on the first field to mend.
    expect(document.activeElement).toBe(field('promo.field.name'))
  })

  it('says a last day before the first, and a discount past 90', async () => {
    const { onSubmit } = form()
    await userEvent.type(field('promo.field.name'), 'Весна')
    setDate('promo.field.startsOn', '2027-03-08')
    setDate('promo.field.endsOn', '2027-03-05')
    await userEvent.type(field('promo.field.discount'), '95')
    await save()
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText('promo.error.endBeforeStart')).toBeInTheDocument()
    expect(screen.getByText('promo.error.discount')).toBeInTheDocument()
  })

  it('wants the brands named when the promotion is on brands', async () => {
    const { onSubmit } = form()
    await userEvent.type(field('promo.field.name'), 'Весна')
    setDate('promo.field.startsOn', '2027-03-05')
    await userEvent.selectOptions(field('promo.field.scope'), 'brands')
    await save()
    expect(onSubmit).not.toHaveBeenCalled()
    expect(screen.getByText('promo.error.scopeItemsRequired')).toBeInTheDocument()
  })
})

describe('what the form sends', () => {
  it('sends the contract’s body for what was filled in', async () => {
    const { onSubmit } = form()
    await userEvent.type(field('promo.field.name'), 'Весняний розпродаж')
    setDate('promo.field.startsOn', '2027-03-05')
    // A one-day promotion is the common case: the last day follows the first.
    expect(field('promo.field.endsOn').value).toBe('2027-03-05')
    setDate('promo.field.endsOn', '2027-03-08')
    fireEvent.change(field('promo.field.startTime'), { target: { value: '18:00' } })
    await userEvent.type(field('promo.field.discount'), '20')
    await userEvent.selectOptions(field('promo.field.scope'), 'brands')
    await userEvent.click(screen.getByRole('button', { name: 'COSRX' }))
    await userEvent.click(screen.getByRole('button', { name: 'promo.channel.instagram' }))
    await userEvent.click(screen.getByRole('button', { name: 'promo.channel.sms' }))
    await save()

    expect(onSubmit).toHaveBeenCalledWith({
      name: 'Весняний розпродаж', starts_on: '2027-03-05', ends_on: '2027-03-08',
      start_time: '18:00', kind: 'other', discount_pct: 20, scope: 'brands',
      scope_items: ['COSRX'], channels: ['sms', 'instagram'], sms_campaign: null,
      note: null, source: 'form',
    })
  })

  it('opens a promotion as it stands and sends it changed', async () => {
    const promotion: Promotion = {
      id: 12, version: 3, name: 'Весняний розпродаж', starts_on: '2027-03-05', ends_on: '2027-03-08',
      start_time: null, kind: 'other', discount_pct: 20, scope: 'store', scope_items: [],
      channels: ['sms'], sms_campaign: null, note: null, status: 'planned',
      created_at: '2026-11-19T10:12:03+02:00', updated_at: '2026-11-20T09:01:44+02:00', source: 'form',
    }
    const { onSubmit } = form({ promotion })
    expect(screen.getByText('promo.formEdit')).toBeInTheDocument()
    expect(field('promo.field.discount').value).toBe('20')
    await userEvent.clear(field('promo.field.discount'))
    await userEvent.type(field('promo.field.discount'), '25')
    await save()
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ discount_pct: 25, channels: ['sms'] }))
  })

  it('closes without sending', async () => {
    const { onSubmit, onClose } = form()
    await userEvent.click(screen.getByRole('button', { name: 'promo.close' }))
    expect(onClose).toHaveBeenCalled()
    expect(onSubmit).not.toHaveBeenCalled()
  })
})

describe('a promotion from an SMS campaign', () => {
  it('is not offered to a reader who may not see SMS campaigns', () => {
    form({ smsCampaigns: undefined })
    expect(screen.queryByLabelText('promo.field.fromSms')).not.toBeInTheDocument()
  })

  it('takes the campaign’s name and the SMS channel, and says where the version came from', async () => {
    const { onSubmit } = form({ smsCampaigns: CAMPAIGNS })
    await userEvent.selectOptions(field('promo.field.fromSms'), 'nov-bf')
    expect(field('promo.field.name').value).toBe('nov-bf')
    expect(screen.getByRole('button', { name: 'promo.channel.sms' })).toHaveAttribute('aria-pressed', 'true')

    setDate('promo.field.startsOn', '2026-11-21')
    await save()
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({
      name: 'nov-bf', sms_campaign: 'nov-bf', channels: ['sms'], source: 'form_from_sms',
    }))
  })

  it('keeps a name the person typed, and lists the newest campaign first', async () => {
    form({ smsCampaigns: CAMPAIGNS })
    const options = [...(field('promo.field.fromSms') as unknown as HTMLSelectElement).options]
      .map((o) => o.value)
    expect(options).toEqual(['', 'nov-bf', 'oct-sale'])
    await userEvent.type(field('promo.field.name'), 'Чорна п’ятниця')
    await userEvent.selectOptions(field('promo.field.fromSms'), 'nov-bf')
    expect(field('promo.field.name').value).toBe('Чорна п’ятниця')
  })

  it('takes the link back when the choice is cleared', async () => {
    const { onSubmit } = form({ smsCampaigns: CAMPAIGNS })
    await userEvent.selectOptions(field('promo.field.fromSms'), 'nov-bf')
    await userEvent.selectOptions(field('promo.field.fromSms'), '')
    setDate('promo.field.startsOn', '2026-11-21')
    await save()
    expect(onSubmit).toHaveBeenCalledWith(expect.objectContaining({ sms_campaign: null, source: 'form' }))
  })
})
