import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen, fireEvent } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { SmsSendDialog } from '../SmsSendDialog'
import type { SmsCampaignSummary } from '../../types/api'

// src/lib/i18n.ts reads localStorage while the module is being imported, and
// jsdom here exposes the object without its methods. vi.hoisted runs before
// the import graph is evaluated, which a plain assignment would not.
vi.hoisted(() => {
  const store = new Map<string, string>()
  Object.defineProperty(globalThis, 'localStorage', {
    configurable: true,
    value: {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, String(v)),
      removeItem: (k: string) => void store.delete(k),
      clear: () => store.clear(),
    },
  })
})

const mutate = vi.fn()
const addToast = vi.fn()

vi.mock('../../hooks/useApi', () => ({
  useSendSmsCampaign: () => ({ mutate, isPending: false }),
  useSmsChannels: () => ({ data: { sms: true, viber: true } }),
}))

vi.mock('../Toast', () => ({ useToast: () => ({ addToast }) }))

const campaign: SmsCampaignSummary = {
  campaign: 'aug-promo',
  ltvBasis: 'margin',
  salesType: 'retail',
  holdoutPct: 10,
  promocode: 'KS-AUG',
  exportedAt: '2026-08-05T10:00:00',
  sentAt: null,
  notes: null,
  members: 6178,
  target: 5560,
  holdout: 618,
}

function sendButton() {
  return screen.getByRole('button', { name: 'sms.sendNow' })
}

beforeEach(() => {
  mutate.mockClear()
  addToast.mockClear()
})

describe('SmsSendDialog', () => {
  it('will not send without the campaign name typed back', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    const [message] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'Знижка 20%')

    expect(sendButton()).toBeDisabled()
    expect(mutate).not.toHaveBeenCalled()
  })

  it('will not send with the wrong name typed', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, 'aug-prom')

    expect(sendButton()).toBeDisabled()
  })

  it('will not send an empty message', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(confirm, 'aug-promo')

    expect(sendButton()).toBeDisabled()
  })

  it('sends the trimmed text once both gates pass', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(message, '  Знижка 20%  ')
    await userEvent.type(confirm, 'aug-promo')
    await userEvent.click(sendButton())

    expect(mutate).toHaveBeenCalledTimes(1)
    expect(mutate.mock.calls[0][0]).toEqual({
      campaign: 'aug-promo',
      text: 'Знижка 20%',
      channel: 'sms',
      viber: undefined,
    })
  })

  it('defaults to SMS only, so a channel change is always deliberate', () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    expect(
      screen.getByRole('radio', { name: 'sms.channelSms' }),
    ).toHaveAttribute('aria-checked', 'true')
  })

  it('spells the link out in the SMS arm, which has no button', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    await userEvent.click(screen.getByRole('radio', { name: 'sms.channelViberSms' }))
    const [message, caption, url, confirm] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'День народження')
    await userEvent.type(caption, 'Korean Story')
    await userEvent.type(url, 'https://example.com')
    await userEvent.type(confirm, 'aug-promo')
    await userEvent.click(sendButton())

    expect(mutate.mock.calls[0][0]).toEqual({
      campaign: 'aug-promo',
      text: 'День народження\nhttps://example.com',
      channel: 'viber_sms',
      viber: {
        viberText: 'День народження',
        buttonCaption: 'Korean Story',
        buttonUrl: 'https://example.com',
      },
    })
  })

  it('will not send a half-built button', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    await userEvent.click(screen.getByRole('radio', { name: 'sms.channelViberSms' }))
    const [message, caption, , confirm] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'День народження')
    await userEvent.type(caption, 'Korean Story')
    await userEvent.type(confirm, 'aug-promo')

    expect(sendButton()).toBeDisabled()
  })

  it('names the control group so its absence is not a surprise', () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    // The recipients line is present and carries both arms (values are
    // interpolated at runtime; the shared test i18n mock returns bare keys).
    expect(screen.getByText('sms.sendRecipients')).toBeTruthy()
  })

  it('reminds about the promo code when the campaign carries one', () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    expect(screen.getByText('sms.promoReminder')).toBeTruthy()
  })

  it('omits the promo reminder when there is no code', () => {
    render(
      <SmsSendDialog campaign={{ ...campaign, promocode: null }} onClose={() => {}} />,
    )
    expect(screen.queryByText('sms.promoReminder')).toBeNull()
  })

  it('refuses a message over the gateway limit', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(confirm, 'aug-promo')
    fireEvent.change(message, { target: { value: 'x'.repeat(601) } })

    expect(sendButton()).toBeDisabled()

    fireEvent.change(message, { target: { value: 'x'.repeat(600) } })
    expect(sendButton()).not.toBeDisabled()
  })
})
