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
// What the gateway said when the dialog opened. Mutable per test.
const gateway: { balance: number | null; isLoading: boolean } = {
  balance: 1_000_000, isLoading: false,
}
// Whether a send is under way, and how far the roster says it has got.
const sending: { isPending: boolean } = { isPending: false }
const restMutate = vi.fn()
type Progress = {
  total: number; done: number; pending: number; accepted: number; failed: number
  stoplisted: number; uncertain: number; notSent: number; batchSize?: number
}
const progress: { data: Progress | undefined } = { data: undefined }

function roster(over: Partial<Progress>): Progress {
  return { total: 5560, done: 0, pending: 0, accepted: 0, failed: 0, stoplisted: 0,
           uncertain: 0, notSent: 0, batchSize: 1000, ...over }
}

vi.mock('../../hooks/useApi', () => ({
  useSendSmsCampaign: () => ({ mutate, isPending: sending.isPending }),
  useSmsSendProgress: () => progress,
  useSendRestOfSmsCampaign: () => ({ mutate: restMutate, isPending: false }),
  useSmsChannels: () => ({ data: { sms: true, viber: true } }),
  useSmsBalance: () => ({
    data: { sms: true, viber: true, smsSender: 'KoreanStory', viberSender: null,
            pricePerPart: 1.28, balance: gateway.balance },
    isLoading: gateway.isLoading,
  }),
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
  mutate.mockReset()
  restMutate.mockReset()
  addToast.mockClear()
  gateway.balance = 1_000_000
  gateway.isLoading = false
  sending.isPending = false
  progress.data = undefined
})

describe('SmsSendDialog, while and after it sends', () => {
  it('shows the send moving, read off the roster, instead of a bare "Sending…"', () => {
    // 2026-11-26: a minute of "Sending…" with nothing else on screen.
    sending.isPending = true
    progress.data = roster({ done: 2000, accepted: 2000, pending: 3560 })
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    const status = screen.getByRole('status')
    expect(status.textContent).toContain('sms.breakdown.running')
    expect(status.querySelector('[data-part="accepted"]')?.getAttribute('style'))
      .toContain('width: 35.97')
  })

  it('after a send that broke off, offers to send only the rest', async () => {
    mutate.mockImplementation((_vars, opts) => {
      progress.data = roster({ done: 5560, accepted: 3000, uncertain: 1000, notSent: 1560 })
      opts.onError(new Error('gateway gave no answer'))
    })
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, campaign.campaign)
    fireEvent.click(sendButton())

    // Who got it, who may have it and who never got it, apart.
    expect(screen.getByText('sms.breakdown.uncertain')).toBeTruthy()
    expect(screen.getByText('sms.breakdown.notSent')).toBeTruthy()
    const rest = screen.getByRole('button', { name: 'sms.sendRest' })
    fireEvent.click(rest)
    expect(restMutate).toHaveBeenCalledWith(
      { campaign: campaign.campaign, confirmCampaign: campaign.campaign },
      expect.anything(),
    )
  })

  it('opened for the rest, sends the campaign\'s own text to the never-sent only', async () => {
    progress.data = roster({ done: 5560, accepted: 2157, notSent: 3403 })
    render(<SmsSendDialog campaign={{ ...campaign, messageText: 'BLACK FRIDAY -50%' }}
                          mode="rest" onClose={() => {}} />)

    expect(screen.getByText('BLACK FRIDAY -50%')).toBeTruthy()
    // No text to edit: one campaign is one message.
    expect(screen.getAllByRole('textbox')).toHaveLength(1)
    const rest = screen.getByRole('button', { name: 'sms.sendRest' })
    expect(rest).toBeDisabled()
    await userEvent.type(screen.getByRole('textbox'), campaign.campaign)
    expect(rest).toBeEnabled()
    fireEvent.click(rest)
    expect(restMutate).toHaveBeenCalledTimes(1)
    expect(mutate).not.toHaveBeenCalled()
  })

  it('keeps the dialog open on a partial send, with the rest one press away', async () => {
    const onClose = vi.fn()
    mutate.mockImplementation((_vars, opts) => {
      progress.data = roster({ done: 5560, accepted: 4560, notSent: 1000 })
      opts.onSuccess({ campaign: campaign.campaign, channel: 'sms', accepted: 4560,
                       stoplisted: 0, failed: 0, unsent: 1000, partialError: '405' })
    })
    render(<SmsSendDialog campaign={campaign} onClose={onClose} />)
    const [message, confirm] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, campaign.campaign)
    fireEvent.click(sendButton())

    expect(onClose).not.toHaveBeenCalled()
    expect(screen.getByRole('button', { name: 'sms.sendRest' })).toBeEnabled()
  })

  it('draws no progress until a send is under way', () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    expect(screen.queryByRole('status')).toBeNull()
  })

  it('says what went wrong in the dialog itself, and offers only to close it', async () => {
    mutate.mockImplementation((_vars, opts) => opts.onError(new Error('gateway gave no answer')))
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, campaign.campaign)
    fireEvent.click(sendButton())

    // A toast is gone in seconds; the reason stays on the dialog.
    expect(screen.getByRole('alert').textContent).toBe('gateway gave no answer')
    expect(screen.getByRole('button', { name: 'sms.close' })).toBeEnabled()
    expect(sendButton()).toBeDisabled()
  })
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
      confirmCampaign: 'aug-promo',
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
      confirmCampaign: 'aug-promo',
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

  it('sends the name as typed, so the server checks it too', async () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, '  aug-promo ')
    await userEvent.click(sendButton())

    // Trimmed, as the gate above compared it; the server trims and compares too.
    expect(mutate.mock.calls[0][0].confirmCampaign).toBe('aug-promo')
  })
})

describe('the balance beside the name field', () => {
  it('shows who gets it, what it costs and what the balance is', () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    expect(screen.getByText('sms.budgetCostFrom')).toBeTruthy()
    expect(screen.getByText(/sms\.budgetBalance/)).toBeTruthy()
    expect(screen.getByText(/sms\.budgetCovered/)).toBeTruthy()
  })

  it('holds the send when the balance is known to fall short', async () => {
    // 886 ₴ against 5 560 recipients — the morning of 17 October.
    gateway.balance = 886
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, 'aug-promo')

    expect(screen.getByText(/sms\.budgetShort/)).toBeTruthy()
    expect(sendButton()).toBeDisabled()
  })

  it('counts both parts of a two-part message against the balance', async () => {
    // One part for 5 560 is 7 117 ₴ (+2% = 7 259); two parts is twice that.
    gateway.balance = 10_000
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(confirm, 'aug-promo')
    fireEvent.change(message, { target: { value: 'я'.repeat(70) } })
    expect(sendButton()).not.toBeDisabled()

    fireEvent.change(message, { target: { value: 'я'.repeat(71) } })
    expect(sendButton()).toBeDisabled()
  })

  it('leaves an unreadable balance to the server, which reads it again', async () => {
    gateway.balance = null
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')

    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, 'aug-promo')

    expect(screen.getByText('sms.budgetUnknown')).toBeTruthy()
    expect(sendButton()).not.toBeDisabled()
  })
})

describe('a follow-up', () => {
  it('says whose people get a second message, above the name field', () => {
    render(
      <SmsSendDialog
        campaign={{ ...campaign, campaign: 'aug-last-day', holdout: 0,
                    holdoutPct: 0, followUpOf: 'aug-promo' }}
        onClose={() => {}}
      />,
    )

    expect(screen.getByText('sms.followUpRecipients')).toBeTruthy()
    expect(screen.getByText('sms.followUpNoControl')).toBeTruthy()
  })

  it('says nothing of the kind for a campaign of its own', () => {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)

    expect(screen.queryByText('sms.followUpNoControl')).toBeNull()
  })
})

describe('a refusal the server names', () => {
  async function pressSend() {
    render(<SmsSendDialog campaign={campaign} onClose={() => {}} />)
    const [message, confirm] = screen.getAllByRole('textbox')
    await userEvent.type(message, 'Знижка 20%')
    await userEvent.type(confirm, 'aug-promo')
    await userEvent.click(sendButton())
  }

  it("is said in the reader's language, not as the server's English", async () => {
    const { ApiError } = await import('../../api/client')
    await pressSend()
    const { onError } = mutate.mock.calls[0][1]
    onError(new ApiError(409, 'the TurboSMS balance is 4650.00 ₴ and this send costs …',
      'CLIENT_ERROR', { reason: 'balance_short', balance: 4650, cost: 4746.12,
                       shortfall: 96.12, recipients: 5560, parts: 1, pricePerPart: 1.28,
                       margin: 1.02 }))

    const toast = addToast.mock.calls.at(-1)![0]
    // Not "the gateway refused": a guard refused before the gateway was asked.
    expect(toast.title).toBe('sms.sendRefused')
    expect(toast.message).toMatch(/^sms\.refusal\.balance_short/)
    expect(toast.message).not.toContain('TurboSMS balance is')
  })

  it('falls back to what the server said when it names nothing', async () => {
    const { ApiError } = await import('../../api/client')
    await pressSend()
    const { onError } = mutate.mock.calls[0][1]
    onError(new ApiError(502, 'TurboSMS rejected the request: 103 INVALID_SENDER'))

    const toast = addToast.mock.calls.at(-1)![0]
    expect(toast.title).toBe('sms.sendFailed')
    expect(toast.message).toBe('TurboSMS rejected the request: 103 INVALID_SENDER')
  })
})
