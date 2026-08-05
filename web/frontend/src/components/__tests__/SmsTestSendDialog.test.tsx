import { describe, it, expect, vi, beforeEach } from 'vitest'
import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { SmsTestSendDialog } from '../SmsTestSendDialog'

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
  useSendTestSms: () => ({ mutate, isPending: false }),
}))

vi.mock('../Toast', () => ({ useToast: () => ({ addToast }) }))

function sendButton() {
  return screen.getByRole('button', { name: 'sms.testSendNow' })
}

function fields() {
  // Phone is type=tel, message is the textarea; both expose role textbox.
  return screen.getAllByRole('textbox')
}

beforeEach(() => {
  mutate.mockClear()
  addToast.mockClear()
})

describe('SmsTestSendDialog', () => {
  it('will not send without a number', async () => {
    render(<SmsTestSendDialog onClose={() => {}} />)
    const [, message] = fields()

    await userEvent.type(message, 'Знижка 20%')

    expect(sendButton()).toBeDisabled()
    expect(mutate).not.toHaveBeenCalled()
  })

  it('will not send to a number a campaign could never contain', async () => {
    render(<SmsTestSendDialog onClose={() => {}} />)
    const [phone, message] = fields()

    await userEvent.type(phone, '0961111111')
    await userEvent.type(message, 'Знижка 20%')

    expect(sendButton()).toBeDisabled()
  })

  it('will not send an empty message', async () => {
    render(<SmsTestSendDialog onClose={() => {}} />)
    const [phone] = fields()

    await userEvent.type(phone, '380961111111')

    expect(sendButton()).toBeDisabled()
  })

  it('strips formatting from the number and trims the text', async () => {
    render(<SmsTestSendDialog onClose={() => {}} />)
    const [phone, message] = fields()

    await userEvent.type(phone, '+38 (096) 111-11-11')
    await userEvent.type(message, '  Знижка 20%  ')
    await userEvent.click(sendButton())

    expect(mutate).toHaveBeenCalledWith(
      { phone: '380961111111', text: 'Знижка 20%' },
      expect.anything(),
    )
  })

  it('carries a text handed to it, so the campaign wording can be rehearsed', () => {
    render(<SmsTestSendDialog initialText="День народження" onClose={() => {}} />)
    const [, message] = fields()

    expect((message as HTMLTextAreaElement).value).toBe('День народження')
  })

  it('shows the Cyrillic segment count while typing, not after sending', async () => {
    render(<SmsTestSendDialog onClose={() => {}} />)
    const [, message] = fields()

    await userEvent.type(message, 'я'.repeat(71))

    // 71 UCS-2 characters do not fit one 70-character segment.
    expect(screen.getByText(/sms\.costParts/)).toBeTruthy()
    expect(screen.getByText('71 / 600')).toBeTruthy()
  })

  it('surfaces a stoplist refusal instead of reporting success', async () => {
    mutate.mockImplementation((_vars, opts) =>
      opts.onSuccess({
        phone: '380961111111', accepted: false, stoplisted: true,
        messageId: null, code: 404, status: 'NOT_ALLOWED_NUMBER_STOPLIST',
        cost: { encoding: 'ucs2', characters: 10, parts: 1 },
      }),
    )
    render(<SmsTestSendDialog onClose={() => {}} />)
    const [phone, message] = fields()

    await userEvent.type(phone, '380961111111')
    await userEvent.type(message, 'Знижка')
    await userEvent.click(sendButton())

    expect(addToast).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'error', title: 'sms.testStoplisted' }),
    )
  })

  it('stays open after a send so the gateway answer can be read', async () => {
    const onClose = vi.fn()
    mutate.mockImplementation((_vars, opts) =>
      opts.onSuccess({
        phone: '380961111111', accepted: true, stoplisted: false,
        messageId: 'msg-1', code: 0, status: 'OK',
        cost: { encoding: 'ucs2', characters: 6, parts: 1 },
      }),
    )
    render(<SmsTestSendDialog onClose={onClose} />)
    const [phone, message] = fields()

    await userEvent.type(phone, '380961111111')
    await userEvent.type(message, 'Знижка')
    await userEvent.click(sendButton())

    expect(onClose).not.toHaveBeenCalled()
    expect(screen.getByRole('status')).toBeTruthy()
  })
})
