import { describe, it, expect, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { SmsSendBreakdown } from '../SmsSendBreakdown'
import type { SmsSendProgress } from '../../types/api'

vi.mock('react-i18next', () => ({
  initReactI18next: { type: '3rdParty', init: () => {} },
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) =>
      opts ? `${key}(${JSON.stringify(opts)})` : key,
  }),
}))

function roster(over: Partial<SmsSendProgress> = {}): SmsSendProgress {
  return {
    campaign: 'bf', total: 8403, done: 0, pending: 8403, accepted: 0, stoplisted: 0,
    failed: 0, uncertain: 0, notSent: 0, sentAt: null, batchSize: 1000, ...over,
  }
}

function width(container: HTMLElement, part: string): string | null {
  return container.querySelector(`div[data-part="${part}"]`)?.getAttribute('style') ?? null
}

describe('SmsSendBreakdown, while a send runs', () => {
  it('says which batch is going and about how long is left', () => {
    render(<SmsSendBreakdown running total={8403}
                             progress={roster({ accepted: 3000, pending: 5403 })} />)
    // 3 000 of 8 403 in batches of 1 000: the 4th of 9, six left at ~5 s.
    expect(screen.getByRole('status').textContent).toContain(
      'sms.breakdown.running({"done":"3,000","total":"8,403","batch":4,"batches":9,"eta":30})')
  })

  it('fills the bar with what was answered and leaves the rest as the track', () => {
    const { container } = render(
      <SmsSendBreakdown running total={8403} progress={roster({ accepted: 3000, pending: 5403 })} />)
    expect(width(container, 'accepted')).toContain('width: 35.7')
    expect(width(container, 'pending')).toBeNull()
  })

  it('counts from the roster size before the first read comes back', () => {
    render(<SmsSendBreakdown running total={8403} />)
    expect(screen.getByRole('status').textContent).toContain('"done":"0","total":"8,403"')
  })
})

describe('SmsSendBreakdown, after a send that broke off', () => {
  const broke = roster({ accepted: 5000, uncertain: 1000, notSent: 2353, failed: 40,
                         stoplisted: 10, pending: 0 })

  it('keeps who got it, who may have it and who never got it apart', () => {
    const { container } = render(<SmsSendBreakdown running={false} total={8403} progress={broke} />)
    for (const part of ['accepted', 'refused', 'uncertain', 'notSent']) {
      expect(width(container, part), part).not.toBeNull()
    }
    expect(screen.getByText('sms.breakdown.uncertain')).toBeTruthy()
    expect(screen.getByText('sms.breakdown.notSent')).toBeTruthy()
    // A refusal is a refusal, whether the number was invalid or stoplisted.
    const refused = container.querySelector('li[data-part="refused"]')
    expect(refused?.textContent).toContain('50')
  })

  it('is not a live region once the send is over', () => {
    render(<SmsSendBreakdown running={false} total={8403} progress={broke} />)
    expect(screen.queryByRole('status')).toBeNull()
    expect(screen.getByText(/sms\.breakdown\.done/)).toBeTruthy()
  })

  it('names only the outcomes that happened, and always who received it', () => {
    const { container } = render(
      <SmsSendBreakdown running={false} total={8403}
                        progress={roster({ accepted: 8403, pending: 0 })} />)
    const named = [...container.querySelectorAll('li')].map((li) => li.dataset.part)
    expect(named).toEqual(['accepted'])
  })
})
