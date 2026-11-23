import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { QueryClient, QueryObserver } from '@tanstack/react-query'
import { api } from '../client'
import type { PromotionIn, PromotionsResponse } from '../../types/api'

// The client hands a GET the same GET already in flight, so a burst of filter
// clicks costs one request. A refetch asked for after a save must not be
// handed one that left before the save answered: that one may have been read
// before the save committed, and its answer stands until the next refetch.
// Review of #397 (2026-11-20): after two quick saves the promotions list
// stayed as it was before the second one. And nothing is answered from the
// browser's own cache, which nginx used to fill for a minute.

interface Call {
  url: string
  method: string
  init: RequestInit | undefined
  answer: (response: Response) => void
}

let calls: Call[] = []

beforeEach(() => {
  calls = []
  vi.stubGlobal('fetch', vi.fn((url: string, init?: RequestInit) => new Promise<Response>((resolve) => {
    calls.push({ url, method: init?.method ?? 'GET', init, answer: resolve })
  })))
})

afterEach(async () => {
  // Nothing left in flight for the next test to be handed.
  for (const call of calls) call.answer(json({ promotions: [] }))
  await new Promise((r) => setTimeout(r, 0))
  vi.unstubAllGlobals()
})

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

const list = (...names: string[]) => json({ promotions: names.map((name, i) => ({ id: i + 1, name })) })
const gets = () => calls.filter((c) => c.method === 'GET')
const writes = () => calls.filter((c) => c.method !== 'GET')

const BODY = {
  name: 'Y', starts_on: '2027-03-05', ends_on: '2027-03-08', start_time: null, kind: 'other',
  discount_pct: null, scope: 'store', scope_items: [], channels: [], sms_campaign: null,
  note: null, source: 'form',
} as PromotionIn

const promotions = () => api.getPromotions({ includeCancelled: true })

describe('a refetch after a save', () => {
  it('is not handed the list that left before the save answered', async () => {
    // usePromotions as the page has it, and the page's two saves in a row.
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const observer = new QueryObserver<PromotionsResponse>(client, {
      queryKey: ['promotions'], queryFn: () => promotions(), staleTime: 5 * 60 * 1000,
    })
    const unsubscribe = observer.subscribe(() => {})
    const names = () => observer.getCurrentResult().data?.promotions.map((p) => p.name)

    await vi.waitFor(() => expect(gets()).toHaveLength(1))
    gets()[0].answer(list('X', 'Y'))
    await vi.waitFor(() => expect(names()).toEqual(['X', 'Y']))

    // Save 1 answers, and the list is asked again: R1 leaves.
    const first = api.updatePromotion(2, { ...BODY, name: 'Y1' })
    writes()[0].answer(json({ id: 2, name: 'Y1' }))
    await first
    void client.invalidateQueries({ queryKey: ['promotions'] })
    await vi.waitFor(() => expect(gets()).toHaveLength(2))

    // Save 2 answers while R1 is still on its way, and the list is asked again.
    const second = api.updatePromotion(2, { ...BODY, name: 'Y2' })
    writes()[1].answer(json({ id: 2, name: 'Y2' }))
    await second
    void client.invalidateQueries({ queryKey: ['promotions'] })
    await vi.waitFor(() => expect(gets()).toHaveLength(3))

    // R1 was read between the saves; the new request after both.
    gets()[1].answer(list('X', 'Y1'))
    gets()[2].answer(list('X', 'Y2'))
    await vi.waitFor(() => expect(names()).toEqual(['X', 'Y2']))
    unsubscribe()
  })

  it('is not handed a request that left while the save was on its way', async () => {
    const save = api.cancelPromotion(1)
    const during = promotions()
    writes()[0].answer(json({ id: 1, name: 'X' }))
    await save

    const after = promotions()
    expect(gets()).toHaveLength(2)
    gets()[0].answer(list('X'))
    gets()[1].answer(list())
    // Each caller has its own request's answer.
    expect((await during).promotions).toHaveLength(1)
    expect((await after).promotions).toHaveLength(0)
  })

  it('is not handed one either when the save was refused', async () => {
    // A refusal can still have been written (a timeout), and a 409 means the
    // promotion moved under the page: the list on screen is the one to correct.
    const save = api.restorePromotion(1)
    void promotions()
    writes()[0].answer(json({ detail: 'Promotion 1 is planned' }, 409))
    await expect(save).rejects.toThrow('Promotion 1 is planned')

    void promotions()
    expect(gets()).toHaveLength(2)
  })
})

describe('the same GET with no write between', () => {
  it('is still one request, answered to both callers', async () => {
    const a = promotions()
    const b = promotions()
    expect(gets()).toHaveLength(1)
    gets()[0].answer(list('X'))
    expect(await a).toEqual(await b)
  })

  it('is asked anew once the first has answered', async () => {
    const a = promotions()
    gets()[0].answer(list('X'))
    await a
    void promotions()
    expect(gets()).toHaveLength(2)
  })
})

describe('the browser’s own cache', () => {
  it('never answers a GET', async () => {
    void promotions()
    void api.getRevenueForecast('retail')
    expect(gets().map((c) => c.init?.cache)).toEqual(['no-store', 'no-store'])
  })
})
