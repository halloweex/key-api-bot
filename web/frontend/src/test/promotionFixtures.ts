/**
 * The promotions calendar on illustrative data, for its stories: a few
 * promotions around today — one running, a Black Friday saved three times, a
 * spring sale started from an SMS campaign, two past, one cancelled — and an
 * in-memory `/api/promotions` that keeps every save as a version, as the
 * server does, so a story can add, change, cancel and restore.
 *
 * Dates are counted from today in Kyiv, so the lists look the same any day.
 * Names and figures are made up.
 */
import type {
  CurrentUserResponse,
  FeaturePermissions,
  Permissions,
  Promotion,
  PromotionIn,
  PromotionVersion,
  SmsCampaignSummary,
} from '../types/api'
import { addDays, kyivToday } from '../utils/smsDates'

/** A Kyiv timestamp `days` from today at `time`. */
function at(today: string, days: number, time: string): string {
  return `${addDays(today, days)}T${time}+02:00`
}

type Base = Omit<PromotionVersion, 'version' | 'recorded_at' | 'recorded_by_me' | 'status' | 'source'>

function base(over: Partial<Base> & Pick<Base, 'id' | 'name' | 'starts_on' | 'ends_on'>): Base {
  return {
    start_time: null, kind: 'other', discount_pct: null, scope: 'store', scope_items: [],
    channels: [], sms_campaign: null, note: null, ...over,
  }
}

function version(b: Base, v: number, recordedAt: string, over: Partial<PromotionVersion> = {}): PromotionVersion {
  return {
    ...b, version: v, status: 'planned', source: 'form', recorded_at: recordedAt,
    recorded_by_me: false, ...over,
  }
}

/** Every promotion's versions, oldest first. */
export function promotionHistories(today: string = kyivToday()): PromotionVersion[][] {
  const anua = base({
    id: 1, name: 'Тиждень Anua', starts_on: addDays(today, -2), ends_on: addDays(today, 1),
    discount_pct: 15, scope: 'brands', scope_items: ['Anua'], channels: ['instagram', 'telegram'],
  })
  const friday = base({
    id: 2, name: 'Чорна п’ятниця', kind: 'black_friday',
    starts_on: addDays(today, 9), ends_on: addDays(today, 11), discount_pct: 25,
    channels: ['sms', 'instagram', 'telegram'],
  })
  const fridayMoved = { ...friday, starts_on: addDays(today, 8), discount_pct: 30 }
  const spring = base({
    id: 3, name: 'Весняний розпродаж', starts_on: addDays(today, 100), ends_on: addDays(today, 103),
    start_time: '18:00', discount_pct: 20, scope: 'categories', scope_items: ['Догляд за обличчям', 'Сонцезахист'],
    channels: ['sms'], sms_campaign: 'mar-spring', note: 'Промокод SPRING, у розсилці й у сторіс',
  })
  const cosmetics = base({
    id: 4, name: 'День косметики', starts_on: addDays(today, -30), ends_on: addDays(today, -29),
    discount_pct: 10, channels: ['instagram'],
  })
  const cashback = base({
    id: 5, name: 'Літній кешбек', starts_on: addDays(today, -75), ends_on: addDays(today, -70),
    channels: ['site'],
  })
  const skin = base({
    id: 6, name: 'Розпродаж Skin1004', starts_on: addDays(today, 20), ends_on: addDays(today, 22),
    discount_pct: 20, scope: 'brands', scope_items: ['Skin1004'], channels: ['sms'],
  })
  return [
    [version(anua, 1, at(today, -9, '11:20:00'))],
    [
      version(friday, 1, at(today, -20, '10:12:03')),
      version(fridayMoved, 2, at(today, -6, '09:01:44'), { recorded_by_me: true }),
      version({ ...fridayMoved, channels: ['sms', 'instagram', 'telegram', 'site'] }, 3,
        at(today, -1, '16:40:00'), { recorded_by_me: true }),
    ],
    [version(spring, 1, at(today, -2, '12:00:00'), { source: 'form_from_sms', recorded_by_me: true })],
    [version(cosmetics, 1, at(today, -45, '09:30:00'))],
    [version(cashback, 1, at(today, -90, '15:00:00'))],
    [
      version(skin, 1, at(today, -12, '10:00:00')),
      version(skin, 2, at(today, -3, '18:05:00'), { status: 'cancelled' }),
    ],
  ]
}

/** A promotion as the list answers it: its latest version. */
export function currentOf(versions: PromotionVersion[]): Promotion {
  const last = versions[versions.length - 1]
  const { recorded_at: recordedAt, recorded_by_me: _byMe, ...rest } = last
  void _byMe
  return { ...rest, id: last.id ?? 0, created_at: versions[0].recorded_at, updated_at: recordedAt }
}

export const PROMOTION_SMS_CAMPAIGNS = [
  { campaign: 'nov-black-friday', promocode: 'BF26', sentAt: null, exportedAt: '2026-11-20T15:00:00Z' },
  { campaign: 'mar-spring', promocode: 'SPRING', sentAt: null, exportedAt: '2026-11-17T12:00:00Z' },
  { campaign: 'oct-anua', promocode: null, sentAt: '2026-10-09T09:30:00Z', exportedAt: '2026-10-08T17:00:00Z' },
] as SmsCampaignSummary[]

const NONE: FeaturePermissions = { view: false, edit: false, delete: false }

/** A session for `useAuth`: `marketing` read or edit, and the SMS tab or not. */
export function promotionsSession(marketing: 'view' | 'edit', sms = false): CurrentUserResponse {
  const permissions = Object.fromEntries(
    ['dashboard', 'expenses', 'inventory', 'analytics', 'customers', 'reports', 'user_management',
      'sms', 'traffic', 'products', 'marketing', 'margin', 'tgbot'].map((f) => [f, NONE]),
  ) as unknown as Permissions
  permissions.marketing = { view: true, edit: marketing === 'edit', delete: false }
  if (sms) permissions.sms = { view: true, edit: false, delete: false }
  return {
    user: { id: 1, username: 'marketer', first_name: 'A', last_name: 'Marketer', photo_url: '',
      role: marketing === 'edit' ? 'editor' : 'viewer' },
    permissions,
  }
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })

/** What the fake answers from; each story sets its own. */
interface FakeState {
  store: Map<number, PromotionVersion[]>
  nextId: number
  session?: CurrentUserResponse
}

let fake: FakeState | null = null
let installed = false

/**
 * `/api/promotions` in memory, every save a version, with the refusals the
 * contract names (409 on a cancelled promotion, 400 naming a field). A
 * promotion named «помилка» is refused, so a story can show the toast.
 *
 * Installed once per page and never taken down; each call only replaces the
 * data it answers from. A story switch renders the next story before the
 * last one's cleanup runs, so an install-and-undo pair would undo the next
 * story's fake. Paths it does not know go to the fetch it wrapped.
 */
export function installPromotionsApi(
  histories: PromotionVersion[][] = promotionHistories(),
  session?: CurrentUserResponse,
): void {
  const store = new Map(histories.map((h) => [h[0].id ?? 0, h.map((v) => ({ ...v }))]))
  fake = { store, nextId: Math.max(0, ...store.keys()) + 1, session }
  if (installed) return
  installed = true
  const real = window.fetch.bind(window)
  window.fetch = async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = new URL(typeof input === 'string' ? input : input.toString(), window.location.origin)
    const answer = fake ? respond(fake, url, init) : null
    return answer ?? real(input, init)
  }
}

function save(state: FakeState, id: number, body: Partial<PromotionIn>, status?: 'planned' | 'cancelled') {
  const versions = state.store.get(id) ?? []
  const last = versions[versions.length - 1]
  const next: PromotionVersion = {
    ...(last ?? base({ id, name: '', starts_on: '', ends_on: '' })),
    ...(body as Partial<PromotionVersion>),
    id,
    version: (last?.version ?? 0) + 1,
    status: status ?? last?.status ?? 'planned',
    source: body.source ?? last?.source ?? 'form',
    recorded_at: new Date().toISOString(),
    recorded_by_me: true,
  }
  state.store.set(id, [...versions, next])
  return currentOf(state.store.get(id)!)
}

/** The fake's answer, or null for a path it does not serve. */
function respond(state: FakeState, url: URL, init?: RequestInit): Response | null {
  const path = url.pathname
  const method = (init?.method ?? 'GET').toUpperCase()
  if (path === '/api/me') return state.session ? json(state.session) : null
  if (path === '/api/customers/sms-campaigns') return json({ campaigns: PROMOTION_SMS_CAMPAIGNS })
  if (path === '/api/brands') {
    return json(['Anua', 'Beauty of Joseon', 'COSRX', 'Isntree', 'Medicube', 'Skin1004'].map((name) => ({ name })))
  }
  if (path === '/api/categories') {
    return json(['Догляд за обличчям', 'Сонцезахист', 'Макіяж'].map((name, i) => ({ id: i + 1, name })))
  }
  if (!path.startsWith('/api/promotions')) return null

  if (path === '/api/promotions' && method === 'GET') {
    const all = [...state.store.values()].map(currentOf)
    const shown = url.searchParams.get('include_cancelled') === 'true'
      ? all : all.filter((p) => p.status === 'planned')
    shown.sort((a, b) => a.starts_on.localeCompare(b.starts_on) || a.id - b.id)
    return json({ promotions: shown })
  }
  const body = init?.body ? JSON.parse(String(init.body)) as PromotionIn : null
  if (body && body.name.trim().toLowerCase() === 'помилка') {
    return json({ detail: 'name: refused by the story to show a refusal' }, 400)
  }
  if (path === '/api/promotions' && method === 'POST' && body) {
    return json(save(state, state.nextId++, body, 'planned'), 201)
  }
  const m = /^\/api\/promotions\/(\d+)(?:\/(versions|cancel|restore))?$/.exec(path)
  const versions = m ? state.store.get(Number(m[1])) : undefined
  if (!m || !versions) return json({ detail: 'Not Found' }, 404)
  const id = Number(m[1])
  const status = versions[versions.length - 1].status
  if (m[2] === 'versions') return json({ versions: [...versions].reverse() })
  if (m[2] === 'cancel') {
    return status === 'cancelled'
      ? json({ detail: 'promotion: already cancelled' }, 409)
      : json(save(state, id, {}, 'cancelled'))
  }
  if (m[2] === 'restore') {
    return status !== 'cancelled'
      ? json({ detail: 'promotion: not cancelled' }, 409)
      : json(save(state, id, {}, 'planned'))
  }
  if (method === 'PUT' && body) {
    return status === 'cancelled'
      ? json({ detail: 'promotion: cancelled, restore it first' }, 409)
      : json(save(state, id, body))
  }
  return json({ detail: 'Not Found' }, 404)
}
