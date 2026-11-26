import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
import i18n from '../../lib/i18n'
import { useFilterStore } from '../../store/filterStore'
import type { SmartGoalsResponse } from '../../types/api'

// The card is read for what it says, so it gets the real dictionaries.
vi.unmock('react-i18next')

const state = vi.hoisted(() => ({
  goals: { data: undefined as unknown, isLoading: false },
}))

vi.mock('../../hooks', () => ({
  useSmartGoals: () => state.goals,
  useRevenueTrend: () => ({ data: undefined }),
}))

import { MilestoneProgress } from '../MilestoneProgress'

// jsdom has no IntersectionObserver; the card only uses it to pause sparkles.
class NoObserver {
  observe() {}
  unobserve() {}
  disconnect() {}
}

const plain = (s: string | null | undefined) => (s ?? '').replace(/[\u00a0\u202f]/g, ' ')

function period(amount: number | null, isCustom = false) {
  return {
    amount, isCustom, suggestedAmount: amount, basedOnAverage: 0, trend: 0,
    confidence: 'medium' as const,
  }
}

function goals(method: string, amounts: { daily: number | null; weekly: number | null; monthly: number | null },
               custom: Partial<Record<'daily' | 'weekly' | 'monthly', number>> = {}): SmartGoalsResponse {
  const of = (key: 'daily' | 'weekly' | 'monthly') =>
    custom[key] != null ? period(custom[key]!, true) : period(amounts[key])
  return {
    daily: of('daily'),
    weekly: { ...of('weekly'), weeklyBreakdown: {} },
    monthly: {
      ...of('monthly'),
      lastYearRevenue: 0, recent3MonthAvg: 0, yoyGoal: 0, recentGoal: 0, mlForecastGoal: 0,
      growthRate: 0.2911, seasonalityIndex: 1.3556,
      calculationMethod: method as SmartGoalsResponse['monthly']['calculationMethod'],
      blendWeights: null,
    },
    metadata: { overallYoY: 0.5, monthlyYoY: 0.29, calculatedAt: '2026-11-18T23:43:52+02:00' },
  }
}

const NONE = { daily: null, weekly: null, monthly: null }

function show(data: SmartGoalsResponse | undefined, periodName: string, revenue = 21_140) {
  state.goals = { data, isLoading: false }
  useFilterStore.setState({ period: periodName as never })
  const { container } = render(<MilestoneProgress revenue={revenue} />)
  return { container, text: plain(container.textContent) }
}

beforeEach(async () => {
  vi.stubGlobal('IntersectionObserver', NoObserver)
  await i18n.changeLanguage('ru')
})
afterEach(async () => {
  vi.unstubAllGlobals()
  useFilterStore.setState({ period: 'month' as never })
  await i18n.changeLanguage('en')
})

describe('a sales type with no history of its own', () => {
  it('says there is no goal, in a line, and draws no target of its own', () => {
    const { text } = show(goals('no_history', NONE), 'month')
    expect(text).toContain('Месячная цель')
    expect(text).toContain('Цели нет: у этого канала ещё нет своей истории продаж.')
    // Not the hardcoded targets, which are retail's size: ₴1M … ₴4M a month.
    expect(text).not.toMatch(/\dM/)
    expect(text).not.toContain('%')
  })

  it('says it for the day and the week too', () => {
    expect(show(goals('no_history', NONE), 'today').text).toContain('Ежедневная цель')
    expect(show(goals('no_history', NONE), 'today').text).not.toContain('200K')
    expect(show(goals('no_history', NONE), 'week').text).toContain('Недельная цель')
  })

  it('still takes a goal typed by hand for the period it was typed for', () => {
    const typed = goals('no_history', NONE, { monthly: 300_000 })
    const month = show(typed, 'month').text
    expect(month).not.toContain('Цели нет')
    expect(month).toContain('300K')
    expect(show(typed, 'today').text).toContain('Цели нет')
  })
})

describe('a goal the server rounded to nothing', () => {
  it('draws nothing rather than retail-sized targets', () => {
    const { container } = show(goals('recent_trend', { daily: 0, weekly: 0, monthly: 100_000 }), 'today')
    expect(container).toBeEmptyDOMElement()
  })
})

describe('the cases that did not change', () => {
  it('a goal is drawn as the bar, its amount the last milestone', () => {
    const { text } = show(goals('yoy_growth', { daily: 160_000, weekly: 1_100_000, monthly: 4_700_000 }), 'month', 2_150_000)
    expect(text).toContain('4.7M')
    expect(text).not.toContain('Цели нет')
  })

  it('a request that failed still shows the hardcoded targets', () => {
    const { text } = show(undefined, 'month', 2_150_000)
    expect(text).toContain('₴4.0M')
    expect(text).not.toContain('Цели нет')
  })
})
