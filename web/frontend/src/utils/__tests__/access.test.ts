import { describe, it, expect } from 'vitest'
import { tabSummary, roleTabs, firstAllowedPath } from '../access'
import type { Permissions, TabFeature } from '../../types/api'

const label = (tab: TabFeature) => tab

describe('tabSummary', () => {
  it('names the tabs behind "as the role" instead of only saying it', () => {
    // The reading an admin reported: "Как у роли" answers where the answer
    // comes from, not which tabs the person opens.
    expect(tabSummary(null, label, 'as the role', 'none', ['dashboard', 'traffic']))
      .toBe('as the role: dashboard, traffic')
  })

  it('falls back to the bare words when the matrix has not loaded', () => {
    expect(tabSummary(null, label, 'as the role', 'none')).toBe('as the role')
  })

  it('keeps an explicit set and an empty one apart', () => {
    expect(tabSummary(['traffic'], label, 'as the role', 'none', ['dashboard']))
      .toBe('traffic')
    expect(tabSummary([], label, 'as the role', 'none', ['dashboard'])).toBe('none')
  })
})

describe('roleTabs', () => {
  const matrix = {
    dashboard: { view: true, edit: false, delete: false },
    traffic: { view: true, edit: false, delete: false },
    expenses: { view: true, edit: false, delete: false },
    margin: { view: false, edit: false, delete: false },
    user_management: { view: true, edit: false, delete: false },
  }

  it('returns the granted tabs in sidebar order', () => {
    expect(roleTabs(matrix)).toEqual(['dashboard', 'traffic', 'expenses'])
  })

  it('includes expenses, which is a tab without a page', () => {
    // Ordering this by TAB_PATHS would have dropped it silently.
    expect(roleTabs(matrix)).toContain('expenses')
  })

  it('never returns something that is not a grantable tab', () => {
    expect(roleTabs(matrix)).not.toContain('user_management')
  })

  it('says nothing when the matrix has not loaded', () => {
    expect(roleTabs(undefined)).toEqual([])
  })
})

describe('firstAllowedPath', () => {
  const permissions = (granted: string[]) =>
    Object.fromEntries(
      ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing',
       'margin', 'expenses', 'sms', 'analytics', 'customers', 'user_management']
        .map((f) => [f, { view: granted.includes(f), edit: false, delete: false }]),
    ) as unknown as Permissions

  it('sends a traffic-only account to /traffic, not to /', () => {
    expect(firstAllowedPath(permissions(['traffic']))).toBe('/traffic')
  })

  it('returns null when nothing is open, so the caller can say so', () => {
    expect(firstAllowedPath(permissions([]))).toBeNull()
  })
})
