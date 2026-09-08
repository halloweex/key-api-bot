import { describe, it, expect } from 'vitest'
import { tabSummary, defaultTabsFor, firstAllowedPath } from '../access'
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

describe('defaultTabsFor', () => {
  // Read from the server, never derived from the permissions matrix: since
  // the matrix became uniform depth, every level has `view` on everything, and
  // deriving it lit nine chips on an inheriting row instead of six — where one
  // click would have written an explicit eight-tab set and granted margin,
  // expenses and the SMS roster by touching an unrelated tab.
  const defaults = {
    viewer: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing'],
    editor: ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing', 'expenses'],
    admin: null,
  } as Record<string, TabFeature[] | null>

  it('returns the level default the server sent', () => {
    expect(defaultTabsFor(defaults, 'viewer')).toEqual(
      ['dashboard', 'products', 'traffic', 'inventory', 'reports', 'marketing'])
    expect(defaultTabsFor(defaults, 'editor')).toContain('expenses')
  })

  it('reads null as every tab, which is what "not narrowed" means', () => {
    expect(defaultTabsFor(defaults, 'admin')).toHaveLength(9)
  })

  it('says nothing when the matrix has not loaded or the role is unknown', () => {
    expect(defaultTabsFor(undefined, 'viewer')).toEqual([])
    expect(defaultTabsFor(defaults, 'nope')).toEqual([])
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
