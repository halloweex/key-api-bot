import { describe, it, expect, vi } from 'vitest'

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

import { render, screen, fireEvent } from '@testing-library/react'
import { UserAccessEditor } from '../UserAccessEditor'
import type { TabFeature } from '../../types/api'

const ROLE_TABS: TabFeature[] = ['dashboard', 'products', 'traffic', 'inventory',
  'reports', 'marketing']

describe('UserAccessEditor with no override', () => {
  it('lights the tabs the role opens instead of showing nothing', () => {
    // All chips grey said "no access at all" about somebody with six tabs.
    render(
      <UserAccessEditor
        value={null}
        roleFeatures={ROLE_TABS}
        onChange={() => {}}
        onPreset={() => {}}
      />,
    )
    const traffic = screen.getByRole('button', { name: 'access.tab.traffic' })
    const margin = screen.getByRole('button', { name: 'access.tab.margin' })
    // The lit ones carry the active background; the rest do not.
    expect(traffic.className).not.toContain('bg-slate-100')
    expect(margin.className).toContain('bg-slate-100')
  })

  it('starts the first click from what the person already has', () => {
    // It used to start from an empty set, so ticking one tab on an inheriting
    // account silently revoked the five it was already seeing.
    const onChange = vi.fn()
    render(
      <UserAccessEditor
        value={null}
        roleFeatures={ROLE_TABS}
        onChange={onChange}
        onPreset={() => {}}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'access.tab.margin' }))
    expect(onChange).toHaveBeenCalledWith([...ROLE_TABS, 'margin'].sort(
      (a, b) => ['dashboard', 'products', 'traffic', 'inventory', 'reports',
                 'marketing', 'margin', 'expenses', 'sms'].indexOf(a)
              - ['dashboard', 'products', 'traffic', 'inventory', 'reports',
                 'marketing', 'margin', 'expenses', 'sms'].indexOf(b),
    ))
  })

  it('removing an inherited tab writes the rest explicitly', () => {
    const onChange = vi.fn()
    render(
      <UserAccessEditor
        value={null}
        roleFeatures={ROLE_TABS}
        onChange={onChange}
        onPreset={() => {}}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'access.tab.reports' }))
    expect(onChange).toHaveBeenCalledWith(
      ROLE_TABS.filter((t) => t !== 'reports'),
    )
  })
})

describe('UserAccessEditor with an explicit set', () => {
  it('shows exactly that set', () => {
    const onChange = vi.fn()
    render(
      <UserAccessEditor
        value={['traffic']}
        roleFeatures={ROLE_TABS}
        onChange={onChange}
        onPreset={() => {}}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'access.tab.reports' }))
    expect(onChange).toHaveBeenCalledWith(['traffic', 'reports'])
  })
})
