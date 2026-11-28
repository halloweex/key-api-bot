import { describe, it, expect, vi, beforeEach } from 'vitest'

// src/lib/i18n.ts reads localStorage while being imported; see AdminUsersPage.test.tsx.
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
import { PeriodFilter } from '../PeriodFilter'
import { useFilterStore } from '../../store/filterStore'

describe('PeriodFilter', () => {
  beforeEach(() => {
    useFilterStore.setState({ period: 'month', startDate: null, endDate: null })
  })

  it('offers the whole history and picking it sets the period', () => {
    render(<PeriodFilter />)
    fireEvent.click(screen.getByRole('button', { name: 'filter.allTime' }))
    expect(useFilterStore.getState().period).toBe('all_time')
  })
})
