import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { useState } from 'react'
import { useTelegramBackClosesLayers } from '../useTelegramButtons'
import { useLayer } from '../../utils/layerStack'

// Telegram's BackButton as a page sees it: records what it is asked and hands
// back the listener the page registered.
function fakeWebApp() {
  const listeners: Array<() => void> = []
  return {
    platform: 'ios',
    isVersionAtLeast: () => true,
    MainButton: { setParams: vi.fn(), show: vi.fn(), hide: vi.fn(), onClick: vi.fn(), offClick: vi.fn() },
    BackButton: {
      listeners,
      show: vi.fn(),
      hide: vi.fn(),
      onClick: vi.fn((cb: () => void) => { listeners.push(cb) }),
      offClick: vi.fn((cb: () => void) => {
        const i = listeners.indexOf(cb)
        if (i >= 0) listeners.splice(i, 1)
      }),
      press: () => listeners.forEach((cb) => cb()),
    },
  }
}

/** A page with no back of its own and one sheet, as /traffic is. */
function Page() {
  useTelegramBackClosesLayers()
  const [open, setOpen] = useState(false)
  useLayer(open, () => setOpen(false))
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>open</button>
      {open && <p>the sheet</p>}
    </>
  )
}

let tg: ReturnType<typeof fakeWebApp>

beforeEach(() => {
  tg = fakeWebApp()
  ;(window as unknown as { Telegram: object }).Telegram = { WebApp: tg }
})
afterEach(() => {
  delete (window as unknown as { Telegram?: object }).Telegram
})

describe("Telegram's Back on a page without a back of its own", () => {
  it('stays hidden while nothing is open, so the header keeps its Close', () => {
    render(<Page />)
    expect(tg.BackButton.show).not.toHaveBeenCalled()
  })

  it('shows while a sheet is open and closes the sheet, not the app', async () => {
    render(<Page />)
    await userEvent.click(screen.getByRole('button', { name: 'open' }))
    expect(screen.getByText('the sheet')).toBeInTheDocument()
    expect(tg.BackButton.show).toHaveBeenCalled()

    tg.BackButton.hide.mockClear()
    await act(async () => tg.BackButton.press())
    expect(screen.queryByText('the sheet')).toBeNull()
    expect(tg.BackButton.hide).toHaveBeenCalled()
  })

  it('takes its listener away with the page', () => {
    const { unmount } = render(<Page />)
    expect(tg.BackButton.listeners).toHaveLength(1)
    unmount()
    expect(tg.BackButton.listeners).toEqual([])
  })
})
