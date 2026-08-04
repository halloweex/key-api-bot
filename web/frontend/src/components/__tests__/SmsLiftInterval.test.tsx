import { describe, it, expect, vi } from 'vitest'
import { render } from '@testing-library/react'
import { SmsLiftInterval } from '../SmsLiftInterval'
import type { SmsComparison } from '../../types/api'

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}))

function comparison(over: Partial<SmsComparison> = {}): SmsComparison {
  return {
    conversionTarget: 30,
    conversionHoldout: 20,
    liftPp: 10,
    liftRelativePct: 50,
    ci95Pp: [4, 16],
    pValue: 0.001,
    significant: true,
    incrementalRevenuePerContact: 100,
    incrementalMarginPerContact: 60,
    incrementalRevenueTotal: 10000,
    incrementalMarginTotal: 6000,
    ...over,
  }
}

/** Percent offsets are written as inline `left`/`width` styles. */
function offsets(container: HTMLElement): number[] {
  return Array.from(container.querySelectorAll<HTMLElement>('[style*="left"]'))
    .map((el) => parseFloat(el.style.left))
}

describe('SmsLiftInterval', () => {
  it('places a wholly-positive interval to the right of zero', () => {
    const { container } = render(<SmsLiftInterval comparison={comparison()} />)

    // Every mark sits past the centre line, so the eye reads "clear of zero"
    expect(offsets(container).every((x) => x > 50)).toBe(true)
  })

  it('straddles the centre when the interval crosses zero', () => {
    const { container } = render(
      <SmsLiftInterval comparison={comparison({ ci95Pp: [-5, 9], significant: false })} />,
    )
    const xs = offsets(container)

    expect(Math.min(...xs)).toBeLessThan(50)
    expect(Math.max(...xs)).toBeGreaterThan(50)
  })

  it('mutes an inconclusive interval and highlights a proven one', () => {
    const { container: proven } = render(<SmsLiftInterval comparison={comparison()} />)
    expect(proven.querySelector('.bg-emerald-600')).not.toBeNull()

    const { container: unproven } = render(
      <SmsLiftInterval comparison={comparison({ ci95Pp: [-2, 12], significant: false })} />,
    )
    expect(unproven.querySelector('.bg-emerald-600')).toBeNull()
    expect(unproven.querySelector('.bg-slate-400')).not.toBeNull()
  })

  it('keeps a near-zero-width interval visible', () => {
    const { container } = render(
      <SmsLiftInterval comparison={comparison({ liftPp: 0, ci95Pp: [0, 0] })} />,
    )
    const bar = container.querySelector<HTMLElement>('[style*="width"]')

    expect(parseFloat(bar!.style.width)).toBeGreaterThan(0)
  })

  it('labels both interval bounds', () => {
    const { getByText } = render(
      <SmsLiftInterval comparison={comparison({ ci95Pp: [-1.5, 7.25] })} />,
    )

    expect(getByText('-1.5')).toBeTruthy()
    expect(getByText('+7.3')).toBeTruthy()
  })
})
