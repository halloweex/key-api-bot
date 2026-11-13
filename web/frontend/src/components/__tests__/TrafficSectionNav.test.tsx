import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { TrafficSectionNav } from '../TrafficSectionNav'
import { useFilterStore } from '../../store/filterStore'

// The setup's `t` answers with the key, so a line is seen by its key.
const shown = () => document.body.textContent ?? ''

afterEach(() => {
  useFilterStore.setState({
    period: 'month', startDate: null, endDate: null, salesType: 'retail', sourceId: null, funnelCountry: 'ua',
  })
})

describe('whose visits the funnel counts', () => {
  it('sits beside the period on the funnel, with Ukraine chosen', () => {
    render(<TrafficSectionNav tab="funnel" />)
    const group = screen.getByRole('radiogroup', { name: 'traffic.funnel.countryLabel' })
    expect(within(group).getByRole('radio', { name: 'traffic.funnel.country.ua' })).toHaveAttribute('aria-checked', 'true')
    expect(within(group).getByRole('radio', { name: 'traffic.funnel.country.all' })).toHaveAttribute('aria-checked', 'false')
  })

  it('is not on the sections that read the CRM', () => {
    render(<TrafficSectionNav tab="sales" />)
    expect(screen.queryByRole('radiogroup', { name: 'traffic.funnel.countryLabel' })).toBeNull()
    expect(shown()).not.toContain('traffic.funnel.country')
  })

  it('counts every country after one tap', async () => {
    render(<TrafficSectionNav tab="funnel" />)
    await userEvent.click(screen.getByRole('radio', { name: 'traffic.funnel.country.all' }))
    expect(useFilterStore.getState().funnelCountry).toBe('all')
  })
})

describe('whose visits the funnel counts, on a phone', () => {
  const original = window.matchMedia
  beforeEach(() => {
    window.matchMedia = ((query: string) => ({
      matches: query.includes('max-width'), media: query, onchange: null,
      addEventListener: () => {}, removeEventListener: () => {},
      addListener: () => {}, removeListener: () => {}, dispatchEvent: () => false,
    })) as unknown as typeof window.matchMedia
  })
  afterEach(() => { window.matchMedia = original })

  it('is a chip, and its sheet says what each choice means', async () => {
    render(<TrafficSectionNav tab="funnel" />)
    const chip = screen.getByRole('button', { name: 'traffic.funnel.countryChip' })
    expect(chip).toHaveAttribute('aria-haspopup', 'dialog')
    expect(chip.textContent).toContain('traffic.funnel.country.ua')

    await userEvent.click(chip)
    const sheet = screen.getByRole('dialog', { name: 'traffic.funnel.countrySheetTitle' })
    expect(within(sheet).getByRole('radio', { name: /traffic\.funnel\.countryOption\.ua/ })).toHaveAttribute('aria-checked', 'true')
    expect(sheet.textContent).toContain('traffic.funnel.countryNote.ua')
    await userEvent.click(within(sheet).getByRole('radio', { name: /traffic\.funnel\.countryOption\.all/ }))

    expect(useFilterStore.getState().funnelCountry).toBe('all')
    expect(screen.queryByRole('dialog', { name: 'traffic.funnel.countrySheetTitle' })).toBeNull()
  })
})

describe('the line under the /traffic sections', () => {
  it('says the period on the funnel, and not the filters, while the header is as it opens', () => {
    render(<TrafficSectionNav tab="funnel" />)
    expect(shown()).toContain('traffic.sectionPeriod')
    expect(shown()).not.toContain('traffic.sectionScope.funnel')
  })

  it('says the sales type does not reach the funnel once one other than retail is chosen', () => {
    useFilterStore.setState({ salesType: 'b2b' })
    render(<TrafficSectionNav tab="funnel" />)
    expect(shown()).toContain('traffic.sectionScope.funnel')
  })

  it('says the source does not reach the funnel once one is chosen', () => {
    useFilterStore.setState({ sourceId: 4 })
    render(<TrafficSectionNav tab="funnel" />)
    expect(shown()).toContain('traffic.sectionScope.funnel')
  })

  it('always says what the filters do on the other sections', () => {
    render(<TrafficSectionNav tab="sales" />)
    expect(shown()).toContain('traffic.sectionScope.sales')
  })
})
