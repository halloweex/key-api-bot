import { afterEach, describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { TrafficSectionNav } from '../TrafficSectionNav'
import { useFilterStore } from '../../store/filterStore'

// The setup's `t` answers with the key, so a line is seen by its key.
const shown = () => document.body.textContent ?? ''

afterEach(() => {
  useFilterStore.setState({ period: 'month', startDate: null, endDate: null, salesType: 'retail', sourceId: null })
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
