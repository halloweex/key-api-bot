import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { FunnelCountryFilter } from './FunnelCountryFilter'
import { SectionNav } from './SectionNav'
import { Wrapper } from './Wrapper'
import { useFilterStore } from '../store/filterStore'
import { periodDates } from '../utils/periodDates'
import { formatSpan } from '../utils/smsDates'
import { TRAFFIC_TABS, trafficPath, type TrafficTab } from '../utils/trafficTabs'

// The /traffic sections and, under them, what the header's filters mean here:
// the chosen period in words, and which of the filters reach these blocks.
// On the funnel the filters' line is said only when it changes something a
// reader might expect: a sales type other than retail, or a source, chosen in
// the header for the other sections — neither reaches a funnel Google
// Analytics counts on the website. With the header as it opens, the line
// would say so on every visit (the owner, 2026-11-05: too much text).
// On the funnel the line also holds the funnel's own filter, whose visits it
// counts (`FunnelCountryFilter`), so the control costs no row of its own.

export const TrafficSectionNav = memo(function TrafficSectionNav({ tab }: { tab: TrafficTab }) {
  const { t } = useTranslation()
  const { period, startDate, endDate, salesType, sourceId } = useFilterStore()
  const dates = periodDates(period, startDate, endDate)
  const items = TRAFFIC_TABS.map((s) => ({
    href: s.path,
    label: t(`traffic.section.${s.tab}`),
    shortLabel: t(`traffic.sectionShort.${s.tab}`),
  }))
  const scope = tab !== 'funnel' || salesType !== 'retail' || sourceId != null
    ? t(`traffic.sectionScope.${tab}`)
    : null
  // The funnel's own filter, beside the period it is read with: whose visits
  // count. Not in the header's bar, where it would be a row of its own.
  const country = tab === 'funnel' ? <FunnelCountryFilter /> : null

  return (
    <Wrapper dir="column" gap="sm">
      <SectionNav items={items} active={trafficPath(tab)} ariaLabel={t('traffic.sectionsLabel')} />
      {(dates || scope || country) && (
        <Wrapper dir="row" wrap gap="sm" align="center">
          {(dates || scope) && (
            <p className="text-[13px] leading-relaxed text-slate-600">
              {dates && (
                <span className="font-semibold text-slate-900">
                  {t('traffic.sectionPeriod', { dates: formatSpan(dates.start, dates.end) })}
                </span>
              )}
              {dates && scope && ' · '}
              {scope && (dates ? scope : scope.charAt(0).toLocaleUpperCase() + scope.slice(1))}
            </p>
          )}
          {country}
        </Wrapper>
      )}
    </Wrapper>
  )
})
