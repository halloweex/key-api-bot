import { memo } from 'react'
import { useTranslation } from 'react-i18next'
import { SectionNav } from './SectionNav'
import { Wrapper } from './Wrapper'
import { useFilterStore } from '../store/filterStore'
import { periodDates } from '../utils/periodDates'
import { formatSpan } from '../utils/smsDates'
import { TRAFFIC_TABS, trafficPath, type TrafficTab } from '../utils/trafficTabs'

// The /traffic sections and, under them, what the header's filters mean here:
// the chosen period in words, and which of the filters reach these blocks.
// The funnel's line is the one that matters — sales type and order source
// stay chosen in the header for the other sections, and change nothing in a
// funnel Google Analytics counts on the website.

export const TrafficSectionNav = memo(function TrafficSectionNav({ tab }: { tab: TrafficTab }) {
  const { t } = useTranslation()
  const { period, startDate, endDate } = useFilterStore()
  const dates = periodDates(period, startDate, endDate)
  const items = TRAFFIC_TABS.map((s) => ({
    href: s.path,
    label: t(`traffic.section.${s.tab}`),
    shortLabel: t(`traffic.sectionShort.${s.tab}`),
  }))
  const scope = t(`traffic.sectionScope.${tab}`)

  return (
    <Wrapper dir="column" gap="sm">
      <SectionNav items={items} active={trafficPath(tab)} ariaLabel={t('traffic.sectionsLabel')} />
      <p className="text-[13px] leading-relaxed text-slate-600">
        {dates ? (
          <>
            <span className="font-semibold text-slate-900">
              {t('traffic.sectionPeriod', { dates: formatSpan(dates.start, dates.end) })}
            </span>
            {' · '}{scope}
          </>
        ) : (
          scope.charAt(0).toLocaleUpperCase() + scope.slice(1)
        )}
      </p>
    </Wrapper>
  )
})
