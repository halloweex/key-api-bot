import { PeriodFilter } from './PeriodFilter'
import { SalesTypeFilter } from './SalesTypeFilter'
import { SourceFilter } from './SourceFilter'
import { CategoryFilter } from './CategoryFilter'
import { BrandFilter } from './BrandFilter'
import { PromocodeFilter } from './PromocodeFilter'
import { useRouter } from '../hooks/useRouter'
import { filtersForPath } from '../utils/pageFilters'

// A control is shown only where the page behind it applies the filter. It is
// the same map `useQueryParams` uses to decide what to send, so the bar cannot
// offer a choice the request then drops — which is what /traffic did with
// category, brand and promocode.
export function FilterBar() {
  const applies = filtersForPath(useRouter())

  return (
    <div className="flex flex-col gap-3 sm:gap-4">
      {/* Period filter - scrollable on mobile, with margins for sidebar icons */}
      <div className="overflow-x-auto mx-9 sm:mx-0 scrollbar-hide">
        <PeriodFilter />
      </div>

      {/* Other filters - 2-column grid on mobile, flex on larger screens */}
      <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:items-center sm:gap-3">
        <SalesTypeFilter />
        {applies.has('sourceId') && <SourceFilter />}
        {applies.has('categoryId') && <CategoryFilter />}
        {applies.has('brand') && <BrandFilter />}
        {applies.has('promocode') && <PromocodeFilter />}
      </div>
    </div>
  )
}
