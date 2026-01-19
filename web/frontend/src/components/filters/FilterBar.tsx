import { PeriodFilter } from './PeriodFilter'
import { SalesTypeFilter } from './SalesTypeFilter'
import { SourceFilter } from './SourceFilter'
import { CategoryFilter } from './CategoryFilter'
import { BrandFilter } from './BrandFilter'

export function FilterBar() {
  return (
    <div className="flex flex-col gap-3 sm:gap-4">
      {/* Period filter - scrollable on mobile */}
      <div className="overflow-x-auto -mx-3 px-3 sm:mx-0 sm:px-0 scrollbar-hide">
        <PeriodFilter />
      </div>

      {/* Other filters - wrap on mobile */}
      <div className="flex flex-wrap items-center gap-2 sm:gap-3">
        <SalesTypeFilter />
        <SourceFilter />
        <CategoryFilter />
        <BrandFilter />
      </div>
    </div>
  )
}
