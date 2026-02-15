import { PeriodFilter } from './PeriodFilter'
import { SalesTypeFilter } from './SalesTypeFilter'
import { SourceFilter } from './SourceFilter'
import { CategoryFilter } from './CategoryFilter'
import { BrandFilter } from './BrandFilter'

export function FilterBar() {
  return (
    <div className="flex flex-col gap-3 sm:gap-4">
      {/* Period filter - scrollable on mobile, with margins for sidebar icons */}
      <div className="overflow-x-auto mx-9 sm:mx-0 scrollbar-hide">
        <PeriodFilter />
      </div>

      {/* Other filters - 2-column grid on mobile, flex on larger screens */}
      <div className="grid grid-cols-2 gap-2 sm:flex sm:flex-wrap sm:items-center sm:gap-3">
        <SalesTypeFilter />
        <SourceFilter />
        <CategoryFilter />
        <BrandFilter />
      </div>
    </div>
  )
}
