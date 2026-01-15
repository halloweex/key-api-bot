import { PeriodFilter } from './PeriodFilter'
import { SalesTypeFilter } from './SalesTypeFilter'
import { SourceFilter } from './SourceFilter'
import { CategoryFilter } from './CategoryFilter'
import { BrandFilter } from './BrandFilter'

export function FilterBar() {
  return (
    <div className="flex flex-wrap items-center gap-4">
      {/* Primary filters */}
      <div className="flex items-center gap-4">
        <PeriodFilter />
        <SalesTypeFilter />
      </div>

      {/* Secondary filters */}
      <div className="flex items-center gap-2">
        <SourceFilter />
        <CategoryFilter />
        <BrandFilter />
      </div>
    </div>
  )
}
