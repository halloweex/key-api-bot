import { useState, useRef, useEffect } from 'react'
import { PeriodFilter } from './PeriodFilter'
import { SalesTypeFilter } from './SalesTypeFilter'
import { SourceFilter } from './SourceFilter'
import { CategoryFilter } from './CategoryFilter'
import { BrandFilter } from './BrandFilter'

export function FilterBar() {
  const scrollRef = useRef<HTMLDivElement>(null)
  const [showLeftFade, setShowLeftFade] = useState(false)
  const [showRightFade, setShowRightFade] = useState(false)

  useEffect(() => {
    const el = scrollRef.current
    if (!el) return

    const updateFades = () => {
      const { scrollLeft, scrollWidth, clientWidth } = el
      setShowLeftFade(scrollLeft > 8)
      setShowRightFade(scrollLeft < scrollWidth - clientWidth - 8)
    }

    updateFades()
    el.addEventListener('scroll', updateFades, { passive: true })
    window.addEventListener('resize', updateFades)

    return () => {
      el.removeEventListener('scroll', updateFades)
      window.removeEventListener('resize', updateFades)
    }
  }, [])

  return (
    <div className="flex flex-col gap-3 sm:gap-4">
      {/* Period filter - scrollable on mobile with fade indicators */}
      <div className="relative">
        {/* Left fade indicator */}
        {showLeftFade && (
          <div className="absolute left-0 top-0 bottom-0 w-8 bg-gradient-to-r from-white to-transparent z-10 pointer-events-none sm:hidden" />
        )}

        {/* Scrollable container */}
        <div
          ref={scrollRef}
          className="overflow-x-auto -mx-3 px-3 sm:mx-0 sm:px-0 scrollbar-hide scroll-snap-x"
        >
          <PeriodFilter />
        </div>

        {/* Right fade indicator */}
        {showRightFade && (
          <div className="absolute right-0 top-0 bottom-0 w-8 bg-gradient-to-l from-white to-transparent z-10 pointer-events-none sm:hidden" />
        )}
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
