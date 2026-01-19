import { FilterBar } from '../filters'

export function Header() {
  return (
    <header className="sticky top-0 z-50 bg-white/80 backdrop-blur-xl border-b border-slate-200/60 px-3 sm:px-6 py-3 sm:py-4">
      <div className="flex flex-col gap-3 sm:gap-4 max-w-[1800px] mx-auto">
        {/* Title row */}
        <div className="flex items-center justify-between gap-2">
          <div className="flex items-center gap-2 sm:gap-3 min-w-0">
            {/* Logo/Brand */}
            <div className="w-7 h-7 sm:w-8 sm:h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center shadow-sm flex-shrink-0">
              <svg className="w-4 h-4 sm:w-5 sm:h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-1.5">
                <h1 className="text-base sm:text-xl font-semibold text-slate-900 tracking-tight truncate">
                  KoreanStory
                </h1>
                <span className="text-[10px] font-medium text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded-full border border-blue-100 flex-shrink-0">
                  v2
                </span>
              </div>
              <p className="text-[10px] sm:text-xs text-slate-500 hidden xs:block">Sales & Performance</p>
            </div>
          </div>
          <a
            href="/"
            className="text-xs sm:text-sm text-slate-500 hover:text-slate-700 transition-colors flex items-center gap-1 px-2 sm:px-3 py-1.5 rounded-lg hover:bg-slate-100 flex-shrink-0"
          >
            <svg className="w-3.5 h-3.5 sm:w-4 sm:h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
            </svg>
            <span className="hidden sm:inline">Back to</span> v1
          </a>
        </div>

        {/* Filters row */}
        <FilterBar />
      </div>
    </header>
  )
}
