import { FilterBar } from '../filters'

export function Header() {
  return (
    <header className="sticky top-0 z-50 bg-white/80 backdrop-blur-xl border-b border-slate-200/60 px-6 py-4">
      <div className="flex flex-col gap-4 max-w-[1800px] mx-auto">
        {/* Title row */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            {/* Logo/Brand */}
            <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-blue-600 rounded-lg flex items-center justify-center shadow-sm">
              <svg className="w-5 h-5 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
              </svg>
            </div>
            <div>
              <h1 className="text-xl font-semibold text-slate-900 tracking-tight">
                KoreanStory Analytics
              </h1>
              <p className="text-xs text-slate-500">Sales & Performance Dashboard</p>
            </div>
            <span className="ml-1 text-[10px] font-medium text-blue-600 bg-blue-50 px-1.5 py-0.5 rounded-full border border-blue-100">
              v2
            </span>
          </div>
          <a
            href="/"
            className="text-sm text-slate-500 hover:text-slate-700 transition-colors flex items-center gap-1 px-3 py-1.5 rounded-lg hover:bg-slate-100"
          >
            <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M10 19l-7-7m0 0l7-7m-7 7h18" />
            </svg>
            Back to v1
          </a>
        </div>

        {/* Filters row */}
        <FilterBar />
      </div>
    </header>
  )
}
