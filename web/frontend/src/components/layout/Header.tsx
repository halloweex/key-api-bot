import { FilterBar } from '../filters'

export function Header() {
  return (
    <header className="bg-white border-b border-slate-200 px-6 py-4 shadow-sm">
      <div className="flex flex-col gap-4">
        {/* Title row */}
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold text-slate-900">
            KoreanStory Analytics
            <span className="ml-2 text-xs text-blue-600 font-normal bg-blue-50 px-2 py-0.5 rounded">
              v2
            </span>
          </h1>
          <a
            href="/"
            className="text-sm text-slate-500 hover:text-slate-900 transition-colors"
          >
            ← Back to v1
          </a>
        </div>

        {/* Filters row */}
        <FilterBar />
      </div>
    </header>
  )
}
