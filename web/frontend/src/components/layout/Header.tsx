import { FilterBar } from '../filters'

export function Header() {
  return (
    <header className="bg-slate-800 border-b border-slate-700 px-6 py-4">
      <div className="flex flex-col gap-4">
        {/* Title row */}
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold text-white">
            KoreanStory Analytics
            <span className="ml-2 text-xs text-slate-400 font-normal bg-slate-700 px-2 py-0.5 rounded">
              v2
            </span>
          </h1>
          <a
            href="/"
            className="text-sm text-slate-400 hover:text-white transition-colors"
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
