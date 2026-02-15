import { FilterBar } from '../filters'

export function Header() {
  return (
    <header className="sticky top-0 z-50 bg-white/90 backdrop-blur-xl border-b border-slate-200 py-2 px-3 shadow-sm">
      <div className="max-w-[1800px]">
        <FilterBar />
      </div>
    </header>
  )
}
