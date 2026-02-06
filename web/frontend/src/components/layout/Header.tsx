import { useQuery } from '@tanstack/react-query'
import { FilterBar } from '../filters'
import { api } from '../../api/client'
import type { HealthResponse } from '../../types/api'

export function Header() {
  const { data: health } = useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: () => api.getHealth(),
    staleTime: 5 * 60 * 1000, // 5 minutes
    refetchOnWindowFocus: false,
  })

  return (
    <header className="sticky top-0 z-50 bg-white/80 backdrop-blur-xl border-b border-slate-200/60 px-3 sm:px-6 py-3 sm:py-4">
      <div className="flex flex-col gap-3 sm:gap-4 max-w-[1800px] mx-auto">
        {/* Title row */}
        <div className="flex items-center gap-2">
          {/* Logo/Brand */}
          <div className="flex items-center gap-2 sm:gap-3 min-w-0">
            <div className="w-7 h-7 sm:w-8 sm:h-8 rounded-lg flex items-center justify-center shadow-sm flex-shrink-0 overflow-hidden">
              <svg className="w-full h-full" viewBox="0 0 24 24">
                <defs>
                  <linearGradient id="logoGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#2563EB"/>
                    <stop offset="100%" stopColor="#7C3AED"/>
                  </linearGradient>
                </defs>
                <rect width="24" height="24" rx="4" fill="url(#logoGrad)"/>
                <path fill="#fff" d="M9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"/>
              </svg>
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <h1 className="text-base sm:text-xl font-semibold text-slate-900 tracking-tight truncate">
                  KoreanStory Analytics
                </h1>
                {health?.version && (
                  <span className="text-[10px] sm:text-xs text-slate-400 font-medium">
                    v{health.version}
                  </span>
                )}
              </div>
              <p className="text-[10px] sm:text-xs text-slate-500 hidden xs:block">Sales & Performance</p>
            </div>
          </div>

          {/* Spacer for toggle button area */}
          <div className="flex-1" />
          <div className="w-[140px] sm:w-[160px]" />
        </div>

        {/* Filters row */}
        <FilterBar />
      </div>
    </header>
  )
}
