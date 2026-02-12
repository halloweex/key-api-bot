import { useQuery } from '@tanstack/react-query'
import { FilterBar } from '../filters'
import { LiveIndicator } from '../ui/LiveIndicator'
import { useWebSocket } from '../../hooks/useWebSocket'
import { api } from '../../api/client'
import type { HealthResponse } from '../../types/api'

export function Header() {
  const { data: health } = useQuery<HealthResponse>({
    queryKey: ['health'],
    queryFn: () => api.getHealth(),
    staleTime: 5 * 60 * 1000, // 5 minutes
    refetchOnWindowFocus: false,
  })

  // WebSocket for real-time updates
  const { connectionState, lastMessageTime } = useWebSocket({
    enabled: true,
    room: 'dashboard',
  })

  return (
    <header className="sticky top-0 z-50 bg-white/90 backdrop-blur-xl border-b border-slate-200/60 px-3 sm:px-6 py-3 sm:py-4 shadow-sm">
      <div className="flex flex-col gap-3 sm:gap-4 max-w-[1800px] mx-auto">
        {/* Title row */}
        <div className="flex items-center gap-2">
          {/* Logo/Brand */}
          <div className="flex items-center gap-2 sm:gap-3 min-w-0 group">
            <div className="w-8 h-8 sm:w-9 sm:h-9 rounded-xl flex items-center justify-center shadow-md flex-shrink-0 overflow-hidden transition-transform duration-200 group-hover:scale-105">
              <svg className="w-full h-full" viewBox="0 0 24 24">
                <defs>
                  <linearGradient id="logoGrad" x1="0%" y1="0%" x2="100%" y2="100%">
                    <stop offset="0%" stopColor="#2563EB"/>
                    <stop offset="100%" stopColor="#7C3AED"/>
                  </linearGradient>
                </defs>
                <rect width="24" height="24" rx="5" fill="url(#logoGrad)"/>
                <path fill="#fff" d="M9 17H7v-7h2v7zm4 0h-2V7h2v10zm4 0h-2v-4h2v4z"/>
              </svg>
            </div>
            <div className="min-w-0">
              <div className="flex items-center gap-2">
                <h1 className="text-sm sm:text-xl font-bold text-slate-900 tracking-tight whitespace-nowrap">
                  KoreanStory Analytics
                </h1>
                {health?.version && (
                  <span className="text-[10px] sm:text-xs text-slate-400 font-medium bg-slate-100 px-1.5 py-0.5 rounded-md">
                    v{health.version}
                  </span>
                )}
              </div>
              <p className="text-[10px] sm:text-xs text-slate-500 hidden xs:block">Sales & Performance Dashboard</p>
            </div>
          </div>

          {/* Live indicator - shows real-time connection status */}
          <div className="hidden sm:flex items-center">
            <LiveIndicator
              connectionState={connectionState}
              lastMessageTime={lastMessageTime}
            />
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
