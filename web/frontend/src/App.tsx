import { useQuery } from '@tanstack/react-query'
import { api } from './api/client'
import { useQueryParams } from './store/filterStore'
import { formatCurrency, formatNumber } from './utils/formatters'

function App() {
  const queryParams = useQueryParams()

  // Fetch summary data
  const { data: summary, isLoading, error } = useQuery({
    queryKey: ['summary', queryParams],
    queryFn: () => api.getSummary(queryParams),
  })

  return (
    <div className="min-h-screen bg-slate-900">
      {/* Header */}
      <header className="bg-slate-800 border-b border-slate-700 px-6 py-4">
        <div className="flex items-center justify-between">
          <h1 className="text-2xl font-bold text-white">
            KoreanStory Analytics
            <span className="ml-2 text-xs text-slate-400 font-normal">v2 (React)</span>
          </h1>
          <div className="text-sm text-slate-400">
            React + TypeScript + TanStack Query
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="p-6">
        {/* Status Banner */}
        <div className="mb-6 bg-blue-900/30 border border-blue-700 rounded-lg p-4">
          <p className="text-blue-300">
            <span className="font-semibold">Phase 1 Complete:</span> React infrastructure is working.
            This is the /v2 test route running alongside the original dashboard.
          </p>
        </div>

        {/* Summary Cards */}
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
          {isLoading ? (
            // Loading skeletons
            <>
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="bg-slate-800 rounded-lg p-4 animate-pulse">
                  <div className="h-4 bg-slate-700 rounded w-20 mb-2"></div>
                  <div className="h-8 bg-slate-700 rounded w-32"></div>
                </div>
              ))}
            </>
          ) : error ? (
            <div className="col-span-4 bg-red-900/30 border border-red-700 rounded-lg p-4">
              <p className="text-red-300">Error loading data: {String(error)}</p>
            </div>
          ) : summary ? (
            <>
              <SummaryCard
                label="Total Orders"
                value={formatNumber(summary.totalOrders)}
                color="blue"
              />
              <SummaryCard
                label="Total Revenue"
                value={formatCurrency(summary.totalRevenue)}
                color="green"
              />
              <SummaryCard
                label="Avg Check"
                value={formatCurrency(summary.avgCheck)}
                color="purple"
              />
              <SummaryCard
                label="Returns"
                value={formatNumber(summary.totalReturns)}
                color="orange"
              />
            </>
          ) : null}
        </div>

        {/* Placeholder for charts */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="bg-slate-800 rounded-lg p-6">
            <h3 className="text-lg font-semibold text-white mb-4">Revenue Trend</h3>
            <div className="h-64 flex items-center justify-center border-2 border-dashed border-slate-700 rounded">
              <span className="text-slate-500">Chart will be added in Phase 2</span>
            </div>
          </div>
          <div className="bg-slate-800 rounded-lg p-6">
            <h3 className="text-lg font-semibold text-white mb-4">Sales by Source</h3>
            <div className="h-64 flex items-center justify-center border-2 border-dashed border-slate-700 rounded">
              <span className="text-slate-500">Chart will be added in Phase 2</span>
            </div>
          </div>
        </div>
      </main>
    </div>
  )
}

// Summary Card Component
function SummaryCard({
  label,
  value,
  color,
}: {
  label: string
  value: string
  color: 'blue' | 'green' | 'purple' | 'orange'
}) {
  const colorClasses = {
    blue: 'text-blue-400',
    green: 'text-green-400',
    purple: 'text-purple-400',
    orange: 'text-orange-400',
  }

  return (
    <div className="bg-slate-800 rounded-lg p-4">
      <p className="text-sm text-slate-400 mb-1">{label}</p>
      <p className={`text-2xl font-bold ${colorClasses[color]}`}>{value}</p>
    </div>
  )
}

export default App
