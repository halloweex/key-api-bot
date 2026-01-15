import { useMemo } from 'react'
import { Card, CardContent, SkeletonCard } from '../ui'
import { useSummary } from '../../hooks'
import { formatCurrency, formatNumber, formatPercent } from '../../utils/formatters'

interface StatCardProps {
  label: string
  value: string
  subtitle?: string
  color: 'blue' | 'green' | 'purple' | 'orange' | 'red'
}

const colorClasses = {
  blue: 'text-blue-400',
  green: 'text-green-400',
  purple: 'text-purple-400',
  orange: 'text-orange-400',
  red: 'text-red-400',
}

function StatCard({ label, value, subtitle, color }: StatCardProps) {
  return (
    <Card>
      <CardContent className="py-3">
        <p className="text-sm text-slate-400 mb-1">{label}</p>
        <p className={`text-2xl font-bold ${colorClasses[color]}`}>{value}</p>
        {subtitle && (
          <p className="text-xs text-slate-500 mt-1">{subtitle}</p>
        )}
      </CardContent>
    </Card>
  )
}

export function SummaryCards() {
  const { data, isLoading, error } = useSummary()

  const returnRate = useMemo(() => {
    if (!data || data.totalOrders === 0) return 0
    return (data.totalReturns / (data.totalOrders + data.totalReturns)) * 100
  }, [data])

  if (isLoading) {
    return (
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
        <SkeletonCard />
      </div>
    )
  }

  if (error || !data) {
    return (
      <div className="bg-red-900/30 border border-red-700 rounded-lg p-4">
        <p className="text-red-300">Failed to load summary data</p>
      </div>
    )
  }

  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
      <StatCard
        label="Total Orders"
        value={formatNumber(data.totalOrders)}
        subtitle={`${data.startDate} - ${data.endDate}`}
        color="blue"
      />
      <StatCard
        label="Total Revenue"
        value={formatCurrency(data.totalRevenue)}
        color="green"
      />
      <StatCard
        label="Average Check"
        value={formatCurrency(data.avgCheck)}
        color="purple"
      />
      <StatCard
        label="Returns"
        value={formatNumber(data.totalReturns)}
        subtitle={returnRate > 0 ? `${formatPercent(returnRate)} return rate` : undefined}
        color="orange"
      />
    </div>
  )
}
