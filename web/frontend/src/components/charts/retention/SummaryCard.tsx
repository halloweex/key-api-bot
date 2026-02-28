import { memo, type ReactNode } from 'react'

interface SummaryCardProps {
  label: string
  value: string
  subtitle?: string
  variant?: 'default' | 'emerald' | 'blue' | 'amber' | 'red'
  icon?: ReactNode
  trend?: number | null
}

const variantStyles = {
  default: 'from-slate-100 to-slate-50 border-slate-200',
  emerald: 'from-emerald-50 to-emerald-100/50 border-emerald-200',
  blue: 'from-blue-50 to-blue-100/50 border-blue-200',
  amber: 'from-amber-50 to-amber-100/50 border-amber-200',
  red: 'from-red-50 to-red-100/50 border-red-200',
}

const valueStyles = {
  default: 'text-slate-800',
  emerald: 'text-emerald-800',
  blue: 'text-blue-800',
  amber: 'text-amber-800',
  red: 'text-red-800',
}

export const SummaryCard = memo(function SummaryCard({
  label,
  value,
  subtitle,
  variant = 'default',
  icon,
  trend,
}: SummaryCardProps) {
  return (
    <div className={`relative bg-gradient-to-br ${variantStyles[variant]} border rounded-xl p-4`}>
      {icon && (
        <div className="absolute top-3 right-3 opacity-[0.15]">
          {icon}
        </div>
      )}
      <p className="text-xs text-slate-600 font-medium">{label}</p>
      <div className="flex items-baseline gap-2">
        <p className={`text-xl font-bold ${valueStyles[variant]}`}>{value}</p>
        {trend != null && trend !== 0 && (
          <span className={`text-xs font-semibold px-1.5 py-0.5 rounded ${
            trend > 0
              ? 'bg-emerald-100 text-emerald-700'
              : 'bg-red-100 text-red-700'
          }`}>
            {trend > 0 ? '+' : ''}{trend.toFixed(1)}pp
          </span>
        )}
      </div>
      {subtitle && (
        <p className="text-xs text-slate-500 mt-0.5">{subtitle}</p>
      )}
    </div>
  )
})
