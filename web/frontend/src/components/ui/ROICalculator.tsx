import { useState, useEffect, useCallback, useMemo } from 'react'
import { Card, CardHeader, CardTitle, CardContent } from './Card'
import { useSummary } from '../../hooks'
import { formatCurrency, formatPercent } from '../../utils/formatters'
import { getStoredExpenses, setStoredExpenses } from '../../utils/localStorage'

// ─── Icons ────────────────────────────────────────────────────────────────────

const CalculatorIcon = () => (
  <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M9 7h6m0 10v-3m-3 3h.01M9 17h.01M9 14h.01M12 14h.01M15 11h.01M12 11h.01M9 11h.01M7 21h10a2 2 0 002-2V5a2 2 0 00-2-2H7a2 2 0 00-2 2v14a2 2 0 002 2z" />
  </svg>
)

const InfoIcon = () => (
  <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
    <path strokeLinecap="round" strokeLinejoin="round" d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
  </svg>
)

// ─── Metric Card Component ────────────────────────────────────────────────────

interface MetricCardProps {
  label: string
  value: string
  colorClass: string
}

function MetricCard({ label, value, colorClass }: MetricCardProps) {
  return (
    <div className={`bg-slate-50 rounded-lg p-3 sm:p-4 border border-slate-100`}>
      <div className="text-xs sm:text-sm text-slate-500 mb-1">{label}</div>
      <div className={`text-lg sm:text-xl font-semibold ${colorClass}`}>{value}</div>
    </div>
  )
}

// ─── ROI Calculator Component ─────────────────────────────────────────────────

export function ROICalculator() {
  const { data: summary, isLoading } = useSummary()
  const [expenses, setExpenses] = useState<number>(0)
  const [inputValue, setInputValue] = useState<string>('')
  const [showTooltip, setShowTooltip] = useState(false)

  // Load expenses from localStorage on mount
  useEffect(() => {
    const stored = getStoredExpenses()
    setExpenses(stored)
    setInputValue(stored > 0 ? stored.toString() : '')
  }, [])

  // Debounced save to localStorage
  useEffect(() => {
    const timer = setTimeout(() => {
      setStoredExpenses(expenses)
    }, 500)
    return () => clearTimeout(timer)
  }, [expenses])

  const handleInputChange = useCallback((e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value
    // Allow only numbers and one decimal point
    if (value === '' || /^\d*\.?\d*$/.test(value)) {
      setInputValue(value)
      const numValue = parseFloat(value) || 0
      setExpenses(numValue)
    }
  }, [])

  const handlePaste = useCallback((e: React.ClipboardEvent<HTMLInputElement>) => {
    e.preventDefault()
    const pastedText = e.clipboardData.getData('text')
    // Clean pasted value: remove everything except digits and decimal point
    const cleaned = pastedText.replace(/[^\d.]/g, '').replace(/(\..*)\./g, '$1')
    if (cleaned) {
      setInputValue(cleaned)
      const numValue = parseFloat(cleaned) || 0
      setExpenses(numValue)
    }
  }, [])

  // Calculate metrics
  const metrics = useMemo(() => {
    if (!summary) {
      return {
        profit: 0,
        roi: 0,
        profitMargin: 0,
        costPerOrder: 0,
      }
    }

    const revenue = summary.totalRevenue
    const totalOrders = summary.totalOrders

    const profit = revenue - expenses
    const roi = expenses > 0 ? (profit / expenses) * 100 : 0
    const profitMargin = revenue > 0 ? (profit / revenue) * 100 : 0
    const costPerOrder = totalOrders > 0 ? expenses / totalOrders : 0

    return {
      profit,
      roi,
      profitMargin,
      costPerOrder,
    }
  }, [summary, expenses])

  const hasExpenses = expenses > 0

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between">
        <div className="flex items-center gap-2">
          <div className="p-2 bg-indigo-50 rounded-lg text-indigo-600">
            <CalculatorIcon />
          </div>
          <CardTitle>ROI Calculator</CardTitle>
        </div>
        <div className="relative">
          <button
            className="p-1.5 text-slate-400 hover:text-slate-600 hover:bg-slate-100 rounded-md transition-colors"
            onMouseEnter={() => setShowTooltip(true)}
            onMouseLeave={() => setShowTooltip(false)}
            onClick={() => setShowTooltip(!showTooltip)}
            aria-label="Info about ROI Calculator"
          >
            <InfoIcon />
          </button>
          {showTooltip && (
            <div className="absolute right-0 top-full mt-2 w-64 p-3 bg-slate-800 text-white text-xs rounded-lg shadow-lg z-10">
              <p className="mb-2">Enter your custom expenses (advertising, logistics, etc.) to calculate profitability metrics.</p>
              <p className="text-slate-300">Values are saved automatically and persist across sessions.</p>
            </div>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {/* Expenses Input */}
        <div className="mb-4 sm:mb-6">
          <label htmlFor="expenses-input" className="block text-sm font-medium text-slate-700 mb-2">
            Custom Expenses
          </label>
          <div className="relative">
            <span className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-500">
              ₴
            </span>
            <input
              id="expenses-input"
              type="text"
              inputMode="decimal"
              value={inputValue}
              onChange={handleInputChange}
              onPaste={handlePaste}
              placeholder="0"
              className="w-full sm:w-64 pl-8 pr-4 py-2 border border-slate-300 rounded-lg focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 outline-none transition-all text-slate-800"
              disabled={isLoading}
            />
          </div>
        </div>

        {/* Metrics Grid */}
        {isLoading ? (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
            {Array.from({ length: 4 }).map((_, i) => (
              <div key={i} className="bg-slate-100 rounded-lg p-4 animate-pulse h-20" />
            ))}
          </div>
        ) : hasExpenses ? (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-3 sm:gap-4">
            <MetricCard
              label="Profit"
              value={formatCurrency(metrics.profit)}
              colorClass={metrics.profit >= 0 ? 'text-green-600' : 'text-red-600'}
            />
            <MetricCard
              label="ROI"
              value={formatPercent(metrics.roi, 1)}
              colorClass="text-blue-600"
            />
            <MetricCard
              label="Profit Margin"
              value={formatPercent(metrics.profitMargin, 1)}
              colorClass="text-purple-600"
            />
            <MetricCard
              label="Cost per Order"
              value={formatCurrency(metrics.costPerOrder)}
              colorClass="text-orange-600"
            />
          </div>
        ) : (
          <div className="text-center py-6 text-slate-500">
            <p>Enter your expenses above to see profitability metrics</p>
          </div>
        )}
      </CardContent>
    </Card>
  )
}
