import { memo, useEffect, useMemo, useRef, useState } from 'react'
import { ChartContainer } from './ChartContainer'
import { InfoPopover } from '../ui/InfoPopover'
import { VirtualList } from '../ui/VirtualList'
import { useSkuRotation } from '../../hooks'
import { formatCurrency, formatNumber } from '../../utils/formatters'
import type {
  SkuRotationItem,
  AbcClass,
  VelocityTier,
  DeadStockDecision,
} from '../../types/api'

// ─── Constants ───────────────────────────────────────────────────────────────

const LEAD_TIME_DAYS = 14
const SAFETY_DAYS = 7
const TARGET_COVER_DAYS = LEAD_TIME_DAYS + SAFETY_DAYS  // 21
const MARGIN_FLOOR = 0.05  // never recommend discount that gives <5% margin
const MAX_DISCOUNT = 0.5
const MIN_DISCOUNT = 0.2

const TIER_BG: Record<VelocityTier, string> = {
  hot: 'bg-emerald-50',
  healthy: 'bg-emerald-50/70',
  warm: 'bg-amber-50',
  cold: 'bg-orange-50',
  frozen: 'bg-red-50',
}
const TIER_TEXT: Record<VelocityTier, string> = {
  hot: 'text-emerald-700',
  healthy: 'text-emerald-700',
  warm: 'text-amber-700',
  cold: 'text-orange-700',
  frozen: 'text-red-700',
}
const DECISION_BADGE: Record<DeadStockDecision, string> = {
  HOLD: 'bg-slate-100 text-slate-600',
  PROMO: 'bg-amber-100 text-amber-700',
  LIQUIDATE: 'bg-red-100 text-red-700',
}

const ABC_OPTIONS: AbcClass[] = ['A', 'B', 'C']
const TIER_OPTIONS: VelocityTier[] = ['hot', 'healthy', 'warm', 'cold', 'frozen']
const DECISION_OPTIONS: DeadStockDecision[] = ['HOLD', 'PROMO', 'LIQUIDATE']

type PresetKey = 'all' | 'discount' | 'reorder' | 'skip' | 'decelerating'

interface PresetDef {
  key: PresetKey
  label: string
  emoji: string
  description: string
  match: (it: SkuRotationItem) => boolean
  defaultSort: SortKey
  defaultSortDir: SortDir
}

const PRESETS: PresetDef[] = [
  {
    key: 'all',
    label: 'All',
    emoji: '🔍',
    description: 'Все SKU без фильтра',
    match: () => true,
    defaultSort: 'excessCapitalCost',
    defaultSortDir: 'desc',
  },
  {
    key: 'discount',
    label: 'Discount candidates',
    emoji: '🔴',
    description: 'Кандидаты на скидку для cashflow recovery (frozen/cold + LIQUIDATE)',
    match: (it) =>
      it.decision === 'LIQUIDATE' ||
      ((it.velocityTier === 'frozen' || it.velocityTier === 'cold') && it.excessCapitalCost > 0),
    defaultSort: 'excessCapitalCost',
    defaultSortDir: 'desc',
  },
  {
    key: 'reorder',
    label: 'Reorder now',
    emoji: '🛒',
    description: 'Хорошо продаётся + остатков < 30 дней',
    match: (it) => {
      if (!(it.velocityTier === 'hot' || it.velocityTier === 'healthy')) return false
      if (it.avgDailySales30d <= 0) return false
      const cover = it.units / it.avgDailySales30d
      return cover < 30
    },
    defaultSort: 'qtySold30d',
    defaultSortDir: 'desc',
  },
  {
    key: 'skip',
    label: 'Skip on next PO',
    emoji: '🚫',
    description: 'Frozen с большим избытком — не закупать',
    match: (it) => it.velocityTier === 'frozen' && it.excessCapitalCost > 5000,
    defaultSort: 'excessCapitalCost',
    defaultSortDir: 'desc',
  },
  {
    key: 'decelerating',
    label: 'Decelerating',
    emoji: '📉',
    description: 'Темп продаж за 30 дней упал относительно 90 дней (early warning)',
    match: (it) =>
      it.velocityRatio30to90 != null &&
      it.velocityRatio30to90 < 0.7 &&
      it.units > 10,
    defaultSort: 'velocityRatio30to90',
    defaultSortDir: 'asc',
  },
]

type SortKey =
  | 'sku' | 'name' | 'brand' | 'abcClass' | 'velocityTier'
  | 'units' | 'saleValue' | 'excessCapitalCost' | 'daysOfSupply' | 'gmroi'
  | 'velocityRatio30to90' | 'decision' | 'qtySold30d' | 'qtySold90d'
  | 'revenue90d' | 'daysSinceSale'

type SortDir = 'asc' | 'desc'

// ─── Smart suggestions ───────────────────────────────────────────────────────

function suggestedDiscount(it: SkuRotationItem): { pct: number; recovery: number } {
  if (it.saleValue <= 0 || it.costBasis <= 0) return { pct: 0, recovery: 0 }
  const costRatio = it.costBasis / it.saleValue
  // Max we can discount keeping MARGIN_FLOOR margin: 1 - costRatio - MARGIN_FLOOR
  const maxAllowed = 1 - costRatio - MARGIN_FLOOR
  const pct = Math.min(MAX_DISCOUNT, Math.max(MIN_DISCOUNT, maxAllowed))
  const recovery = it.saleValue * (1 - pct)
  return { pct, recovery }
}

function suggestedReorderQty(it: SkuRotationItem): { qty: number; daysCover: number } {
  if (it.avgDailySales30d <= 0) return { qty: 0, daysCover: Infinity }
  const target = it.avgDailySales30d * TARGET_COVER_DAYS
  // Boost for accelerating SKU
  const ratio = it.velocityRatio30to90 ?? 1
  const adjusted = ratio > 1.2 ? target * 1.3 : target
  const need = Math.max(0, adjusted - it.units)
  const daysCover = it.units / it.avgDailySales30d
  return { qty: Math.ceil(need), daysCover }
}

// ─── CSV export ──────────────────────────────────────────────────────────────

function exportCsv(rows: SkuRotationItem[], preset: PresetKey, filename: string) {
  const baseHeaders = [
    'SKU', 'Name', 'Brand', 'Category', 'ABC', 'Velocity', 'Units',
    'Retail ₴', 'Excess retail ₴', 'DOS',
    'Days since sale', 'Qty sold 30d', 'Qty sold 90d', 'Revenue 90d ₴',
    '30/90 ratio', 'GMROI %', 'Decision',
  ]
  const presetHeaders: Record<PresetKey, string[]> = {
    all: [],
    discount: ['Suggested discount %', 'Recovery ₴'],
    reorder: ['Suggested qty', 'Days cover'],
    skip: ['Carrying saved/yr ₴'],
    decelerating: ['30d/day rate', '90d/day rate'],
  }
  const headers = [...baseHeaders, ...presetHeaders[preset]]

  const escape = (v: unknown): string => {
    if (v === null || v === undefined) return ''
    const s = String(v)
    if (s.includes(',') || s.includes('"') || s.includes('\n')) {
      return `"${s.replace(/"/g, '""')}"`
    }
    return s
  }

  const lines = [headers.join(',')]
  for (const it of rows) {
    const excessUnits = Math.max(0, it.units - it.avgDailySales90d * 60)
    const excessRetail = excessUnits * it.price
    const base = [
      it.sku, it.name ?? '', it.brand ?? '', it.categoryName ?? '',
      it.abcClass, it.velocityTier, it.units,
      Math.round(it.saleValue), Math.round(excessRetail),
      it.daysOfSupply ?? '',
      it.daysSinceSale ?? '', it.qtySold30d, it.qtySold90d,
      Math.round(it.revenue90d),
      it.velocityRatio30to90 ?? '',
      it.gmroi != null ? Math.round(it.gmroi * 100) : '',
      it.decision,
    ]
    let extras: (string | number)[] = []
    if (preset === 'discount') {
      const s = suggestedDiscount(it)
      extras = [Math.round(s.pct * 100), Math.round(s.recovery)]
    } else if (preset === 'reorder') {
      const s = suggestedReorderQty(it)
      extras = [s.qty, isFinite(s.daysCover) ? Math.round(s.daysCover) : '']
    } else if (preset === 'skip') {
      extras = [Math.round(it.costBasis * 0.25)]
    } else if (preset === 'decelerating') {
      extras = [it.avgDailySales30d.toFixed(2), it.avgDailySales90d.toFixed(2)]
    }
    lines.push([...base, ...extras].map(escape).join(','))
  }

  // UTF-8 BOM for Excel Cyrillic compatibility
  const blob = new Blob(['﻿' + lines.join('\n')], { type: 'text/csv;charset=utf-8' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}

// ─── Props ───────────────────────────────────────────────────────────────────

interface SkuRotationTableProps {
  brandFilter?: string | null
  presetOverride?: PresetKey | null
  onClearExternalFilter?: () => void
}

// ─── Main component ──────────────────────────────────────────────────────────

function SkuRotationTableComponent({
  brandFilter,
  presetOverride,
  onClearExternalFilter,
}: SkuRotationTableProps) {
  const { data: skus, isLoading, error } = useSkuRotation()

  const [preset, setPreset] = useState<PresetKey>('all')
  const [sortKey, setSortKey] = useState<SortKey>('excessCapitalCost')
  const [sortDir, setSortDir] = useState<SortDir>('desc')
  const [search, setSearch] = useState('')
  const [abcFilter, setAbcFilter] = useState<Set<AbcClass>>(new Set())
  const [tierFilter, setTierFilter] = useState<Set<VelocityTier>>(new Set())
  const [decisionFilter, setDecisionFilter] = useState<Set<DeadStockDecision>>(new Set())
  const [brandFilterInternal, setBrandFilterInternal] = useState<Set<string>>(new Set())
  const [categoryFilter, setCategoryFilter] = useState<Set<string>>(new Set())
  const [expanded, setExpanded] = useState<Set<number>>(new Set())

  const containerRef = useRef<HTMLDivElement>(null)

  // Apply external preset/brand filter (from BrandRotationCard click)
  useEffect(() => {
    if (presetOverride) {
      const p = PRESETS.find((p) => p.key === presetOverride)
      if (p) {
        setPreset(p.key)
        setSortKey(p.defaultSort)
        setSortDir(p.defaultSortDir)
      }
    }
    if (brandFilter) {
      setBrandFilterInternal(new Set([brandFilter]))
      // Scroll into view
      setTimeout(() => containerRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' }), 100)
    }
  }, [brandFilter, presetOverride])

  // Apply preset switch — reset sort to preset's default
  const onPresetChange = (key: PresetKey) => {
    const p = PRESETS.find((p) => p.key === key)
    if (!p) return
    setPreset(p.key)
    setSortKey(p.defaultSort)
    setSortDir(p.defaultSortDir)
  }

  // Build option lists from data
  const brandOptions = useMemo(() => {
    if (!skus) return []
    const set = new Set<string>()
    for (const it of skus) if (it.brand) set.add(it.brand)
    return Array.from(set).sort()
  }, [skus])

  const categoryOptions = useMemo(() => {
    if (!skus) return []
    const set = new Set<string>()
    for (const it of skus) if (it.categoryName) set.add(it.categoryName)
    return Array.from(set).sort()
  }, [skus])

  // Filter + sort pipeline
  const filtered = useMemo(() => {
    if (!skus) return []
    const presetDef = PRESETS.find((p) => p.key === preset) ?? PRESETS[0]
    const lowerSearch = search.trim().toLowerCase()

    const result = skus.filter((it) => {
      if (!presetDef.match(it)) return false
      if (lowerSearch) {
        const hay = `${it.sku} ${it.name ?? ''}`.toLowerCase()
        if (!hay.includes(lowerSearch)) return false
      }
      if (abcFilter.size > 0 && !abcFilter.has(it.abcClass)) return false
      if (tierFilter.size > 0 && !tierFilter.has(it.velocityTier)) return false
      if (decisionFilter.size > 0 && !decisionFilter.has(it.decision)) return false
      if (brandFilterInternal.size > 0 && !brandFilterInternal.has(it.brand ?? '')) return false
      if (categoryFilter.size > 0 && !categoryFilter.has(it.categoryName ?? '')) return false
      return true
    })

    const dir = sortDir === 'asc' ? 1 : -1
    result.sort((a, b) => {
      const va = (a as unknown as Record<string, unknown>)[sortKey]
      const vb = (b as unknown as Record<string, unknown>)[sortKey]
      if (va == null && vb == null) return 0
      if (va == null) return 1
      if (vb == null) return -1
      if (typeof va === 'number' && typeof vb === 'number') return (va - vb) * dir
      return String(va).localeCompare(String(vb)) * dir
    })

    return result
  }, [skus, preset, search, abcFilter, tierFilter, decisionFilter, brandFilterInternal, categoryFilter, sortKey, sortDir])

  // Preset counters
  const presetCounters = useMemo(() => {
    if (!skus) return {} as Record<PresetKey, number>
    const out: Record<string, number> = {}
    for (const p of PRESETS) {
      out[p.key] = skus.filter(p.match).length
    }
    return out as Record<PresetKey, number>
  }, [skus])

  // Action footer aggregates (retail-based)
  const aggregates = useMemo(() => {
    const totalRetail = filtered.reduce((s, it) => s + it.saleValue, 0)
    const totalExcessRetail = filtered.reduce((s, it) => {
      const excessUnits = Math.max(0, it.units - it.avgDailySales90d * 60)
      return s + excessUnits * it.price
    }, 0)
    const totalUnits = filtered.reduce((s, it) => s + it.units, 0)
    const totalRev90 = filtered.reduce((s, it) => s + it.revenue90d, 0)
    // Carrying cost is intrinsically tied to actual capital invested (cost basis),
    // computed but never displayed as cost basis — only as a savings metric.
    const totalCarryingSaved = filtered.reduce((s, it) => s + it.costBasis * 0.25, 0)

    if (preset === 'discount') {
      let recovery = 0
      let pctSum = 0
      for (const it of filtered) {
        const s = suggestedDiscount(it)
        recovery += s.recovery
        pctSum += s.pct
      }
      const avgPct = filtered.length > 0 ? pctSum / filtered.length : 0
      return { kind: 'discount' as const, count: filtered.length, totalRetail, totalExcessRetail, recovery, avgPct, carrySaved: totalCarryingSaved }
    }
    if (preset === 'reorder') {
      let qty = 0
      let retail = 0
      for (const it of filtered) {
        const s = suggestedReorderQty(it)
        qty += s.qty
        retail += s.qty * it.price
      }
      const projectedRev30d = filtered.reduce((s, it) => s + it.avgDailySales30d * 30 * it.price, 0)
      return { kind: 'reorder' as const, count: filtered.length, qty, retail, projectedRev30d }
    }
    if (preset === 'skip') {
      return { kind: 'skip' as const, count: filtered.length, totalRetail, totalExcessRetail, carrySaved: totalCarryingSaved }
    }
    if (preset === 'decelerating') {
      return { kind: 'decelerating' as const, count: filtered.length, atRisk: totalRetail, totalUnits }
    }
    return { kind: 'all' as const, count: filtered.length, totalRetail, totalRev90, totalUnits }
  }, [filtered, preset])

  // Sort helper
  const onSort = (key: SortKey) => {
    if (sortKey === key) {
      setSortDir(sortDir === 'asc' ? 'desc' : 'asc')
    } else {
      setSortKey(key)
      setSortDir(['name', 'sku', 'brand'].includes(key) ? 'asc' : 'desc')
    }
  }

  const toggleSetItem = <T,>(set: Set<T>, item: T, setter: (s: Set<T>) => void) => {
    const next = new Set(set)
    if (next.has(item)) next.delete(item)
    else next.add(item)
    setter(next)
  }

  const clearAllFilters = () => {
    setSearch('')
    setAbcFilter(new Set())
    setTierFilter(new Set())
    setDecisionFilter(new Set())
    setBrandFilterInternal(new Set())
    setCategoryFilter(new Set())
    onClearExternalFilter?.()
  }

  const hasAnyFilter =
    !!search || abcFilter.size > 0 || tierFilter.size > 0 ||
    decisionFilter.size > 0 || brandFilterInternal.size > 0 || categoryFilter.size > 0

  // ─── Render ────────────────────────────────────────────────────────────────

  const renderRow = (it: SkuRotationItem) => {
    const isExp = expanded.has(it.offerId)
    const ratioVal = it.velocityRatio30to90
    const ratioColor =
      ratioVal == null ? 'text-slate-400' :
      ratioVal < 0.7 ? 'text-red-600' :
      ratioVal < 0.9 ? 'text-amber-600' :
      ratioVal > 1.3 ? 'text-emerald-700' :
      ratioVal > 1.1 ? 'text-emerald-600' :
      'text-slate-500'
    const ratioArrow =
      ratioVal == null ? '' :
      ratioVal < 0.85 ? '↓' :
      ratioVal > 1.15 ? '↑' :
      '→'
    const gmroiPct = it.gmroi != null ? Math.round(it.gmroi * 100) : null
    const gmroiColor =
      gmroiPct == null ? 'text-slate-400' :
      gmroiPct < 100 ? 'text-red-600' :
      gmroiPct < 200 ? 'text-amber-600' : 'text-emerald-600'

    // Conditional column data per preset
    let conditional: { label: string; value: string; tooltip?: string }[] = []
    if (preset === 'discount') {
      const s = suggestedDiscount(it)
      if (s.pct > 0) {
        conditional = [
          { label: 'Discount', value: `${Math.round(s.pct * 100)}%` },
          { label: 'Recovery', value: formatCurrency(s.recovery) },
        ]
      }
    } else if (preset === 'reorder') {
      const s = suggestedReorderQty(it)
      if (s.qty > 0) {
        conditional = [
          { label: 'Order qty', value: formatNumber(s.qty) },
          { label: 'Cover', value: isFinite(s.daysCover) ? `${Math.round(s.daysCover)}d` : '—' },
        ]
      }
    } else if (preset === 'decelerating' && ratioVal != null) {
      conditional = [
        { label: '30d/day', value: it.avgDailySales30d.toFixed(2) },
        { label: '90d/day', value: it.avgDailySales90d.toFixed(2) },
      ]
    }

    const presetHasConditional = preset === 'discount' || preset === 'reorder' || preset === 'decelerating'

    return (
      <div className={`border-b border-white/60 ${TIER_BG[it.velocityTier]} hover:brightness-95 transition`}>
        <div className="grid grid-cols-[28px_minmax(200px,2.4fr)_minmax(80px,0.7fr)_minmax(40px,0.4fr)_minmax(70px,0.5fr)_minmax(60px,0.45fr)_minmax(110px,0.85fr)_minmax(60px,0.5fr)_minmax(75px,0.6fr)_minmax(70px,0.55fr)_minmax(70px,0.55fr)_minmax(90px,0.7fr)_minmax(150px,1fr)] gap-2 items-center px-3 py-1.5 text-sm">
          <button
            onClick={() => toggleSetItem(expanded, it.offerId, setExpanded)}
            className="text-slate-400 hover:text-slate-700 text-xs"
            aria-label={isExp ? 'Collapse' : 'Expand'}
          >
            {isExp ? '▾' : '▸'}
          </button>
          <div className="min-w-0">
            <div className="truncate text-slate-700 font-medium" title={it.name ?? it.sku}>
              {it.name ?? it.sku}
            </div>
            <div className="text-[10px] text-slate-500 flex gap-1.5 items-center">
              <span className="font-mono">{it.sku}</span>
              <span>·</span>
              <span>{it.categoryName ?? '—'}</span>
              {it.costQuality === 'fallback' && (
                <span className="text-amber-600" title="Закупочная цена не задана — внутренние расчёты используют оценку по портфельному ratio">~est</span>
              )}
            </div>
          </div>
          <button
            onClick={() => toggleSetItem(brandFilterInternal, it.brand ?? '', setBrandFilterInternal)}
            className="text-left text-slate-600 truncate text-xs hover:text-blue-600"
            title={`Filter to brand: ${it.brand ?? '—'}`}
          >
            {it.brand ?? '—'}
          </button>
          <span className="font-mono text-xs text-slate-600 text-center">{it.abcClass}</span>
          <span className={`text-xs font-medium ${TIER_TEXT[it.velocityTier]} text-center`}>
            {it.velocityTier}
          </span>
          <span className="text-right tabular-nums text-slate-700">{formatNumber(it.units)}</span>
          {(() => {
            const excessUnits = Math.max(0, it.units - it.avgDailySales90d * 60)
            const excessRetail = excessUnits * it.price
            return (
              <div className="text-right tabular-nums">
                <div className="font-medium text-slate-800" title="Retail value = units × sale price">
                  {formatCurrency(it.saleValue)}
                </div>
                {excessRetail > 0 && (
                  <div className="text-[10px] text-red-600" title="Стоимость остатков сверх 60-дневной нормы (по retail)">
                    excess {formatCurrency(excessRetail)}
                  </div>
                )}
              </div>
            )
          })()}
          <span className="text-right tabular-nums text-slate-600">
            {it.daysOfSupply != null ? `${it.daysOfSupply}d` : '—'}
          </span>
          <span
            className={`text-right tabular-nums ${
              it.daysSinceSale == null ? 'text-slate-400' :
              it.daysSinceSale <= 30 ? 'text-emerald-700' :
              it.daysSinceSale <= 90 ? 'text-amber-600' :
              'text-red-600 font-medium'
            }`}
            title={it.daysSinceSale != null ? `Last sale ${it.daysSinceSale} days ago` : 'Never sold'}
          >
            {it.daysSinceSale != null ? `${it.daysSinceSale}d` : 'never'}
          </span>
          <span className={`text-right font-medium tabular-nums ${gmroiColor}`}>
            {gmroiPct != null ? `${gmroiPct}%` : '—'}
          </span>
          <span className={`text-right tabular-nums font-medium ${ratioColor}`} title="30d daily rate / 90d daily rate. <0.7 = decay">
            {ratioVal != null ? `${ratioArrow} ${ratioVal.toFixed(2)}` : '—'}
          </span>
          <span className="text-right">
            <span className={`text-[11px] font-semibold px-2 py-0.5 rounded ${DECISION_BADGE[it.decision]}`}>
              {it.decision}
            </span>
          </span>
          <span className="text-right text-xs text-slate-600 tabular-nums">
            {presetHasConditional && conditional.length > 0 ? (
              <span className="space-x-2">
                {conditional.map((c) => (
                  <span key={c.label} title={c.tooltip}>
                    <span className="text-[10px] text-slate-400">{c.label}:</span>{' '}
                    <span className="font-medium">{c.value}</span>
                  </span>
                ))}
              </span>
            ) : ''}
          </span>
        </div>
        {isExp && (
          <div className="px-12 py-2 bg-white/50 border-t border-slate-100 grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            <div>
              <div className="text-slate-400 text-[10px] uppercase tracking-wide">Sales</div>
              <div className="text-slate-700 mt-0.5">
                30d: <strong>{formatNumber(it.qtySold30d)}</strong> units / {formatCurrency(it.revenue30d)}
              </div>
              <div className="text-slate-700">
                90d: <strong>{formatNumber(it.qtySold90d)}</strong> units / {formatCurrency(it.revenue90d)}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-[10px] uppercase tracking-wide">Aging</div>
              <div className="text-slate-700 mt-0.5">In stock: {it.daysInStock ?? '—'}d</div>
              <div className="text-slate-700">Last sale: {it.daysSinceSale != null ? `${it.daysSinceSale}d ago` : 'never'}</div>
            </div>
            <div>
              <div className="text-slate-400 text-[10px] uppercase tracking-wide">Pricing</div>
              <div className="text-slate-700 mt-0.5">
                Price: <strong>{formatCurrency(it.price)}</strong>
              </div>
              <div className="text-slate-700">
                Margin: <strong>
                  {it.price > 0
                    ? `${Math.round((1 - it.effectiveUnitCost / it.price) * 100)}%`
                    : '—'}
                </strong>
                {it.costQuality === 'fallback' && (
                  <span className="text-amber-600 ml-1" title="оценочно">~est</span>
                )}
              </div>
            </div>
            <div>
              <div className="text-slate-400 text-[10px] uppercase tracking-wide">NPV</div>
              <div className="text-slate-700 mt-0.5">Hold: {formatCurrency(it.npvHold)}</div>
              <div className="text-slate-700">Liquidate: {formatCurrency(it.npvLiquidate)}</div>
            </div>
          </div>
        )}
      </div>
    )
  }

  return (
    <ChartContainer
      title="SKU rotation table"
      titleExtra={
        <InfoPopover title="SKU rotation table">
          <div className="space-y-2 text-xs text-slate-300 max-w-md">
            <p>Полная таблица 388 SKU с 4 пресетами под конкретные решения:</p>
            <p><strong className="text-red-300">🔴 Discount</strong> — frozen/cold + LIQUIDATE. Suggested % = max скидка с margin floor 5%.</p>
            <p><strong className="text-emerald-300">🛒 Reorder</strong> — hot/healthy + cover &lt; 30d. Suggested qty = avg_daily_30d × {TARGET_COVER_DAYS}d − units (boost +30% если accelerating).</p>
            <p><strong className="text-slate-300">🚫 Skip</strong> — frozen с excess &gt; ₴5k. Список «не закупать в следующий PO».</p>
            <p><strong className="text-amber-300">📉 Decelerating</strong> — 30d/90d ratio &lt; 0.7, ранний сигнал ухудшения.</p>
            <p className="text-[10px] text-slate-400 border-t border-slate-700 pt-2">
              Lead time {LEAD_TIME_DAYS}d + safety {SAFETY_DAYS}d = target cover {TARGET_COVER_DAYS}d. Cover = units / avg_daily_30d.
            </p>
          </div>
        </InfoPopover>
      }
      isLoading={isLoading}
      error={error}
      ariaLabel="SKU rotation table"
    >
      {skus && (
        <div ref={containerRef} className="space-y-3">
          {/* Presets bar */}
          <div className="flex flex-wrap gap-1.5">
            {PRESETS.map((p) => (
              <button
                key={p.key}
                onClick={() => onPresetChange(p.key)}
                className={`px-2.5 py-1 rounded-full text-xs font-medium transition border ${
                  preset === p.key
                    ? 'bg-blue-600 text-white border-blue-600'
                    : 'bg-white text-slate-700 border-slate-200 hover:bg-slate-50'
                }`}
                title={p.description}
              >
                <span className="mr-1">{p.emoji}</span>
                {p.label}
                <span className={`ml-1.5 text-[10px] ${preset === p.key ? 'text-blue-100' : 'text-slate-400'}`}>
                  ({presetCounters[p.key] ?? 0})
                </span>
              </button>
            ))}
          </div>

          {/* Search + multi-filter chips */}
          <div className="flex flex-wrap gap-2 items-center text-xs">
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search SKU or name…"
              className="px-2 py-1 border border-slate-200 rounded text-sm w-48 focus:outline-none focus:border-blue-400"
            />
            <FilterChipGroup
              label="ABC"
              options={ABC_OPTIONS}
              selected={abcFilter}
              onToggle={(v) => toggleSetItem(abcFilter, v, setAbcFilter)}
            />
            <FilterChipGroup
              label="Velocity"
              options={TIER_OPTIONS}
              selected={tierFilter}
              onToggle={(v) => toggleSetItem(tierFilter, v, setTierFilter)}
            />
            <FilterChipGroup
              label="Decision"
              options={DECISION_OPTIONS}
              selected={decisionFilter}
              onToggle={(v) => toggleSetItem(decisionFilter, v, setDecisionFilter)}
            />
            <DropdownMultiSelect
              label="Brand"
              options={brandOptions}
              selected={brandFilterInternal}
              onChange={setBrandFilterInternal}
            />
            <DropdownMultiSelect
              label="Category"
              options={categoryOptions}
              selected={categoryFilter}
              onChange={setCategoryFilter}
            />
            {hasAnyFilter && (
              <button
                onClick={clearAllFilters}
                className="text-xs text-slate-500 underline hover:text-slate-700"
              >
                Clear filters
              </button>
            )}
            <div className="ml-auto flex items-center gap-2">
              <span className="text-slate-500">{filtered.length} / {skus.length}</span>
              <button
                onClick={() => exportCsv(filtered, preset, `sku-${preset}-${new Date().toISOString().slice(0, 10)}.csv`)}
                disabled={filtered.length === 0}
                className="px-2 py-1 text-xs bg-emerald-600 text-white rounded hover:bg-emerald-700 disabled:bg-slate-300"
              >
                ⬇ CSV
              </button>
            </div>
          </div>

          {/* Table header */}
          <div className="border border-slate-200 rounded overflow-hidden">
            <div className="grid grid-cols-[28px_minmax(200px,2.4fr)_minmax(80px,0.7fr)_minmax(40px,0.4fr)_minmax(70px,0.5fr)_minmax(60px,0.45fr)_minmax(110px,0.85fr)_minmax(60px,0.5fr)_minmax(75px,0.6fr)_minmax(70px,0.55fr)_minmax(70px,0.55fr)_minmax(90px,0.7fr)_minmax(150px,1fr)] gap-2 px-3 py-2 bg-slate-50 text-[10px] uppercase tracking-wide text-slate-500 font-medium">
              <div></div>
              <SortHeader label="Name" sortKey="name" current={sortKey} dir={sortDir} onSort={onSort} />
              <SortHeader label="Brand" sortKey="brand" current={sortKey} dir={sortDir} onSort={onSort} />
              <SortHeader label="ABC" sortKey="abcClass" current={sortKey} dir={sortDir} onSort={onSort} align="center" />
              <SortHeader label="Velocity" sortKey="velocityTier" current={sortKey} dir={sortDir} onSort={onSort} align="center" />
              <SortHeader label="Units" sortKey="units" current={sortKey} dir={sortDir} onSort={onSort} align="right" />
              <SortHeader label="Retail ₴" sortKey="saleValue" current={sortKey} dir={sortDir} onSort={onSort} align="right" tooltip="Розничная стоимость остатка (units × sale price). Excess строкой ниже — что сверх 60-дневной нормы." />
              <SortHeader label="DOS" sortKey="daysOfSupply" current={sortKey} dir={sortDir} onSort={onSort} align="right" tooltip="Days of supply at 90d sales pace" />
              <SortHeader label="Last sale" sortKey="daysSinceSale" current={sortKey} dir={sortDir} onSort={onSort} align="right" tooltip="Days since last actual sale (0d = today, 'never' = no sales recorded)" />
              <SortHeader label="GMROI" sortKey="gmroi" current={sortKey} dir={sortDir} onSort={onSort} align="right" tooltip="Annualized gross profit / capital invested. <100% = SKU теряет деньги на хранении. Бенчмарк cosmetics 200-400%." />
              <SortHeader label="Δ30/90" sortKey="velocityRatio30to90" current={sortKey} dir={sortDir} onSort={onSort} align="right" tooltip="30d daily rate / 90d daily rate. <0.7 = deceleration" />
              <SortHeader label="Decision" sortKey="decision" current={sortKey} dir={sortDir} onSort={onSort} align="right" />
              <div className="text-right">{
                preset === 'discount' ? 'Discount / recovery' :
                preset === 'reorder' ? 'Order / cover' :
                preset === 'decelerating' ? '30d/90d rate' :
                ''
              }</div>
            </div>

            {filtered.length === 0 ? (
              <div className="text-center text-slate-500 py-12 text-sm">
                Нет SKU под текущие фильтры
              </div>
            ) : (
              <VirtualList
                items={filtered}
                estimateSize={56}
                height={520}
                overscan={8}
                renderItem={(it) => renderRow(it)}
              />
            )}
          </div>

          {/* Action footer */}
          <ActionFooter aggregates={aggregates} />
        </div>
      )}
    </ChartContainer>
  )
}

// ─── Sort header ─────────────────────────────────────────────────────────────

interface SortHeaderProps {
  label: string
  sortKey: SortKey
  current: SortKey
  dir: SortDir
  onSort: (k: SortKey) => void
  align?: 'left' | 'right' | 'center'
  tooltip?: string
}

function SortHeader({ label, sortKey, current, dir, onSort, align = 'left', tooltip }: SortHeaderProps) {
  const isActive = current === sortKey
  return (
    <button
      onClick={() => onSort(sortKey)}
      title={tooltip}
      className={`text-${align} hover:text-blue-600 ${isActive ? 'text-blue-700' : ''}`}
    >
      {label} {isActive && (dir === 'asc' ? '↑' : '↓')}
    </button>
  )
}

// ─── Filter chip group ───────────────────────────────────────────────────────

interface FilterChipGroupProps<T extends string> {
  label: string
  options: T[]
  selected: Set<T>
  onToggle: (v: T) => void
}

function FilterChipGroup<T extends string>({ label, options, selected, onToggle }: FilterChipGroupProps<T>) {
  return (
    <div className="flex items-center gap-1">
      <span className="text-slate-500">{label}:</span>
      {options.map((opt) => {
        const active = selected.has(opt)
        return (
          <button
            key={opt}
            onClick={() => onToggle(opt)}
            className={`px-1.5 py-0.5 text-[11px] rounded border ${
              active
                ? 'bg-blue-100 text-blue-700 border-blue-300'
                : 'bg-white text-slate-600 border-slate-200 hover:bg-slate-50'
            }`}
          >
            {opt}
          </button>
        )
      })}
    </div>
  )
}

// ─── Dropdown multi-select ───────────────────────────────────────────────────

interface DropdownMultiSelectProps {
  label: string
  options: string[]
  selected: Set<string>
  onChange: (s: Set<string>) => void
}

function DropdownMultiSelect({ label, options, selected, onChange }: DropdownMultiSelectProps) {
  const [open, setOpen] = useState(false)
  const [filter, setFilter] = useState('')
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const onClick = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('click', onClick)
    return () => document.removeEventListener('click', onClick)
  }, [])

  const toggle = (opt: string) => {
    const next = new Set(selected)
    if (next.has(opt)) next.delete(opt)
    else next.add(opt)
    onChange(next)
  }

  const filteredOptions = filter
    ? options.filter((o) => o.toLowerCase().includes(filter.toLowerCase()))
    : options

  return (
    <div ref={ref} className="relative">
      <button
        onClick={() => setOpen(!open)}
        className={`px-2 py-1 text-[11px] rounded border flex items-center gap-1 ${
          selected.size > 0
            ? 'bg-blue-100 text-blue-700 border-blue-300'
            : 'bg-white text-slate-600 border-slate-200'
        }`}
      >
        {label} {selected.size > 0 && `(${selected.size})`} ▾
      </button>
      {open && (
        <div className="absolute z-10 mt-1 w-56 max-h-60 overflow-auto bg-white border border-slate-200 rounded shadow-lg">
          <input
            type="text"
            placeholder={`Filter ${label.toLowerCase()}…`}
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            className="w-full px-2 py-1 border-b border-slate-100 text-xs focus:outline-none"
            onClick={(e) => e.stopPropagation()}
          />
          {filteredOptions.length === 0 && (
            <div className="px-2 py-1 text-xs text-slate-400">No matches</div>
          )}
          {filteredOptions.map((opt) => (
            <label
              key={opt}
              className="flex items-center gap-2 px-2 py-1 text-xs hover:bg-slate-50 cursor-pointer"
            >
              <input
                type="checkbox"
                checked={selected.has(opt)}
                onChange={() => toggle(opt)}
              />
              <span className="truncate">{opt}</span>
            </label>
          ))}
          {selected.size > 0 && (
            <button
              onClick={() => onChange(new Set())}
              className="w-full px-2 py-1 text-[11px] text-slate-500 hover:bg-slate-50 border-t border-slate-100"
            >
              Clear
            </button>
          )}
        </div>
      )}
    </div>
  )
}

// ─── Action footer ───────────────────────────────────────────────────────────

interface ActionFooterProps {
  aggregates: ReturnType<typeof buildAggregates>
}

// Helper for aggregates type narrowing
function buildAggregates() {
  return {} as
    | { kind: 'all'; count: number; totalRetail: number; totalRev90: number; totalUnits: number }
    | { kind: 'discount'; count: number; totalRetail: number; totalExcessRetail: number; recovery: number; avgPct: number; carrySaved: number }
    | { kind: 'reorder'; count: number; qty: number; retail: number; projectedRev30d: number }
    | { kind: 'skip'; count: number; totalRetail: number; totalExcessRetail: number; carrySaved: number }
    | { kind: 'decelerating'; count: number; atRisk: number; totalUnits: number }
}

function ActionFooter({ aggregates }: ActionFooterProps) {
  if (aggregates.count === 0) return null

  const wrap = (children: React.ReactNode, accent: string) => (
    <div className={`p-3 rounded border-l-4 ${accent} bg-slate-50 text-sm`}>
      {children}
    </div>
  )

  if (aggregates.kind === 'discount') {
    return wrap(
      <div className="flex flex-wrap gap-x-4 gap-y-1 items-center">
        <span><strong>{aggregates.count}</strong> SKU</span>
        <span>· retail <strong className="text-red-700">{formatCurrency(aggregates.totalRetail)}</strong></span>
        <span>· avg discount <strong>{Math.round(aggregates.avgPct * 100)}%</strong></span>
        <span>· recovery <strong className="text-emerald-700">{formatCurrency(aggregates.recovery)}</strong></span>
        <span>· экономит carrying <strong className="text-emerald-700">{formatCurrency(aggregates.carrySaved)}/yr</strong></span>
      </div>,
      'border-red-400'
    )
  }
  if (aggregates.kind === 'reorder') {
    return wrap(
      <div className="flex flex-wrap gap-x-4 gap-y-1 items-center">
        <span><strong>{aggregates.count}</strong> SKU нужно дозаказать</span>
        <span>· total qty <strong>{formatNumber(aggregates.qty)}</strong> units</span>
        <span>· retail <strong>{formatCurrency(aggregates.retail)}</strong></span>
        <span>· projected revenue 30d <strong className="text-emerald-700">{formatCurrency(aggregates.projectedRev30d)}</strong></span>
      </div>,
      'border-emerald-400'
    )
  }
  if (aggregates.kind === 'skip') {
    return wrap(
      <div className="flex flex-wrap gap-x-4 gap-y-1 items-center">
        <span><strong>{aggregates.count}</strong> SKU не закупать</span>
        <span>· retail <strong className="text-red-700">{formatCurrency(aggregates.totalRetail)}</strong></span>
        <span>· excess <strong>{formatCurrency(aggregates.totalExcessRetail)}</strong></span>
        <span>· экономит <strong className="text-emerald-700">{formatCurrency(aggregates.carrySaved)}/yr</strong> carrying</span>
      </div>,
      'border-slate-400'
    )
  }
  if (aggregates.kind === 'decelerating') {
    return wrap(
      <div className="flex flex-wrap gap-x-4 gap-y-1 items-center">
        <span><strong>{aggregates.count}</strong> SKU замедляются (30d/90d &lt; 0.7)</span>
        <span>· под риском <strong className="text-amber-700">{formatCurrency(aggregates.atRisk)}</strong> retail</span>
        <span>· {formatNumber(aggregates.totalUnits)} units на складе</span>
      </div>,
      'border-amber-400'
    )
  }
  return wrap(
    <div className="flex flex-wrap gap-x-4 gap-y-1 items-center">
      <span><strong>{aggregates.count}</strong> SKU</span>
      <span>· total retail <strong>{formatCurrency(aggregates.totalRetail)}</strong></span>
      <span>· 90d revenue <strong>{formatCurrency(aggregates.totalRev90)}</strong></span>
      <span>· {formatNumber(aggregates.totalUnits)} units</span>
    </div>,
    'border-slate-300'
  )
}

export const SkuRotationTable = memo(SkuRotationTableComponent)
