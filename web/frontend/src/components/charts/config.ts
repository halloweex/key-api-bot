/**
 * Centralized chart configuration for consistent styling across all charts.
 * Single source of truth for colors, dimensions, and Recharts props.
 */

import type { CSSProperties } from 'react'

// ─── Theme Colors ────────────────────────────────────────────────────────────

export const CHART_THEME = {
  // Base colors (light theme)
  grid: '#E2E8F0',
  axis: '#64748B',
  label: '#64748B',
  background: '#FFFFFF',
  border: '#E2E8F0',
  text: '#1E293B',
  muted: '#94A3B8',

  // Semantic colors
  primary: '#2563EB',
  accent: '#7C3AED',
  success: '#16A34A',
  warning: '#F59E0B',
  danger: '#EF4444',
  info: '#06B6D4',
} as const

// ─── Dimensions ──────────────────────────────────────────────────────────────

export const CHART_DIMENSIONS = {
  /** Standard chart heights */
  height: {
    sm: 192,
    md: 256,
    lg: 288,
    xl: 320,
    xxl: 420,  // For Top 10 product charts (10 bars × ~42px each)
  },
  /** Standard margins */
  margin: {
    default: { top: 5, right: 20, left: 10, bottom: 5 },
    withRightLabel: { top: 5, right: 60, left: 10, bottom: 5 },
    vertical: { top: 5, right: 20, left: 10, bottom: 5 },
  },
  /** Font sizes */
  fontSize: {
    xs: 10,
    sm: 11,
    md: 12,
    lg: 14,
  },
  /** Y-axis widths */
  yAxisWidth: {
    sm: 60,
    md: 80,
    lg: 120,
    xl: 150,
  },
} as const

// ─── Recharts Props ──────────────────────────────────────────────────────────

/** Standard tooltip styling */
export const TOOLTIP_STYLE: CSSProperties = {
  backgroundColor: CHART_THEME.background,
  border: `1px solid ${CHART_THEME.border}`,
  borderRadius: '8px',
}

/** Standard tooltip label styling */
export const TOOLTIP_LABEL_STYLE: CSSProperties = {
  color: CHART_THEME.text,
}

/** Standard cartesian grid props */
export const GRID_PROPS = {
  strokeDasharray: '3 3',
  stroke: CHART_THEME.grid,
} as const

/** Standard X-axis props */
export const X_AXIS_PROPS = {
  stroke: CHART_THEME.axis,
  fontSize: CHART_DIMENSIONS.fontSize.md,
  tickLine: false,
} as const

/** Standard Y-axis props */
export const Y_AXIS_PROPS = {
  stroke: CHART_THEME.axis,
  fontSize: CHART_DIMENSIONS.fontSize.md,
  tickLine: false,
} as const

/** Standard legend props */
export const LEGEND_PROPS = {
  wrapperStyle: { color: CHART_THEME.axis },
} as const

/** Standard line props */
export const LINE_PROPS = {
  strokeWidth: 2,
  dot: false,
  activeDot: { r: 4 },
} as const

/** Standard bar props */
export const BAR_PROPS = {
  radius: [0, 4, 4, 0] as [number, number, number, number],
} as const

/** Standard pie props */
export const PIE_PROPS = {
  labelLine: false,
} as const

// ─── Formatters ──────────────────────────────────────────────────────────────

/** Format large numbers as "Xk" */
export const formatAxisK = (value: number): string => {
  if (value >= 1000) {
    return `${(value / 1000).toFixed(0)}k`
  }
  return String(value)
}

/** Truncate long text with ellipsis */
export const truncateText = (text: string, maxLength = 25): string => {
  if (!text) return 'Unknown'
  if (text.length <= maxLength) return text
  return `${text.slice(0, maxLength - 3)}...`
}

/** Wrap text into multiple lines for chart labels */
export const wrapText = (text: string, maxCharsPerLine = 20): string[] => {
  if (!text) return ['Unknown']

  const words = text.split(' ')
  const lines: string[] = []
  let currentLine = ''

  for (const word of words) {
    if (currentLine.length + word.length + 1 <= maxCharsPerLine) {
      currentLine = currentLine ? `${currentLine} ${word}` : word
    } else {
      if (currentLine) lines.push(currentLine)
      currentLine = word.length > maxCharsPerLine
        ? word.slice(0, maxCharsPerLine - 3) + '...'
        : word
    }
  }

  if (currentLine) lines.push(currentLine)

  // Limit to 2 lines max
  if (lines.length > 2) {
    return [lines[0], lines[1].slice(0, maxCharsPerLine - 3) + '...']
  }

  return lines
}

/** Format percentage for pie chart labels */
export const formatPieLabel = (percent: number, threshold = 0.05): string => {
  if (percent < threshold) return ''
  return `${(percent * 100).toFixed(0)}%`
}

// ─── Data Helpers ────────────────────────────────────────────────────────────

/**
 * Safely transforms parallel arrays into chart-ready data.
 * Handles undefined/null values with defaults.
 */
export function transformChartData<T extends Record<string, unknown>>(
  labels: string[] | undefined,
  dataMap: { [K in keyof T]: (T[K] | undefined)[] | undefined },
  defaults: T
): (T & { label: string })[] {
  if (!labels?.length) return []

  return labels.map((label, index) => {
    const result = { label } as T & { label: string }

    for (const key in dataMap) {
      const arr = dataMap[key]
      const defaultValue = defaults[key]
      // @ts-expect-error - Dynamic key assignment
      result[key] = arr?.[index] ?? defaultValue
    }

    return result
  })
}

// ─── Type Exports ────────────────────────────────────────────────────────────

export type ChartHeight = keyof typeof CHART_DIMENSIONS.height
export type ChartMargin = keyof typeof CHART_DIMENSIONS.margin
