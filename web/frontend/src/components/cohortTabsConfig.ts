export type TabId = 'retention' | 'revenue' | 'timing' | 'ltv' | 'at-risk'

export interface Tab {
  id: TabId
  label: string
  shortLabel: string
}

export const TABS: Tab[] = [
  { id: 'retention', label: 'retention.customerRetention', shortLabel: 'retention.tabRetention' },
  { id: 'revenue', label: 'retention.revenueRetention', shortLabel: 'retention.tabRevenue' },
  { id: 'timing', label: 'retention.purchaseTiming', shortLabel: 'retention.tabTiming' },
  { id: 'ltv', label: 'retention.lifetimeValue', shortLabel: 'retention.tabLTV' },
  { id: 'at-risk', label: 'retention.atRiskCustomers', shortLabel: 'retention.tabAtRisk' },
]

export const PERIOD_OPTIONS = [
  { value: 6, label: '6' },
  { value: 12, label: '12' },
  { value: 18, label: '18' },
  { value: 24, label: '24' },
]

export const DEPTH_OPTIONS = [
  { value: 3, label: 'M0-M3' },
  { value: 6, label: 'M0-M6' },
  { value: 9, label: 'M0-M9' },
  { value: 12, label: 'M0-M12' },
]

export const THRESHOLD_OPTIONS = [
  { value: 30, label: '30' },
  { value: 60, label: '60' },
  { value: 90, label: '90' },
  { value: 180, label: '180' },
  { value: 365, label: '365' },
]
