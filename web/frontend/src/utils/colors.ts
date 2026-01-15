// Chart colors matching the current dashboard
export const COLORS = {
  primary: '#2563EB',    // Blue - Telegram
  accent: '#7C3AED',     // Purple - Instagram
  success: '#16A34A',    // Green
  warning: '#F59E0B',    // Orange
  shopify: '#eb4200',    // Orange-red - Shopify
  pink: '#EC4899',
  violet: '#8B5CF6',
  cyan: '#06B6D4',
}

// Source-specific colors
export const SOURCE_COLORS: Record<number, string> = {
  1: COLORS.accent,     // Instagram - purple
  2: COLORS.primary,    // Telegram - blue
  4: COLORS.shopify,    // Shopify - orange-red
}

// Category chart color palette
export const CATEGORY_COLORS = [
  COLORS.accent,
  COLORS.primary,
  COLORS.success,
  COLORS.warning,
  COLORS.shopify,
  COLORS.pink,
  COLORS.violet,
  COLORS.cyan,
]

// Customer chart colors
export const CUSTOMER_COLORS = {
  new: COLORS.primary,
  returning: COLORS.accent,
}

// Expense chart colors
export const EXPENSE_COLORS = {
  revenue: COLORS.primary,
  expenses: COLORS.shopify,
  profit: COLORS.success,
}
