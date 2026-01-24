const EXPENSES_KEY = 'koreanstory_custom_expenses'

export function getStoredExpenses(): number {
  try {
    const stored = localStorage.getItem(EXPENSES_KEY)
    return stored ? parseFloat(stored) : 0
  } catch {
    // localStorage may be unavailable in private browsing or disabled
    return 0
  }
}

export function setStoredExpenses(value: number): void {
  try {
    localStorage.setItem(EXPENSES_KEY, String(value))
  } catch {
    // Silently fail if localStorage is unavailable
  }
}
