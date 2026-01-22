const EXPENSES_KEY = 'koreanstory_custom_expenses'

export function getStoredExpenses(): number {
  const stored = localStorage.getItem(EXPENSES_KEY)
  return stored ? parseFloat(stored) : 0
}

export function setStoredExpenses(value: number): void {
  localStorage.setItem(EXPENSES_KEY, String(value))
}
