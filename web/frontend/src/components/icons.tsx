import type { ComponentType } from 'react'

/**
 * Shared icon components — Lucide re-exports with backward-compatible aliases.
 *
 * IconComponent is the project-wide icon contract: components accept the icon
 * as a *component* (`icon={Users}`), never as a pre-styled element
 * (`icon={<Users className="w-4 h-4" />}`). Size and colour are cosmetics and
 * belong to the component that renders the icon — the call site only names
 * which glyph to use. Lucide icons satisfy this contract as-is.
 */
export type IconComponent = ComponentType<{ className?: string; 'aria-hidden'?: boolean }>

export {
  UserPlus,
  Users,
  ShoppingCart,
  ShoppingBag,
  CircleDollarSign,
  Calculator,
  RefreshCw,
  Undo2,
  Heart,
  Calendar,
  Tag,
  Sparkles,
  PieChart,
  Info,
  Trash2,
} from 'lucide-react'

// Backward aliases (match old component names)
export { UserPlus as UserPlusIcon } from 'lucide-react'
export { Users as UserGroupIcon } from 'lucide-react'
export { ShoppingCart as ShoppingCartIcon } from 'lucide-react'
export { ShoppingBag as ShoppingBagIcon } from 'lucide-react'
export { CircleDollarSign as CurrencyIcon } from 'lucide-react'
export { Calculator as CalculatorIcon } from 'lucide-react'
export { RefreshCw as RefreshIcon } from 'lucide-react'
export { Undo2 as ArrowUturnLeftIcon } from 'lucide-react'
export { Heart as HeartIcon } from 'lucide-react'
export { Calendar as CalendarIcon } from 'lucide-react'
export { Tag as TagIcon } from 'lucide-react'
export { Sparkles as TrophyIcon } from 'lucide-react'
export { PieChart as ChartPieIcon } from 'lucide-react'
export { Info as InfoIcon } from 'lucide-react'
export { Trash2 as TrashIcon } from 'lucide-react'
