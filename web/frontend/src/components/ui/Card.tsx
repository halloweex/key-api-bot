import { type ReactNode, forwardRef } from 'react'

interface CardProps {
  children: ReactNode
  className?: string
  /** Enable hover shadow elevation effect */
  interactive?: boolean
  /** Apply elevated shadow by default */
  elevated?: boolean
  /** Add subtle glow effect on hover */
  glow?: boolean
  /** Enable glass morphism effect */
  glass?: boolean
  onClick?: () => void
}

export const Card = forwardRef<HTMLDivElement, CardProps>(
  ({ children, className = '', interactive = false, elevated = false, glow = false, glass = false, onClick }, ref) => {
    return (
      <div
        ref={ref}
        onClick={onClick}
        className={`
          rounded-xl border
          ${glass ? 'glass border-white/30' : 'bg-white border-slate-200/60'}
          ${elevated ? 'shadow-[var(--shadow-md)]' : 'shadow-[var(--shadow-card)]'}
          ${interactive ? 'card-hover hover:border-slate-300/80 cursor-pointer ripple' : ''}
          ${glow ? 'hover:shadow-[0_0_20px_rgba(99,102,241,0.15)] hover:border-indigo-200/60' : ''}
          transition-all duration-200
          ${className}
        `}
      >
        {children}
      </div>
    )
  }
)

Card.displayName = 'Card'

interface CardHeaderProps {
  children: ReactNode
  className?: string
}

export function CardHeader({ children, className = '' }: CardHeaderProps) {
  return (
    <div className={`px-3 sm:px-5 py-3 sm:py-4 border-b border-slate-100 ${className}`}>
      {children}
    </div>
  )
}

interface CardTitleProps {
  children: ReactNode
  className?: string
}

export function CardTitle({ children, className = '' }: CardTitleProps) {
  return (
    <h3 className={`text-base font-semibold text-slate-800 tracking-tight ${className}`}>
      {children}
    </h3>
  )
}

interface CardContentProps {
  children: ReactNode
  className?: string
}

export function CardContent({ children, className = '' }: CardContentProps) {
  return (
    <div className={`p-3 sm:p-5 ${className}`}>
      {children}
    </div>
  )
}
