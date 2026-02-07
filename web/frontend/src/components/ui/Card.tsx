import { type ReactNode } from 'react'

interface CardProps {
  children: ReactNode
  className?: string
  /** Enable hover shadow elevation effect */
  interactive?: boolean
  /** Apply elevated shadow by default */
  elevated?: boolean
}

export function Card({ children, className = '', interactive = false, elevated = false }: CardProps) {
  return (
    <div
      className={`
        bg-white rounded-xl border border-slate-200/60
        ${elevated ? 'shadow-[var(--shadow-md)]' : 'shadow-[var(--shadow-card)]'}
        ${interactive ? 'hover:shadow-[var(--shadow-card-hover)] hover:border-slate-300/80 transition-all duration-200 cursor-pointer' : ''}
        ${className}
      `}
    >
      {children}
    </div>
  )
}

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
