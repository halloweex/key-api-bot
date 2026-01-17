import { type ButtonHTMLAttributes, forwardRef } from 'react'

type ButtonVariant = 'primary' | 'secondary' | 'ghost'
type ButtonSize = 'sm' | 'md' | 'lg'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  active?: boolean
}

const variantStyles: Record<ButtonVariant, string> = {
  primary: 'bg-gradient-to-r from-blue-600 to-blue-500 text-white hover:from-blue-700 hover:to-blue-600 active:from-blue-800 active:to-blue-700 shadow-sm hover:shadow',
  secondary: 'bg-white text-slate-700 hover:bg-slate-50 active:bg-slate-100 border border-slate-200 shadow-sm',
  ghost: 'bg-transparent text-slate-600 hover:bg-slate-100 hover:text-slate-900',
}

const sizeStyles: Record<ButtonSize, string> = {
  sm: 'px-3 py-1.5 text-xs',
  md: 'px-4 py-2 text-sm',
  lg: 'px-5 py-2.5 text-base',
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ variant = 'secondary', size = 'md', active, className = '', children, ...props }, ref) => {
    const activeStyles = active
      ? 'ring-2 ring-blue-500/30 bg-blue-50 text-blue-700 border-blue-200 shadow-sm'
      : ''

    return (
      <button
        ref={ref}
        className={`
          inline-flex items-center justify-center
          font-medium rounded-lg
          transition-all duration-200
          focus:outline-none focus:ring-2 focus:ring-blue-500/40 focus:ring-offset-1
          disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none
          ${variantStyles[variant]}
          ${sizeStyles[size]}
          ${activeStyles}
          ${className}
        `}
        {...props}
      >
        {children}
      </button>
    )
  }
)

Button.displayName = 'Button'
