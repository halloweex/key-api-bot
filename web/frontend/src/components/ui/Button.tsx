import { type ButtonHTMLAttributes, forwardRef } from 'react'

type ButtonVariant = 'primary' | 'secondary' | 'ghost'
type ButtonSize = 'sm' | 'md' | 'lg'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  active?: boolean
}

const variantStyles: Record<ButtonVariant, string> = {
  primary: 'bg-blue-600 text-white hover:bg-blue-700 active:bg-blue-800',
  secondary: 'bg-slate-100 text-slate-700 hover:bg-slate-200 active:bg-slate-300 border border-slate-300',
  ghost: 'bg-transparent text-slate-600 hover:bg-slate-200 hover:text-slate-900',
}

const sizeStyles: Record<ButtonSize, string> = {
  sm: 'px-2.5 py-1.5 text-xs',
  md: 'px-3 py-2 text-sm',
  lg: 'px-4 py-2.5 text-base',
}

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({ variant = 'secondary', size = 'md', active, className = '', children, ...props }, ref) => {
    const activeStyles = active ? 'ring-2 ring-blue-500 bg-blue-50 text-blue-700 border-blue-300' : ''

    return (
      <button
        ref={ref}
        className={`
          inline-flex items-center justify-center
          font-medium rounded-lg
          transition-colors duration-150
          focus:outline-none focus:ring-2 focus:ring-blue-500/50
          disabled:opacity-50 disabled:cursor-not-allowed
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
