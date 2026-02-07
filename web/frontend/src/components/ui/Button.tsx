import { type ButtonHTMLAttributes, forwardRef, type ReactNode } from 'react'

type ButtonVariant = 'primary' | 'secondary' | 'ghost' | 'danger'
type ButtonSize = 'sm' | 'md' | 'lg'

interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: ButtonVariant
  size?: ButtonSize
  active?: boolean
  loading?: boolean
  icon?: ReactNode
  iconPosition?: 'left' | 'right'
}

const variantStyles: Record<ButtonVariant, string> = {
  primary: 'bg-gradient-to-r from-blue-600 to-purple-600 text-white hover:from-blue-700 hover:to-purple-700 active:from-blue-800 active:to-purple-800 shadow-sm hover:shadow-md',
  secondary: 'bg-white text-slate-700 hover:bg-slate-50 active:bg-slate-100 border border-slate-200 shadow-sm hover:shadow',
  ghost: 'bg-transparent text-slate-600 hover:bg-slate-100 hover:text-slate-900',
  danger: 'bg-gradient-to-r from-red-500 to-rose-600 text-white hover:from-red-600 hover:to-rose-700 active:from-red-700 active:to-rose-800 shadow-sm hover:shadow-md',
}

const sizeStyles: Record<ButtonSize, string> = {
  sm: 'px-3 py-1.5 text-xs gap-1.5 min-h-[32px]',
  md: 'px-4 py-2 text-sm gap-2 min-h-[40px]',
  lg: 'px-5 py-2.5 text-base gap-2 min-h-[48px]',
}

const LoadingSpinner = () => (
  <svg
    className="animate-spin h-4 w-4"
    fill="none"
    viewBox="0 0 24 24"
  >
    <circle
      className="opacity-25"
      cx="12"
      cy="12"
      r="10"
      stroke="currentColor"
      strokeWidth="4"
    />
    <path
      className="opacity-75"
      fill="currentColor"
      d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
    />
  </svg>
)

export const Button = forwardRef<HTMLButtonElement, ButtonProps>(
  ({
    variant = 'secondary',
    size = 'md',
    active,
    loading = false,
    icon,
    iconPosition = 'left',
    className = '',
    children,
    disabled,
    ...props
  }, ref) => {
    const activeStyles = active
      ? 'ring-2 ring-purple-500/30 bg-purple-50 text-purple-700 border-purple-200 shadow-sm'
      : ''

    const isDisabled = disabled || loading

    return (
      <button
        ref={ref}
        aria-pressed={active !== undefined ? active : undefined}
        aria-busy={loading}
        disabled={isDisabled}
        className={`
          inline-flex items-center justify-center
          font-medium rounded-lg
          transition-all duration-200
          focus:outline-none focus:ring-2 focus:ring-purple-500/40 focus:ring-offset-1
          disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none
          animate-press touch-target
          ${variantStyles[variant]}
          ${sizeStyles[size]}
          ${activeStyles}
          ${className}
        `}
        {...props}
      >
        {loading ? (
          <>
            <LoadingSpinner />
            <span className="ml-2">{children}</span>
          </>
        ) : (
          <>
            {icon && iconPosition === 'left' && (
              <span className="flex-shrink-0">{icon}</span>
            )}
            {children}
            {icon && iconPosition === 'right' && (
              <span className="flex-shrink-0">{icon}</span>
            )}
          </>
        )}
      </button>
    )
  }
)

Button.displayName = 'Button'
