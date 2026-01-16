import { Component, type ReactNode } from 'react'

// ─── Types ───────────────────────────────────────────────────────────────────

interface Props {
  children: ReactNode
  fallback?: ReactNode
  onReset?: () => void
  onError?: (error: Error, errorInfo: React.ErrorInfo) => void
}

interface State {
  hasError: boolean
  error: Error | null
}

// ─── Error Boundary Component ────────────────────────────────────────────────

export class ErrorBoundary extends Component<Props, State> {
  constructor(props: Props) {
    super(props)
    this.state = { hasError: false, error: null }
  }

  static getDerivedStateFromError(error: Error): State {
    return { hasError: true, error }
  }

  componentDidCatch(error: Error, errorInfo: React.ErrorInfo): void {
    // Log to console in development
    if (import.meta.env.DEV) {
      console.error('[ErrorBoundary]', error)
      console.error('[ErrorBoundary] Component Stack:', errorInfo.componentStack)
    }

    // Call optional error handler (e.g., for error reporting service)
    this.props.onError?.(error, errorInfo)
  }

  handleReset = (): void => {
    this.setState({ hasError: false, error: null })
    this.props.onReset?.()
  }

  handleRefresh = (): void => {
    window.location.reload()
  }

  render(): ReactNode {
    if (this.state.hasError) {
      // Use custom fallback if provided
      if (this.props.fallback) {
        return this.props.fallback
      }

      // Default error UI
      return (
        <div
          role="alert"
          className="min-h-screen bg-slate-900 flex items-center justify-center p-6"
        >
          <div className="bg-slate-800 border border-slate-700 rounded-lg p-8 max-w-lg w-full shadow-xl">
            {/* Error Icon */}
            <div className="flex justify-center mb-4">
              <div className="w-16 h-16 bg-red-900/30 rounded-full flex items-center justify-center">
                <svg
                  className="w-8 h-8 text-red-400"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                  aria-hidden="true"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                  />
                </svg>
              </div>
            </div>

            {/* Error Message */}
            <h1 className="text-xl font-bold text-white text-center mb-2">
              Something went wrong
            </h1>
            <p className="text-slate-400 text-center mb-6">
              The dashboard encountered an unexpected error.
            </p>

            {/* Error Details (development only) */}
            {import.meta.env.DEV && this.state.error && (
              <details className="mb-6">
                <summary className="text-sm text-slate-500 cursor-pointer hover:text-slate-400">
                  Error details
                </summary>
                <pre className="mt-2 bg-slate-900 p-3 rounded text-xs text-red-400 overflow-auto max-h-40">
                  {this.state.error.message}
                  {this.state.error.stack && (
                    <>
                      {'\n\n'}
                      {this.state.error.stack}
                    </>
                  )}
                </pre>
              </details>
            )}

            {/* Action Buttons */}
            <div className="flex gap-3 justify-center">
              <button
                onClick={this.handleReset}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors font-medium"
              >
                Try Again
              </button>
              <button
                onClick={this.handleRefresh}
                className="px-4 py-2 bg-slate-700 text-white rounded-lg hover:bg-slate-600 transition-colors font-medium"
              >
                Refresh Page
              </button>
            </div>

            {/* Help Text */}
            <p className="text-xs text-slate-500 text-center mt-6">
              If the problem persists, try clearing your browser cache or contact support.
            </p>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}

// ─── Hook for Resetting Error Boundary ───────────────────────────────────────

/**
 * Creates a key that can be used to reset an error boundary.
 * When the key changes, the error boundary resets.
 *
 * Usage:
 * ```tsx
 * const [resetKey, reset] = useErrorBoundaryReset()
 * <ErrorBoundary key={resetKey} onReset={reset}>
 *   <Component />
 * </ErrorBoundary>
 * ```
 */
import { useState, useCallback } from 'react'

export function useErrorBoundaryReset(): [number, () => void] {
  const [key, setKey] = useState(0)
  const reset = useCallback(() => setKey(k => k + 1), [])
  return [key, reset]
}
