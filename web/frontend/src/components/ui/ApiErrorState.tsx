import { memo, useCallback, useEffect, useState } from 'react'
import { ApiError, NetworkError, TimeoutError } from '../../api/client'

// ─── Types ───────────────────────────────────────────────────────────────────

interface ApiErrorStateProps {
  error: Error | null
  onRetry?: () => void
  title?: string
}

interface ErrorInfo {
  title: string
  message: string
  icon: 'warning' | 'offline' | 'timeout' | 'error'
  canAutoRetry: boolean
}

// ─── Error Classification ────────────────────────────────────────────────────

function getErrorInfo(error: Error | null, defaultTitle: string): ErrorInfo {
  if (!error) {
    return {
      title: defaultTitle,
      message: 'An unexpected error occurred',
      icon: 'error',
      canAutoRetry: false,
    }
  }

  // Network errors (offline, connection failed)
  if (error instanceof NetworkError) {
    if (error.code === 'OFFLINE') {
      return {
        title: 'No internet connection',
        message: 'Check your network and try again',
        icon: 'offline',
        canAutoRetry: true,
      }
    }
    return {
      title: 'Connection failed',
      message: 'Server may be restarting. Retrying...',
      icon: 'warning',
      canAutoRetry: true,
    }
  }

  // Timeout errors
  if (error instanceof TimeoutError) {
    return {
      title: 'Request timed out',
      message: 'Server is taking too long. Please try again.',
      icon: 'timeout',
      canAutoRetry: true,
    }
  }

  // API errors (HTTP status codes)
  if (error instanceof ApiError) {
    const status = error.status

    if (status === 502 || status === 503 || status === 504) {
      return {
        title: 'Server is warming up',
        message: 'This usually takes a few seconds...',
        icon: 'warning',
        canAutoRetry: true,
      }
    }

    if (status === 429) {
      return {
        title: 'Too many requests',
        message: 'Please wait a moment before trying again',
        icon: 'warning',
        canAutoRetry: true,
      }
    }

    if (status >= 500) {
      return {
        title: 'Server error',
        message: 'Something went wrong. Please try again.',
        icon: 'error',
        canAutoRetry: true,
      }
    }

    return {
      title: defaultTitle,
      message: error.message,
      icon: 'error',
      canAutoRetry: false,
    }
  }

  // Generic errors
  return {
    title: defaultTitle,
    message: error.message || 'An unexpected error occurred',
    icon: 'error',
    canAutoRetry: false,
  }
}

// ─── Icons ───────────────────────────────────────────────────────────────────

const icons = {
  warning: (
    <svg className="w-6 h-6 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  ),
  offline: (
    <svg className="w-6 h-6 text-slate-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M18.364 5.636a9 9 0 010 12.728m0 0l-2.829-2.829m2.829 2.829L21 21M15.536 8.464a5 5 0 010 7.072m0 0l-2.829-2.829m-4.243 2.829a4.978 4.978 0 01-1.414-2.83m-1.414 5.658a9 9 0 01-2.167-9.238m7.824 2.167a1 1 0 111.414 1.414m-1.414-1.414L3 3" />
    </svg>
  ),
  timeout: (
    <svg className="w-6 h-6 text-amber-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  ),
  error: (
    <svg className="w-6 h-6 text-red-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
    </svg>
  ),
}

// ─── Component ───────────────────────────────────────────────────────────────

export const ApiErrorState = memo(function ApiErrorState({
  error,
  onRetry,
  title = 'Failed to load data',
}: ApiErrorStateProps) {
  const [retryCount, setRetryCount] = useState(0)
  const [autoRetrying, setAutoRetrying] = useState(false)
  const errorInfo = getErrorInfo(error, title)

  // Auto-retry for recoverable errors (max 3 times)
  useEffect(() => {
    if (!errorInfo.canAutoRetry || !onRetry || retryCount >= 3) {
      setAutoRetrying(false)
      return
    }

    setAutoRetrying(true)
    const delay = Math.min(2000 * Math.pow(2, retryCount), 10000) // 2s, 4s, 8s, max 10s
    const timer = setTimeout(() => {
      setRetryCount((c) => c + 1)
      onRetry()
    }, delay)

    return () => clearTimeout(timer)
  }, [errorInfo.canAutoRetry, onRetry, retryCount])

  // Reset retry count when error changes
  useEffect(() => {
    setRetryCount(0)
  }, [error])

  const handleManualRetry = useCallback(() => {
    setRetryCount(0)
    onRetry?.()
  }, [onRetry])

  return (
    <div
      role="alert"
      className="bg-slate-50 border border-slate-200 rounded-xl p-6 flex flex-col items-center text-center"
    >
      <div className="mb-3">{icons[errorInfo.icon]}</div>
      <h3 className="text-slate-900 font-medium mb-1">{errorInfo.title}</h3>
      <p className="text-slate-500 text-sm mb-4">{errorInfo.message}</p>

      {autoRetrying && retryCount < 3 && (
        <div className="flex items-center gap-2 text-sm text-slate-500 mb-3">
          <div className="w-4 h-4 border-2 border-slate-300 border-t-blue-500 rounded-full animate-spin" />
          <span>Retrying automatically...</span>
        </div>
      )}

      {onRetry && !autoRetrying && (
        <button
          onClick={handleManualRetry}
          className="px-4 py-2 bg-blue-600 text-white text-sm font-medium rounded-lg hover:bg-blue-700 transition-colors"
        >
          Try Again
        </button>
      )}
    </div>
  )
})
