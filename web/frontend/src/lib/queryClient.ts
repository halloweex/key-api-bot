/**
 * TanStack Query client configuration with optimized defaults.
 */

import { QueryClient, QueryCache, MutationCache } from '@tanstack/react-query'
import { ApiError, NetworkError, TimeoutError } from '../api/client'

// ─── Error Handler ───────────────────────────────────────────────────────────

function handleQueryError(error: unknown): void {
  // Log errors for debugging (could send to monitoring service)
  if (import.meta.env.DEV) {
    console.error('[Query Error]', error)
  }

  // Could integrate with toast notifications here
  // toast.error(getErrorMessage(error))
}

// ─── Retry Logic ─────────────────────────────────────────────────────────────

/**
 * Determines if a failed query should be retried.
 * - Don't retry client errors (4xx)
 * - Retry server errors (5xx) and network errors
 * - Max 2 retries with exponential backoff
 */
function shouldRetry(failureCount: number, error: unknown): boolean {
  // Max 2 retries
  if (failureCount >= 2) return false

  // Don't retry client errors
  if (error instanceof ApiError && error.status < 500) {
    return false
  }

  // Retry server errors, network errors, and timeouts
  if (
    error instanceof ApiError ||
    error instanceof NetworkError ||
    error instanceof TimeoutError
  ) {
    return true
  }

  return false
}

/**
 * Exponential backoff delay for retries.
 * 1st retry: 1s, 2nd retry: 2s
 */
function retryDelay(attemptIndex: number): number {
  return Math.min(1000 * 2 ** attemptIndex, 5000)
}

// ─── Query Client ────────────────────────────────────────────────────────────

export function createQueryClient(): QueryClient {
  return new QueryClient({
    queryCache: new QueryCache({
      onError: handleQueryError,
    }),
    mutationCache: new MutationCache({
      onError: handleQueryError,
    }),
    defaultOptions: {
      queries: {
        // Data considered fresh for 5 minutes
        staleTime: 5 * 60 * 1000,

        // Keep unused data in cache for 30 minutes
        gcTime: 30 * 60 * 1000,

        // Retry configuration
        retry: shouldRetry,
        retryDelay,

        // Don't refetch on window focus (data changes infrequently)
        refetchOnWindowFocus: false,

        // Don't refetch on reconnect automatically
        refetchOnReconnect: 'always',

        // Don't refetch on mount if data is fresh
        refetchOnMount: true,

        // Network mode: always attempt fetch even if offline
        // (will fail fast with NetworkError)
        networkMode: 'always',

        // Throw errors to error boundaries
        throwOnError: false,
      },
      mutations: {
        // Retry mutations once on network errors
        retry: 1,
        retryDelay: 1000,
        networkMode: 'always',
      },
    },
  })
}

// ─── Singleton Instance ──────────────────────────────────────────────────────

let queryClient: QueryClient | null = null

export function getQueryClient(): QueryClient {
  if (!queryClient) {
    queryClient = createQueryClient()
  }
  return queryClient
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * Extracts a user-friendly message from an error.
 */
export function getErrorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    return error.message
  }
  if (error instanceof NetworkError) {
    return error.message
  }
  if (error instanceof TimeoutError) {
    return 'Request timed out. Please try again.'
  }
  if (error instanceof Error) {
    return error.message
  }
  return 'An unexpected error occurred'
}

/**
 * Checks if an error is retryable.
 */
export function isRetryableError(error: unknown): boolean {
  if (error instanceof ApiError) {
    return error.status >= 500
  }
  return error instanceof NetworkError || error instanceof TimeoutError
}
