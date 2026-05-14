import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { render, screen, act, fireEvent } from '@testing-library/react'
import { ApiErrorState } from '../ApiErrorState'
import { ApiError, NetworkError, TimeoutError } from '../../api/client'

describe('ApiErrorState', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('shows manual retry button when error is not auto-recoverable', () => {
    const onRetry = vi.fn()
    render(
      <ApiErrorState error={new ApiError(400, 'bad request')} onRetry={onRetry} />,
    )
    expect(screen.getByText('error.tryAgain')).toBeInTheDocument()
  })

  it('auto-retries on NetworkError up to 3 times', async () => {
    const onRetry = vi.fn()
    render(<ApiErrorState error={new NetworkError('offline', 'OFFLINE')} onRetry={onRetry} />)

    // First retry after 2s
    await act(async () => {
      vi.advanceTimersByTime(2000)
    })
    expect(onRetry).toHaveBeenCalledTimes(1)

    // Second retry after 4s (exponential)
    await act(async () => {
      vi.advanceTimersByTime(4000)
    })
    expect(onRetry).toHaveBeenCalledTimes(2)

    // Third retry after 8s
    await act(async () => {
      vi.advanceTimersByTime(8000)
    })
    expect(onRetry).toHaveBeenCalledTimes(3)

    // No fourth retry (cap = 3)
    await act(async () => {
      vi.advanceTimersByTime(20000)
    })
    expect(onRetry).toHaveBeenCalledTimes(3)
  })

  it('manual retry button calls onRetry', () => {
    const onRetry = vi.fn()
    render(<ApiErrorState error={new ApiError(404, 'not found')} onRetry={onRetry} />)

    fireEvent.click(screen.getByText('error.tryAgain'))
    expect(onRetry).toHaveBeenCalledTimes(1)
  })

  it('classifies 503 as server-warmup (auto-retryable)', async () => {
    const onRetry = vi.fn()
    render(<ApiErrorState error={new ApiError(503, 'warming up')} onRetry={onRetry} />)

    expect(screen.getByText('error.serverWarmup')).toBeInTheDocument()

    await act(async () => {
      vi.advanceTimersByTime(2000)
    })
    expect(onRetry).toHaveBeenCalled()
  })

  it('classifies TimeoutError as auto-retryable', async () => {
    const onRetry = vi.fn()
    render(<ApiErrorState error={new TimeoutError(5000)} onRetry={onRetry} />)

    expect(screen.getByText('error.requestTimeout')).toBeInTheDocument()
    await act(async () => {
      vi.advanceTimersByTime(2000)
    })
    expect(onRetry).toHaveBeenCalled()
  })
})
