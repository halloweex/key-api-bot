import type { Meta, StoryObj } from '@storybook/react-vite'
import { ApiErrorState } from './ApiErrorState'
import { ApiError, TimeoutError } from '../api/client'

const meta = {
  title: 'Feedback/ApiErrorState',
  component: ApiErrorState,
} satisfies Meta<typeof ApiErrorState>

export default meta
type Story = StoryObj<typeof meta>

export const Generic: Story = {
  args: {
    error: new Error('Something went wrong'),
    onRetry: () => {},
  },
}

export const Timeout: Story = {
  args: {
    error: new TimeoutError(15000),
    onRetry: () => {},
  },
}

export const ServerWarmup: Story = {
  args: {
    error: new ApiError(503, 'Service unavailable'),
    onRetry: () => {},
    title: 'Failed to load revenue trend',
  },
}
