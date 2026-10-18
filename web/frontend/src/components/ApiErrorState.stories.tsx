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

/**
 * A refusal the caller can say better than the server's English `detail`:
 * an SMS follow-up of a campaign not sent yet, translated by
 * `utils/smsRefusal.ts` and passed as `message`.
 */
export const TranslatedRefusal: Story = {
  args: {
    error: new ApiError(409, "campaign 'oct-promo' has not been sent — a follow-up goes to the people the first message reached",
                        'CLIENT_ERROR', { reason: 'follow_up_root_unsent', root: 'oct-promo' }),
    title: 'Не удалось посчитать эту аудиторию',
    message: 'Кампания oct-promo ещё не отправлена, а продолжение идёт тем, до кого дошло первое сообщение.',
    onRetry: () => {},
  },
}
