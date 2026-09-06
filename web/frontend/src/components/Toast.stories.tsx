import type { Meta, StoryObj } from '@storybook/react-vite'
import { ToastProvider, useToast } from './Toast'
import { Button } from './Button'
import { Wrapper } from './Wrapper'

const meta = {
  title: 'Feedback/Toast',
  component: ToastProvider,
} satisfies Meta<typeof ToastProvider>

export default meta
type Story = StoryObj<typeof meta>

function Demo() {
  const { addToast } = useToast()
  return (
    <Wrapper dir="row" gap="md" wrap>
      <Button onClick={() => addToast({ type: 'success', title: 'Campaign frozen', message: '412 recipients' })}>
        Success
      </Button>
      <Button onClick={() => addToast({ type: 'error', title: 'Send failed', message: 'Gateway returned 503' })}>
        Error
      </Button>
      <Button onClick={() => addToast({ type: 'warning', title: 'Balance low' })}>
        Warning
      </Button>
      <Button onClick={() => addToast({ type: 'info', title: 'Model retrained' })}>
        Info
      </Button>
    </Wrapper>
  )
}

export const Playground: Story = {
  args: { children: null },
  render: () => (
    <ToastProvider>
      <Demo />
    </ToastProvider>
  ),
}
