import type { Meta, StoryObj } from '@storybook/react-vite'
import {
  SkeletonCard,
  SkeletonChart,
  SkeletonHorizontalBars,
  SkeletonTable,
  SkeletonMomentum,
  SkeletonRetentionMatrix,
  SkeletonVerticalBars,
} from './Skeleton'

const meta = {
  title: 'Feedback/Skeleton',
  component: SkeletonChart,
  parameters: {
    docs: {
      description: {
        component:
          'Named loading scenes. The base shimmer block is private — consumers can only ' +
          'use these finished compositions, so every loading state looks intentional.',
      },
    },
  },
} satisfies Meta<typeof SkeletonChart>

export default meta
type Story = StoryObj<typeof meta>

export const Card: Story = { render: () => <SkeletonCard /> }
export const Chart: Story = { render: () => <SkeletonChart /> }
export const HorizontalBars: Story = { render: () => <SkeletonHorizontalBars /> }
export const Table: Story = { render: () => <SkeletonTable /> }
export const Momentum: Story = { render: () => <SkeletonMomentum /> }
export const RetentionMatrix: Story = { render: () => <SkeletonRetentionMatrix /> }
export const VerticalBars: Story = { render: () => <SkeletonVerticalBars /> }
