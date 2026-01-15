import { type ReactNode } from 'react'
import { Card, CardHeader, CardTitle, CardContent, SkeletonChart } from '../ui'

interface ChartContainerProps {
  title: string
  children: ReactNode
  isLoading?: boolean
  error?: Error | null
  className?: string
  action?: ReactNode
}

export function ChartContainer({
  title,
  children,
  isLoading,
  error,
  className = '',
  action,
}: ChartContainerProps) {
  if (isLoading) {
    return <SkeletonChart />
  }

  if (error) {
    return (
      <Card className={className}>
        <CardHeader>
          <CardTitle>{title}</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-64 flex items-center justify-center">
            <p className="text-red-400">Failed to load data</p>
          </div>
        </CardContent>
      </Card>
    )
  }

  return (
    <Card className={className}>
      <CardHeader className="flex flex-row items-center justify-between">
        <CardTitle>{title}</CardTitle>
        {action}
      </CardHeader>
      <CardContent>
        {children}
      </CardContent>
    </Card>
  )
}
