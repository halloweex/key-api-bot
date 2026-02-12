import { useRef, type ReactNode } from 'react'
import { useVirtualizer } from '@tanstack/react-virtual'

interface VirtualListProps<T> {
  items: T[]
  estimateSize: number
  renderItem: (item: T, index: number) => ReactNode
  className?: string
  height?: number | string
  overscan?: number
}

/**
 * Virtual list component for efficiently rendering large lists.
 * Only renders visible items plus a small overscan buffer.
 *
 * @example
 * <VirtualList
 *   items={products}
 *   estimateSize={50}
 *   height={400}
 *   renderItem={(product, index) => (
 *     <div key={product.id}>{product.name}</div>
 *   )}
 * />
 */
export function VirtualList<T>({
  items,
  estimateSize,
  renderItem,
  className = '',
  height = 400,
  overscan = 5,
}: VirtualListProps<T>) {
  const parentRef = useRef<HTMLDivElement>(null)

  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimateSize,
    overscan,
  })

  const virtualItems = virtualizer.getVirtualItems()

  return (
    <div
      ref={parentRef}
      className={`overflow-auto ${className}`}
      style={{ height: typeof height === 'number' ? `${height}px` : height }}
    >
      <div
        style={{
          height: `${virtualizer.getTotalSize()}px`,
          width: '100%',
          position: 'relative',
        }}
      >
        {virtualItems.map((virtualItem) => (
          <div
            key={virtualItem.key}
            style={{
              position: 'absolute',
              top: 0,
              left: 0,
              width: '100%',
              height: `${virtualItem.size}px`,
              transform: `translateY(${virtualItem.start}px)`,
            }}
          >
            {renderItem(items[virtualItem.index], virtualItem.index)}
          </div>
        ))}
      </div>
    </div>
  )
}

interface VirtualTableProps<T> {
  items: T[]
  columns: {
    key: string
    header: ReactNode
    render: (item: T) => ReactNode
    width?: string
    className?: string
  }[]
  estimateSize?: number
  height?: number | string
  overscan?: number
  className?: string
  headerClassName?: string
  rowClassName?: string | ((item: T, index: number) => string)
  onRowClick?: (item: T, index: number) => void
}

/**
 * Virtual table component for efficiently rendering large tables.
 * Header stays fixed while rows are virtualized.
 *
 * @example
 * <VirtualTable
 *   items={orders}
 *   columns={[
 *     { key: 'id', header: 'Order #', render: (o) => o.id },
 *     { key: 'total', header: 'Total', render: (o) => formatCurrency(o.total) },
 *   ]}
 *   height={500}
 *   onRowClick={(order) => openOrderDetail(order)}
 * />
 */
export function VirtualTable<T>({
  items,
  columns,
  estimateSize = 48,
  height = 400,
  overscan = 5,
  className = '',
  headerClassName = '',
  rowClassName = '',
  onRowClick,
}: VirtualTableProps<T>) {
  const parentRef = useRef<HTMLDivElement>(null)

  const virtualizer = useVirtualizer({
    count: items.length,
    getScrollElement: () => parentRef.current,
    estimateSize: () => estimateSize,
    overscan,
  })

  const virtualItems = virtualizer.getVirtualItems()

  const getRowClassName = (item: T, index: number): string => {
    if (typeof rowClassName === 'function') {
      return rowClassName(item, index)
    }
    return rowClassName
  }

  return (
    <div className={`flex flex-col ${className}`}>
      {/* Fixed header */}
      <div
        className={`flex border-b border-slate-200 bg-slate-50 font-medium text-sm text-slate-600 ${headerClassName}`}
      >
        {columns.map((col) => (
          <div
            key={col.key}
            className={`px-3 py-2 ${col.className || ''}`}
            style={{ width: col.width || 'auto', flex: col.width ? 'none' : '1' }}
          >
            {col.header}
          </div>
        ))}
      </div>

      {/* Virtualized rows */}
      <div
        ref={parentRef}
        className="overflow-auto"
        style={{ height: typeof height === 'number' ? `${height}px` : height }}
      >
        <div
          style={{
            height: `${virtualizer.getTotalSize()}px`,
            width: '100%',
            position: 'relative',
          }}
        >
          {virtualItems.map((virtualItem) => {
            const item = items[virtualItem.index]
            return (
              <div
                key={virtualItem.key}
                className={`flex items-center border-b border-slate-100 hover:bg-slate-50 transition-colors ${
                  onRowClick ? 'cursor-pointer' : ''
                } ${getRowClassName(item, virtualItem.index)}`}
                style={{
                  position: 'absolute',
                  top: 0,
                  left: 0,
                  width: '100%',
                  height: `${virtualItem.size}px`,
                  transform: `translateY(${virtualItem.start}px)`,
                }}
                onClick={onRowClick ? () => onRowClick(item, virtualItem.index) : undefined}
              >
                {columns.map((col) => (
                  <div
                    key={col.key}
                    className={`px-3 py-2 truncate ${col.className || ''}`}
                    style={{ width: col.width || 'auto', flex: col.width ? 'none' : '1' }}
                  >
                    {col.render(item)}
                  </div>
                ))}
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

export default VirtualList
