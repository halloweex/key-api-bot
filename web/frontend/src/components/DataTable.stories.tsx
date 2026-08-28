import { useState } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'
import { DataTable, Tr, Th, Td, SortableTh, type SortDirection } from './DataTable'

const ROWS = [
  { name: 'Snail Mucin Essence', brand: 'COSRX', qty: 412, revenue: 284000 },
  { name: 'Rice Cream', brand: 'Beauty of Joseon', qty: 351, revenue: 241000 },
  { name: 'Sun Stick', brand: "isntree", qty: 289, revenue: 152000 },
]

const meta = {
  title: 'Data display/DataTable',
  component: DataTable,
} satisfies Meta<typeof DataTable>

export default meta
type Story = StoryObj<typeof meta>

export const Feature: Story = {
  args: { children: null },
  render: () => (
    <DataTable variant="feature">
      <thead>
        <Tr header>
          <Th>Product</Th>
          <Th hideBelow="sm">Brand</Th>
          <Th align="right">Qty</Th>
          <Th align="right">Revenue</Th>
        </Tr>
      </thead>
      <tbody>
        {ROWS.map((r) => (
          <Tr key={r.name}>
            <Td bold>{r.name}</Td>
            <Td hideBelow="sm">{r.brand}</Td>
            <Td align="right" tabular>{r.qty}</Td>
            <Td align="right" tabular>₴{r.revenue.toLocaleString('uk-UA')}</Td>
          </Tr>
        ))}
      </tbody>
    </DataTable>
  ),
}

export const Admin: Story = {
  args: { children: null },
  render: () => (
    <DataTable variant="admin">
      <thead>
        <Tr header variant="admin">
          <Th variant="admin">User</Th>
          <Th variant="admin">Role</Th>
          <Th variant="admin" align="right">Last seen</Th>
        </Tr>
      </thead>
      <tbody>
        <Tr variant="admin">
          <Td variant="admin" bold>Olena</Td>
          <Td variant="admin">admin</Td>
          <Td variant="admin" align="right">today</Td>
        </Tr>
        <Tr variant="admin" faded>
          <Td variant="admin" bold>Denys</Td>
          <Td variant="admin">viewer</Td>
          <Td variant="admin" align="right">updating…</Td>
        </Tr>
      </tbody>
    </DataTable>
  ),
}

function SortableDemo() {
  const [sortBy, setSortBy] = useState('revenue')
  const [sortDir, setSortDir] = useState<SortDirection>('desc')
  const onSort = (col: string) => {
    if (col === sortBy) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'))
    else { setSortBy(col); setSortDir('desc') }
  }
  const rows = [...ROWS].sort((a, b) => {
    const k = sortBy as 'qty' | 'revenue'
    return sortDir === 'asc' ? a[k] - b[k] : b[k] - a[k]
  })
  return (
    <DataTable variant="feature">
      <thead>
        <tr className="bg-slate-50 border-b border-slate-200">
          <SortableTh column="name" label="Product" sortBy={sortBy} sortDir={sortDir} onSort={onSort} />
          <SortableTh column="qty" label="Qty" align="right" sortBy={sortBy} sortDir={sortDir} onSort={onSort} />
          <SortableTh column="revenue" label="Revenue" align="right" sortBy={sortBy} sortDir={sortDir} onSort={onSort} hideBelow="sm" />
        </tr>
      </thead>
      <tbody>
        {rows.map((r) => (
          <Tr key={r.name}>
            <Td>{r.name}</Td>
            <Td align="right" tabular>{r.qty}</Td>
            <Td align="right" tabular hideBelow="sm">₴{r.revenue.toLocaleString('uk-UA')}</Td>
          </Tr>
        ))}
      </tbody>
    </DataTable>
  )
}

export const Sortable: Story = {
  args: { children: null },
  render: () => <SortableDemo />,
}
