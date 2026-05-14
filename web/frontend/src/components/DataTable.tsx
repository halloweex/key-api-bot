import { memo, type ReactNode, type MouseEvent } from 'react'

// ─── DataTable + cells ───────────────────────────────────────────────────────
//
// Tabular data primitive. Chrome (overflow wrapper, table base, default
// hover, header styling) owned here. Cell-level alignment, responsive
// hide and tabular-nums are expressed via semantic props on <Th>/<Td>.
//
//   <DataTable variant="feature">
//     <thead>
//       <Tr header>
//         <Th>Name</Th>
//         <Th align="right" hideBelow="sm">Qty</Th>
//       </Tr>
//     </thead>
//     <tbody>
//       {rows.map(r => (
//         <Tr key={r.id}>
//           <Td>{r.name}</Td>
//           <Td align="right" tabular hideBelow="sm">{r.qty}</Td>
//         </Tr>
//       ))}
//     </tbody>
//   </DataTable>
//
// Variants:
//   feature  — text-sm body, comfortable cell padding, plain header
//   admin    — base text, denser cell padding, uppercase-tracked header
//
// stickyHeader: opt-in. Adds max-height scrolling + sticky thead row.

type Variant = 'feature' | 'admin'
type Align = 'left' | 'right' | 'center'
type HideBelow = 'sm' | 'md' | 'lg'

const variantTableClass: Record<Variant, string> = {
  feature: 'w-full text-sm',
  admin: 'w-full',
}

const variantCellPad: Record<Variant, string> = {
  feature: 'py-2.5 px-3',
  admin: 'py-3 px-4',
}

const alignClass: Record<Align, string> = {
  left: 'text-left',
  right: 'text-right',
  center: 'text-center',
}

const hideClass: Record<HideBelow, string> = {
  sm: 'hidden sm:table-cell',
  md: 'hidden md:table-cell',
  lg: 'hidden lg:table-cell',
}

// ─── DataTable wrapper ───────────────────────────────────────────────────────

interface DataTableProps {
  children: ReactNode
  variant?: Variant
  /** Pins the header row to the top while body scrolls (admin user list). */
  stickyHeader?: boolean
}

// Variant is passed down explicitly via props on each <Th>/<Td>/<Tr>
// (no React.Context) — keeps the API explicit and avoids a hidden coupling.

export const DataTable = memo(function DataTable({
  children,
  variant = 'feature',
  stickyHeader = false,
}: DataTableProps) {
  const wrapper = stickyHeader
    ? 'overflow-x-auto max-h-[70vh] overflow-y-auto'
    : 'overflow-x-auto'

  return (
    <div className={wrapper} data-table-variant={variant} data-sticky-header={stickyHeader || undefined}>
      <table className={variantTableClass[variant]}>{children}</table>
    </div>
  )
})

// ─── Row ─────────────────────────────────────────────────────────────────────

interface TrProps {
  children: ReactNode
  /** Header row gets the uppercase / divider styling. */
  header?: boolean
  /** Body rows get hover; opt-out for non-interactive rows. */
  hover?: boolean
  /** Whole-row dim (e.g. updating in flight). */
  faded?: boolean
  /** Pins header tr to top when DataTable stickyHeader is true. */
  sticky?: boolean
  onClick?: (e: MouseEvent<HTMLTableRowElement>) => void
  /** Variant controls header text style (uppercase for admin, plain for feature). */
  variant?: Variant
}

const headerRowBase = 'border-b border-slate-200 text-left'

const headerRowVariant: Record<Variant, string> = {
  feature: '',
  admin: 'text-xs font-medium text-slate-500 uppercase tracking-wider',
}

export const Tr = memo(function Tr({
  children,
  header = false,
  hover = true,
  faded = false,
  sticky = false,
  onClick,
  variant = 'feature',
}: TrProps) {
  const base = header
    ? `${headerRowBase} ${headerRowVariant[variant]}`
    : 'border-b border-slate-100 transition-colors'
  const interactive = !header && hover ? 'hover:bg-slate-50/50' : ''
  const stickyClass = sticky ? 'sticky top-0 z-10 bg-white' : ''
  const opacity = faded ? 'opacity-50' : ''

  return (
    <tr
      onClick={onClick}
      className={`${base} ${interactive} ${stickyClass} ${opacity}`}
    >
      {children}
    </tr>
  )
})

// ─── Th / Td cells ───────────────────────────────────────────────────────────

interface CellProps {
  children?: ReactNode
  align?: Align
  hideBelow?: HideBelow
  variant?: Variant
  /** Renders a sticky-background cell (paired with DataTable stickyHeader). */
  sticky?: boolean
  colSpan?: number
}

interface TdProps extends CellProps {
  /** Use tabular-nums for monospaced digits (revenue / counts). */
  tabular?: boolean
  /** Bold body cell (primary column / emphasised value). */
  bold?: boolean
  /** Dimmed + pointer-events-none (cell-level pending state). */
  faded?: boolean
}

const thBaseFeature = 'font-semibold text-slate-600'
const thBaseAdmin = ''

export const Th = memo(function Th({
  children,
  align = 'left',
  hideBelow,
  variant = 'feature',
  sticky = false,
  colSpan,
}: CellProps) {
  const base = variant === 'admin' ? thBaseAdmin : thBaseFeature
  return (
    <th
      colSpan={colSpan}
      className={[
        variantCellPad[variant],
        base,
        alignClass[align],
        hideBelow ? hideClass[hideBelow] : '',
        sticky ? 'bg-white' : '',
      ]
        .filter(Boolean)
        .join(' ')}
    >
      {children}
    </th>
  )
})

export const Td = memo(function Td({
  children,
  align = 'left',
  hideBelow,
  variant = 'feature',
  tabular = false,
  bold = false,
  sticky = false,
  faded = false,
  colSpan,
}: TdProps) {
  return (
    <td
      colSpan={colSpan}
      className={[
        variantCellPad[variant],
        alignClass[align],
        hideBelow ? hideClass[hideBelow] : '',
        tabular ? 'tabular-nums' : '',
        bold ? 'font-medium text-slate-800' : '',
        sticky ? 'bg-white' : '',
        faded ? 'opacity-50 pointer-events-none' : '',
      ]
        .filter(Boolean)
        .join(' ')}
    >
      {children}
    </td>
  )
})
