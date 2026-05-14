import { type ReactNode, type ElementType, createElement } from 'react'

// ─── Design tokens ───────────────────────────────────────────────────────────
//
// Wrapper is the project-wide layout primitive. It is the *only* place where
// padding/gap/flex props are allowed — every other component owns its visuals
// internally and does not expose layout overrides.
//
// Rule of thumb (from the architecture guide):
//   "How a component looks"   → lives inside the component
//   "How elements sit next to each other" → wrap them in <Wrapper>

type Spacing = 'none' | 'xs' | 'sm' | 'md' | 'lg' | 'xl'
type Direction = 'row' | 'column' | 'row-responsive'
type Align = 'start' | 'center' | 'end' | 'stretch' | 'baseline'
type Justify = 'start' | 'center' | 'end' | 'between' | 'around' | 'evenly'
type FlexValue = 0 | 1 | 'auto'

interface WrapperProps {
  children: ReactNode
  /** HTML element to render as. Defaults to <div>. */
  as?: ElementType
  /** Flex direction. `row-responsive` = column on mobile, row on sm+. */
  dir?: Direction
  /** Gap between children. */
  gap?: Spacing
  /** Padding on all sides (overridden by paddingX/paddingY when set). */
  padding?: Spacing
  paddingX?: Spacing
  paddingY?: Spacing
  /** Margin against siblings (the only "outside" property — never visual). */
  margin?: Spacing
  marginX?: Spacing
  marginY?: Spacing
  marginTop?: Spacing
  marginBottom?: Spacing
  align?: Align
  justify?: Justify
  /** Flex grow/shrink claim against parent. */
  flex?: FlexValue
  wrap?: boolean
  fullWidth?: boolean
  fullHeight?: boolean
  /** Optional accessible role/aria — layout containers are usually purely presentational. */
  role?: string
  ariaLabel?: string
}

// ─── Class maps (literal strings so Tailwind JIT can pick them up) ───────────

const dirClass: Record<Direction, string> = {
  row: 'flex-row',
  column: 'flex-col',
  'row-responsive': 'flex-col sm:flex-row',
}

const gapClass: Record<Spacing, string> = {
  none: '',
  xs: 'gap-1',
  sm: 'gap-2',
  md: 'gap-3',
  lg: 'gap-4',
  xl: 'gap-6',
}

const padClass: Record<Spacing, string> = {
  none: '',
  xs: 'p-1',
  sm: 'p-2',
  md: 'p-3',
  lg: 'p-4',
  xl: 'p-6',
}

const padXClass: Record<Spacing, string> = {
  none: '',
  xs: 'px-1',
  sm: 'px-2',
  md: 'px-3',
  lg: 'px-4',
  xl: 'px-6',
}

const padYClass: Record<Spacing, string> = {
  none: '',
  xs: 'py-1',
  sm: 'py-2',
  md: 'py-3',
  lg: 'py-4',
  xl: 'py-6',
}

const marginClass: Record<Spacing, string> = {
  none: '',
  xs: 'm-1',
  sm: 'm-2',
  md: 'm-3',
  lg: 'm-4',
  xl: 'm-6',
}

const marginXClass: Record<Spacing, string> = {
  none: '',
  xs: 'mx-1',
  sm: 'mx-2',
  md: 'mx-3',
  lg: 'mx-4',
  xl: 'mx-6',
}

const marginYClass: Record<Spacing, string> = {
  none: '',
  xs: 'my-1',
  sm: 'my-2',
  md: 'my-3',
  lg: 'my-4',
  xl: 'my-6',
}

const marginTopClass: Record<Spacing, string> = {
  none: '',
  xs: 'mt-1',
  sm: 'mt-2',
  md: 'mt-3',
  lg: 'mt-4',
  xl: 'mt-6',
}

const marginBottomClass: Record<Spacing, string> = {
  none: '',
  xs: 'mb-1',
  sm: 'mb-2',
  md: 'mb-3',
  lg: 'mb-4',
  xl: 'mb-6',
}

const alignClass: Record<Align, string> = {
  start: 'items-start',
  center: 'items-center',
  end: 'items-end',
  stretch: 'items-stretch',
  baseline: 'items-baseline',
}

const justifyClass: Record<Justify, string> = {
  start: 'justify-start',
  center: 'justify-center',
  end: 'justify-end',
  between: 'justify-between',
  around: 'justify-around',
  evenly: 'justify-evenly',
}

const flexClass: Record<string, string> = {
  '0': 'flex-none',
  '1': 'flex-1',
  auto: 'flex-auto',
}

// ─── Component ───────────────────────────────────────────────────────────────

export function Wrapper({
  children,
  as = 'div',
  dir = 'column',
  gap = 'none',
  padding,
  paddingX,
  paddingY,
  margin,
  marginX,
  marginY,
  marginTop,
  marginBottom,
  align,
  justify,
  flex,
  wrap = false,
  fullWidth = false,
  fullHeight = false,
  role,
  ariaLabel,
}: WrapperProps) {
  const classes = [
    'flex',
    dirClass[dir],
    gapClass[gap],
    padding && padClass[padding],
    paddingX && padXClass[paddingX],
    paddingY && padYClass[paddingY],
    margin && marginClass[margin],
    marginX && marginXClass[marginX],
    marginY && marginYClass[marginY],
    marginTop && marginTopClass[marginTop],
    marginBottom && marginBottomClass[marginBottom],
    align && alignClass[align],
    justify && justifyClass[justify],
    flex !== undefined && flexClass[String(flex)],
    wrap && 'flex-wrap',
    fullWidth && 'w-full',
    fullHeight && 'h-full',
  ]
    .filter(Boolean)
    .join(' ')

  return createElement(
    as,
    { className: classes, role, 'aria-label': ariaLabel },
    children,
  )
}
