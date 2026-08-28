// ─── Spinner ─────────────────────────────────────────────────────────────────
//
// The loading spinner and the slots it fills. One glyph, two placements:
//
//   variant="fill"    — centred inside a flex parent's remaining space
//                       (page slot while a lazy route loads)
//   variant="screen"  — centred on a full-height page ground
//                       (route guards before auth resolves)
//
// Visual is owned entirely here; there is no size/colour escape.

type SpinnerVariant = 'fill' | 'screen'

const containerClass: Record<SpinnerVariant, string> = {
  fill: 'flex-1 flex items-center justify-center',
  screen: 'min-h-screen bg-slate-50 flex items-center justify-center',
}

interface SpinnerProps {
  variant?: SpinnerVariant
}

export function Spinner({ variant = 'fill' }: SpinnerProps) {
  return (
    <div className={containerClass[variant]} role="status" aria-label="Loading">
      <div className="w-8 h-8 border-4 border-purple-500 border-t-transparent rounded-full animate-spin" />
    </div>
  )
}
