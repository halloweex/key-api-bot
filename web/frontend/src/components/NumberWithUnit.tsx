import { useState } from 'react'
import { Minus, Plus } from 'lucide-react'

// ─── NumberWithUnit ──────────────────────────────────────────────────────────
//
// A whole number with its unit said beside it — "[ − 2 + ] заказа", "₴ [5000]"
// — because a bare box is how 92 days became 92 orders: «Не меньше [92]»
// names no unit, so nothing on the screen said which one was being typed.
//
// Behaviour that reads as detail and is not:
//
// - `inputMode="numeric"`, so a phone opens its number pad rather than the
//   full keyboard (and no spinner arrows on desktop, which scroll-wheel a
//   value by accident).
// - What is being typed is held as text until it is a number. Clamping on
//   every keystroke turned "365" into 3 → 30 → 306 → 365 under the cursor; the
//   value is clamped once, on blur.
// - Every complete number is passed up as it is typed — out of range too —
//   so a hint about it ("92 orders? did you mean days?") appears while the
//   person is still looking at it.
// - The steppers are 36px targets on a phone (NN/g, "Input steppers").
//
// The unit is passed already in the right form for the value: the caller
// knows whether it is «заказ», «заказа» or «заказов».

export function NumberWithUnit({
  value,
  onChange,
  unit,
  prefix,
  min = 0,
  max = Number.MAX_SAFE_INTEGER,
  step = 1,
  stepper = false,
  placeholder,
  ariaLabel,
  decreaseLabel = '−',
  increaseLabel = '+',
}: {
  value: number | null
  onChange: (value: number | null) => void
  /** Said after the number. */
  unit?: string
  /** Said before it, inside the box — a currency sign. */
  prefix?: string
  min?: number
  max?: number
  step?: number
  /** Show − and + — for small counts, not for money. */
  stepper?: boolean
  placeholder?: string
  ariaLabel: string
  decreaseLabel?: string
  increaseLabel?: string
}) {
  // The text being typed, and the value it was typed as. A value set from
  // outside (a stepper, a chip, a template) no longer matches it, so it wins.
  const [draft, setDraft] = useState<{ text: string; as: number | null } | null>(null)

  const clamp = (n: number) => Math.min(max, Math.max(min, n))
  const shown = draft && draft.as === value ? draft.text : value == null ? '' : String(value)

  function type(raw: string) {
    const digits = raw.replace(/[^\d]/g, '')
    const parsed = digits === '' ? null : Number(digits)
    setDraft({ text: digits, as: parsed })
    onChange(parsed)
  }

  function blur() {
    if (value != null && clamp(value) !== value) onChange(clamp(value))
    setDraft(null)
  }

  function nudge(delta: number) {
    const base = value ?? (delta > 0 ? min - step : min)
    onChange(clamp(base + delta))
  }

  const stepButton =
    'grid h-9 w-9 shrink-0 place-items-center text-slate-500 hover:bg-slate-100 ' +
    'hover:text-slate-800 disabled:opacity-30 disabled:cursor-not-allowed sm:h-8 sm:w-8'

  return (
    <span className="inline-flex items-center gap-2">
      <span className="inline-flex items-center overflow-hidden rounded-lg border border-slate-200
                       bg-white focus-within:border-purple-300 focus-within:ring-2
                       focus-within:ring-purple-500/20">
        {stepper && (
          <button
            type="button"
            className={stepButton}
            aria-label={decreaseLabel}
            disabled={value != null && value <= min}
            onClick={() => nudge(-step)}
          >
            <Minus className="h-4 w-4" aria-hidden />
          </button>
        )}
        {prefix && <span className="pl-3 text-sm text-slate-500" aria-hidden>{prefix}</span>}
        <input
          type="text"
          inputMode="numeric"
          pattern="[0-9]*"
          aria-label={ariaLabel}
          placeholder={placeholder}
          value={shown}
          onChange={(e) => type(e.target.value)}
          onBlur={blur}
          className={`min-w-0 bg-transparent py-2 text-sm text-slate-900 tabular-nums
                      placeholder:text-slate-300 focus:outline-none ${
            stepper ? 'w-12 text-center' : prefix ? 'w-24 pl-1 pr-3' : 'w-20 px-3'
          }`}
        />
        {stepper && (
          <button
            type="button"
            className={stepButton}
            aria-label={increaseLabel}
            disabled={value != null && value >= max}
            onClick={() => nudge(step)}
          >
            <Plus className="h-4 w-4" aria-hidden />
          </button>
        )}
      </span>
      {unit && <span className="text-sm text-slate-600">{unit}</span>}
    </span>
  )
}
