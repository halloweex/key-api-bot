import { memo } from 'react'
import { X } from 'lucide-react'
import { Input } from './Input'
import { Select } from './Select'
import type { IconComponent } from './icons'

// ─── SmsAudienceControls ─────────────────────────────────────────────────────
//
// The small pieces an SMS audience is built from: a choice chip, a family of
// filters under the question it answers, one labelled field with its example,
// a from–to pair, and a picker that turns a list into removable chips.
//
// They lived inside <SmsAudienceForm> until an audience became several groups:
// every group now carries the same families, and the campaign-wide controls
// (value level, basis) use the same field and chip, so the pieces are shared
// rather than copied into each place that draws them.

/** A toggle among a few named choices. */
export const SmsChoiceChip = memo(function SmsChoiceChip({
  active, onClick, children,
}: {
  active: boolean
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className={`px-3.5 py-2 text-sm rounded-lg border transition-colors tabular-nums ${
        active
          ? 'border-purple-400 bg-purple-50 text-purple-800 font-medium'
          : 'border-slate-200 text-slate-600 hover:border-slate-300 hover:bg-slate-50'
      }`}
    >
      {children}
    </button>
  )
})

/** One family of filters, under the question it answers. */
export const SmsFilterFamily = memo(function SmsFilterFamily({
  icon: Icon, title, hint, children,
}: {
  icon: IconComponent
  title: string
  hint: string
  children: React.ReactNode
}) {
  return (
    <section className="rounded-lg border border-slate-200 bg-slate-50/70 p-4">
      <div className="flex items-center gap-2">
        <span className="text-slate-400"><Icon className="w-4 h-4" aria-hidden /></span>
        <h4 className="text-sm font-semibold text-slate-800">{title}</h4>
      </div>
      <p className="text-xs text-slate-500 mt-1 mb-3 leading-snug">{hint}</p>
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">{children}</div>
    </section>
  )
})

/** One control, its icon, and the example that says what goes in it. */
export const SmsFilterField = memo(function SmsFilterField({
  icon: Icon, label, example, children,
}: {
  icon: IconComponent
  label: string
  example?: string
  children: React.ReactNode
}) {
  return (
    <div>
      <div className="flex items-center gap-1.5 mb-1.5">
        <span className="text-slate-400"><Icon className="w-4 h-4" aria-hidden /></span>
        <span className="text-sm font-medium text-slate-700">{label}</span>
      </div>
      {children}
      {example && (
        <p className="mt-1.5 text-xs text-slate-500 leading-snug">{example}</p>
      )}
    </div>
  )
})

/** A from–to pair. The unit sits beside the boxes, not inside the label. */
export const SmsNumberRange = memo(function SmsNumberRange({
  label, unit, fromPlaceholder, toPlaceholder, from, to, onFrom, onTo, step,
}: {
  label: string
  unit?: string
  fromPlaceholder?: string
  toPlaceholder?: string
  from: number | null | undefined
  to: number | null | undefined
  onFrom: (v: number | undefined) => void
  onTo: (v: number | undefined) => void
  step?: number
}) {
  const parse = (raw: string) => (raw === '' ? undefined : Number(raw))

  return (
    <div className="flex items-center gap-2">
      <Input
        type="number" step={step} width="narrow"
        aria-label={`${label} min`} placeholder={fromPlaceholder}
        value={from ?? ''} onChange={(v) => onFrom(parse(v))}
      />
      <span className="text-slate-400 text-sm">—</span>
      <Input
        type="number" step={step} width="narrow"
        aria-label={`${label} max`} placeholder={toPlaceholder}
        value={to ?? ''} onChange={(v) => onTo(parse(v))}
      />
      {unit && <span className="text-xs text-slate-500">{unit}</span>}
    </div>
  )
})

/** A picker that turns a list into removable chips. */
export const SmsMultiPicker = memo(function SmsMultiPicker({
  options, values, onChange, placeholder, label,
}: {
  options: Array<{ value: string; label: string }>
  values: string[]
  onChange: (next: string[]) => void
  placeholder: string
  label: string
}) {
  const remaining = options.filter((o) => !values.includes(o.value))

  return (
    <>
      <Select
        options={remaining}
        value=""
        onChange={(v) => v && onChange([...values, v])}
        placeholder={placeholder}
        aria-label={label}
      />
      {values.length > 0 && (
        <div className="mt-2 flex flex-wrap gap-1.5">
          {values.map((v) => (
            <span
              key={v}
              className="inline-flex items-center gap-1 px-2.5 py-1 text-xs rounded-md
                         bg-purple-50 text-purple-800 border border-purple-100"
            >
              {options.find((o) => o.value === v)?.label ?? v}
              <button
                type="button"
                aria-label={`remove ${v}`}
                onClick={() => onChange(values.filter((x) => x !== v))}
                className="text-purple-300 hover:text-purple-800"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </span>
          ))}
        </div>
      )}
    </>
  )
})
