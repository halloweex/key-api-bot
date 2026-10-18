import { type ReactNode } from 'react'
import { AlertTriangle, X } from 'lucide-react'
import { Button } from './Button'
import type { IconComponent } from './icons'

// ─── ConditionRow ────────────────────────────────────────────────────────────
//
// One condition, said as one sentence: «Сделал(а) [2+ заказа] [с 1 янв по
// 14 окт 2026]». The fixed words are plain text; the parts the person chose
// are blanks — underlined, and each one opens the editor — so what is set and
// what can be changed are told apart without a single label.
//
// This is the Klaviyo / Shopify shape: a sentence per condition instead of a
// form per group. The form it replaced showed about fifteen inputs per group
// whether or not they were used; here a group shows only what it says.
//
// The row does not know what it describes. The caller passes the sentence and
// the editor; the editor is drawn inline under the sentence when `editing`
// (desktop — "edited in place"), or the caller puts it in a bottom sheet and
// passes nothing (phone). What is wrong with the condition — unfinished,
// backwards, suspicious — is said under the sentence, in both cases, so it is
// on screen while the sheet is closed.

export function ConditionRow({
  icon: Icon,
  label,
  blanks,
  editing,
  onEdit,
  onDone,
  onRemove,
  removeLabel,
  editLabel,
  doneLabel,
  problem,
  hint,
  children,
}: {
  icon: IconComponent
  /** The fixed words: «Сделал(а)». */
  label: string
  /** The chosen parts, each a tappable blank. */
  blanks: string[]
  editing: boolean
  onEdit: () => void
  onDone: () => void
  onRemove: () => void
  removeLabel: string
  /** How a blank is announced: "Change: 2+ orders". */
  editLabel: (blank: string) => string
  doneLabel: string
  /** Why the condition is not applied yet — shown, and the row is marked. */
  problem?: string | null
  /** Anything else worth saying under the sentence (a suspected slip). */
  hint?: ReactNode
  /** The editor, drawn inline while `editing`. */
  children?: ReactNode
}) {
  const inline = editing && children

  return (
    <div
      role="group"
      aria-label={label}
      className={`rounded-lg border bg-white px-3 py-2.5 transition-colors ${
        problem ? 'border-amber-200' : editing ? 'border-purple-300' : 'border-slate-200'
      }`}
    >
      <div className="flex items-start gap-2">
        <span className="mt-0.5 text-slate-400"><Icon className="h-4 w-4" aria-hidden /></span>
        <p className="min-w-0 flex-1 text-sm leading-7 text-slate-700">
          <span>{label}</span>
          {blanks.map((b, i) => (
            <span key={i}>
              {' '}
              <button
                type="button"
                onClick={onEdit}
                aria-expanded={editing}
                aria-label={editLabel(b)}
                className="rounded px-0.5 font-medium text-purple-800 underline
                           decoration-purple-300 decoration-dotted decoration-2
                           underline-offset-4 hover:bg-purple-50 focus:outline-none
                           focus-visible:ring-2 focus-visible:ring-purple-500/40"
              >
                {b}
              </button>
            </span>
          ))}
        </p>
        <button
          type="button"
          onClick={onRemove}
          aria-label={removeLabel}
          className="grid h-8 w-8 shrink-0 place-items-center rounded-md text-slate-400
                     hover:bg-slate-100 hover:text-slate-700"
        >
          <X className="h-4 w-4" aria-hidden />
        </button>
      </div>

      {problem && (
        <p className="ml-6 mt-1 flex items-start gap-1.5 text-xs text-amber-800">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" aria-hidden />
          {problem}
        </p>
      )}
      {hint && <div className="ml-6 mt-1.5">{hint}</div>}

      {inline && (
        <div className="ml-6 mt-3 flex flex-col gap-3 border-t border-slate-100 pt-3">
          {children}
          <div>
            <Button size="sm" variant="secondary" onClick={onDone}>{doneLabel}</Button>
          </div>
        </div>
      )}
    </div>
  )
}
