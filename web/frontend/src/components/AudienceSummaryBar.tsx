import { type ReactNode } from 'react'
import { Button } from './Button'

// ─── AudienceSummaryBar ──────────────────────────────────────────────────────
//
// The audience in one sentence, how many people it is, and the way forward —
// pinned under the groups: sticky at the bottom of the screen on a phone,
// where the thumb is, and at the foot of the step on desktop.
//
// The sentence is generated from the definition (never typed), so it says
// exactly what the count counted: «Клиенты, у которых 2+ заказа с 1 янв по
// 14 окт 2026, или 1+ заказ с 16 июл по 14 окт 2026». The total is people,
// de-duplicated across groups, with how many of them several groups share —
// the answer to "why is the total less than the groups added up".
//
// While something holds the step (an unfinished condition, no name) the
// button is disabled and the bar says why in place of the counts, so a
// greyed-out button is never the only clue.

export function AudienceSummaryBar({
  sentence,
  stats,
  blocked,
  action,
}: {
  sentence: string
  /** «4 101 человек · 1 035 в нескольких группах», or a loading line. */
  stats?: ReactNode
  /** Why the step cannot be left yet; replaces `stats` when set. */
  blocked?: string | null
  /** The step's action — «Далее · 3 691 получателей». */
  action: { label: string; onClick: () => void; disabled?: boolean }
}) {
  return (
    <div
      className="sticky bottom-0 z-20 -mx-3 border-t border-slate-200 bg-white px-3 py-3
                 shadow-[0_-4px_12px_-6px_rgba(15,23,42,0.12)]
                 pb-[max(0.75rem,env(safe-area-inset-bottom))]
                 sm:static sm:mx-0 sm:rounded-xl sm:border sm:pb-3 sm:shadow-none"
    >
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div className="min-w-0">
          <p className="line-clamp-3 text-sm text-slate-800 sm:line-clamp-none">{sentence}</p>
          {/* The numbers are announced as they settle; the sentence is not,
              or every keystroke in a condition would be read out. */}
          <p className={`mt-1 text-xs tabular-nums ${blocked ? 'font-medium text-amber-700' : 'text-slate-500'}`}
             aria-live="polite">
            {blocked || stats}
          </p>
        </div>
        <div className="shrink-0">
          <Button variant="primary" onClick={action.onClick} disabled={action.disabled}
                  fullWidth>
            {action.label}
          </Button>
        </div>
      </div>
    </div>
  )
}
