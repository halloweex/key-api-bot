import { useState } from 'react'
import { formatDayRange, inclusiveDays, isIsoDate } from '../utils/smsDates'

// ─── DateRangeField ──────────────────────────────────────────────────────────
//
// A period as two native date inputs inside one frame: «[01.01.2026] —
// [14.10.2026]», then what the two mean in words, «1 янв – 14 окт 2026 · 287
// дней».
//
// Native on purpose. `<input type="date">` is the phone's own wheel or
// calendar, in the reader's locale, with nothing to load — and the form it
// replaces had one date field for a period, so the owner put the period's END
// into it, because it was the only date there was.
//
// It cannot be reversed: the end's `min` is the start and the start's `max`
// is the end, so a picker never offers the wrong side. A browser that lets a
// reversed date through anyway (typed into a desktop field) gets the two
// swapped and a line saying so, rather than a period that selects nobody.
//
// Either end may be left empty when `allowOpen` — "since 1 January", "up to
// 14 October" — and the caller decides what an open end means.

export function DateRangeField({
  from,
  to,
  onChange,
  fromLabel,
  toLabel,
  allowOpen = false,
  describe,
  swappedNote,
}: {
  from: string
  to: string
  onChange: (next: { from: string; to: string }) => void
  fromLabel: string
  toLabel: string
  allowOpen?: boolean
  /** The range in words, e.g. "· 287 days"; given the number of days. */
  describe?: (days: number) => string
  /** Said once when a reversed pair was put the right way round. */
  swappedNote: string
}) {
  const [swapped, setSwapped] = useState(false)

  function set(next: { from: string; to: string }) {
    if (isIsoDate(next.from) && isIsoDate(next.to) && next.from > next.to) {
      setSwapped(true)
      onChange({ from: next.to, to: next.from })
      return
    }
    setSwapped(false)
    // An emptied end stays empty only where an open end means something.
    if (!allowOpen && (!next.from || !next.to)) {
      onChange({ from: next.from || from, to: next.to || to })
      return
    }
    onChange(next)
  }

  const both = isIsoDate(from) && isIsoDate(to)
  const input =
    'min-w-0 flex-1 bg-transparent px-2 py-2 text-sm text-slate-900 tabular-nums ' +
    'focus:outline-none [color-scheme:light]'

  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center rounded-lg border border-slate-200 bg-white
                      focus-within:border-purple-300 focus-within:ring-2
                      focus-within:ring-purple-500/20">
        <input
          type="date"
          aria-label={fromLabel}
          className={input}
          value={from}
          max={isIsoDate(to) ? to : undefined}
          onChange={(e) => set({ from: e.target.value, to })}
        />
        <span className="px-1 text-slate-400" aria-hidden>—</span>
        <input
          type="date"
          aria-label={toLabel}
          className={input}
          value={to}
          min={isIsoDate(from) ? from : undefined}
          onChange={(e) => set({ from, to: e.target.value })}
        />
      </div>
      {both && (
        <p className="text-xs text-slate-500 tabular-nums">
          {formatDayRange(from, to)}
          {describe ? ` ${describe(inclusiveDays(from, to))}` : ''}
        </p>
      )}
      {swapped && <p className="text-xs text-amber-700">{swappedNote}</p>}
    </div>
  )
}
