import { useId, useMemo, useState } from 'react'
import { Check, Plus, Search, X } from 'lucide-react'

// ─── MultiPick ───────────────────────────────────────────────────────────────
//
// Several values out of a long list: a search box, the matching options under
// it, and what is picked as removable chips above. For brands (hundreds),
// categories and cities.
//
// The list is drawn inline, under the box, not as a floating dropdown: it
// lives inside a popover on desktop and a bottom sheet on a phone, and a
// dropdown inside either is a scroll container inside a scroll container,
// which is where a thumb loses its place.
//
// `allowCustom` turns what is typed into a value of its own (Enter or the
// "add" row) — cities are free text in KeyCRM and there is no list to offer.

export interface PickOption { value: string; label: string }

const SHOWN = 60

export function MultiPick({
  options,
  values,
  onChange,
  ariaLabel,
  placeholder,
  allowCustom = false,
  addLabel,
  emptyText,
  removeLabel = (label: string) => `× ${label}`,
}: {
  options: PickOption[]
  values: string[]
  onChange: (next: string[]) => void
  ariaLabel: string
  placeholder: string
  allowCustom?: boolean
  /** "Add «{value}»" for the custom row. */
  addLabel?: (value: string) => string
  /** Said when nothing matches the search. */
  emptyText: string
  removeLabel?: (label: string) => string
}) {
  const [query, setQuery] = useState('')
  const listId = useId()
  const labelOf = (v: string) => options.find((o) => o.value === v)?.label ?? v

  const matches = useMemo(() => {
    const q = query.trim().toLowerCase()
    const all = q ? options.filter((o) => o.label.toLowerCase().includes(q)) : options
    return all.slice(0, SHOWN)
  }, [options, query])

  const typed = query.trim()
  const canAdd = allowCustom && typed !== ''
    && !values.some((v) => v.toLowerCase() === typed.toLowerCase())
    && !options.some((o) => o.label.toLowerCase() === typed.toLowerCase())

  function toggle(value: string) {
    onChange(values.includes(value) ? values.filter((v) => v !== value) : [...values, value])
  }

  function add() {
    if (!canAdd) return
    onChange([...values, typed])
    setQuery('')
  }

  return (
    <div className="flex flex-col gap-2">
      {values.length > 0 && (
        <ul className="flex flex-wrap gap-1.5">
          {values.map((v) => (
            <li key={v}>
              <span className="inline-flex items-center gap-1 rounded-full border border-purple-200
                               bg-purple-50 py-1 pl-3 pr-1 text-sm text-purple-900">
                {labelOf(v)}
                <button
                  type="button"
                  aria-label={removeLabel(labelOf(v))}
                  onClick={() => toggle(v)}
                  className="grid h-6 w-6 place-items-center rounded-full text-purple-400
                             hover:bg-purple-100 hover:text-purple-800"
                >
                  <X className="h-3.5 w-3.5" aria-hidden />
                </button>
              </span>
            </li>
          ))}
        </ul>
      )}

      <label className="flex items-center gap-2 rounded-lg border border-slate-200 bg-white px-3
                        focus-within:border-purple-300 focus-within:ring-2
                        focus-within:ring-purple-500/20">
        <Search className="h-4 w-4 shrink-0 text-slate-400" aria-hidden />
        <input
          type="search"
          aria-controls={listId}
          aria-label={ariaLabel}
          placeholder={placeholder}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter') {
              e.preventDefault()
              if (canAdd) add()
              else if (matches.length === 1) toggle(matches[0].value)
            }
          }}
          className="min-w-0 flex-1 bg-transparent py-2 text-sm text-slate-900
                     placeholder:text-slate-400 focus:outline-none"
        />
      </label>

      {/* Toggle buttons in a list rather than a listbox of options: each
          row is a button a screen reader announces as pressed or not, and
          nothing interactive sits inside anything else interactive. */}
      <ul
        id={listId}
        className="max-h-56 overflow-y-auto overscroll-contain rounded-lg border border-slate-100"
      >
        {canAdd && (
          <li>
            <button
              type="button"
              onClick={add}
              className="flex w-full items-center gap-2 px-3 py-2.5 text-left text-sm
                         text-purple-800 hover:bg-purple-50"
            >
              <Plus className="h-4 w-4" aria-hidden />
              {addLabel ? addLabel(typed) : typed}
            </button>
          </li>
        )}
        {matches.map((o) => {
          const on = values.includes(o.value)
          return (
            <li key={o.value}>
              <button
                type="button"
                aria-pressed={on}
                onClick={() => toggle(o.value)}
                className={`flex w-full items-center justify-between gap-2 px-3 py-2.5
                            text-left text-sm hover:bg-slate-50 ${
                  on ? 'font-medium text-purple-900' : 'text-slate-700'
                }`}
              >
                {o.label}
                {on && <Check className="h-4 w-4 text-purple-600" aria-hidden />}
              </button>
            </li>
          )
        })}
        {!matches.length && !canAdd && (
          <li className="px-3 py-2.5 text-sm text-slate-500">{emptyText}</li>
        )}
      </ul>
    </div>
  )
}
