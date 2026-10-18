import { useEffect, useRef, type ReactNode } from 'react'

// ─── TemplateChips ───────────────────────────────────────────────────────────
//
// «С чего начнём?» — the audiences a campaign usually starts from, one tap
// each: everybody, recent buyers, new ones, repeat buyers, the ones who came
// once and left, the ones drifting away, the lost, the VIPs, a brand's buyers.
//
// A template is a starting point, not a black box (Mailchimp's pre-built
// segments, Klaviyo's segment library): it fills the groups with ordinary
// conditions that are then edited like any other. Its hint line says what it
// will put there before it is tapped.
//
// A row that scrolls sideways on a phone — nine chips wrapping would push the
// groups below the fold — and wraps on desktop. `extra` holds the two other
// ways in, after the chips: like a past campaign, and a saved audience.
//
// Each card says how many people it would select today, beside its name, so
// the choice is made on the number and not only on the description. The
// counts cost a preview each, and the preview is rate-limited, so the row
// tells its caller once it has been seen (`onVisible`) and the caller asks
// for them then, one at a time. «…» while a count is on its way; nothing when
// it could not be had — a template still works without its number.

export interface TemplateChip {
  key: string
  label: string
  hint: string
  /** How many it would select, formatted; «…» while asked; absent when unknown. */
  count?: string
}

export function TemplateChips({
  title,
  items,
  onPick,
  extra,
  onVisible,
}: {
  title: string
  items: TemplateChip[]
  onPick: (key: string) => void
  extra?: ReactNode
  /** Called once, when the row first comes into view. */
  onVisible?: () => void
}) {
  const row = useRef<HTMLElement>(null)
  const seen = useRef(onVisible)
  useEffect(() => { seen.current = onVisible }, [onVisible])
  useEffect(() => {
    const node = row.current
    if (!node || !seen.current) return
    // Where nothing can say what is on screen, it counts as seen.
    if (typeof IntersectionObserver === 'undefined') {
      seen.current()
      return
    }
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((e) => e.isIntersecting)) {
        seen.current?.()
        observer.disconnect()
      }
    })
    observer.observe(node)
    return () => observer.disconnect()
  }, [])

  return (
    <section ref={row} aria-label={title} className="flex flex-col gap-2">
      <h3 className="text-sm font-semibold text-slate-800">{title}</h3>
      <ul
        className="-mx-3 flex snap-x gap-2 overflow-x-auto px-3 pb-1 [scrollbar-width:none]
                   sm:mx-0 sm:flex-wrap sm:overflow-visible sm:px-0"
      >
        {items.map((item) => (
          <li key={item.key} className="snap-start shrink-0">
            <button
              type="button"
              onClick={() => onPick(item.key)}
              title={item.hint}
              className="flex min-h-[44px] flex-col items-start rounded-xl border border-slate-200
                         bg-white px-3 py-2 text-left transition-colors hover:border-purple-300
                         hover:bg-purple-50/50 focus:outline-none focus-visible:ring-2
                         focus-visible:ring-purple-500/40 sm:min-h-0"
            >
              <span className="flex items-baseline gap-2 whitespace-nowrap">
                <span className="text-sm font-medium text-slate-800">{item.label}</span>
                {item.count != null && (
                  <span className="text-xs font-semibold tabular-nums text-purple-700">
                    {item.count}
                  </span>
                )}
              </span>
              <span className="text-[11px] text-slate-500 whitespace-nowrap">{item.hint}</span>
            </button>
          </li>
        ))}
      </ul>
      {extra}
    </section>
  )
}
