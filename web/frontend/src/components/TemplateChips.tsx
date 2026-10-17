import { type ReactNode } from 'react'

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

export interface TemplateChip {
  key: string
  label: string
  hint: string
}

export function TemplateChips({
  title,
  items,
  onPick,
  extra,
}: {
  title: string
  items: TemplateChip[]
  onPick: (key: string) => void
  extra?: ReactNode
}) {
  return (
    <section aria-label={title} className="flex flex-col gap-2">
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
              <span className="text-sm font-medium text-slate-800 whitespace-nowrap">{item.label}</span>
              <span className="text-[11px] text-slate-500 whitespace-nowrap">{item.hint}</span>
            </button>
          </li>
        ))}
      </ul>
      {extra}
    </section>
  )
}
