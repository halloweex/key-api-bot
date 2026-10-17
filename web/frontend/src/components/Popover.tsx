import { useEffect, useId, useRef, type ReactNode } from 'react'

// ─── Popover ─────────────────────────────────────────────────────────────────
//
// <BottomSheet>'s desktop counterpart: the same content, anchored under the
// control that opened it instead of rising from the bottom of the screen. A
// light panel, not the dark help tooltip <InfoPopover> is — this holds
// choices, not an explanation.
//
// The caller renders the trigger and owns `open`; the popover only places
// itself and closes on a click outside it or on Escape. It is positioned in
// the flow of its wrapper (absolute under it), so it scrolls with the page and
// never needs measuring.

export function Popover({
  open,
  onClose,
  trigger,
  align = 'start',
  label,
  children,
}: {
  open: boolean
  onClose: () => void
  /** The control that opens it; stays in place while it is open. */
  trigger: ReactNode
  /** Which edge of the trigger the panel lines up with. */
  align?: 'start' | 'end'
  /** What the panel is, to a screen reader. */
  label: string
  children: ReactNode
}) {
  const wrapper = useRef<HTMLDivElement>(null)
  const panel = useRef<HTMLDivElement>(null)
  const id = useId()

  // Into the panel when it opens, so the keyboard is where the eye is.
  useEffect(() => {
    if (!open) return
    panel.current?.querySelector<HTMLElement>(
      'button:not([disabled]), input, select, [tabindex]:not([tabindex="-1"])',
    )?.focus()
  }, [open])
  const close = useRef(onClose)
  useEffect(() => { close.current = onClose }, [onClose])

  useEffect(() => {
    if (!open) return
    const onDown = (e: MouseEvent) => {
      if (wrapper.current && !wrapper.current.contains(e.target as Node)) close.current()
    }
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') close.current()
    }
    document.addEventListener('mousedown', onDown)
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('mousedown', onDown)
      document.removeEventListener('keydown', onKey)
    }
  }, [open])

  return (
    <div ref={wrapper} className="relative inline-block">
      {trigger}
      {open && (
        <div
          ref={panel}
          id={id}
          role="dialog"
          aria-label={label}
          className={`absolute top-full z-40 mt-2 w-[min(26rem,calc(100vw-2rem))]
                      rounded-xl border border-slate-200 bg-white p-2 shadow-xl ${
            align === 'end' ? 'right-0' : 'left-0'
          }`}
        >
          {children}
        </div>
      )}
    </div>
  )
}
