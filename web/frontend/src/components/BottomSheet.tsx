import { useEffect, useId, useRef, type ReactNode } from 'react'
import { createPortal } from 'react-dom'
import { X } from 'lucide-react'
import { pushLayer } from '../utils/layerStack'

// ─── BottomSheet ─────────────────────────────────────────────────────────────
//
// The phone's way of asking a question without leaving the page: a panel that
// rises from the bottom, under the thumb, over a dimmed page (NN/g "Bottom
// Sheets", Material 3). Its desktop counterpart is <Popover>, which anchors
// the same content to the control that opened it.
//
// One level only. A sheet never opens another sheet on top of itself: a menu
// that leads to an editor closes and the editor opens in its place, so "back"
// is always one tap and nothing is buried under anything.
//
// Closing: the × in the header, a tap on the dimmed page, Escape, or the
// Telegram Mini App's BackButton (the sheet is on `utils/layerStack`). The
// footer slot holds the one action that ends the task («Готово»), sticky
// above the home indicator, so it is reachable however long the body is.
//
// `placement="side"` is for a record read beside a table (NN/g "Data Tables":
// a side panel, not an inline accordion): from `sm` up the same sheet stands
// at the right edge, full height; below it it is the phone's bottom sheet.

type Placement = 'bottom' | 'side'

export function BottomSheet({
  open,
  title,
  onClose,
  footer,
  closeLabel = 'Close',
  placement = 'bottom',
  children,
}: {
  open: boolean
  title: string
  onClose: () => void
  footer?: ReactNode
  closeLabel?: string
  placement?: Placement
  children: ReactNode
}) {
  const side = placement === 'side'
  const panel = useRef<HTMLDivElement>(null)
  const titleId = useId()
  // Held in a ref: a caller passing a fresh closure on every render must not
  // re-run the effect below, which moves focus — it would pull the cursor out
  // of the field being typed into on each keystroke.
  const close = useRef(onClose)
  useEffect(() => { close.current = onClose }, [onClose])

  useEffect(() => {
    if (!open) return
    const opener = document.activeElement as HTMLElement | null
    // The page behind must not scroll under a finger dragging inside the sheet.
    const overflow = document.body.style.overflow
    document.body.style.overflow = 'hidden'
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') close.current()
      // A modal sheet keeps Tab inside itself: the page behind is dimmed and
      // not part of the task, so focus must not wander into it.
      if (e.key === 'Tab' && panel.current) {
        const items = [...panel.current.querySelectorAll<HTMLElement>(
          'a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), '
          + 'textarea:not([disabled]), [tabindex]:not([tabindex="-1"])',
        )]
        if (!items.length) return
        const first = items[0]
        const last = items[items.length - 1]
        const active = document.activeElement
        if (e.shiftKey && (active === first || !panel.current.contains(active))) {
          e.preventDefault()
          last.focus()
        } else if (!e.shiftKey && (active === last || !panel.current.contains(active))) {
          e.preventDefault()
          first.focus()
        }
      }
    }
    document.addEventListener('keydown', onKey)
    // "Back" closes the sheet before anything under it — Telegram's
    // BackButton asks the stack (`utils/layerStack.ts`).
    const popLayer = pushLayer(() => close.current())
    // Into the sheet, so a keyboard or a screen reader is where the eye is.
    const first = panel.current?.querySelector<HTMLElement>(
      'input, select, textarea, button:not([data-sheet-close]), [tabindex]:not([tabindex="-1"])',
    )
    first?.focus()
    return () => {
      document.body.style.overflow = overflow
      document.removeEventListener('keydown', onKey)
      popLayer()
      opener?.focus?.()
    }
  }, [open])

  if (!open) return null

  return createPortal(
    <div className={side
      ? 'fixed inset-0 z-50 flex flex-col justify-end sm:flex-row'
      : 'fixed inset-0 z-50 flex flex-col justify-end'}
    >
      <div
        className="absolute inset-0 bg-slate-900/40"
        aria-hidden
        onClick={onClose}
      />
      <div
        ref={panel}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className={side
          ? 'relative flex max-h-[88vh] flex-col rounded-t-2xl bg-white shadow-2xl '
            + 'sm:h-full sm:max-h-none sm:w-[420px] sm:max-w-full sm:rounded-none'
          : 'relative flex max-h-[88vh] flex-col rounded-t-2xl bg-white shadow-2xl'}
      >
        <div className={side ? 'flex justify-center pt-2 sm:hidden' : 'flex justify-center pt-2'} aria-hidden>
          <span className="h-1 w-10 rounded-full bg-slate-300" />
        </div>
        <div className="flex items-center justify-between gap-3 px-4 pb-2 pt-1">
          <h2 id={titleId} className="text-base font-semibold text-slate-900">{title}</h2>
          <button
            type="button"
            data-sheet-close
            onClick={onClose}
            aria-label={closeLabel}
            className="grid h-9 w-9 place-items-center rounded-full text-slate-500
                       hover:bg-slate-100 hover:text-slate-800"
          >
            <X className="h-5 w-5" aria-hidden />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto overscroll-contain px-4 pb-4">
          {children}
        </div>
        {footer && (
          <div className="border-t border-slate-100 px-4 pt-3
                          pb-[max(0.75rem,env(safe-area-inset-bottom))]">
            {footer}
          </div>
        )}
      </div>
    </div>,
    document.body,
  )
}
