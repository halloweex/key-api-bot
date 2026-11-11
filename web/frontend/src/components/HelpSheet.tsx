import { useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { CircleHelp } from 'lucide-react'
import { BottomSheet } from './BottomSheet'
import { Popover } from './Popover'
import { useIsPhone } from '../hooks/useMediaQuery'

// ─── HelpSheet ───────────────────────────────────────────────────────────────
//
// An explanation one tap away, for text a block no longer needs to say every
// time it is opened. A phone gets a bottom sheet under the thumb; a wider
// screen a light panel under the control that opened it.
//
// The control is the round "?" — 44 px to the thumb, 18 px to the eye — or,
// with `link`, a line of text that says what it opens («почему не совпадает
// с воронкой»): an icon tells a reader only that there is more, a sentence
// tells them whether they want it. <InfoPopover>, the dark panel elsewhere on
// the dashboard, overflows a phone's edge and its 16 px button is too small to
// hit; text moved out of a block must not move somewhere nobody can open.

export function HelpSheet({
  title,
  label,
  link = false,
  children,
}: {
  /** The sheet's or panel's heading. */
  title: string
  /** The control's name to a screen reader; with `link`, its text. */
  label: string
  /** A line of text opens it, instead of the round "?". */
  link?: boolean
  children: ReactNode
}) {
  const { t } = useTranslation()
  const phone = useIsPhone()
  const [open, setOpen] = useState(false)

  const trigger = link ? (
    <button
      type="button"
      onClick={() => setOpen((v) => !v)}
      aria-expanded={open}
      className="min-h-11 text-left text-[13px] text-blue-700 underline decoration-blue-300 underline-offset-2
                 hover:text-blue-800 sm:min-h-0"
    >
      {label}<span aria-hidden="true"> ›</span>
    </button>
  ) : (
    <button
      type="button"
      onClick={() => setOpen((v) => !v)}
      aria-label={label}
      aria-expanded={open}
      className="-m-2.5 grid h-11 w-11 shrink-0 place-items-center rounded-full text-slate-400
                 hover:text-slate-600 sm:-m-1.5 sm:h-8 sm:w-8"
    >
      <CircleHelp className="h-[18px] w-[18px]" aria-hidden />
    </button>
  )

  const body = <div className="flex flex-col gap-2.5 text-sm leading-relaxed text-slate-700">{children}</div>

  if (phone) {
    return (
      <>
        {trigger}
        <BottomSheet open={open} title={title} onClose={() => setOpen(false)} closeLabel={t('traffic.close')}>
          {body}
        </BottomSheet>
      </>
    )
  }
  return (
    <Popover open={open} onClose={() => setOpen(false)} trigger={trigger} label={title}>
      <div className="flex flex-col gap-2 p-2">
        <h4 className="text-sm font-semibold text-slate-900">{title}</h4>
        {body}
      </div>
    </Popover>
  )
}
