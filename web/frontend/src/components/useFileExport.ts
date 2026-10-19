import { useCallback, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useToast } from './Toast'
import type { Delivery } from '../utils/deliverFile'

/**
 * Runs an export from `utils/deliverFile` and says what happened. A browser
 * download speaks for itself; a file the bot sent, or one that failed, does
 * not — inside Telegram the button used to do nothing and say nothing.
 */
export function useFileExport() {
  const { t } = useTranslation()
  const { addToast } = useToast()
  const [busy, setBusy] = useState(false)

  const run = useCallback(async (deliver: () => Promise<Delivery>) => {
    setBusy(true)
    try {
      if ((await deliver()) === 'sent') {
        addToast({ type: 'success', title: t('fileExport.sent'), message: t('fileExport.sentMessage') })
      }
    } catch (e) {
      addToast({
        type: 'error',
        title: t('fileExport.failed'),
        message: e instanceof Error ? e.message : String(e),
      })
    } finally {
      setBusy(false)
    }
  }, [addToast, t])

  return { run, busy }
}
