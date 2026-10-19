import { useCallback } from 'react'
import { useQueryParams } from '../store/filterStore'
import { exportCsvFromUrl } from '../utils/deliverFile'
import { useFileExport } from './useFileExport'

export function useDownloadCsv() {
  const queryParams = useQueryParams()
  const { run } = useFileExport()

  return useCallback(
    (type: 'summary' | 'top_products', extra?: string) => {
      const params = new URLSearchParams(queryParams)
      params.set('type', type)
      if (extra) {
        const extraParams = new URLSearchParams(extra)
        extraParams.forEach((v, k) => params.set(k, v))
      }
      void run(() => exportCsvFromUrl(`/api/reports/export/csv?${params.toString()}`, `report-${type}.csv`))
    },
    [queryParams, run],
  )
}
