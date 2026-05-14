import { useCallback } from 'react'
import { useQueryParams } from '../store/filterStore'

export function useDownloadCsv() {
  const queryParams = useQueryParams()

  return useCallback(
    (type: 'summary' | 'top_products', extra?: string) => {
      const params = new URLSearchParams(queryParams)
      params.set('type', type)
      if (extra) {
        const extraParams = new URLSearchParams(extra)
        extraParams.forEach((v, k) => params.set(k, v))
      }
      window.open(`/api/reports/export/csv?${params.toString()}`, '_blank')
    },
    [queryParams],
  )
}
