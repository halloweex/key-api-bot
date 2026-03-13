import { useCallback, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Select } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { usePromocodes } from '../../hooks'

export function PromocodeFilter() {
  const { t } = useTranslation()
  const { promocode, setPromocode } = useFilterStore()
  const { data: promocodes, isLoading } = usePromocodes()

  const options = useMemo(() => {
    if (!promocodes) return []
    return promocodes
      .map(p => ({
        value: p.name,
        label: p.name,
      }))
      .sort((a, b) => a.label.localeCompare(b.label))
  }, [promocodes])

  const handleChange = useCallback((value: string | null) => {
    setPromocode(value)
  }, [setPromocode])

  return (
    <Select
      options={options}
      value={promocode}
      onChange={handleChange}
      placeholder={t('filter.allPromocodes')}
      disabled={isLoading}
    />
  )
}
