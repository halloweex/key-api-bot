import { useCallback, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Select } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { useBrands } from '../../hooks'

export function BrandFilter() {
  const { t } = useTranslation()
  const { brand, setBrand } = useFilterStore()
  const { data: brands, isLoading } = useBrands()

  const options = useMemo(() => {
    if (!brands) return []
    return brands
      .map(b => ({
        value: b.name,
        label: b.name,
      }))
      .sort((a, b) => a.label.localeCompare(b.label))
  }, [brands])

  const handleChange = useCallback((value: string | null) => {
    setBrand(value)
  }, [setBrand])

  return (
    <Select
      options={options}
      value={brand}
      onChange={handleChange}
      placeholder={t('filter.allBrands')}
      disabled={isLoading}
    />
  )
}
