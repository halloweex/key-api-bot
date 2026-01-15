import { useCallback, useMemo } from 'react'
import { Select } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { useBrands } from '../../hooks'

export function BrandFilter() {
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
      placeholder="All Brands"
      disabled={isLoading}
    />
  )
}
