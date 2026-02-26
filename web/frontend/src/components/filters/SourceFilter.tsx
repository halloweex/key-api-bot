import { useCallback, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Select } from '../ui'
import { useFilterStore } from '../../store/filterStore'

const SOURCES = [
  { id: 1, name: 'Instagram' },
  { id: 2, name: 'Telegram' },
  { id: 4, name: 'Shopify' },
]

export function SourceFilter() {
  const { t } = useTranslation()
  const { sourceId, setSourceId } = useFilterStore()

  const options = useMemo(() =>
    SOURCES.map(source => ({
      value: String(source.id),
      label: source.name,
    })),
    []
  )

  const handleChange = useCallback((value: string | null) => {
    setSourceId(value ? Number(value) : null)
  }, [setSourceId])

  return (
    <Select
      options={options}
      value={sourceId ? String(sourceId) : null}
      onChange={handleChange}
      placeholder={t('filter.allSources')}
    />
  )
}
