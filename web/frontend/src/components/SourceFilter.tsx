import { useCallback, useMemo } from 'react'
import { useTranslation } from 'react-i18next'
import { Select } from './Select'
import { useFilterStore } from '../store/filterStore'
import { OFFLINE_STORE_SOURCE_ID, sourceName } from '../utils/sources'

// The sources a page can be narrowed to. The offline store answers on every
// route that takes `source_id`; the exhibition (5) is refused there, so it is
// not offered.
const SOURCES = [
  { id: 1, name: 'Instagram' },
  { id: 2, name: 'Telegram' },
  { id: 4, name: 'Shopify' },
  { id: OFFLINE_STORE_SOURCE_ID, name: 'Офлайн-магазин' },
]

export function SourceFilter() {
  const { t } = useTranslation()
  const { sourceId, setSourceId } = useFilterStore()

  const options = useMemo(() =>
    SOURCES.map(source => ({
      value: String(source.id),
      label: sourceName(source.id, source.name, t),
    })),
    [t]
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
