import { useCallback, useMemo } from 'react'
import { Select } from '../ui'
import { useFilterStore } from '../../store/filterStore'
import { useCategories, useChildCategories } from '../../hooks'

export function CategoryFilter() {
  const { categoryId, setCategoryId } = useFilterStore()
  const { data: categories, isLoading: loadingCategories } = useCategories()

  // Find selected category to determine if it's a parent
  const selectedParentId = useMemo(() => {
    if (!categoryId || !categories?.length) return null
    const isParent = categories.some(c => c.id === categoryId)
    return isParent ? categoryId : null
  }, [categoryId, categories])

  const { data: childCategories } = useChildCategories(selectedParentId)

  // Build flat options list with hierarchy indication
  const options = useMemo(() => {
    if (!categories) return []

    const result: { value: string; label: string }[] = []

    categories.forEach(parent => {
      result.push({
        value: String(parent.id),
        label: parent.name,
      })
    })

    return result
  }, [categories])

  const childOptions = useMemo(() => {
    if (!childCategories) return []
    return childCategories.map(child => ({
      value: String(child.id),
      label: child.name,
    }))
  }, [childCategories])

  const handleParentChange = useCallback((value: string | null) => {
    setCategoryId(value ? Number(value) : null)
  }, [setCategoryId])

  const handleChildChange = useCallback((value: string | null) => {
    if (value) {
      setCategoryId(Number(value))
    } else if (selectedParentId) {
      // Reset to parent when clearing child
      setCategoryId(selectedParentId)
    }
  }, [setCategoryId, selectedParentId])

  // Determine current child selection
  const currentChildId = useMemo(() => {
    if (!categoryId || !childCategories?.length) return null
    const isChild = childCategories.some(c => c.id === categoryId)
    return isChild ? categoryId : null
  }, [categoryId, childCategories])

  return (
    <div className="flex items-center gap-2">
      <Select
        options={options}
        value={selectedParentId ? String(selectedParentId) : (categoryId ? String(categoryId) : null)}
        onChange={handleParentChange}
        placeholder="All Categories"
        disabled={loadingCategories}
      />
      {childOptions.length > 0 && (
        <Select
          options={childOptions}
          value={currentChildId ? String(currentChildId) : null}
          onChange={handleChildChange}
          placeholder="All Subcategories"
        />
      )}
    </div>
  )
}
