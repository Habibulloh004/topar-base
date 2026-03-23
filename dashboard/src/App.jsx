import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'

function resolveApiBaseUrl() {
  const configured = String(import.meta.env.VITE_API_BASE_URL || '').trim()
  if (configured) return configured.replace(/\/+$/, '')

  // In production, prefer same-origin Nginx proxy (/api -> backend).
  if (typeof window !== 'undefined') {
    const host = String(window.location.hostname || '').toLowerCase()
    if (host !== 'localhost' && host !== '127.0.0.1') return '/api'
  }

  return 'http://localhost:8090'
}

const API_BASE_URL = resolveApiBaseUrl()
const DEFAULT_PRODUCTS_LIMIT = 20
const MAX_PRODUCTS_LIMIT = 100
const MAX_PAGE_SIZE = 200
const PAGE_SIZE_OPTIONS = [5, 10, 20, 30, 40, 50, 100, 200]
const CARD_COLUMNS_OPTIONS = [2, 3, 4, 5, 6]
const EMPTY_SELECTED_KEYS = Object.freeze([])
const ACTIVE_PAGE_STORAGE_KEY = 'topar_dashboard_active_page'
const MAIN_PRODUCTS_LIMIT_STORAGE_KEY = 'topar_dashboard_main_products_limit'
const MAIN_PRODUCTS_VIEW_MODE_STORAGE_KEY = 'topar_dashboard_main_products_view_mode'
const MAIN_PRODUCTS_CARD_COLUMNS_STORAGE_KEY = 'topar_dashboard_main_products_card_columns'
const MAIN_PRODUCTS_WITHOUT_ISBN_ONLY_STORAGE_KEY = 'topar_dashboard_main_products_without_isbn_only'
const SEARCH_DEBOUNCE_MS = 300
const META_CACHE_STORAGE_KEY = 'topar_dashboard_meta_cache_v1'
const META_CACHE_MAX_AGE_MS = 24 * 60 * 60 * 1000
const AGE_FILTER_OPTIONS = ['0+', '6+', '12+', '16+', '18+']
const ALLOWED_ACTIVE_PAGES = ['mainProducts', 'duplicates']
const MAIN_PRODUCTS_OTHER_NODE_KEY = 'other'

const EMPTY_META = {
  subjects: [],
  brands: [],
  series: [],
  publishers: [],
  authors: [],
  tags: [],
  genres: []
}

function normalizeMetaPayload(data) {
  return {
    subjects: Array.isArray(data?.subjects) ? data.subjects : [],
    brands: Array.isArray(data?.brands) ? data.brands : [],
    series: Array.isArray(data?.series) ? data.series : [],
    publishers: Array.isArray(data?.publishers) ? data.publishers : [],
    authors: Array.isArray(data?.authors) ? data.authors : [],
    tags: Array.isArray(data?.tags) ? data.tags : [],
    genres: Array.isArray(data?.genres) ? data.genres : []
  }
}

function readMetaCache() {
  if (typeof window === 'undefined') return null
  try {
    const raw = window.localStorage.getItem(META_CACHE_STORAGE_KEY)
    if (!raw) return null
    const parsed = JSON.parse(raw)
    const savedAt = Number(parsed?.savedAt || 0)
    if (!Number.isFinite(savedAt) || Date.now() - savedAt > META_CACHE_MAX_AGE_MS) return null
    return normalizeMetaPayload(parsed?.data || EMPTY_META)
  } catch (_) {
    return null
  }
}

function writeMetaCache(data) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(
      META_CACHE_STORAGE_KEY,
      JSON.stringify({
        savedAt: Date.now(),
        data: normalizeMetaPayload(data)
      })
    )
  } catch (_) {}
}

function readStoredMainProductsLimit() {
  if (typeof window === 'undefined') return DEFAULT_PRODUCTS_LIMIT
  const raw = String(window.localStorage.getItem(MAIN_PRODUCTS_LIMIT_STORAGE_KEY) || '').trim()
  if (!raw) return DEFAULT_PRODUCTS_LIMIT
  return clampQuantity(raw)
}

function readStoredMainProductsViewMode() {
  if (typeof window === 'undefined') return 'list'
  const raw = String(window.localStorage.getItem(MAIN_PRODUCTS_VIEW_MODE_STORAGE_KEY) || '').trim()
  return raw === 'cards' || raw === 'list' ? raw : 'list'
}

function readStoredMainProductsCardColumns() {
  if (typeof window === 'undefined') return 4
  const raw = String(window.localStorage.getItem(MAIN_PRODUCTS_CARD_COLUMNS_STORAGE_KEY) || '').trim()
  if (!raw) return 4
  return clampCardColumns(raw)
}

function readStoredMainProductsWithoutIsbnOnly() {
  if (typeof window === 'undefined') return false
  const raw = String(window.localStorage.getItem(MAIN_PRODUCTS_WITHOUT_ISBN_ONLY_STORAGE_KEY) || '')
    .trim()
    .toLowerCase()
  return raw === '1' || raw === 'true'
}

function writeDashboardSetting(key, value) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(key, String(value))
  } catch (_) {}
}

const EMPTY_MAIN_PRODUCT_FORM = {
  name: '',
  isbn: '',
  authorCover: '',
  authorNames: '',
  authorRefsJson: '',
  tagRefsJson: '',
  genreRefsJson: '',
  tagNames: '',
  genreNames: '',
  annotation: '',
  coverUrl: '',
  coverUrls: [],
  pages: '',
  format: '',
  paperType: '',
  bindingType: '',
  ageRestriction: '',
  subjectName: '',
  nicheName: '',
  brandName: '',
  seriesName: '',
  publisherName: '',
  categoryPath: '',
  quantity: '',
  price: '',
  categoryId: '',
  sourceGuidNom: '',
  sourceGuid: '',
  sourceNomcode: ''
}

const FRONTEND_UPLOAD_DEBUG = String(import.meta.env.VITE_UPLOAD_DEBUG || '1') !== '0'

function frontendDebugLog(level, event, details = undefined) {
  if (!FRONTEND_UPLOAD_DEBUG) return
  const timestamp = new Date().toISOString()
  const prefix = `[main-products-ui] ${timestamp} ${event}`
  if (details === undefined) {
    console[level](prefix)
    return
  }
  console[level](prefix, details)
}

function useDebounce(value, delay) {
  const [debouncedValue, setDebouncedValue] = useState(value)

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedValue(value)
    }, delay)
    return () => clearTimeout(timer)
  }, [value, delay])

  return debouncedValue
}

function normalizeSearchQuery(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
}

function isEditableElement(target) {
  if (!target || typeof target !== 'object') return false
  const tagName = String(target.tagName || '').toUpperCase()
  return Boolean(target.isContentEditable || tagName === 'INPUT' || tagName === 'TEXTAREA' || tagName === 'SELECT')
}

function isMongoObjectId(value) {
  return /^[a-f\d]{24}$/i.test(String(value || '').trim())
}

function getEksmoProductKey(product) {
  return product.guidNom || product.guid || product.id
}

function getEksmoMongoId(product) {
  if (isMongoObjectId(product?.id)) return String(product.id)
  return ''
}

function getMainProductId(product) {
  if (isMongoObjectId(product?.id)) return String(product.id)
  return ''
}

function pickFirstFilled(...values) {
  for (const value of values) {
    const text = String(value || '').trim()
    if (text) return text
  }
  return ''
}

function normalizeCompareCode(value) {
  return String(value || '')
    .trim()
    .toUpperCase()
    .replace(/[^\p{L}\p{N}]+/gu, '')
}

function getProductCompareValue(product, field) {
  if (field === 'isbn') {
    const raw = pickFirstFilled(product?.isbn, product?.isbnNormalized)
    return { raw, normalized: normalizeCompareCode(raw) }
  }
  if (field === 'code') {
    const raw = pickFirstFilled(product?.code, product?.nomcode, product?.sourceNomcode, product?.sku)
    return { raw, normalized: normalizeCompareCode(raw) }
  }
  if (field === 'barcode') {
    const raw = pickFirstFilled(product?.barcode, product?.nomcode, product?.sourceNomcode, product?.code)
    return { raw, normalized: normalizeCompareCode(raw) }
  }
  if (field === 'gtin') {
    const raw = pickFirstFilled(product?.gtin)
    return { raw, normalized: normalizeCompareCode(raw) }
  }
  return { raw: '', normalized: '' }
}

function getProductComparableEntries(product) {
  const fields = ['isbn', 'code', 'barcode', 'gtin']
  const seen = new Set()
  const result = []

  for (const field of fields) {
    const value = getProductCompareValue(product, field)
    if (!value.normalized) continue
    const key = `${field}:${value.normalized}`
    if (seen.has(key)) continue
    seen.add(key)
    result.push({
      field,
      rawValue: value.raw || value.normalized,
      normalizedValue: value.normalized
    })
  }

  return result
}

function buildDuplicateGroups(products) {
  if (!Array.isArray(products) || products.length === 0) return []
  const groupsByValue = new Map()

  for (const product of products) {
    const productKey = getEksmoMongoId(product) || String(getEksmoProductKey(product) || '').trim()
    if (!productKey) continue

    const entries = getProductComparableEntries(product)
    if (entries.length === 0) continue

    // Deduplicate equal values inside one product; we compare with other products only.
    const valueToFields = new Map()
    const valueToRaw = new Map()
    for (const entry of entries) {
      if (!valueToFields.has(entry.normalizedValue)) valueToFields.set(entry.normalizedValue, new Set())
      valueToFields.get(entry.normalizedValue).add(entry.field)
      if (!valueToRaw.has(entry.normalizedValue)) valueToRaw.set(entry.normalizedValue, entry.rawValue)
    }

    for (const [normalizedValue, fieldsSet] of valueToFields.entries()) {
      const groupKey = `value:${normalizedValue}`
      let group = groupsByValue.get(groupKey)
      if (!group) {
        group = {
          key: groupKey,
          rawValue: valueToRaw.get(normalizedValue) || normalizedValue,
          normalizedValue,
          productsMap: new Map(),
          fieldsSet: new Set()
        }
        groupsByValue.set(groupKey, group)
      }

      const fields = [...fieldsSet].sort()
      fields.forEach((field) => group.fieldsSet.add(field))
      group.productsMap.set(productKey, {
        product,
        fields
      })
    }
  }

  return [...groupsByValue.values()]
    .filter((group) => group.productsMap.size > 1)
    .map((group) => ({
      key: group.key,
      rawValue: group.rawValue,
      normalizedValue: group.normalizedValue,
      fields: [...group.fieldsSet].sort(),
      products: [...group.productsMap.values()].sort((left, right) =>
        String(left?.product?.name || '').localeCompare(String(right?.product?.name || ''))
      )
    }))
    .sort((left, right) => {
      if (right.products.length !== left.products.length) return right.products.length - left.products.length
      return left.normalizedValue.localeCompare(right.normalizedValue)
    })
}

function clampQuantity(value) {
  const parsed = Number.parseInt(String(value), 10)
  if (!Number.isFinite(parsed) || parsed < 1) return 1
  if (parsed > MAX_PAGE_SIZE) return MAX_PAGE_SIZE
  return parsed
}

function clampCardColumns(value) {
  const parsed = Number.parseInt(String(value), 10)
  if (!Number.isFinite(parsed)) return 4
  if (parsed < 2) return 2
  if (parsed > 6) return 6
  return parsed
}

function splitIntoChunks(items, chunkSize) {
  if (!Array.isArray(items) || items.length === 0) return []
  const safeChunkSize = Math.max(1, Number(chunkSize) || 1)
  const result = []
  for (let index = 0; index < items.length; index += safeChunkSize) {
    result.push(items.slice(index, index + safeChunkSize))
  }
  return result
}

function getPaginationItems(currentPage, totalPages) {
  if (!Number.isFinite(totalPages) || totalPages <= 0) return []

  if (totalPages <= 7) {
    return Array.from({ length: totalPages }, (_, index) => ({ type: 'page', value: index + 1 }))
  }

  const current = Math.min(Math.max(1, currentPage), totalPages)
  const items = [{ type: 'page', value: 1 }]

  if (current <= 4) {
    for (let page = 2; page <= 5; page += 1) items.push({ type: 'page', value: page })
    items.push({ type: 'ellipsis', value: 'right' })
  } else if (current >= totalPages - 3) {
    items.push({ type: 'ellipsis', value: 'left' })
    for (let page = totalPages - 4; page <= totalPages - 1; page += 1) items.push({ type: 'page', value: page })
  } else {
    items.push({ type: 'ellipsis', value: 'left' })
    items.push({ type: 'page', value: current - 1 })
    items.push({ type: 'page', value: current })
    items.push({ type: 'page', value: current + 1 })
    items.push({ type: 'ellipsis', value: 'right' })
  }

  items.push({ type: 'page', value: totalPages })
  return items
}

function formatMainMetric(value, maxFractionDigits = 2) {
  const numeric = Number(value)
  if (!Number.isFinite(numeric)) return '-'
  return numeric.toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: maxFractionDigits
  })
}

function flattenMainCategoryOptions(nodes, path = []) {
  if (!Array.isArray(nodes) || nodes.length === 0) return []
  const result = []
  for (const node of nodes) {
    const currentPath = [...path, node.name]
    result.push({ id: node.id, path: currentPath, label: currentPath.join(' / ') })
    if (Array.isArray(node.children) && node.children.length > 0) {
      result.push(...flattenMainCategoryOptions(node.children, currentPath))
    }
  }
  return result
}

function flattenTreeNodeIds(nodes) {
  if (!Array.isArray(nodes) || nodes.length === 0) return []
  const result = []
  const walk = (items) => {
    for (const item of items) {
      const id = String(item?.id || '').trim()
      if (id) result.push(id)
      if (Array.isArray(item?.children) && item.children.length > 0) walk(item.children)
    }
  }
  walk(nodes)
  return result
}

function filterTreeNodesByQuery(nodes, normalizedQuery) {
  if (!Array.isArray(nodes) || nodes.length === 0) return []
  if (!normalizedQuery) return nodes

  const result = []
  for (const node of nodes) {
    const nodeName = String(node?.name || '').toLowerCase()
    const children = Array.isArray(node?.children) ? node.children : []
    const filteredChildren = filterTreeNodesByQuery(children, normalizedQuery)
    if (nodeName.includes(normalizedQuery) || filteredChildren.length > 0) {
      result.push({
        ...node,
        children: filteredChildren
      })
    }
  }
  return result
}

function filterEksmoTreeNodes(nodes, normalizedQuery) {
  if (!Array.isArray(nodes) || nodes.length === 0) return []
  if (!normalizedQuery) return nodes

  const matches = (value) => String(value || '').toLowerCase().includes(normalizedQuery)

  const walk = (items) => {
    const result = []
    for (const node of items) {
      const children = Array.isArray(node.children) ? node.children : []
      const subjects = Array.isArray(node.subjects) ? node.subjects : []
      const nodeMatches = matches(node.name)

      if (nodeMatches) {
        result.push(node)
        continue
      }

      const filteredChildren = walk(children)
      const filteredSubjects = subjects.filter((subject) => matches(subject?.name))
      if (filteredChildren.length === 0 && filteredSubjects.length === 0) continue

      result.push({
        ...node,
        children: filteredChildren,
        subjects: filteredSubjects,
      })
    }
    return result
  }

  return walk(nodes)
}

function splitCommaValue(value) {
  if (!value) return []
  return String(value)
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function normalizeStringOptions(values) {
  if (!Array.isArray(values) || values.length === 0) return []
  const seen = new Set()
  const result = []
  for (const value of values) {
    const text = String(value || '').trim()
    if (!text || seen.has(text)) continue
    seen.add(text)
    result.push(text)
  }
  return result
}

function joinFilterValues(values) {
  if (!Array.isArray(values) || values.length === 0) return ''
  return values.join(',')
}

function parseOptionalNumber(value) {
  const parsed = Number.parseFloat(String(value).trim().replace(',', '.'))
  if (!Number.isFinite(parsed)) return 0
  return parsed
}

function parseOptionalInteger(value) {
  const parsed = Number.parseInt(String(value).trim(), 10)
  if (!Number.isFinite(parsed) || parsed < 0) return 0
  return parsed
}

function formatOptionalNumberForInput(value) {
  const parsed = Number(value)
  if (!Number.isFinite(parsed) || parsed === 0) return ''
  return String(parsed)
}

function splitPathValue(value) {
  const text = String(value || '').trim()
  if (!text) return []
  if (text.includes(' / ')) {
    return text
      .split(' / ')
      .map((item) => item.trim())
      .filter(Boolean)
  }
  return text
    .replace(/\|/g, ',')
    .split(',')
    .map((item) => item.trim())
    .filter(Boolean)
}

function formatRefsJsonInput(value) {
  if (!Array.isArray(value) || value.length === 0) return ''
  try {
    return JSON.stringify(value, null, 2)
  } catch (_) {
    return ''
  }
}

function parseRefsJsonInput(value, fieldLabel) {
  const text = String(value || '').trim()
  if (!text) return []

  let parsed
  try {
    parsed = JSON.parse(text)
  } catch (_) {
    throw new Error(`Поле «${fieldLabel} (JSON)» содержит неверный JSON.`)
  }

  if (!Array.isArray(parsed)) {
    throw new Error(`Поле «${fieldLabel} (JSON)» должно быть JSON-массивом.`)
  }

  return parsed
}

function sanitizeAuthorRefs(values) {
  if (!Array.isArray(values) || values.length === 0) return []
  return values
    .map((item) => {
      if (!item || typeof item !== 'object') return null
      const guid = String(item.guid || '').trim()
      const code = String(item.code || '').trim()
      const name = String(item.name || '').trim()
      if (!guid && !code && !name) return null
      return {
        guid,
        code,
        name,
        isWriter: Boolean(item.isWriter),
        isTranslator: Boolean(item.isTranslator),
        isArtist: Boolean(item.isArtist)
      }
    })
    .filter(Boolean)
}

function sanitizeTagGenreRefs(values) {
  if (!Array.isArray(values) || values.length === 0) return []
  return values
    .map((item) => {
      if (!item || typeof item !== 'object') return null
      const guid = String(item.guid || '').trim()
      const name = String(item.name || '').trim()
      if (!guid && !name) return null
      return { guid, name }
    })
    .filter(Boolean)
}

function extractRefNames(values) {
  if (!Array.isArray(values) || values.length === 0) return []
  return values
    .map((item) => String(item?.name || '').trim())
    .filter(Boolean)
}

function normalizeCoverUrls(values) {
  if (!Array.isArray(values)) return []
  const seen = new Set()
  const result = []
  for (const raw of values) {
    const value = String(raw || '').trim()
    if (!value || seen.has(value)) continue
    seen.add(value)
    result.push(value)
  }
  return result
}

function extractCoverUrlsFromProduct(product) {
  const result = []
  if (product?.coverUrl) result.push(String(product.coverUrl))
  if (product?.covers && typeof product.covers === 'object') {
    Object.entries(product.covers)
      .sort(([left], [right]) => left.localeCompare(right, undefined, { numeric: true }))
      .forEach(([, coverUrl]) => result.push(String(coverUrl || '')))
  }
  return normalizeCoverUrls(result)
}

function normalizeImageValue(value) {
  const text = String(value || '').trim()
  if (!text) return ''
  if (/^https?:\/\//i.test(text) || text.startsWith('data:') || text.startsWith('blob:') || text.startsWith('//')) {
    return text
  }
  if (text.startsWith('/')) return text
  return `/${text}`
}

function resolveImageUrl(value) {
  const normalized = normalizeImageValue(value)
  if (!normalized) return ''
  if (/^https?:\/\//i.test(normalized) || normalized.startsWith('data:') || normalized.startsWith('blob:') || normalized.startsWith('//')) {
    return normalized
  }

  const base = String(API_BASE_URL || '').trim().replace(/\/+$/, '')
  if (!base) return normalized
  return `${base}${normalized}`
}

function App() {
  const [activePage, setActivePage] = useState(() => {
    if (typeof window === 'undefined') return 'mainProducts'
    const stored = String(window.localStorage.getItem(ACTIVE_PAGE_STORAGE_KEY) || '').trim()
    return ALLOWED_ACTIVE_PAGES.includes(stored) ? stored : 'mainProducts'
  }) // mainProducts | duplicates

  const [mainCategories, setMainCategories] = useState([])
  const [mainCategoriesLoading, setMainCategoriesLoading] = useState(true)
  const [mainCategoriesExpanded, setMainCategoriesExpanded] = useState({})
  const [mainProductsExpanded, setMainProductsExpanded] = useState({})

  const [selectedMainCategory, setSelectedMainCategory] = useState(null) // for copy target
  const [selectedMainProductsCategories, setSelectedMainProductsCategories] = useState([]) // for category linking from sidebar

  const [eksmoTree, setEksmoTree] = useState([])
  const [eksmoTreeLoading, setEksmoTreeLoading] = useState(true)
  const [eksmoExpanded, setEksmoExpanded] = useState({})
  const [isCategoryDropdownOpen, setIsCategoryDropdownOpen] = useState(false)
  const [selectedEksmoNodes, setSelectedEksmoNodes] = useState([])
  const [categorySearchQuery, setCategorySearchQuery] = useState('')
  const categoryDropdownRef = useRef(null)
  const categoryDropdownButtonRef = useRef(null)
  const categoryDropdownMenuRef = useRef(null)
  const productsSearchInputRef = useRef(null)
  const mainProductsSearchInputRef = useRef(null)
  const [categoryDropdownStyle, setCategoryDropdownStyle] = useState({})

  const [products, setProducts] = useState([])
  const [productsLoading, setProductsLoading] = useState(true)
  const [productsError, setProductsError] = useState('')
  const [productsPage, setProductsPage] = useState(1)
  const [productsLimit, setProductsLimit] = useState(DEFAULT_PRODUCTS_LIMIT)
  const [productsTotalPages, setProductsTotalPages] = useState(0)
  const [productsTotalItems, setProductsTotalItems] = useState(0)

  const [searchInput, setSearchInput] = useState('')
  const normalizedProductsSearchInput = useMemo(() => normalizeSearchQuery(searchInput), [searchInput])
  const search = useDebounce(normalizedProductsSearchInput, SEARCH_DEBOUNCE_MS)
  const [brandFilter, setBrandFilter] = useState([])
  const [serieFilter, setSerieFilter] = useState([])
  const [publisherFilter, setPublisherFilter] = useState([])
  const [authorFilter, setAuthorFilter] = useState([])
  const [genreFilter, setGenreFilter] = useState([])
  const [ageFilter, setAgeFilter] = useState([])

  const [selectedProductIds, setSelectedProductIds] = useState([])
  const [viewMode, setViewMode] = useState('document')
  const [copying, setCopying] = useState(false)
  const [copyStatus, setCopyStatus] = useState('')
  const [eksmoActionKey, setEksmoActionKey] = useState('')

  const [meta, setMeta] = useState(EMPTY_META)
  const [metaLoading, setMetaLoading] = useState(true)

  const [syncing, setSyncing] = useState(false)
  const [syncStatus, setSyncStatus] = useState('')
  const [syncVersion, setSyncVersion] = useState(0)

  const [mainProducts, setMainProducts] = useState([])
  const [mainProductsLoading, setMainProductsLoading] = useState(true)
  const [mainProductsError, setMainProductsError] = useState('')
  const [mainProductsPage, setMainProductsPage] = useState(1)
  const [mainProductsLimit, setMainProductsLimit] = useState(() => readStoredMainProductsLimit())
  const [mainProductsTotalPages, setMainProductsTotalPages] = useState(0)
  const [mainProductsTotalItems, setMainProductsTotalItems] = useState(0)
  const [mainProductsViewMode, setMainProductsViewMode] = useState(() => readStoredMainProductsViewMode())
  const [mainProductsCardColumns, setMainProductsCardColumns] = useState(() => readStoredMainProductsCardColumns())
  const [mainProductsSearchInput, setMainProductsSearchInput] = useState('')
  const normalizedMainProductsSearchInput = useMemo(() => normalizeSearchQuery(mainProductsSearchInput), [mainProductsSearchInput])
  const mainProductsSearch = useDebounce(normalizedMainProductsSearchInput, SEARCH_DEBOUNCE_MS)
  const [mainProductsCategoryFilter, setMainProductsCategoryFilter] = useState([])
  const [mainProductsWithoutISBNOnly, setMainProductsWithoutISBNOnly] = useState(() => readStoredMainProductsWithoutIsbnOnly())
  const [mainProductsSourceCategories, setMainProductsSourceCategories] = useState([])
  const [mainProductsSourceCategoriesLoading, setMainProductsSourceCategoriesLoading] = useState(false)
  const [mainProductsVersion, setMainProductsVersion] = useState(0)
  const [mainProductsStatus, setMainProductsStatus] = useState('')
  const [mainProductsActionKey, setMainProductsActionKey] = useState('')
  const [selectedMainProductIds, setSelectedMainProductIds] = useState([])
  const [selectAllFilteredMainProducts, setSelectAllFilteredMainProducts] = useState(false)
  const [excludedFilteredMainProductIds, setExcludedFilteredMainProductIds] = useState([])
  const [mainProductsBillzSyncing, setMainProductsBillzSyncing] = useState(false)
  const [mainProductsImporting, setMainProductsImporting] = useState(false)
  const [mainProductModalOpen, setMainProductModalOpen] = useState(false)
  const [mainProductCreating, setMainProductCreating] = useState(false)
  const [mainProductEditModalOpen, setMainProductEditModalOpen] = useState(false)
  const [mainProductEditing, setMainProductEditing] = useState(false)
  const [mainProductImageUploading, setMainProductImageUploading] = useState(false)
  const [editingMainProductId, setEditingMainProductId] = useState('')
  const [mainProductForm, setMainProductForm] = useState(EMPTY_MAIN_PRODUCT_FORM)
  const [duplicateProducts, setDuplicateProducts] = useState([])
  const [duplicatesLoading, setDuplicatesLoading] = useState(false)
  const [duplicatesError, setDuplicatesError] = useState('')
  const [duplicatesStatus, setDuplicatesStatus] = useState('')
  const [duplicateDeleteId, setDuplicateDeleteId] = useState('')
  const [duplicateBulkActionKey, setDuplicateBulkActionKey] = useState('')
  const [selectedDuplicateIds, setSelectedDuplicateIds] = useState([])
  const [duplicatesScanKey, setDuplicatesScanKey] = useState(0)
  const [duplicatesLastScannedKey, setDuplicatesLastScannedKey] = useState(-1)
  const [toast, setToast] = useState(null)
  const [detailsProduct, setDetailsProduct] = useState(null)
  const mainProductsImportInputRef = useRef(null)
  const productsSearchPending = normalizedProductsSearchInput !== search
  const mainProductsSearchPending = normalizedMainProductsSearchInput !== mainProductsSearch
  const selectedAuthorGuids = useMemo(() => {
    if (!Array.isArray(authorFilter) || authorFilter.length === 0) return []
    const selectedNameSet = new Set(authorFilter)
    return normalizeStringOptions(
      meta.authors
        .filter((author) => selectedNameSet.has(String(author?.name || '').trim()))
        .map((author) => author?.guid)
    )
  }, [authorFilter, meta.authors])
  const normalizedCategorySearchQuery = useMemo(
    () =>
      String(categorySearchQuery || '')
        .trim()
        .toLowerCase(),
    [categorySearchQuery]
  )
  const filteredEksmoTree = useMemo(
    () => filterEksmoTreeNodes(eksmoTree, normalizedCategorySearchQuery),
    [eksmoTree, normalizedCategorySearchQuery]
  )
  const mainCategoryOptions = useMemo(() => flattenMainCategoryOptions(mainCategories), [mainCategories])
  const mainProductsSourceCategoryIdSet = useMemo(
    () => new Set(flattenTreeNodeIds(mainProductsSourceCategories)),
    [mainProductsSourceCategories]
  )
  const selectedMainProductsSourceCategoryKeys = useMemo(() => {
    if (!Array.isArray(mainProductsCategoryFilter) || mainProductsCategoryFilter.length === 0) return EMPTY_SELECTED_KEYS
    const keys = []
    const seen = new Set()
    for (const selectedValue of mainProductsCategoryFilter) {
      if (!mainProductsSourceCategoryIdSet.has(selectedValue) || seen.has(selectedValue)) continue
      seen.add(selectedValue)
      keys.push(selectedValue)
    }
    if (keys.length === 0) return EMPTY_SELECTED_KEYS
    return keys
  }, [mainProductsCategoryFilter, mainProductsSourceCategoryIdSet])
  const selectedMainProductsSourceCategoryKeysParam = useMemo(
    () => selectedMainProductsSourceCategoryKeys.join(','),
    [selectedMainProductsSourceCategoryKeys]
  )
  const includeMainProductsWithoutCategory = useMemo(
    () => selectedMainProductsSourceCategoryKeys.some((value) => value === MAIN_PRODUCTS_OTHER_NODE_KEY),
    [selectedMainProductsSourceCategoryKeys]
  )
  const duplicateGroups = useMemo(() => buildDuplicateGroups(duplicateProducts), [duplicateProducts])
  const duplicateGroupsProductCount = useMemo(() => {
    const ids = new Set()
    for (const group of duplicateGroups) {
      for (const entry of group.products) {
        const key = getEksmoMongoId(entry?.product) || String(getEksmoProductKey(entry?.product) || '').trim()
        if (key) ids.add(key)
      }
    }
    return ids.size
  }, [duplicateGroups])
  const duplicateProductIds = useMemo(() => {
    const ids = new Set()
    for (const group of duplicateGroups) {
      for (const entry of group.products) {
        const id = getEksmoMongoId(entry?.product)
        if (id) ids.add(id)
      }
    }
    return [...ids]
  }, [duplicateGroups])
  const duplicateGroupProductIds = useMemo(() => {
    const groups = []
    for (const group of duplicateGroups) {
      const ids = []
      const seen = new Set()
      for (const entry of group.products) {
        const id = getEksmoMongoId(entry?.product)
        if (!id || seen.has(id)) continue
        seen.add(id)
        ids.push(id)
      }
      if (ids.length > 1) groups.push(ids)
    }
    return groups
  }, [duplicateGroups])
  const duplicateGroupsByProductId = useMemo(() => {
    const map = new Map()
    for (const groupIDs of duplicateGroupProductIds) {
      for (const id of groupIDs) {
        if (!map.has(id)) map.set(id, [])
        map.get(id).push(groupIDs)
      }
    }
    return map
  }, [duplicateGroupProductIds])
  const maxDuplicateSelectionIds = useMemo(() => {
    const selected = new Set(duplicateProductIds)
    for (const groupIDs of duplicateGroupProductIds) {
      if (groupIDs.length < 2) continue
      selected.delete(groupIDs[0])
    }
    return [...selected]
  }, [duplicateProductIds, duplicateGroupProductIds])
  const maxDuplicateSelectionIdSet = useMemo(() => new Set(maxDuplicateSelectionIds), [maxDuplicateSelectionIds])
  const selectedDuplicateIdSet = useMemo(() => new Set(selectedDuplicateIds), [selectedDuplicateIds])
  const duplicateProductIdSet = useMemo(() => new Set(duplicateProductIds), [duplicateProductIds])
  const allDuplicateProductsSelected = useMemo(() => {
    if (maxDuplicateSelectionIds.length === 0) return false
    if (selectedDuplicateIds.length !== maxDuplicateSelectionIdSet.size) return false
    return maxDuplicateSelectionIds.every((id) => selectedDuplicateIdSet.has(id))
  }, [maxDuplicateSelectionIds, maxDuplicateSelectionIdSet, selectedDuplicateIds.length, selectedDuplicateIdSet])
  const duplicateActionBusy = duplicateDeleteId !== '' || duplicateBulkActionKey !== ''

  useEffect(() => {
    setSelectedDuplicateIds((prev) => prev.filter((id) => duplicateProductIdSet.has(id)))
  }, [duplicateProductIdSet])

  useEffect(() => {
    if (typeof window === 'undefined') return
    window.localStorage.setItem(ACTIVE_PAGE_STORAGE_KEY, activePage)
  }, [activePage])

  useEffect(() => {
    writeDashboardSetting(MAIN_PRODUCTS_LIMIT_STORAGE_KEY, clampQuantity(mainProductsLimit))
  }, [mainProductsLimit])

  useEffect(() => {
    writeDashboardSetting(MAIN_PRODUCTS_VIEW_MODE_STORAGE_KEY, mainProductsViewMode === 'cards' ? 'cards' : 'list')
  }, [mainProductsViewMode])

  useEffect(() => {
    writeDashboardSetting(MAIN_PRODUCTS_CARD_COLUMNS_STORAGE_KEY, clampCardColumns(mainProductsCardColumns))
  }, [mainProductsCardColumns])

  useEffect(() => {
    writeDashboardSetting(MAIN_PRODUCTS_WITHOUT_ISBN_ONLY_STORAGE_KEY, mainProductsWithoutISBNOnly ? '1' : '0')
  }, [mainProductsWithoutISBNOnly])

  useEffect(() => {
    if (!toast) return
    const timer = setTimeout(() => setToast(null), 3000)
    return () => clearTimeout(timer)
  }, [toast])

  useEffect(() => {
    const handleWindowError = (event) => {
      frontendDebugLog('error', 'window_error', {
        message: event?.message || '',
        source: event?.filename || '',
        line: event?.lineno || 0,
        column: event?.colno || 0
      })
    }
    const handleUnhandledRejection = (event) => {
      const reason = event?.reason
      frontendDebugLog('error', 'unhandled_rejection', {
        message: reason?.message || String(reason || '')
      })
    }

    window.addEventListener('error', handleWindowError)
    window.addEventListener('unhandledrejection', handleUnhandledRejection)
    return () => {
      window.removeEventListener('error', handleWindowError)
      window.removeEventListener('unhandledrejection', handleUnhandledRejection)
    }
  }, [])

  useEffect(() => {
    if (!detailsProduct || typeof document === 'undefined') return

    const previousOverflow = document.body.style.overflow
    const handleEscape = (event) => {
      if (event.key === 'Escape') setDetailsProduct(null)
    }

    document.body.style.overflow = 'hidden'
    document.addEventListener('keydown', handleEscape)

    return () => {
      document.body.style.overflow = previousOverflow
      document.removeEventListener('keydown', handleEscape)
    }
  }, [detailsProduct])

  useEffect(() => {
    const modalOpen = mainProductModalOpen || mainProductEditModalOpen
    if (!modalOpen || typeof document === 'undefined') return

    const previousOverflow = document.body.style.overflow
    const handleEscape = (event) => {
      if (event.key !== 'Escape') return
      if (mainProductCreating || mainProductEditing || mainProductImageUploading) return
      setMainProductModalOpen(false)
      setMainProductEditModalOpen(false)
    }
    document.body.style.overflow = 'hidden'
    document.addEventListener('keydown', handleEscape)
    return () => {
      document.body.style.overflow = previousOverflow
      document.removeEventListener('keydown', handleEscape)
    }
  }, [mainProductModalOpen, mainProductEditModalOpen, mainProductCreating, mainProductEditing, mainProductImageUploading])

  useEffect(() => {
    const handleSearchShortcut = (event) => {
      if (event.defaultPrevented) return
      if (event.key !== '/') return
      if (event.metaKey || event.ctrlKey || event.altKey) return
      if (isEditableElement(event.target)) return

      event.preventDefault()
      const input =
        activePage === 'mainProducts'
          ? mainProductsSearchInputRef.current
          : activePage === 'eksmo'
            ? productsSearchInputRef.current
            : null
      if (!input) return
      input.focus()
      input.select()
    }

    window.addEventListener('keydown', handleSearchShortcut)
    return () => window.removeEventListener('keydown', handleSearchShortcut)
  }, [activePage])

  useEffect(() => {
    const handleClickOutside = (event) => {
      const clickedInsideButton = categoryDropdownRef.current?.contains(event.target)
      const clickedInsideMenu = categoryDropdownMenuRef.current?.contains(event.target)
      if (!clickedInsideButton && !clickedInsideMenu) setIsCategoryDropdownOpen(false)
    }
    document.addEventListener('mousedown', handleClickOutside)
    return () => document.removeEventListener('mousedown', handleClickOutside)
  }, [])

  useEffect(() => {
    if (!isCategoryDropdownOpen) return

    const updateDropdownPosition = () => {
      if (!categoryDropdownButtonRef.current) return
      const rect = categoryDropdownButtonRef.current.getBoundingClientRect()
      const horizontalPadding = 12
      const desiredWidth = 300
      const width = Math.min(desiredWidth, window.innerWidth - horizontalPadding * 2)

      let left = rect.left
      if (left + width > window.innerWidth - horizontalPadding) left = window.innerWidth - width - horizontalPadding
      if (left < horizontalPadding) left = horizontalPadding

      const top = rect.bottom + 6
      const maxHeight = Math.min(420, Math.max(180, window.innerHeight - top - horizontalPadding))

      setCategoryDropdownStyle({
        left: `${left}px`,
        top: `${top}px`,
        width: `${width}px`,
        maxHeight: `${maxHeight}px`
      })
    }

    updateDropdownPosition()
    window.addEventListener('resize', updateDropdownPosition)
    window.addEventListener('scroll', updateDropdownPosition, true)

    return () => {
      window.removeEventListener('resize', updateDropdownPosition)
      window.removeEventListener('scroll', updateDropdownPosition, true)
    }
  }, [isCategoryDropdownOpen])

  useEffect(() => {
    if (isCategoryDropdownOpen) return
    setCategorySearchQuery('')
  }, [isCategoryDropdownOpen])

  useEffect(() => {
    const loadMainCategories = async () => {
      try {
        setMainCategoriesLoading(true)
        const response = await fetch(`${API_BASE_URL}/categories`)
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)
        const payload = await response.json()
        setMainCategories(Array.isArray(payload.data) ? payload.data : [])
      } catch (err) {
        console.error('Failed to load main categories:', err)
      } finally {
        setMainCategoriesLoading(false)
      }
    }
    loadMainCategories()
  }, [])

  useEffect(() => {
    if (activePage !== 'mainProducts') return
    const controller = new AbortController()

    const loadMainProductsSourceCategories = async () => {
      try {
        setMainProductsSourceCategoriesLoading(true)
        const response = await fetch(`${API_BASE_URL}/mainProducts/source-categories`, { signal: controller.signal })
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)
        const payload = await response.json()
        if (controller.signal.aborted) return
        setMainProductsSourceCategories(Array.isArray(payload.data) ? payload.data : [])
      } catch (err) {
        if (err?.name === 'AbortError') return
        setMainProductsSourceCategories([])
        console.error('Failed to load main products source categories:', err)
      } finally {
        if (controller.signal.aborted) return
        setMainProductsSourceCategoriesLoading(false)
      }
    }

    loadMainProductsSourceCategories()
    return () => controller.abort()
  }, [activePage, mainProductsVersion])

  useEffect(() => {
    setMainProductsCategoryFilter((prev) => {
      if (!Array.isArray(prev) || prev.length === 0) return prev
      const filtered = prev.filter((value) => mainProductsSourceCategoryIdSet.has(value))
      return filtered.length === prev.length ? prev : filtered
    })
  }, [mainProductsSourceCategoryIdSet])

  useEffect(() => {
    const loadEksmoTree = async () => {
      try {
        setEksmoTreeLoading(true)
        const response = await fetch(`${API_BASE_URL}/eksmoNichesTree`)
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)
        const payload = await response.json()
        setEksmoTree(Array.isArray(payload.data) ? payload.data : [])
      } catch (err) {
        console.error('Failed to load product tree:', err)
      } finally {
        setEksmoTreeLoading(false)
      }
    }
    loadEksmoTree()
  }, [syncVersion])

  useEffect(() => {
    const loadMeta = async () => {
      const cached = readMetaCache()
      if (cached) {
        setMeta(cached)
        setMetaLoading(false)
      } else {
        setMetaLoading(true)
      }

      try {
        const response = await fetch(`${API_BASE_URL}/eksmoProductsMeta?limit=1200&expanded=1`)
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)
        const payload = await response.json()
        const nextMeta = normalizeMetaPayload(payload.data || EMPTY_META)
        setMeta(nextMeta)
        writeMetaCache(nextMeta)
      } catch (err) {
        console.error('Failed to load meta:', err)
      } finally {
        setMetaLoading(false)
      }
    }
    loadMeta()
  }, [syncVersion])

  useEffect(() => {
    const controller = new AbortController()

    const loadProducts = async () => {
      try {
        setProductsLoading(true)
        setProductsError('')
        const params = new URLSearchParams({
          page: String(productsPage),
          limit: String(productsLimit)
        })

        if (search.trim()) params.set('search', search.trim())
        if (brandFilter.length > 0) params.set('brand', joinFilterValues(brandFilter))
        if (serieFilter.length > 0) params.set('serie', joinFilterValues(serieFilter))
        if (publisherFilter.length > 0) params.set('publisher', joinFilterValues(publisherFilter))
        if (selectedAuthorGuids.length > 0) {
          params.set('authorGuids', joinFilterValues(selectedAuthorGuids))
        } else if (authorFilter.length > 0) {
          params.set('authorName', joinFilterValues(authorFilter))
        }
        if (genreFilter.length > 0) params.set('genres', joinFilterValues(genreFilter))
        if (ageFilter.length > 0) params.set('age', joinFilterValues(ageFilter))

        if (selectedEksmoNodes.length > 0) {
          const subjectGuids = selectedEksmoNodes.filter((n) => n.type === 'subject').map((n) => n.guid)
          const nicheGuids = selectedEksmoNodes.filter((n) => n.type === 'niche').map((n) => n.guid)
          if (subjectGuids.length > 0) params.set('subjectGuid', subjectGuids.join(','))
          if (nicheGuids.length > 0) params.set('nicheGuid', nicheGuids.join(','))
        }

        const response = await fetch(`${API_BASE_URL}/eksmoProducts?${params.toString()}`, { signal: controller.signal })
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)

        const payload = await response.json()
        if (controller.signal.aborted) return
        setProducts(Array.isArray(payload.data) ? payload.data : [])
        setProductsTotalPages(Number(payload.pagination?.totalPages || 0))
        setProductsTotalItems(Number(payload.pagination?.totalItems || 0))
      } catch (err) {
        if (err?.name === 'AbortError') return
        setProductsError(err.message || 'Не удалось загрузить товары')
      } finally {
        if (controller.signal.aborted) return
        setProductsLoading(false)
      }
    }
    loadProducts()
    return () => controller.abort()
  }, [productsPage, productsLimit, search, brandFilter, serieFilter, publisherFilter, authorFilter, selectedAuthorGuids, genreFilter, ageFilter, selectedEksmoNodes, syncVersion, mainProductsVersion])

  useEffect(() => {
    setProductsPage(1)
  }, [search, productsLimit, brandFilter, serieFilter, publisherFilter, authorFilter, genreFilter, ageFilter, selectedEksmoNodes])

  useEffect(() => {
    if (activePage !== 'mainProducts') return

    const controller = new AbortController()

    const loadMainProducts = async () => {
      try {
        setMainProductsLoading(true)
        setMainProductsError('')
        const params = new URLSearchParams({
          page: String(mainProductsPage),
          limit: String(mainProductsLimit)
        })
        if (mainProductsSearch.trim()) params.set('search', mainProductsSearch.trim())
        if (selectedMainProductsSourceCategoryKeysParam) {
          params.set('sourceCategoryKeys', selectedMainProductsSourceCategoryKeysParam)
        }
        if (includeMainProductsWithoutCategory) params.set('withoutCategory', '1')
        if (mainProductsWithoutISBNOnly) params.set('withoutIsbn', '1')

        const response = await fetch(`${API_BASE_URL}/mainProducts?${params.toString()}`, { signal: controller.signal })
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)
        const payload = await response.json()
        if (controller.signal.aborted) return
        setMainProducts(Array.isArray(payload.data) ? payload.data : [])
        setMainProductsTotalPages(Number(payload.pagination?.totalPages || 0))
        setMainProductsTotalItems(Number(payload.pagination?.totalItems || 0))
      } catch (err) {
        if (err?.name === 'AbortError') return
        setMainProductsError(err.message || 'Не удалось загрузить основные товары')
      } finally {
        if (controller.signal.aborted) return
        setMainProductsLoading(false)
      }
    }

    loadMainProducts()
    return () => controller.abort()
  }, [activePage, mainProductsPage, mainProductsLimit, mainProductsSearch, selectedMainProductsSourceCategoryKeysParam, includeMainProductsWithoutCategory, mainProductsWithoutISBNOnly, mainProductsVersion])

  useEffect(() => {
    setMainProductsPage(1)
  }, [mainProductsSearch, selectedMainProductsSourceCategoryKeysParam, includeMainProductsWithoutCategory, mainProductsWithoutISBNOnly, mainProductsLimit])

  useEffect(() => {
    if (activePage !== 'duplicates') return
    if (duplicatesLastScannedKey === duplicatesScanKey) return

    const controller = new AbortController()

    const loadDuplicateProducts = async () => {
      try {
        setDuplicatesLoading(true)
        setDuplicatesError('')
        setDuplicatesStatus('Загрузка дубликатов с сервера...')

        const response = await fetch(`${API_BASE_URL}/eksmoProducts/duplicates`, { signal: controller.signal })
        if (!response.ok) throw new Error(`Ошибка, статус ${response.status}`)
        const payload = await response.json()
        if (controller.signal.aborted) return

        const items = Array.isArray(payload.data) ? payload.data : []
        const scanned = Number(payload.summary?.scanned || 0)
        const duplicateGroups = Number(payload.summary?.duplicateGroups || 0)
        setDuplicateProducts(items)
        if (scanned > 0) {
          setDuplicatesStatus(`Проверено: ${scanned.toLocaleString()} · Групп: ${duplicateGroups.toLocaleString()} · Дубликатов: ${items.length.toLocaleString()}`)
        } else {
          setDuplicatesStatus(`Дубликатов: ${items.length.toLocaleString()}`)
        }
      } catch (err) {
        if (err?.name === 'AbortError') return
        setDuplicatesError(err.message || 'Не удалось загрузить товары для поиска дубликатов')
        setDuplicatesStatus('')
      } finally {
        if (controller.signal.aborted) return
        setDuplicatesLoading(false)
        setDuplicatesLastScannedKey(duplicatesScanKey)
      }
    }

    loadDuplicateProducts()
    return () => controller.abort()
  }, [activePage, duplicatesScanKey, duplicatesLastScannedKey])

  const visibleProductIds = useMemo(() => products.map((p) => getEksmoMongoId(p)).filter(Boolean), [products])
  const visibleProductIdSet = useMemo(() => new Set(visibleProductIds), [visibleProductIds])
  const selectedVisibleCount = useMemo(
    () => selectedProductIds.filter((id) => visibleProductIdSet.has(id)).length,
    [selectedProductIds, visibleProductIdSet]
  )
  const expectedProductsOnPage = useMemo(() => {
    const remaining = Math.max(0, productsTotalItems - (productsPage - 1) * productsLimit)
    return Math.min(productsLimit, remaining)
  }, [productsTotalItems, productsPage, productsLimit])
  const selectPageProductsCount = Math.max(visibleProductIds.length, expectedProductsOnPage)
  const allVisibleSelected = visibleProductIds.length > 0 && selectedVisibleCount === visibleProductIds.length
  const visibleMainProductIds = useMemo(() => mainProducts.map((p) => getMainProductId(p)).filter(Boolean), [mainProducts])
  const visibleMainProductIdSet = useMemo(() => new Set(visibleMainProductIds), [visibleMainProductIds])
  const excludedFilteredMainProductIdSet = useMemo(() => new Set(excludedFilteredMainProductIds), [excludedFilteredMainProductIds])
  const selectedVisibleMainProductsCount = useMemo(
    () =>
      selectAllFilteredMainProducts
        ? visibleMainProductIds.filter((id) => !excludedFilteredMainProductIdSet.has(id)).length
        : selectedMainProductIds.filter((id) => visibleMainProductIdSet.has(id)).length,
    [selectAllFilteredMainProducts, visibleMainProductIds, excludedFilteredMainProductIdSet, selectedMainProductIds, visibleMainProductIdSet]
  )
  const expectedMainProductsOnPage = useMemo(() => {
    const remaining = Math.max(0, mainProductsTotalItems - (mainProductsPage - 1) * mainProductsLimit)
    return Math.min(mainProductsLimit, remaining)
  }, [mainProductsTotalItems, mainProductsPage, mainProductsLimit])
  const selectPageMainProductsCount = Math.max(visibleMainProductIds.length, expectedMainProductsOnPage)
  const allVisibleMainProductsSelected = visibleMainProductIds.length > 0 && selectedVisibleMainProductsCount === visibleMainProductIds.length
  const selectedMainProductsCount = selectAllFilteredMainProducts
    ? Math.max(0, mainProductsTotalItems - excludedFilteredMainProductIds.length)
    : selectedMainProductIds.length

  useEffect(() => {
    setSelectedMainProductIds((prev) => prev.filter((id) => visibleMainProductIdSet.has(id)))
  }, [visibleMainProductIdSet])

  useEffect(() => {
    if (mainProductsTotalItems > 0) return
    if (!selectAllFilteredMainProducts) return
    setSelectAllFilteredMainProducts(false)
    setExcludedFilteredMainProductIds([])
  }, [mainProductsTotalItems, selectAllFilteredMainProducts])

  const toggleEksmoNode = (guid) => setEksmoExpanded((prev) => ({ ...prev, [guid]: !prev[guid] }))
  const toggleMainCategoryNode = (id) => setMainCategoriesExpanded((prev) => ({ ...prev, [id]: !prev[id] }))
  const toggleMainProductsCategoryNode = (id) => setMainProductsExpanded((prev) => ({ ...prev, [id]: !prev[id] }))

  const toggleMainCategorySelection = (node, path) => {
    setSelectedMainCategory((prev) => {
      if (prev?.id === node.id) return null
      return { id: node.id, name: node.name, path }
    })
  }

  const toggleMainProductsCategorySelection = (node, path) => {
    setSelectedMainProductsCategories((prev) => {
      const exists = prev.some((item) => item.id === node.id)
      if (exists) return []
      return [{ id: node.id, name: node.name, path }]
    })
  }

  const toggleEksmoSelection = (node) => {
    setSelectedEksmoNodes((prev) => {
      const exists = prev.find((n) => n.guid === node.guid)
      if (exists) return prev.filter((n) => n.guid !== node.guid)
      return [...prev, node]
    })
  }

  const toggleProductSelection = (productID) => {
    if (eksmoActionKey) return
    if (!isMongoObjectId(productID)) return
    setSelectedProductIds((prev) => {
      if (prev.includes(productID)) return prev.filter((id) => id !== productID)
      if (prev.length >= MAX_PRODUCTS_LIMIT) {
        setCopyStatus(`Можно выбрать не более ${MAX_PRODUCTS_LIMIT} товаров за раз.`)
        return prev
      }
      return [...prev, productID]
    })
  }

  const toggleSelectAllVisible = () => {
    if (eksmoActionKey) return
    if (visibleProductIds.length === 0) return

    setSelectedProductIds((prev) => {
      if (allVisibleSelected) {
        const removeSet = new Set(visibleProductIds)
        return prev.filter((id) => !removeSet.has(id))
      }
      const merged = new Set(prev)
      visibleProductIds.forEach((id) => merged.add(id))
      const ids = [...merged]
      if (ids.length > MAX_PRODUCTS_LIMIT) setCopyStatus(`Выбор ограничен ${MAX_PRODUCTS_LIMIT} товарами.`)
      return ids.slice(0, MAX_PRODUCTS_LIMIT)
    })
  }

  const toggleMainProductSelection = (productID) => {
    if (!isMongoObjectId(productID)) return
    if (selectAllFilteredMainProducts) {
      setExcludedFilteredMainProductIds((prev) => {
        if (prev.includes(productID)) return prev.filter((id) => id !== productID)
        return [...prev, productID]
      })
      return
    }
    setSelectedMainProductIds((prev) => {
      if (prev.includes(productID)) return prev.filter((id) => id !== productID)
      return [...prev, productID]
    })
  }

  const toggleSelectAllVisibleMainProducts = () => {
    if (visibleMainProductIds.length === 0) return
    if (selectAllFilteredMainProducts) {
      setExcludedFilteredMainProductIds((prev) => {
        if (allVisibleMainProductsSelected) {
          const next = new Set(prev)
          visibleMainProductIds.forEach((id) => next.add(id))
          return [...next]
        }
        const removeSet = new Set(visibleMainProductIds)
        return prev.filter((id) => !removeSet.has(id))
      })
      return
    }
    setSelectedMainProductIds((prev) => {
      if (allVisibleMainProductsSelected) {
        const removeSet = new Set(visibleMainProductIds)
        return prev.filter((id) => !removeSet.has(id))
      }
      const merged = new Set(prev)
      visibleMainProductIds.forEach((id) => merged.add(id))
      return [...merged]
    })
  }

  const toggleSelectAllFilteredMainProducts = () => {
    if (mainProductsActionKey !== '') return
    if (mainProductsTotalItems <= 0) return
    const next = !selectAllFilteredMainProducts
    setSelectAllFilteredMainProducts(next)
    if (next) {
      setSelectedMainProductIds([])
      setExcludedFilteredMainProductIds([])
      return
    }
    setExcludedFilteredMainProductIds([])
  }

  const clearFilters = () => {
    setBrandFilter([])
    setSerieFilter([])
    setPublisherFilter([])
    setAuthorFilter([])
    setGenreFilter([])
    setAgeFilter([])
    setSearchInput('')
    setSelectedEksmoNodes([])
    setProductsLimit(DEFAULT_PRODUCTS_LIMIT)
  }

  const clearMainProductsFilters = () => {
    setMainProductsSearchInput('')
    setMainProductsCategoryFilter([])
    setMainProductsWithoutISBNOnly(false)
    setMainProductsLimit(DEFAULT_PRODUCTS_LIMIT)
    setSelectedMainProductIds([])
    setSelectAllFilteredMainProducts(false)
    setExcludedFilteredMainProductIds([])
  }

  const handleProductsLimitChange = (value) => setProductsLimit(clampQuantity(value))
  const handleMainProductsLimitChange = (value) => setMainProductsLimit(clampQuantity(value))

  const handleSync = async () => {
    try {
      setSyncing(true)
      setSyncStatus('Синхронизируются все страницы... подождите.')
      const response = await fetch(`${API_BASE_URL}/syncEksmoProducts?per_page=500&resume=1`, { method: 'POST' })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Синхронизация не удалась (статус ${response.status})`)
      setSyncStatus(`${payload.message}. Получено: ${payload.fetched}, Добавлено/обновлено: ${payload.upserted}, Изменено: ${payload.modified}`)
      setSyncVersion((prev) => prev + 1)
    } catch (err) {
      setSyncStatus(err.message || 'Синхронизация не удалась')
    } finally {
      setSyncing(false)
    }
  }

  const handleResetSync = async () => {
    try {
      setSyncing(true)
      setSyncStatus('Сброс и синхронизация всех страниц...')
      const response = await fetch(`${API_BASE_URL}/syncEksmoProducts?per_page=500&resume=1&reset=1`, { method: 'POST' })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Синхронизация не удалась (статус ${response.status})`)
      setSyncStatus(`Перезапуск выполнен. Получено: ${payload.fetched}, Добавлено/обновлено: ${payload.upserted}, Изменено: ${payload.modified}`)
      setSyncVersion((prev) => prev + 1)
    } catch (err) {
      setSyncStatus(err.message || 'Сброс синхронизации не удался')
    } finally {
      setSyncing(false)
    }
  }

  const buildCopyPayload = (includeSelectedProducts) => {
    const subjectGuids = selectedEksmoNodes.filter((n) => n.type === 'subject').map((n) => n.guid)
    const nicheGuids = selectedEksmoNodes.filter((n) => n.type === 'niche').map((n) => n.guid)

    const payload = {
      categoryId: selectedMainCategory?.id || '',
      quantity: productsLimit,
      page: productsPage,
      search: search.trim(),
      authorName: selectedAuthorGuids.length > 0 ? '' : joinFilterValues(authorFilter),
      authorGuids: selectedAuthorGuids,
      brand: joinFilterValues(brandFilter),
      seriesName: joinFilterValues(serieFilter),
      publisherName: joinFilterValues(publisherFilter),
      ageRestriction: joinFilterValues(ageFilter),
      genreNames: genreFilter,
      subjectGuids,
      nicheGuids
    }

    if (includeSelectedProducts) payload.productIds = selectedProductIds.slice(0, MAX_PRODUCTS_LIMIT)
    return payload
  }

  const handleCopySelected = async () => {
    if (!selectedMainCategory?.id) return setCopyStatus('Сначала выберите одну основную категорию.')
    if (selectedProductIds.length === 0) return setCopyStatus('Выберите хотя бы один товар для копирования.')

    try {
      setCopying(true)
      setCopyStatus('Копирование выбранных товаров...')
      const response = await fetch(`${API_BASE_URL}/copyEksmoProductsToMain`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildCopyPayload(true))
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Копирование не удалось (статус ${response.status})`)
      setCopyStatus(`Скопировано выбранных: ${payload.copied} (обработано: ${payload.processed}, пропущено: ${payload.skipped})`)
      setSelectedProductIds([])
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setCopyStatus(err.message || 'Копирование не удалось')
    } finally {
      setCopying(false)
    }
  }

  const handleCopyGroup = async () => {
    if (!selectedMainCategory?.id) return setCopyStatus('Сначала выберите одну основную категорию.')

    try {
      setCopying(true)
      setCopyStatus('Копирование группы товаров...')
      const response = await fetch(`${API_BASE_URL}/copyEksmoProductsToMain`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(buildCopyPayload(false))
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Копирование не удалось (статус ${response.status})`)
      setCopyStatus(`Скопировано группой: ${payload.copied} (обработано: ${payload.processed}, пропущено: ${payload.skipped})`)
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setCopyStatus(err.message || 'Копирование не удалось')
    } finally {
      setCopying(false)
    }
  }

  const handleCopyMissingToMain = async () => {
    try {
      setCopying(true)
      setCopyStatus('Перенос отсутствующих товаров в Main...')
      const response = await fetch(`${API_BASE_URL}/copyEksmoProductsToMain`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...buildCopyPayload(false),
          categoryId: '',
          page: 1,
          quantity: MAX_PRODUCTS_LIMIT,
          onlyMissing: true,
          allPages: true
        })
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Копирование не удалось (статус ${response.status})`)
      setCopyStatus(
        `Перенесено отсутствующих: ${payload.copied} (проверено: ${payload.scanned || 0}, обработано: ${payload.processed || 0}, пропущено: ${payload.skipped || 0})`
      )
      setMainProductsVersion((prev) => prev + 1)
      setSyncVersion((prev) => prev + 1)
    } catch (err) {
      setCopyStatus(err.message || 'Копирование не удалось')
    } finally {
      setCopying(false)
    }
  }

  const handleDeleteEksmoProduct = async (productID) => {
    if (!isMongoObjectId(productID)) return
    const confirmed = window.confirm('Удалить этот товар из каталога All?\n\nВнимание: удаленный товар нельзя вернуть.')
    if (!confirmed) return

    try {
      setEksmoActionKey(`delete:${productID}`)
      setCopyStatus('Удаление товара из каталога All...')

      const response = await fetch(`${API_BASE_URL}/eksmoProducts/${productID}`, { method: 'DELETE' })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)

      setCopyStatus('Товар удален из каталога All.')
      setSelectedProductIds((prev) => prev.filter((id) => id !== productID))
      setSyncVersion((prev) => prev + 1)
    } catch (err) {
      setCopyStatus(err.message || 'Не удалось удалить товар All')
    } finally {
      setEksmoActionKey('')
    }
  }

  const handleDeleteSelectedEksmoProducts = async () => {
    const productIDs = selectedProductIds.filter((id) => isMongoObjectId(id)).slice(0, MAX_PRODUCTS_LIMIT)
    if (productIDs.length === 0) return setCopyStatus('Выберите хотя бы один товар All.')
    const confirmed = window.confirm(
      `Удалить ${productIDs.length} товар(ов) из каталога All?\n\nВнимание: удаленные товары нельзя вернуть.`
    )
    if (!confirmed) return

    try {
      setEksmoActionKey('bulk-delete')
      setCopyStatus(`Удаление ${productIDs.length} выбранных товаров из каталога All...`)

      const response = await fetch(`${API_BASE_URL}/eksmoProducts`, {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ productIds: productIDs })
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)

      const deleted = Number(payload.deleted || 0)
      const notFound = Number(payload.notFound || 0)
      const invalid = Number(payload.invalid || 0)

      setSelectedProductIds((prev) => prev.filter((id) => !productIDs.includes(id)))
      if (deleted > 0) {
        setSyncVersion((prev) => prev + 1)
      }

      if (notFound > 0 || invalid > 0) {
        setCopyStatus(`Удалено: ${deleted}, не найдено: ${notFound}, некорректно: ${invalid}.`)
      } else {
        setCopyStatus(`Удалено товаров из каталога All: ${deleted}.`)
      }
    } catch (err) {
      setCopyStatus(err.message || 'Не удалось удалить выбранные товары All')
    } finally {
      setEksmoActionKey('')
    }
  }

  const handleReloadDuplicateScan = () => {
    setDuplicatesScanKey((prev) => prev + 1)
  }

  const toggleDuplicateSelection = (productID) => {
    if (duplicateActionBusy) return
    if (!isMongoObjectId(productID)) return
    setSelectedDuplicateIds((prev) => {
      if (prev.includes(productID)) return prev.filter((id) => id !== productID)

      const nextSet = new Set(prev)
      nextSet.add(productID)
      const relatedGroups = duplicateGroupsByProductId.get(productID) || []
      const selectsWholeGroup = relatedGroups.some((groupIDs) => groupIDs.length > 0 && groupIDs.every((id) => nextSet.has(id)))
      if (selectsWholeGroup) {
        setDuplicatesStatus('Нельзя выбрать все товары в группе. Минимум один товар должен остаться.')
        return prev
      }
      return [...nextSet]
    })
  }

  const toggleSelectAllDuplicateProducts = () => {
    if (duplicateActionBusy) return
    if (maxDuplicateSelectionIds.length === 0) return
    if (allDuplicateProductsSelected) {
      setSelectedDuplicateIds([])
      return
    }
    setSelectedDuplicateIds(maxDuplicateSelectionIds)
    setDuplicatesStatus('Выбраны дубликаты для удаления: в каждой группе оставлен минимум один товар.')
  }

  const deleteDuplicateProductIDs = async (productIDs, bulkActionKey, inProgressStatusText, successStatusPrefix) => {
    const validIDs = [...new Set((Array.isArray(productIDs) ? productIDs : []).filter((id) => isMongoObjectId(id)))]
    if (validIDs.length === 0) return false

    try {
      setDuplicateBulkActionKey(bulkActionKey)
      setDuplicatesError('')
      setDuplicatesStatus(inProgressStatusText)

      const batches = splitIntoChunks(validIDs, MAX_PRODUCTS_LIMIT)
      let totalDeleted = 0
      let totalNotFound = 0
      let totalInvalid = 0

      for (const batch of batches) {
        const response = await fetch(`${API_BASE_URL}/eksmoProducts`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ productIds: batch })
        })
        const payload = await response.json()
        if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)
        totalDeleted += Number(payload.deleted || 0)
        totalNotFound += Number(payload.notFound || 0)
        totalInvalid += Number(payload.invalid || 0)
      }

      const removedSet = new Set(validIDs)
      setDuplicateProducts((prev) => prev.filter((item) => !removedSet.has(getEksmoMongoId(item))))
      setSelectedDuplicateIds((prev) => prev.filter((id) => !removedSet.has(id)))
      setSyncVersion((prev) => prev + 1)

      if (totalNotFound > 0 || totalInvalid > 0) {
        setDuplicatesStatus(`${successStatusPrefix}: ${totalDeleted}. Не найдено: ${totalNotFound}, некорректно: ${totalInvalid}.`)
      } else {
        setDuplicatesStatus(`${successStatusPrefix}: ${totalDeleted}.`)
      }
      return true
    } catch (err) {
      setDuplicatesError(err.message || 'Не удалось удалить выбранные дубликаты')
      return false
    } finally {
      setDuplicateBulkActionKey('')
    }
  }

  const handleDeleteSelectedDuplicateProducts = async () => {
    const ids = selectedDuplicateIds.filter((id) => isMongoObjectId(id))
    if (ids.length === 0) return setDuplicatesStatus('Выберите хотя бы один товар-дубликат.')
    const selectedSet = new Set(ids)
    const invalidSelection = duplicateGroupProductIds.some((groupIDs) => groupIDs.length > 0 && groupIDs.every((id) => selectedSet.has(id)))
    if (invalidSelection) {
      return setDuplicatesStatus('Нельзя удалить все товары в группе. Оставьте минимум один товар в каждой группе.')
    }
    const confirmed = window.confirm(`Удалить выбранные дубликаты (${ids.length})?\n\nВнимание: удаленные товары нельзя вернуть.`)
    if (!confirmed) return

    await deleteDuplicateProductIDs(
      ids,
      'bulk-delete-selected',
      `Удаление выбранных дубликатов (${ids.length})...`,
      'Удалено выбранных дубликатов'
    )
  }

  const handleDeleteDuplicatesKeepOne = async () => {
    const keepSet = new Set()
    const allDuplicateSet = new Set()

    for (const group of duplicateGroups) {
      const groupIDs = group.products.map((entry) => getEksmoMongoId(entry?.product)).filter((id) => isMongoObjectId(id))
      if (groupIDs.length === 0) continue
      keepSet.add(groupIDs[0])
      groupIDs.forEach((id) => allDuplicateSet.add(id))
    }

    const deleteIDs = [...allDuplicateSet].filter((id) => !keepSet.has(id))
    if (deleteIDs.length === 0) return setDuplicatesStatus('Нет дубликатов для удаления. В каждой группе уже остается один товар.')
    const confirmed = window.confirm(
      `Удалить ${deleteIDs.length} дубликатов и оставить по одному товару в каждой группе?\n\nВнимание: удаленные товары нельзя вернуть.`
    )
    if (!confirmed) return

    await deleteDuplicateProductIDs(
      deleteIDs,
      'bulk-keep-one',
      `Удаление дубликатов (оставляем по одному): ${deleteIDs.length}...`,
      'Удалено дубликатов, оставлено по одному'
    )
  }

  const handleDeleteDuplicateProduct = async (productID) => {
    if (duplicateActionBusy) return
    if (!isMongoObjectId(productID)) return
    const confirmed = window.confirm('Удалить этот товар из каталога All?\n\nВнимание: удаленный товар нельзя вернуть.')
    if (!confirmed) return

    try {
      setDuplicateDeleteId(productID)
      setDuplicatesError('')
      setDuplicatesStatus('Удаление товара из каталога All...')
      const response = await fetch(`${API_BASE_URL}/eksmoProducts/${productID}`, { method: 'DELETE' })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)

      setDuplicateProducts((prev) => prev.filter((item) => getEksmoMongoId(item) !== productID))
      setSelectedDuplicateIds((prev) => prev.filter((id) => id !== productID))
      setDuplicatesStatus('Товар удален из каталога All.')
      setSyncVersion((prev) => prev + 1)
    } catch (err) {
      setDuplicatesError(err.message || 'Не удалось удалить товар All')
    } finally {
      setDuplicateDeleteId('')
    }
  }

  const handleDeleteMainProduct = async (productID) => {
    if (!isMongoObjectId(productID)) return
    const confirmed = window.confirm('Удалить этот товар из основной модели?\n\nВнимание: все ручные изменения для этого товара будут потеряны.')
    if (!confirmed) return

    try {
      setMainProductsActionKey(`delete:${productID}`)
      setMainProductsStatus('Удаление товара из основной модели...')
      const response = await fetch(`${API_BASE_URL}/mainProducts/${productID}`, { method: 'DELETE' })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)
      setMainProductsStatus('Товар удален из основной модели.')
      setSelectedMainProductIds((prev) => prev.filter((id) => id !== productID))
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось удалить товар')
    } finally {
      setMainProductsActionKey('')
    }
  }

  const handleDeleteSelectedMainProducts = async () => {
    const deleteAllFiltered = selectAllFilteredMainProducts
    const productIDs = deleteAllFiltered ? [] : [...new Set(selectedMainProductIds.filter((id) => isMongoObjectId(id)))]
    if (!deleteAllFiltered && productIDs.length === 0) return setMainProductsStatus('Выберите хотя бы один основной товар.')
    if (deleteAllFiltered && selectedMainProductsCount <= 0) return setMainProductsStatus('Нет выбранных товаров по текущему фильтру.')

    const confirmed = window.confirm(
      deleteAllFiltered
        ? `Удалить выбранные по фильтру товары (${selectedMainProductsCount.toLocaleString()}) из основной модели?\n\nВнимание: все ручные изменения для удаленных товаров будут потеряны.`
        : `Удалить ${productIDs.length} товар(ов) из основной модели?\n\nВнимание: все ручные изменения для удаленных товаров будут потеряны.`
    )
    if (!confirmed) return

    try {
      setMainProductsActionKey('bulk-delete')
      setMainProductsStatus(
        deleteAllFiltered
          ? `Удаление выбранных по фильтру товаров (${selectedMainProductsCount.toLocaleString()})...`
          : `Удаление ${productIDs.length} выбранных товаров...`
      )

      let deleted = 0
      let notFound = 0
      let invalid = 0

      if (deleteAllFiltered) {
        const response = await fetch(`${API_BASE_URL}/mainProducts`, {
          method: 'DELETE',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            productIds: [],
            applyToFiltered: true,
            search: mainProductsSearch.trim(),
            sourceCategoryKeys: selectedMainProductsSourceCategoryKeysParam,
            withoutCategory: includeMainProductsWithoutCategory,
            withoutIsbn: mainProductsWithoutISBNOnly,
            excludeProductIds: excludedFilteredMainProductIds
          })
        })
        const payload = await response.json()
        if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)
        deleted += Number(payload.deleted || 0)
      } else {
        const batches = splitIntoChunks(productIDs, MAX_PRODUCTS_LIMIT)
        for (const batch of batches) {
          const response = await fetch(`${API_BASE_URL}/mainProducts`, {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ productIds: batch })
          })
          const payload = await response.json()
          if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)
          deleted += Number(payload.deleted || 0)
          notFound += Number(payload.notFound || 0)
          invalid += Number(payload.invalid || 0)
        }
      }

      if (deleted > 0) {
        setSelectedMainProductIds((prev) => prev.filter((id) => !productIDs.includes(id)))
        if (deleteAllFiltered) {
          setSelectAllFilteredMainProducts(false)
          setExcludedFilteredMainProductIds([])
        }
        setMainProductsVersion((prev) => prev + 1)
      }

      if (!deleteAllFiltered && (notFound > 0 || invalid > 0)) {
        setMainProductsStatus(`Удалено: ${deleted}, не найдено: ${notFound}, некорректно: ${invalid}.`)
      } else {
        setMainProductsStatus(`Удалено товаров из основной модели: ${deleted}.`)
      }
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось удалить выбранные товары')
    } finally {
      setMainProductsActionKey('')
    }
  }

  const handleLinkSelectedMainProductsToCategory = async () => {
    const selectedCategory = selectedMainProductsCategories.length === 1 ? selectedMainProductsCategories[0] : null
    if (!selectedCategory?.id) return setMainProductsStatus('Слева выберите одну категорию для привязки.')

    const bindAllFiltered = selectAllFilteredMainProducts
    const productIDs = bindAllFiltered ? [] : selectedMainProductIds.filter((id) => isMongoObjectId(id)).slice(0, MAX_PRODUCTS_LIMIT)
    if (!bindAllFiltered && productIDs.length === 0) return setMainProductsStatus('Выберите хотя бы один основной товар.')
    if (bindAllFiltered && selectedMainProductsCount <= 0) return setMainProductsStatus('Нет выбранных товаров по текущему фильтру.')

    const categoryName = selectedCategory.path?.join(' / ') || selectedCategory.name
    const confirmed = window.confirm(
      bindAllFiltered
        ? `Привязать выбранные по фильтру товары (${selectedMainProductsCount.toLocaleString()}) к категории «${categoryName}»?\n\nИзменение заменит текущую категорию у всех найденных товаров.`
        : `Привязать ${productIDs.length} товар(ов) к категории «${categoryName}»?\n\nИзменение заменит текущую категорию у выбранных товаров.`
    )
    if (!confirmed) return

    try {
      setMainProductsActionKey('bulk-link-category')
      setMainProductsStatus(
        bindAllFiltered
          ? `Привязка выбранных по фильтру товаров (${selectedMainProductsCount.toLocaleString()}) к категории...`
          : `Привязка ${productIDs.length} выбранных товаров к категории...`
      )

      const response = await fetch(`${API_BASE_URL}/mainProducts/link-category`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          productIds: productIDs,
          categoryId: selectedCategory.id,
          applyToFiltered: bindAllFiltered,
          search: mainProductsSearch.trim(),
          sourceCategoryKeys: selectedMainProductsSourceCategoryKeysParam,
          withoutCategory: includeMainProductsWithoutCategory,
          withoutIsbn: mainProductsWithoutISBNOnly,
          excludeProductIds: excludedFilteredMainProductIds
        })
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)

      const linked = Number(payload.linked || 0)
      const notFound = Number(payload.notFound || 0)
      const invalid = Number(payload.invalid || 0)

      if (linked > 0) {
        setSelectedMainProductIds((prev) => prev.filter((id) => !productIDs.includes(id)))
        if (bindAllFiltered) {
          setSelectAllFilteredMainProducts(false)
          setExcludedFilteredMainProductIds([])
        }
        setMainProductsVersion((prev) => prev + 1)
      }

      if (!bindAllFiltered && (notFound > 0 || invalid > 0)) {
        setMainProductsStatus(`Привязано: ${linked}, не найдено: ${notFound}, некорректно: ${invalid}.`)
      } else {
        setMainProductsStatus(`Привязано к категории: ${linked}.`)
      }
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось привязать выбранные товары к категории')
    } finally {
      setMainProductsActionKey('')
    }
  }

  const handleUnlinkSelectedMainProductsCategory = async () => {
    const unlinkAllFiltered = selectAllFilteredMainProducts
    const productIDs = unlinkAllFiltered ? [] : selectedMainProductIds.filter((id) => isMongoObjectId(id)).slice(0, MAX_PRODUCTS_LIMIT)
    if (!unlinkAllFiltered && productIDs.length === 0) return setMainProductsStatus('Выберите хотя бы один основной товар.')
    if (unlinkAllFiltered && selectedMainProductsCount <= 0) return setMainProductsStatus('Нет выбранных товаров по текущему фильтру.')

    const confirmed = window.confirm(
      unlinkAllFiltered
        ? `Отвязать категорию у выбранных по фильтру товаров (${selectedMainProductsCount.toLocaleString()})?\n\nУ товаров будет удалена текущая категория.`
        : `Отвязать категорию у ${productIDs.length} выбранных товар(ов)?\n\nУ товаров будет удалена текущая категория.`
    )
    if (!confirmed) return

    try {
      setMainProductsActionKey('bulk-unlink-category')
      setMainProductsStatus(
        unlinkAllFiltered
          ? `Отвязка категории у выбранных по фильтру товаров (${selectedMainProductsCount.toLocaleString()})...`
          : `Отвязка категории у ${productIDs.length} выбранных товаров...`
      )

      const response = await fetch(`${API_BASE_URL}/mainProducts/unlink-category`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          productIds: productIDs,
          applyToFiltered: unlinkAllFiltered,
          search: mainProductsSearch.trim(),
          sourceCategoryKeys: selectedMainProductsSourceCategoryKeysParam,
          withoutCategory: includeMainProductsWithoutCategory,
          withoutIsbn: mainProductsWithoutISBNOnly,
          excludeProductIds: excludedFilteredMainProductIds
        })
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)

      const unlinked = Number(payload.unlinked || 0)
      const notFound = Number(payload.notFound || 0)
      const invalid = Number(payload.invalid || 0)

      if (unlinked > 0) {
        setSelectedMainProductIds((prev) => prev.filter((id) => !productIDs.includes(id)))
        if (unlinkAllFiltered) {
          setSelectAllFilteredMainProducts(false)
          setExcludedFilteredMainProductIds([])
        }
        setMainProductsVersion((prev) => prev + 1)
      }

      if (!unlinkAllFiltered && (notFound > 0 || invalid > 0)) {
        setMainProductsStatus(`Отвязано: ${unlinked}, не найдено: ${notFound}, некорректно: ${invalid}.`)
      } else {
        setMainProductsStatus(`Отвязано от категории: ${unlinked}.`)
      }
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось отвязать выбранные товары от категории')
    } finally {
      setMainProductsActionKey('')
    }
  }

  const handleSyncMainProductsFromBillz = async () => {
    try {
      setMainProductsBillzSyncing(true)
      setMainProductsStatus('Синхронизация с Billz...')

      const response = await fetch(`${API_BASE_URL}/syncMainProductsFromBillz`, { method: 'POST' })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Синхронизация не удалась (статус ${response.status})`)

      const updated = Number(payload.updated || 0)
      setMainProductsStatus(`Синхронизация Billz завершена. Обновлено: ${updated}.`)
      setToast({
        type: 'success',
        message: `Успешно обновлено ${updated.toLocaleString()} товаров`
      })
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setMainProductsStatus(err.message || 'Синхронизация Billz не удалась')
    } finally {
      setMainProductsBillzSyncing(false)
    }
  }

  const buildMainProductsQueryParams = () => {
    const params = new URLSearchParams()
    if (mainProductsSearch.trim()) params.set('search', mainProductsSearch.trim())
    if (selectedMainProductsSourceCategoryKeysParam) {
      params.set('sourceCategoryKeys', selectedMainProductsSourceCategoryKeysParam)
    }
    if (includeMainProductsWithoutCategory) params.set('withoutCategory', '1')
    if (mainProductsWithoutISBNOnly) params.set('withoutIsbn', '1')
    return params
  }

  const openMainProductModal = () => {
    const defaultCategoryId = selectedMainProductsCategories.length === 1 ? selectedMainProductsCategories[0].id : ''
    setEditingMainProductId('')
    setMainProductImageUploading(false)
    setMainProductsStatus('')
    setMainProductForm({ ...EMPTY_MAIN_PRODUCT_FORM, categoryId: defaultCategoryId, coverUrls: [] })
    setMainProductModalOpen(true)
  }

  const openEditMainProductModal = (product) => {
    const productID = getMainProductId(product)
    if (!isMongoObjectId(productID)) {
      setMainProductsStatus('Невозможно редактировать товар: неверный ID.')
      return
    }

    const categoryId = isMongoObjectId(product?.categoryId) ? String(product.categoryId) : ''
    const productCoverUrls = extractCoverUrlsFromProduct(product).map((item) => normalizeImageValue(item))
    const primaryCoverUrl = normalizeImageValue(product?.coverUrl || '') || productCoverUrls[0] || ''
    const authorRefs = Array.isArray(product?.authorRefs) ? product.authorRefs : []
    const tagRefs = Array.isArray(product?.tagRefs) ? product.tagRefs : []
    const genreRefs = Array.isArray(product?.genreRefs) ? product.genreRefs : []
    const authorNames = Array.isArray(product?.authorNames) && product.authorNames.length > 0
      ? product.authorNames
      : extractRefNames(authorRefs)
    const tagNames = Array.isArray(product?.tagNames) && product.tagNames.length > 0
      ? product.tagNames
      : extractRefNames(tagRefs)
    const genreNames = Array.isArray(product?.genreNames) && product.genreNames.length > 0
      ? product.genreNames
      : extractRefNames(genreRefs)

    setMainProductImageUploading(false)
    setMainProductsStatus('')
    setMainProductForm({
      name: String(product?.name || ''),
      isbn: String(product?.isbn || ''),
      authorCover: String(product?.authorCover || ''),
      authorNames: authorNames.join(', '),
      authorRefsJson: formatRefsJsonInput(authorRefs),
      tagRefsJson: formatRefsJsonInput(tagRefs),
      genreRefsJson: formatRefsJsonInput(genreRefs),
      tagNames: tagNames.join(', '),
      genreNames: genreNames.join(', '),
      annotation: String(product?.annotation || ''),
      coverUrl: primaryCoverUrl,
      coverUrls: normalizeCoverUrls(productCoverUrls),
      pages: formatOptionalNumberForInput(product?.pages),
      format: String(product?.format || ''),
      paperType: String(product?.paperType || ''),
      bindingType: String(product?.bindingType || ''),
      ageRestriction: String(product?.ageRestriction || ''),
      subjectName: String(product?.subjectName || ''),
      nicheName: String(product?.nicheName || ''),
      brandName: String(product?.brandName || ''),
      seriesName: String(product?.seriesName || ''),
      publisherName: String(product?.publisherName || ''),
      categoryPath: Array.isArray(product?.categoryPath) ? product.categoryPath.join(' / ') : '',
      quantity: formatOptionalNumberForInput(product?.quantity),
      price: formatOptionalNumberForInput(product?.price),
      categoryId,
      sourceGuidNom: String(product?.sourceGuidNom || ''),
      sourceGuid: String(product?.sourceGuid || ''),
      sourceNomcode: String(product?.sourceNomcode || '')
    })
    setEditingMainProductId(productID)
    setMainProductEditModalOpen(true)
  }

  const handleMainProductFormChange = (field, value) => {
    setMainProductForm((prev) => ({ ...prev, [field]: value }))
  }

  const handleUploadMainProductImages = async (files) => {
    const picked = Array.from(files || [])
    if (picked.length === 0) return

    frontendDebugLog('info', 'upload_start', {
      apiBaseUrl: API_BASE_URL,
      fileCount: picked.length,
      files: picked.map((file) => ({
        name: file?.name || '',
        size: Number(file?.size || 0),
        type: file?.type || ''
      }))
    })

    try {
      setMainProductImageUploading(true)
      setMainProductsStatus(`Загрузка ${picked.length} изображений...`)
      const formData = new FormData()
      picked.forEach((file) => formData.append('images', file))

      const response = await fetch(`${API_BASE_URL}/mainProducts/upload-images`, {
        method: 'POST',
        body: formData
      })
      const rawBody = await response.text()
      let payload = {}
      if (rawBody) {
        try {
          payload = JSON.parse(rawBody)
        } catch (_) {}
      }
      frontendDebugLog('info', 'upload_response', {
        status: response.status,
        ok: response.ok,
        requestId: response.headers.get('X-Request-ID') || '',
        bodyPreview: rawBody.slice(0, 800)
      })
      if (!response.ok) {
        const errorMessage =
          (payload && typeof payload === 'object' && (payload.error || payload.message)) ||
          rawBody ||
          `Ошибка (статус ${response.status})`
        frontendDebugLog('error', 'upload_failed_response', {
          status: response.status,
          requestId: response.headers.get('X-Request-ID') || '',
          errorMessage
        })
        throw new Error(errorMessage)
      }

      const urls = normalizeCoverUrls(
        Array.isArray(payload.data)
          ? payload.data.map((item) => normalizeImageValue(item?.path || item?.url || ''))
          : []
      )
      if (urls.length === 0) {
        setMainProductsStatus('Загрузка завершена, но URL изображений не получены.')
        frontendDebugLog('warn', 'upload_empty_urls', {
          requestId: response.headers.get('X-Request-ID') || '',
          payload
        })
        return
      }
      frontendDebugLog('info', 'upload_success', {
        requestId: response.headers.get('X-Request-ID') || '',
        uploadedUrls: urls
      })

      const merged = normalizeCoverUrls([...urls, ...(Array.isArray(mainProductForm.coverUrls) ? mainProductForm.coverUrls : [])])
      const primary = urls[0] || String(mainProductForm.coverUrl || '').trim() || merged[0] || ''
      const nextForm = { ...mainProductForm, coverUrls: merged, coverUrl: primary }
      setMainProductForm(nextForm)

      const canAutoSave = mainProductEditModalOpen && isMongoObjectId(editingMainProductId) && String(nextForm.name || '').trim() !== ''
      if (canAutoSave) {
        try {
          setMainProductsActionKey(`edit:${editingMainProductId}`)
          setMainProductsStatus('Сохранение загруженного изображения в товар...')
          const payload = buildMainProductPayload(nextForm)
          payload.name = String(nextForm.name || '').trim()
          frontendDebugLog('info', 'auto_save_start', {
            productId: editingMainProductId,
            coverUrl: payload.coverUrl,
            coverUrlsCount: Array.isArray(payload.coverUrls) ? payload.coverUrls.length : 0
          })
          const saveResponse = await fetch(`${API_BASE_URL}/mainProducts/${editingMainProductId}`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
          })
          const saveRawBody = await saveResponse.text()
          let savePayload = {}
          if (saveRawBody) {
            try {
              savePayload = JSON.parse(saveRawBody)
            } catch (_) {}
          }
          frontendDebugLog('info', 'auto_save_response', {
            status: saveResponse.status,
            ok: saveResponse.ok,
            requestId: saveResponse.headers.get('X-Request-ID') || '',
            bodyPreview: saveRawBody.slice(0, 800)
          })
          if (!saveResponse.ok) {
            const saveError =
              (savePayload && typeof savePayload === 'object' && (savePayload.error || savePayload.message)) ||
              saveRawBody ||
              `Ошибка (статус ${saveResponse.status})`
            frontendDebugLog('error', 'auto_save_failed_response', {
              status: saveResponse.status,
              requestId: saveResponse.headers.get('X-Request-ID') || '',
              saveError
            })
            throw new Error(saveError)
          }

          setMainProductsVersion((prev) => prev + 1)
          setMainProductsStatus(`Загружено ${urls.length} изображений и сохранено в товар.`)
          frontendDebugLog('info', 'auto_save_success', {
            productId: editingMainProductId,
            requestId: saveResponse.headers.get('X-Request-ID') || ''
          })
          return
        } catch (saveErr) {
          setMainProductsStatus(`Загружено ${urls.length} изображений, но автосохранение не удалось: ${saveErr.message}. Нажмите «Сохранить изменения».`)
          frontendDebugLog('error', 'auto_save_failed', {
            productId: editingMainProductId,
            message: saveErr?.message || String(saveErr || '')
          })
          return
        } finally {
          setMainProductsActionKey('')
        }
      }

      setMainProductsStatus(`Загружено ${urls.length} изображений. Новое изображение назначено главным. Нажмите «Сохранить».`)
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось загрузить изображения')
      frontendDebugLog('error', 'upload_exception', {
        message: err?.message || String(err || '')
      })
      setToast({
        type: 'error',
        message: err.message || 'Не удалось загрузить изображения'
      })
    } finally {
      setMainProductImageUploading(false)
    }
  }

  const buildMainProductPayload = (form) => {
    const authorRefs = sanitizeAuthorRefs(parseRefsJsonInput(form.authorRefsJson, 'Авторы'))
    const tagRefs = sanitizeTagGenreRefs(parseRefsJsonInput(form.tagRefsJson, 'Теги'))
    const genreRefs = sanitizeTagGenreRefs(parseRefsJsonInput(form.genreRefsJson, 'Жанры'))
    const authorNames = splitCommaValue(form.authorNames)
    const tagNames = splitCommaValue(form.tagNames)
    const genreNames = splitCommaValue(form.genreNames)

    return {
      name: String(form.name || '').trim(),
      isbn: String(form.isbn || '').trim(),
      authorCover: String(form.authorCover || '').trim(),
      authorNames,
      authorRefs,
      tagRefs,
      genreRefs,
      tagNames,
      genreNames,
      annotation: String(form.annotation || '').trim(),
      coverUrl: (() => {
        const typed = normalizeImageValue(form.coverUrl)
        if (typed) return typed
        const uploaded = normalizeCoverUrls(form.coverUrls).map((item) => normalizeImageValue(item))
        return uploaded[0] || ''
      })(),
      coverUrls: (() => {
        const uploaded = normalizeCoverUrls(form.coverUrls).map((item) => normalizeImageValue(item))
        const typed = normalizeImageValue(form.coverUrl)
        if (!typed) return uploaded
        return normalizeCoverUrls([typed, ...uploaded])
      })(),
      pages: parseOptionalInteger(form.pages),
      format: String(form.format || '').trim(),
      paperType: String(form.paperType || '').trim(),
      bindingType: String(form.bindingType || '').trim(),
      ageRestriction: String(form.ageRestriction || '').trim(),
      subjectName: String(form.subjectName || '').trim(),
      nicheName: String(form.nicheName || '').trim(),
      brandName: String(form.brandName || '').trim(),
      seriesName: String(form.seriesName || '').trim(),
      publisherName: String(form.publisherName || '').trim(),
      categoryPath: splitPathValue(form.categoryPath),
      quantity: parseOptionalNumber(form.quantity),
      price: parseOptionalNumber(form.price),
      categoryId: String(form.categoryId || '').trim(),
      sourceGuidNom: String(form.sourceGuidNom || '').trim(),
      sourceGuid: String(form.sourceGuid || '').trim(),
      sourceNomcode: String(form.sourceNomcode || '').trim()
    }
  }

  const handleCreateMainProduct = async (event) => {
    event.preventDefault()
    const name = String(mainProductForm.name || '').trim()
    if (!name) {
      setMainProductsStatus('Поле «Название» обязательно.')
      return
    }

    try {
      const payload = buildMainProductPayload(mainProductForm)
      payload.name = name

      frontendDebugLog('info', 'create_start', {
        name: payload.name,
        categoryId: payload.categoryId,
        coverUrl: payload.coverUrl,
        coverUrlsCount: Array.isArray(payload.coverUrls) ? payload.coverUrls.length : 0
      })

      setMainProductCreating(true)
      setMainProductsStatus('Создание основного товара...')
      const response = await fetch(`${API_BASE_URL}/mainProducts`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
      const raw = await response.text()
      let data = {}
      if (raw) {
        try {
          data = JSON.parse(raw)
        } catch (_) {}
      }
      frontendDebugLog('info', 'create_response', {
        status: response.status,
        ok: response.ok,
        requestId: response.headers.get('X-Request-ID') || '',
        bodyPreview: raw.slice(0, 800)
      })
      if (!response.ok) {
        const errorMessage =
          (data && typeof data === 'object' && (data.error || data.message)) ||
          raw ||
          `Ошибка (статус ${response.status})`
        throw new Error(errorMessage)
      }

      setMainProductsStatus('Основной товар успешно создан.')
      setMainProductModalOpen(false)
      setMainProductForm(EMPTY_MAIN_PRODUCT_FORM)
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось создать товар')
      frontendDebugLog('error', 'create_failed', {
        message: err?.message || String(err || '')
      })
    } finally {
      setMainProductCreating(false)
    }
  }

  const handleUpdateMainProduct = async (event) => {
    event.preventDefault()
    if (!isMongoObjectId(editingMainProductId)) {
      setMainProductsStatus('Неверный ID товара для обновления.')
      return
    }

    const name = String(mainProductForm.name || '').trim()
    if (!name) {
      setMainProductsStatus('Поле «Название» обязательно.')
      return
    }

    try {
      const payload = buildMainProductPayload(mainProductForm)
      payload.name = name

      frontendDebugLog('info', 'update_start', {
        productId: editingMainProductId,
        name: payload.name,
        categoryId: payload.categoryId,
        coverUrl: payload.coverUrl,
        coverUrlsCount: Array.isArray(payload.coverUrls) ? payload.coverUrls.length : 0
      })

      setMainProductEditing(true)
      setMainProductsActionKey(`edit:${editingMainProductId}`)
      setMainProductsStatus('Сохранение изменений...')
      const response = await fetch(`${API_BASE_URL}/mainProducts/${editingMainProductId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      })
      const raw = await response.text()
      let data = {}
      if (raw) {
        try {
          data = JSON.parse(raw)
        } catch (_) {}
      }
      frontendDebugLog('info', 'update_response', {
        status: response.status,
        ok: response.ok,
        requestId: response.headers.get('X-Request-ID') || '',
        bodyPreview: raw.slice(0, 800)
      })
      if (!response.ok) {
        const errorMessage =
          (data && typeof data === 'object' && (data.error || data.message)) ||
          raw ||
          `Ошибка (статус ${response.status})`
        throw new Error(errorMessage)
      }

      setMainProductsStatus('Основной товар успешно обновлен.')
      setMainProductEditModalOpen(false)
      setEditingMainProductId('')
      setMainProductForm(EMPTY_MAIN_PRODUCT_FORM)
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setMainProductsStatus(err.message || 'Не удалось обновить товар')
      frontendDebugLog('error', 'update_failed', {
        productId: editingMainProductId,
        message: err?.message || String(err || '')
      })
    } finally {
      setMainProductEditing(false)
      setMainProductsActionKey('')
    }
  }

  const handleExportMainProducts = async (format) => {
    const exportFormat = format === 'xlsx' ? 'xlsx' : 'csv'
    try {
      setMainProductsStatus(`Экспорт ${exportFormat.toUpperCase()}...`)
      const params = buildMainProductsQueryParams()
      params.set('format', exportFormat)
      const response = await fetch(`${API_BASE_URL}/mainProducts/export?${params.toString()}`)
      if (!response.ok) {
        let message = `Ошибка (статус ${response.status})`
        try {
          const payload = await response.json()
          if (payload?.error) message = payload.error
        } catch (_) {}
        throw new Error(message)
      }

      const blob = await response.blob()
      const contentDisposition = response.headers.get('content-disposition') || ''
      const filenameMatch = contentDisposition.match(/filename="([^"]+)"/i)
      const filename = filenameMatch?.[1] || `main_products.${exportFormat}`
      const url = window.URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      document.body.appendChild(link)
      link.click()
      document.body.removeChild(link)
      window.URL.revokeObjectURL(url)

      setMainProductsStatus(`Экспорт ${exportFormat.toUpperCase()} выполнен.`)
    } catch (err) {
      setMainProductsStatus(err.message || 'Экспорт не удался')
    }
  }

  const handleImportMainProductsClick = () => {
    if (!mainProductsImportInputRef.current || mainProductsImporting) return
    mainProductsImportInputRef.current.click()
  }

  const handleImportMainProductsChange = async (event) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return

    try {
      setMainProductsImporting(true)
      setMainProductsStatus(`Импорт ${file.name}...`)
      const formData = new FormData()
      formData.append('file', file)

      const response = await fetch(`${API_BASE_URL}/mainProducts/import`, {
        method: 'POST',
        body: formData
      })
      const payload = await response.json()
      if (!response.ok) throw new Error(payload.error || `Ошибка (статус ${response.status})`)

      const inserted = Number(payload.inserted || 0)
      const modified = Number(payload.modified || 0)
      const skipped = Number(payload.skipped || 0)
      setMainProductsStatus(`Импорт завершен. Добавлено: ${inserted}, изменено: ${modified}, пропущено: ${skipped}.`)
      setMainProductsVersion((prev) => prev + 1)
    } catch (err) {
      setMainProductsStatus(err.message || 'Импорт не удался')
    } finally {
      setMainProductsImporting(false)
    }
  }

  const selectedEksmoCategoryText = useMemo(() => {
    if (selectedEksmoNodes.length === 0) return 'Все категории'
    if (selectedEksmoNodes.length === 1) return selectedEksmoNodes[0].name
    return `${selectedEksmoNodes.length} выбрано`
  }, [selectedEksmoNodes])
  const eksmoActionBusy = Boolean(eksmoActionKey)

  const authorFilterOptions = useMemo(() => normalizeStringOptions(meta.authors.map((author) => author?.name)), [meta.authors])
  const genreFilterOptions = useMemo(() => normalizeStringOptions(meta.genres), [meta.genres])
  const brandFilterOptions = useMemo(() => normalizeStringOptions(meta.brands), [meta.brands])
  const seriesFilterOptions = useMemo(() => normalizeStringOptions(meta.series), [meta.series])
  const publisherFilterOptions = useMemo(() => normalizeStringOptions(meta.publishers), [meta.publishers])
  const ageFilterOptions = useMemo(() => AGE_FILTER_OPTIONS, [])

  const activeEksmoFilterCount = useMemo(() => {
    let count = 0
    if (brandFilter.length > 0) count++
    if (serieFilter.length > 0) count++
    if (publisherFilter.length > 0) count++
    if (authorFilter.length > 0) count++
    if (genreFilter.length > 0) count++
    if (ageFilter.length > 0) count++
    if (selectedEksmoNodes.length > 0) count++
    if (productsLimit !== DEFAULT_PRODUCTS_LIMIT) count++
    return count
  }, [brandFilter, serieFilter, publisherFilter, authorFilter, genreFilter, ageFilter, selectedEksmoNodes, productsLimit])

  const selectedMainCategoryPathText = selectedMainCategory?.path?.join(' / ') || 'Основная категория не выбрана'
  const selectedMainProductsCategoryPathText =
    selectedMainProductsCategories.length === 0
      ? 'Не выбрана'
      : selectedMainProductsCategories.length === 1
        ? selectedMainProductsCategories[0].path?.join(' / ') || selectedMainProductsCategories[0].name
        : `${selectedMainProductsCategories.length} категорий выбрано`

  const categoryDropdownMenu =
    isCategoryDropdownOpen && typeof document !== 'undefined'
      ? createPortal(
          <div ref={categoryDropdownMenuRef} className="dropdown-content" style={categoryDropdownStyle}>
            <div className="filter-dropdown-search-wrap">
              <input
                type="text"
                className="filter-dropdown-search-input"
                value={categorySearchQuery}
                placeholder="Поиск категорий..."
                autoComplete="off"
                spellCheck={false}
                onChange={(event) => setCategorySearchQuery(event.target.value)}
              />
            </div>
            {eksmoTreeLoading ? (
              <p className="dropdown-loading">Загрузка...</p>
            ) : filteredEksmoTree.length === 0 ? (
              <p className="dropdown-empty">{normalizedCategorySearchQuery ? 'По запросу категории не найдены.' : 'Категории отсутствуют. Сначала выполните синхронизацию.'}</p>
            ) : (
              <div className="tree-container">
                {filteredEksmoTree.map((node) => (
                  <TreeNode
                    key={node.guid}
                    node={node}
                    depth={0}
                    expanded={eksmoExpanded}
                    onToggle={toggleEksmoNode}
                    onSelect={toggleEksmoSelection}
                    selectedGuids={selectedEksmoNodes.map((n) => n.guid)}
                    forceOpen={Boolean(normalizedCategorySearchQuery)}
                  />
                ))}
              </div>
            )}
          </div>,
          document.body
        )
      : null

  return (
    <main className="page">
      {activePage === 'eksmo' ? (
        <div className="layout">
          <aside className="sidebar">
            <div className="sidebar-panel">
              <h2>Основные категории</h2>
              <p className="sidebar-hint">Выберите одну категорию как цель для копирования.</p>
              {selectedMainCategory && (
                <p className="sidebar-selected" title={selectedMainCategoryPathText}>
                  Выбрано: {selectedMainCategoryPathText}
                </p>
              )}
              {mainCategoriesLoading ? (
                <p className="sidebar-loading">Загрузка...</p>
              ) : mainCategories.length === 0 ? (
                <p className="sidebar-empty">Категории не найдены.</p>
              ) : (
                <div className="sidebar-tree">
                  {mainCategories.map((cat) => (
                    <MainCategoryNode
                      key={cat.id}
                      node={cat}
                      depth={0}
                      path={[]}
                      expanded={mainCategoriesExpanded}
                      onToggle={toggleMainCategoryNode}
                      selectedCategoryIds={selectedMainCategory?.id ? [selectedMainCategory.id] : []}
                      onSelectCategory={toggleMainCategorySelection}
                    />
                  ))}
                </div>
              )}
            </div>
          </aside>

          <div className="main-content">
            <header className="header">
              <div className="header-left">
                <h1>Все товары</h1>
                <span className="product-count">{productsTotalItems.toLocaleString()} товаров</span>
              </div>
              {/* <div className="header-right">
                <button className="btn primary" type="button" onClick={handleSync} disabled={syncing}>
                  {syncing ? 'Синхронизация...' : 'Синхронизировать'}
                </button>
                <button className="btn" type="button" onClick={handleResetSync} disabled={syncing}>
                  Перезапустить
                </button>
              </div> */}
            </header>

            {syncStatus && <p className="sync-status">{syncStatus}</p>}
            {copyStatus && <p className="copy-status">{copyStatus}</p>}

            <div className="filter-bar">
              <div className={`search-field ${productsSearchPending ? 'pending' : ''}`} title="Нажмите / для фокуса поиска">
                <SearchIcon />
                <input
                  ref={productsSearchInputRef}
                  type="text"
                  className="search-input"
                  value={searchInput}
                  placeholder="Поиск по названию, ISBN, автору..."
                  autoComplete="off"
                  spellCheck={false}
                  enterKeyHint="search"
                  onKeyDown={(event) => {
                    if (event.key !== 'Escape') return
                    if (searchInput.trim()) {
                      setSearchInput('')
                      return
                    }
                    event.currentTarget.blur()
                  }}
                  onChange={(e) => setSearchInput(e.target.value)}
                />
                {searchInput.trim() !== '' && (
                  <button
                    type="button"
                    className="search-clear-btn"
                    aria-label="Очистить поиск"
                    onClick={() => {
                      setSearchInput('')
                      productsSearchInputRef.current?.focus()
                    }}
                  >
                    ×
                  </button>
                )}
                {productsSearchPending && <span className="search-pending-dot" aria-hidden="true" />}
              </div>

              <div className="category-dropdown" ref={categoryDropdownRef}>
                <button
                  ref={categoryDropdownButtonRef}
                  type="button"
                  className={`dropdown-btn ${selectedEksmoNodes.length > 0 ? 'has-selection' : ''}`}
                  aria-expanded={isCategoryDropdownOpen}
                  aria-haspopup="listbox"
                  onClick={() => setIsCategoryDropdownOpen(!isCategoryDropdownOpen)}
                >
                  <span>{selectedEksmoCategoryText}</span>
                  <span className="arrow">{isCategoryDropdownOpen ? '▲' : '▼'}</span>
                </button>
              </div>
              {categoryDropdownMenu}

              <FilterMultiSelect label="Автор" options={authorFilterOptions} selectedValues={authorFilter} onChange={setAuthorFilter} />

              <FilterMultiSelect label="Жанр" options={genreFilterOptions} selectedValues={genreFilter} onChange={setGenreFilter} />

              <FilterMultiSelect label="Бренд" options={brandFilterOptions} selectedValues={brandFilter} onChange={setBrandFilter} />

              <FilterMultiSelect label="Серия" options={seriesFilterOptions} selectedValues={serieFilter} onChange={setSerieFilter} />

              <FilterMultiSelect label="Издатель" options={publisherFilterOptions} selectedValues={publisherFilter} onChange={setPublisherFilter} />

              <FilterMultiSelect label="Возраст" options={ageFilterOptions} selectedValues={ageFilter} onChange={setAgeFilter} />

              {activeEksmoFilterCount > 0 && (
                <button className="btn clear-btn" type="button" onClick={clearFilters}>
                  Очистить ({activeEksmoFilterCount})
                </button>
              )}
            </div>

            <div className="action-bar">
              <div className="action-left">
                <label className="select-all">
                  <input type="checkbox" checked={allVisibleSelected} onChange={toggleSelectAllVisible} disabled={visibleProductIds.length === 0 || eksmoActionBusy || copying} />
                  <span>Выбрать страницу ({selectedVisibleCount}/{selectPageProductsCount})</span>
                </label>
                <span className="selected-count">Всего выбрано: {selectedProductIds.length}</span>
              </div>

              <div className="action-right">
                <div className="view-toggle" role="group" aria-label="режим отображения">
                  <button type="button" className={viewMode === 'document' ? 'active' : ''} onClick={() => setViewMode('document')}>
                    Карточки
                  </button>
                  <button type="button" className={viewMode === 'list' ? 'active' : ''} onClick={() => setViewMode('list')}>
                    Список
                  </button>
                </div>
                <button className="btn" type="button" onClick={handleCopySelected} disabled={copying || selectedProductIds.length === 0 || !selectedMainCategory?.id || eksmoActionBusy}>
                  {copying ? 'Копирование...' : 'Копировать выбранные'}
                </button>
                <button className="btn primary" type="button" onClick={handleCopyGroup} disabled={copying || !selectedMainCategory?.id || eksmoActionBusy}>
                  {copying ? 'Копирование...' : `Копировать группой (${productsLimit} макс)`}
                </button>
                <button className="btn" type="button" onClick={handleCopyMissingToMain} disabled={copying || eksmoActionBusy}>
                  {copying ? 'Копирование...' : 'Добавить отсутствующие в Main'}
                </button>
                <button className="btn table-btn danger" type="button" onClick={handleDeleteSelectedEksmoProducts} disabled={selectedProductIds.length === 0 || copying || eksmoActionBusy}>
                  {eksmoActionKey === 'bulk-delete' ? 'Удаление...' : `Удалить выбранные (${Math.min(selectedProductIds.length, MAX_PRODUCTS_LIMIT)})`}
                </button>
              </div>
            </div>

            {selectedEksmoNodes.length > 0 && (
              <div className="selected-tags">
                {selectedEksmoNodes.map((node) => (
                  <span key={node.guid} className="tag">
                    {node.name}
                    <button type="button" onClick={() => toggleEksmoSelection(node)}>
                      &times;
                    </button>
                  </span>
                ))}
              </div>
            )}

            {metaLoading && <p className="status">Загрузка фильтров...</p>}
            {productsLoading && <p className="status">Загрузка товаров...</p>}
            {productsError && <p className="status error">{productsError}</p>}
            {!productsLoading && !productsError && products.length === 0 && <p className="status">Товары не найдены.</p>}

            {!productsLoading && !productsError && products.length > 0 && (
              <>
                {viewMode === 'document' ? (
                  <div className="products-grid">
                    {products.map((product) => {
                      const mongoID = getEksmoMongoId(product)
                      return (
                        <ProductCard
                          key={getEksmoProductKey(product)}
                          product={product}
                          checked={mongoID ? selectedProductIds.includes(mongoID) : false}
                          selectable={Boolean(mongoID) && !eksmoActionBusy && !copying}
                          onToggle={() => toggleProductSelection(mongoID)}
                          onViewDetails={() => setDetailsProduct(product)}
                          onDelete={() => handleDeleteEksmoProduct(mongoID)}
                          deleteDisabled={!mongoID || eksmoActionBusy || copying}
                          deleting={eksmoActionKey === `delete:${mongoID}`}
                        />
                      )
                    })}
                  </div>
                ) : (
                  <div className="products-list">
                    {products.map((product) => {
                      const mongoID = getEksmoMongoId(product)
                      return (
                        <ProductListRow
                          key={getEksmoProductKey(product)}
                          product={product}
                          checked={mongoID ? selectedProductIds.includes(mongoID) : false}
                          selectable={Boolean(mongoID) && !eksmoActionBusy && !copying}
                          onToggle={() => toggleProductSelection(mongoID)}
                          onViewDetails={() => setDetailsProduct(product)}
                          onDelete={() => handleDeleteEksmoProduct(mongoID)}
                          deleteDisabled={!mongoID || eksmoActionBusy || copying}
                          deleting={eksmoActionKey === `delete:${mongoID}`}
                        />
                      )
                    })}
                  </div>
                )}

                <Pagination
                  page={productsPage}
                  totalPages={productsTotalPages}
                  totalItems={productsTotalItems}
                  pageSize={productsLimit}
                  onPageChange={setProductsPage}
                  onPageSizeChange={handleProductsLimitChange}
                  itemLabel="товаров"
                />
              </>
            )}
          </div>
        </div>
      ) : activePage === 'mainProducts' ? (
        <div className="layout">
          <aside className="sidebar">
            <div className="sidebar-panel">
              <h2>Основные категории</h2>
              <p className="sidebar-hint">Выберите одну категорию для привязки выбранных основных товаров.</p>
              <p className="sidebar-selected" title={selectedMainProductsCategoryPathText}>
                Категория: {selectedMainProductsCategoryPathText}
              </p>
              {mainCategoriesLoading ? (
                <p className="sidebar-loading">Загрузка...</p>
              ) : mainCategories.length === 0 ? (
                <p className="sidebar-empty">Категории не найдены.</p>
              ) : (
                <div className="sidebar-tree">
                  {mainCategories.map((cat) => (
                    <MainCategoryNode
                      key={cat.id}
                      node={cat}
                      depth={0}
                      path={[]}
                      expanded={mainProductsExpanded}
                      onToggle={toggleMainProductsCategoryNode}
                      selectedCategoryIds={selectedMainProductsCategories.map((item) => item.id)}
                      onSelectCategory={toggleMainProductsCategorySelection}
                    />
                  ))}
                </div>
              )}
            </div>
          </aside>

          <div className="main-content">
            <header className="header">
              <div className="header-left">
                <h1>Основные товары</h1>
                <span className="product-count">{mainProductsTotalItems.toLocaleString()} товаров</span>
              </div>
              <div className="header-right">
                <div className="view-toggle" role="group" aria-label="режим отображения основных товаров">
                  <button type="button" className={mainProductsViewMode === 'cards' ? 'active' : ''} onClick={() => setMainProductsViewMode('cards')}>
                    Карточки
                  </button>
                  <button type="button" className={mainProductsViewMode === 'list' ? 'active' : ''} onClick={() => setMainProductsViewMode('list')}>
                    Список
                  </button>
                </div>
                <button className="btn" type="button" onClick={openMainProductModal} disabled={mainProductCreating || mainProductEditing || mainProductImageUploading || mainProductsImporting}>
                  Добавить товар
                </button>
                {/* <button className="btn primary" type="button" onClick={handleSyncMainProductsFromBillz} disabled={mainProductsBillzSyncing}>
                  {mainProductsBillzSyncing ? 'Синхронизация...' : 'Синхронизировать из Billz'}
                </button> */}
              </div>
            </header>

            {mainProductsStatus && <p className="copy-status">{mainProductsStatus}</p>}

            <div className="filter-bar">
              <div className={`search-field ${mainProductsSearchPending ? 'pending' : ''}`} title="Нажмите / для фокуса поиска">
                <SearchIcon />
                <input
                  ref={mainProductsSearchInputRef}
                  type="text"
                  className="search-input"
                  value={mainProductsSearchInput}
                  placeholder="Поиск основных товаров по названию, ISBN, автору..."
                  autoComplete="off"
                  spellCheck={false}
                  enterKeyHint="search"
                  onKeyDown={(event) => {
                    if (event.key !== 'Escape') return
                    if (mainProductsSearchInput.trim()) {
                      setMainProductsSearchInput('')
                      return
                    }
                    event.currentTarget.blur()
                  }}
                  onChange={(e) => setMainProductsSearchInput(e.target.value)}
                />
                {mainProductsSearchInput.trim() !== '' && (
                  <button
                    type="button"
                    className="search-clear-btn"
                    aria-label="Очистить поиск по основным товарам"
                    onClick={() => {
                      setMainProductsSearchInput('')
                      mainProductsSearchInputRef.current?.focus()
                    }}
                  >
                    ×
                  </button>
                )}
                {mainProductsSearchPending && <span className="search-pending-dot" aria-hidden="true" />}
              </div>
              <label className="select-all">
                <input
                  type="checkbox"
                  checked={allVisibleMainProductsSelected}
                  onChange={toggleSelectAllVisibleMainProducts}
                  disabled={visibleMainProductIds.length === 0 || mainProductsActionKey !== ''}
                />
                <span>Выбрать страницу ({selectedVisibleMainProductsCount}/{selectPageMainProductsCount})</span>
              </label>
              <label className="select-all">
                <input
                  type="checkbox"
                  checked={selectAllFilteredMainProducts}
                  onChange={toggleSelectAllFilteredMainProducts}
                  disabled={mainProductsTotalItems === 0 || mainProductsActionKey !== ''}
                />
                <span>Выбрать все по фильтру ({mainProductsTotalItems.toLocaleString()})</span>
              </label>
              <label className="select-all">
                <input
                  type="checkbox"
                  checked={mainProductsWithoutISBNOnly}
                  onChange={() => setMainProductsWithoutISBNOnly((prev) => !prev)}
                  disabled={mainProductsActionKey !== ''}
                />
                <span>Только без ISBN</span>
              </label>
              <FilterTreeSelect
                label="Категория"
                nodes={mainProductsSourceCategories}
                selectedValues={mainProductsCategoryFilter}
                onChange={setMainProductsCategoryFilter}
                loading={mainProductsSourceCategoriesLoading}
              />
              <button
                className="btn table-btn"
                type="button"
                onClick={handleLinkSelectedMainProductsToCategory}
                disabled={selectedMainProductsCount === 0 || selectedMainProductsCategories.length !== 1 || mainProductsActionKey !== ''}
              >
                {mainProductsActionKey === 'bulk-link-category' ? 'Привязка...' : 'Связать с категорией'}
              </button>
              <button
                className="btn table-btn"
                type="button"
                onClick={handleUnlinkSelectedMainProductsCategory}
                disabled={selectedMainProductsCount === 0 || mainProductsActionKey !== ''}
              >
                {mainProductsActionKey === 'bulk-unlink-category' ? 'Отвязка...' : 'Отвязать от категории'}
              </button>
              <button className="btn table-btn" type="button" onClick={() => handleExportMainProducts('csv')} disabled={mainProductsLoading || mainProductsImporting}>
                Экспорт CSV
              </button>
              <button className="btn table-btn" type="button" onClick={() => handleExportMainProducts('xlsx')} disabled={mainProductsLoading || mainProductsImporting}>
                Экспорт XLSX
              </button>
              <button className="btn table-btn" type="button" onClick={handleImportMainProductsClick} disabled={mainProductsImporting}>
                {mainProductsImporting ? 'Импорт...' : 'Импорт CSV/XLSX'}
              </button>
              <input
                ref={mainProductsImportInputRef}
                className="hidden-file-input"
                type="file"
                accept=".csv,.xlsx"
                onChange={handleImportMainProductsChange}
              />
              <button
                className="btn table-btn danger"
                type="button"
                onClick={handleDeleteSelectedMainProducts}
                disabled={selectedMainProductsCount === 0 || mainProductsActionKey !== ''}
              >
                {mainProductsActionKey === 'bulk-delete'
                  ? 'Удаление...'
                  : selectAllFilteredMainProducts
                    ? `Удалить по фильтру (${selectedMainProductsCount.toLocaleString()})`
                    : `Удалить выбранные (${selectedMainProductIds.length})`}
              </button>
              <button className="btn clear-btn" type="button" onClick={clearMainProductsFilters}>
                Очистить фильтры
              </button>
            </div>

            {mainProductsLoading && <p className="status">Загрузка основных товаров...</p>}
            {mainProductsError && <p className="status error">{mainProductsError}</p>}
            {!mainProductsLoading && !mainProductsError && mainProducts.length === 0 && <p className="status">Основные товары не найдены.</p>}

            {!mainProductsLoading && !mainProductsError && mainProducts.length > 0 && (
              <>
                {mainProductsViewMode === 'cards' ? (
                  <div
                    className="products-grid"
                    style={{ gridTemplateColumns: `repeat(${clampCardColumns(mainProductsCardColumns)}, minmax(0, 1fr))` }}
                  >
                    {mainProducts.map((product) => {
                      const id = getMainProductId(product)
                      const actionDeletingModel = mainProductsActionKey === `delete:${id}`
                      const actionEditingModel = mainProductsActionKey === `edit:${id}`
                      const actionBusy = Boolean(mainProductsActionKey)

                      return (
                        <MainProductCard
                          key={id || product.sourceGuidNom || product.sourceGuid || product.name}
                          product={product}
                          checked={id ? (selectAllFilteredMainProducts ? !excludedFilteredMainProductIdSet.has(id) : selectedMainProductIds.includes(id)) : false}
                          selectable={Boolean(id) && !actionBusy}
                          onToggle={() => toggleMainProductSelection(id)}
                          onViewDetails={() => setDetailsProduct(product)}
                          onEdit={() => openEditMainProductModal(product)}
                          onDelete={() => handleDeleteMainProduct(id)}
                          editDisabled={!id || actionBusy}
                          deleteDisabled={!id || actionBusy}
                          editing={actionEditingModel}
                          deleting={actionDeletingModel}
                        />
                      )
                    })}
                  </div>
                ) : (
                  <div className="main-products-table-wrap">
                    <table className="main-products-table">
                      <thead>
                        <tr>
                          <th style={{ width: '48px' }}></th>
                          <th>Обложка</th>
                          <th>Название</th>
                          <th>Автор</th>
                          <th>ISBN</th>
                          <th>Количество</th>
                          <th>Цена</th>
                          <th>Категория</th>
                          <th>Действия</th>
                        </tr>
                      </thead>
                      <tbody>
                        {mainProducts.map((product) => {
                          const id = getMainProductId(product)
                          const categoryText = Array.isArray(product.categoryPath) && product.categoryPath.length > 0 ? product.categoryPath.join(' / ') : 'Без категории'
                          const actionDeletingModel = mainProductsActionKey === `delete:${id}`
                          const actionEditingModel = mainProductsActionKey === `edit:${id}`
                          const actionBusy = Boolean(mainProductsActionKey)
                          const checked = id ? (selectAllFilteredMainProducts ? !excludedFilteredMainProductIdSet.has(id) : selectedMainProductIds.includes(id)) : false
                          const selectable = Boolean(id) && !actionBusy

                          const handleRowToggle = () => {
                            if (!selectable) return
                            toggleMainProductSelection(id)
                          }

                          const handleRowKeyDown = (event) => {
                            if (!selectable) return
                            if (event.key === 'Enter' || event.key === ' ') {
                              event.preventDefault()
                              toggleMainProductSelection(id)
                            }
                          }

                          return (
                            <tr
                              key={id || product.sourceGuidNom || product.sourceGuid || product.name}
                              className={checked ? 'selected' : ''}
                              onClick={handleRowToggle}
                              onKeyDown={handleRowKeyDown}
                              role={selectable ? 'checkbox' : undefined}
                              aria-checked={selectable ? checked : undefined}
                              tabIndex={selectable ? 0 : -1}
                            >
                              <td>
                                <input
                                  type="checkbox"
                                  checked={checked}
                                  disabled={!selectable}
                                  onClick={(event) => event.stopPropagation()}
                                  onChange={() => toggleMainProductSelection(id)}
                                />
                              </td>
                              <td>
                                <div className="main-products-cover-cell">
                                  {product.coverUrl ? (
                                    <img src={resolveImageUrl(product.coverUrl)} alt={product.name || ''} loading="lazy" />
                                  ) : (
                                    <div className="no-cover">Нет изображения</div>
                                  )}
                                </div>
                              </td>
                              <td>{product.name || 'Без названия'}</td>
                              <td>{product.authorNames?.join(', ') || product.authorCover || '-'}</td>
                              <td>{product.isbn || '-'}</td>
                              <td>{formatMainMetric(product.quantity, 3)}</td>
                              <td>{formatMainMetric(product.price, 2)}</td>
                              <td>{categoryText}</td>
                              <td>
                                <div className="table-actions" onClick={(event) => event.stopPropagation()}>
                                  <button
                                    className="btn table-btn"
                                    type="button"
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      setDetailsProduct(product)
                                    }}
                                  >
                                    <EyeIcon />
                                    Просмотр
                                  </button>
                                  <button
                                    className="btn table-btn"
                                    type="button"
                                    disabled={!id || actionBusy}
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      openEditMainProductModal(product)
                                    }}
                                  >
                                    <EditIcon />
                                    {actionEditingModel ? 'Сохранение...' : 'Изменить'}
                                  </button>
                                  <button
                                    className="btn table-btn danger"
                                    type="button"
                                    disabled={!id || actionBusy}
                                    onClick={(event) => {
                                      event.stopPropagation()
                                      handleDeleteMainProduct(id)
                                    }}
                                  >
                                    {actionDeletingModel ? 'Удаление...' : 'Удалить из модели'}
                                  </button>
                                </div>
                              </td>
                            </tr>
                          )
                        })}
                      </tbody>
                    </table>
                  </div>
                )}

                <Pagination
                  page={mainProductsPage}
                  totalPages={mainProductsTotalPages}
                  totalItems={mainProductsTotalItems}
                  pageSize={mainProductsLimit}
                  onPageChange={setMainProductsPage}
                  onPageSizeChange={handleMainProductsLimitChange}
                  showCardColumnsControl={mainProductsViewMode === 'cards'}
                  cardColumns={mainProductsCardColumns}
                  onCardColumnsChange={(value) => setMainProductsCardColumns(clampCardColumns(value))}
                  itemLabel="товаров"
                />
              </>
            )}
          </div>
        </div>
      ) : (
        <div className="main-content duplicates-page">
          <header className="header">
            <div className="header-left">
              <h1>Дубликаты товаров</h1>
              <span className="product-count">{duplicateGroups.length.toLocaleString()} групп</span>
            </div>
            <div className="header-right">
              <button className="btn" type="button" onClick={handleReloadDuplicateScan} disabled={duplicatesLoading || duplicateActionBusy}>
                {duplicatesLoading ? 'Сканирование...' : 'Обновить'}
              </button>
            </div>
          </header>

          {duplicatesStatus && <p className="copy-status">{duplicatesStatus}</p>}
          {!duplicatesLoading && !duplicatesError && (
            <p className="sync-status">
              Товаров в группах дубликатов: {duplicateGroupsProductCount.toLocaleString()}
            </p>
          )}
          {duplicatesError && <p className="status error">{duplicatesError}</p>}
          {duplicatesLoading && <p className="status">Поиск дубликатов...</p>}
          {!duplicatesLoading && !duplicatesError && duplicateGroups.length === 0 && <p className="status">Дубликаты не найдены.</p>}

          {!duplicatesLoading && !duplicatesError && duplicateGroups.length > 0 && (
            <>
              <div className="action-bar duplicate-action-bar">
                <div className="action-left">
                  <label className="select-all">
                    <input type="checkbox" checked={allDuplicateProductsSelected} onChange={toggleSelectAllDuplicateProducts} disabled={maxDuplicateSelectionIds.length === 0 || duplicateActionBusy} />
                    <span>Выбрать максимум ({selectedDuplicateIds.length}/{duplicateProductIds.length})</span>
                  </label>
                </div>
                <div className="action-right">
                  <button className="btn table-btn danger" type="button" onClick={handleDeleteSelectedDuplicateProducts} disabled={selectedDuplicateIds.length === 0 || duplicateActionBusy}>
                    {duplicateBulkActionKey === 'bulk-delete-selected' ? 'Удаление...' : `Удалить выбранные (${selectedDuplicateIds.length})`}
                  </button>
                  <button className="btn table-btn warn" type="button" onClick={handleDeleteDuplicatesKeepOne} disabled={duplicateActionBusy}>
                    {duplicateBulkActionKey === 'bulk-keep-one' ? 'Удаление...' : 'Удалить дубликаты (оставить по 1)'}
                  </button>
                </div>
              </div>

              <div className="duplicate-groups-list">
                {duplicateGroups.map((group, index) => (
                  <section key={group.key} className="duplicate-group-card">
                    <div className="duplicate-group-head">
                      <h3>Группа {index + 1}</h3>
                      <p>
                        Поля: <strong>{group.fields.map((field) => field.toUpperCase()).join(', ')}</strong> · Значение: <code>{group.rawValue || group.normalizedValue}</code> · Совпадений: {group.products.length}
                      </p>
                    </div>

                    <div className="duplicate-group-items">
                      {group.products.map((entry) => {
                        const product = entry.product
                        const productID = getEksmoMongoId(product)
                        const authors = product.authorNames?.join(', ') || product.authorCover || 'Неизвестно'
                        const isbnValue = getProductCompareValue(product, 'isbn')
                        const codeValue = getProductCompareValue(product, 'code')
                        const barcodeValue = getProductCompareValue(product, 'barcode')
                        const gtinValue = getProductCompareValue(product, 'gtin')
                        const deleting = productID && duplicateDeleteId === productID

                        return (
                          <article key={`${group.key}:${productID || getEksmoProductKey(product)}`} className="duplicate-item">
                            <div className="duplicate-item-select">
                              <input
                                type="checkbox"
                                checked={productID ? selectedDuplicateIds.includes(productID) : false}
                                disabled={!productID || duplicateActionBusy}
                                onChange={() => toggleDuplicateSelection(productID)}
                              />
                            </div>
                            <div className="duplicate-item-main">
                              <h4>{product.name || 'Без названия'}</h4>
                              <p>{authors}</p>
                              <p>Совпавшие поля: {entry.fields.map((field) => field.toUpperCase()).join(', ')}</p>
                              <div className="duplicate-item-codes">
                                <span>ISBN: {isbnValue.raw || '-'}</span>
                                <span>CODE: {codeValue.raw || '-'}</span>
                                <span>BARCODE: {barcodeValue.raw || '-'}</span>
                                <span>GTIN: {gtinValue.raw || '-'}</span>
                              </div>
                            </div>
                            <div className="duplicate-item-actions">
                              <button className="btn table-btn" type="button" onClick={() => setDetailsProduct(product)}>
                                <EyeIcon />
                                Просмотр
                              </button>
                              <button
                                className="btn table-btn danger"
                                type="button"
                                disabled={!productID || duplicateActionBusy}
                                onClick={() => handleDeleteDuplicateProduct(productID)}
                              >
                                {deleting ? 'Удаление...' : 'Удалить'}
                              </button>
                            </div>
                          </article>
                        )
                      })}
                    </div>
                  </section>
                ))}
              </div>
            </>
          )}
        </div>
      )}

      {mainProductModalOpen && (
        <MainProductFormModal
          title="Добавить основной товар"
          submitLabel="Сохранить товар"
          form={mainProductForm}
          statusMessage={mainProductsStatus}
          categoryOptions={mainCategoryOptions}
          loading={mainProductCreating}
          onChange={handleMainProductFormChange}
          onClose={() => {
            if (mainProductCreating || mainProductImageUploading) return
            setMainProductModalOpen(false)
            setMainProductImageUploading(false)
          }}
          onSubmit={handleCreateMainProduct}
          uploadingImages={mainProductImageUploading}
          onUploadImages={handleUploadMainProductImages}
        />
      )}

      {mainProductEditModalOpen && (
        <MainProductFormModal
          title="Редактировать основной товар"
          submitLabel="Сохранить изменения"
          form={mainProductForm}
          statusMessage={mainProductsStatus}
          categoryOptions={mainCategoryOptions}
          loading={mainProductEditing}
          onChange={handleMainProductFormChange}
          onClose={() => {
            if (mainProductEditing || mainProductImageUploading) return
            setMainProductEditModalOpen(false)
            setEditingMainProductId('')
            setMainProductImageUploading(false)
          }}
          onSubmit={handleUpdateMainProduct}
          uploadingImages={mainProductImageUploading}
          onUploadImages={handleUploadMainProductImages}
        />
      )}

      {detailsProduct && <ProductDetailsModal product={detailsProduct} onClose={() => setDetailsProduct(null)} />}

      {toast && (
        <div className="toast-container" role="status" aria-live="polite">
          <div className={`toast ${toast.type === 'success' ? 'success' : ''}`}>
            {toast.message}
          </div>
        </div>
      )}

      <BottomNav activePage={activePage} onChange={setActivePage} />
    </main>
  )
}

function Pagination({
  page,
  totalPages,
  totalItems,
  pageSize,
  onPageChange,
  onPageSizeChange,
  showCardColumnsControl = false,
  cardColumns = 4,
  onCardColumnsChange = () => {},
  itemLabel = 'элементов'
}) {
  const safeTotalPages = Math.max(1, Number(totalPages) || 1)
  const safePage = Math.min(Math.max(1, Number(page) || 1), safeTotalPages)
  const items = getPaginationItems(safePage, safeTotalPages)
  const hasPrev = safePage > 1
  const hasNext = safePage < safeTotalPages
  const totalLabel = Number(totalItems || 0).toLocaleString()
  const [jumpPageValue, setJumpPageValue] = useState(String(safePage))

  useEffect(() => {
    setJumpPageValue(String(safePage))
  }, [safePage])

  const handleJumpSubmit = (event) => {
    event.preventDefault()
    const parsed = Number.parseInt(String(jumpPageValue || '').trim(), 10)
    if (!Number.isFinite(parsed)) {
      setJumpPageValue(String(safePage))
      return
    }
    const targetPage = Math.min(Math.max(parsed, 1), safeTotalPages)
    setJumpPageValue(String(targetPage))
    if (targetPage !== safePage) onPageChange(targetPage)
  }

  return (
    <div className="pagination" aria-label="Пагинация">
      <div className="pagination-main">
        <button
          type="button"
          className="pagination-arrow"
          disabled={!hasPrev}
          onClick={() => {
            if (!hasPrev) return
            onPageChange(safePage - 1)
          }}
          aria-label="Предыдущая страница"
        >
          ‹
        </button>

        {items.map((item, index) => {
          if (item.type === 'ellipsis') {
            return (
              <span key={`${item.value}-${index}`} className="pagination-ellipsis" aria-hidden="true">
                ...
              </span>
            )
          }
          const isActive = item.value === safePage
          return (
            <button
              key={item.value}
              type="button"
              className={`pagination-page ${isActive ? 'active' : ''}`}
              onClick={() => onPageChange(item.value)}
              aria-current={isActive ? 'page' : undefined}
              aria-label={`Страница ${item.value}`}
            >
              {item.value}
            </button>
          )
        })}

        <button
          type="button"
          className="pagination-arrow"
          disabled={!hasNext}
          onClick={() => {
            if (!hasNext) return
            onPageChange(safePage + 1)
          }}
          aria-label="Следующая страница"
        >
          ›
        </button>
      </div>

      <div className="pagination-right">
        <span className="pagination-total">
          {totalLabel} {itemLabel}
        </span>
        {showCardColumnsControl && (
          <label className="pagination-columns-control">
            <span>Колонки</span>
            <select value={clampCardColumns(cardColumns)} onChange={(event) => onCardColumnsChange(event.target.value)} aria-label="Колонок в карточках">
              {CARD_COLUMNS_OPTIONS.map((option) => (
                <option key={option} value={option}>
                  {option}
                </option>
              ))}
            </select>
          </label>
        )}
        <label className="pagination-size-control">
          <span>Показать</span>
          <select value={pageSize} onChange={(event) => onPageSizeChange(event.target.value)} aria-label="Строк на странице">
            {PAGE_SIZE_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </select>
        </label>
        <form className="pagination-jump-control" onSubmit={handleJumpSubmit}>
          <span>Страница</span>
          <input
            type="number"
            min="1"
            max={safeTotalPages}
            inputMode="numeric"
            value={jumpPageValue}
            onChange={(event) => setJumpPageValue(event.target.value.replace(/[^\d]/g, ''))}
            aria-label="Номер страницы"
          />
          <button type="submit" className="pagination-jump-btn">
            Перейти
          </button>
        </form>
      </div>
    </div>
  )
}

function FilterTreeSelect({ label, nodes, selectedValues, onChange, loading = false }) {
  const [isOpen, setIsOpen] = useState(false)
  const [menuStyle, setMenuStyle] = useState({})
  const [searchQuery, setSearchQuery] = useState('')
  const [expanded, setExpanded] = useState({})
  const containerRef = useRef(null)
  const buttonRef = useRef(null)
  const menuRef = useRef(null)

  const normalizedSearchQuery = String(searchQuery || '')
    .trim()
    .toLowerCase()
  const selectedSet = useMemo(() => new Set(Array.isArray(selectedValues) ? selectedValues : []), [selectedValues])
  const visibleNodes = useMemo(
    () => filterTreeNodesByQuery(Array.isArray(nodes) ? nodes : [], normalizedSearchQuery),
    [nodes, normalizedSearchQuery]
  )
  const nodeNameById = useMemo(() => {
    const map = new Map()
    const walk = (items) => {
      for (const item of Array.isArray(items) ? items : []) {
        const id = String(item?.id || '').trim()
        if (id) map.set(id, String(item?.name || '').trim() || id)
        if (Array.isArray(item?.children) && item.children.length > 0) walk(item.children)
      }
    }
    walk(nodes)
    return map
  }, [nodes])

  useEffect(() => {
    if (isOpen) return
    setSearchQuery('')
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) return
    const handleOutsideClick = (event) => {
      const clickedInsideButton = containerRef.current?.contains(event.target)
      const clickedInsideMenu = menuRef.current?.contains(event.target)
      if (!clickedInsideButton && !clickedInsideMenu) setIsOpen(false)
    }
    const handleEscape = (event) => {
      if (event.key === 'Escape') setIsOpen(false)
    }
    document.addEventListener('mousedown', handleOutsideClick)
    document.addEventListener('keydown', handleEscape)
    return () => {
      document.removeEventListener('mousedown', handleOutsideClick)
      document.removeEventListener('keydown', handleEscape)
    }
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) return
    const updateMenuPosition = () => {
      if (!buttonRef.current || typeof window === 'undefined') return
      const rect = buttonRef.current.getBoundingClientRect()
      const viewportWidth = window.innerWidth
      const viewportHeight = window.innerHeight
      const width = Math.max(rect.width, 280)
      const left = Math.max(8, Math.min(rect.left, viewportWidth - width - 8))
      const top = Math.min(rect.bottom + 6, Math.max(8, viewportHeight - 220))
      const maxHeight = Math.min(360, Math.max(160, viewportHeight - top - 12))

      setMenuStyle({
        left: `${left}px`,
        top: `${top}px`,
        width: `${width}px`,
        maxHeight: `${maxHeight}px`
      })
    }
    updateMenuPosition()
    window.addEventListener('resize', updateMenuPosition)
    window.addEventListener('scroll', updateMenuPosition, true)
    return () => {
      window.removeEventListener('resize', updateMenuPosition)
      window.removeEventListener('scroll', updateMenuPosition, true)
    }
  }, [isOpen])

  const toggleValue = (value) => {
    const current = Array.isArray(selectedValues) ? selectedValues : []
    if (current.includes(value)) {
      onChange(current.filter((item) => item !== value))
      return
    }
    onChange([...current, value])
  }

  const selectedCount = Array.isArray(selectedValues) ? selectedValues.length : 0
  const buttonLabel = (() => {
    if (selectedCount === 0) return label
    if (selectedCount > 1) return `${selectedCount} выбрано`
    const selectedId = selectedValues[0]
    return nodeNameById.get(selectedId) || label
  })()
  const title = selectedCount <= 1 ? buttonLabel : `${label}: ${selectedValues.join(', ')}`
  const forceOpen = Boolean(normalizedSearchQuery)

  const renderNode = (node, depth = 0) => {
    const id = String(node?.id || '').trim()
    if (!id) return null
    const children = Array.isArray(node?.children) ? node.children : []
    const hasChildren = children.length > 0
    const isOpenNode = forceOpen || Boolean(expanded[id])
    const checked = selectedSet.has(id)

    return (
      <div key={id} className="tree-node" style={{ paddingLeft: depth * 14 }}>
        <div className="tree-row">
          {hasChildren && !forceOpen ? (
            <button
              className="expand-btn"
              type="button"
              onClick={() => setExpanded((prev) => ({ ...prev, [id]: !prev[id] }))}
            >
              {isOpenNode ? '−' : '+'}
            </button>
          ) : (
            <span className="expand-placeholder" />
          )}
          <label className="tree-label">
            <input type="checkbox" checked={checked} onChange={() => toggleValue(id)} />
            <span>{node?.name || id}</span>
          </label>
        </div>
        {hasChildren && isOpenNode && (
          <div className="tree-children">
            {children.map((child) => renderNode(child, depth + 1))}
          </div>
        )}
      </div>
    )
  }

  const menu =
    isOpen && typeof document !== 'undefined'
      ? createPortal(
          <div ref={menuRef} className="filter-dropdown-menu filter-tree-menu" style={menuStyle}>
            <div className="filter-dropdown-menu-head">
              <span>{label}</span>
              {selectedCount > 0 && (
                <button
                  type="button"
                  className="filter-dropdown-clear"
                  onClick={() => {
                    onChange([])
                  }}
                >
                  Очистить
                </button>
              )}
            </div>
            <div className="filter-dropdown-search-wrap">
              <input
                type="text"
                className="filter-dropdown-search-input"
                value={searchQuery}
                placeholder={`Поиск ${label.toLowerCase()}...`}
                autoComplete="off"
                spellCheck={false}
                onChange={(event) => setSearchQuery(event.target.value)}
              />
            </div>
            <div className="filter-dropdown-options filter-tree-options">
              {loading ? (
                <p className="filter-dropdown-empty">Загрузка...</p>
              ) : visibleNodes.length === 0 ? (
                <p className="filter-dropdown-empty">Нет категорий</p>
              ) : (
                <div className="tree-container filter-tree-container">
                  {visibleNodes.map((node) => renderNode(node, 0))}
                </div>
              )}
            </div>
          </div>,
          document.body
        )
      : null

  return (
    <div className="filter-dropdown filter-tree-dropdown" ref={containerRef}>
      <button
        ref={buttonRef}
        type="button"
        className={`filter-dropdown-btn ${selectedCount > 0 ? 'has-selection' : ''}`}
        aria-expanded={isOpen}
        aria-haspopup="listbox"
        title={title}
        onClick={() => setIsOpen((prev) => !prev)}
      >
        <span className="filter-dropdown-text">{buttonLabel}</span>
        <span className="arrow">{isOpen ? '▲' : '▼'}</span>
      </button>
      {menu}
    </div>
  )
}

function BottomNav({ activePage, onChange }) {
  return (
    <nav className="bottom-nav" aria-label="Навигация по страницам">
      <button type="button" className={`bottom-nav-link ${activePage === 'mainProducts' ? 'active' : ''}`} onClick={() => onChange('mainProducts')}>
        <BoxIcon />
        <span>Основные товары</span>
      </button>
      <button type="button" className={`bottom-nav-link ${activePage === 'duplicates' ? 'active' : ''}`} onClick={() => onChange('duplicates')}>
        <DuplicateIcon />
        <span>Дубликаты</span>
      </button>
    </nav>
  )
}

function HomeIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M3 11.5 12 4l9 7.5v8.5a1 1 0 0 1-1 1h-5v-6h-6v6H4a1 1 0 0 1-1-1z" />
    </svg>
  )
}

function BoxIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="m12 2 9 4.5v11L12 22 3 17.5v-11zm0 2.1L6 6.9l6 3 6-3zM5 8.4v7.8l6 3v-7.8zm14 0-6 3v7.8l6-3z" />
    </svg>
  )
}

function DuplicateIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M8 3a3 3 0 0 0-3 3v10a3 3 0 0 0 3 3h9a3 3 0 0 0 3-3V6a3 3 0 0 0-3-3zm0 2h9a1 1 0 0 1 1 1v10a1 1 0 0 1-1 1H8a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1m-3 4a1 1 0 0 1 1 1v8h9a1 1 0 1 1 0 2H6a2 2 0 0 1-2-2v-8a1 1 0 0 1 1-1" />
    </svg>
  )
}

function FilterMultiSelect({ label, options, selectedValues, onChange }) {
  const [isOpen, setIsOpen] = useState(false)
  const [menuStyle, setMenuStyle] = useState({})
  const [searchQuery, setSearchQuery] = useState('')
  const containerRef = useRef(null)
  const buttonRef = useRef(null)
  const menuRef = useRef(null)

  const normalizedOptions = useMemo(() => normalizeStringOptions(options), [options])
  const selectedSet = useMemo(() => new Set(selectedValues), [selectedValues])
  const normalizedSearchQuery = String(searchQuery || '')
    .trim()
    .toLowerCase()
  const visibleOptions = useMemo(() => {
    if (!normalizedSearchQuery) return normalizedOptions
    return normalizedOptions.filter((option) => option.toLowerCase().includes(normalizedSearchQuery))
  }, [normalizedOptions, normalizedSearchQuery])

  useEffect(() => {
    if (isOpen) return
    setSearchQuery('')
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) return
    const handleOutsideClick = (event) => {
      const clickedInsideButton = containerRef.current?.contains(event.target)
      const clickedInsideMenu = menuRef.current?.contains(event.target)
      if (!clickedInsideButton && !clickedInsideMenu) setIsOpen(false)
    }
    const handleEscape = (event) => {
      if (event.key === 'Escape') setIsOpen(false)
    }
    document.addEventListener('mousedown', handleOutsideClick)
    document.addEventListener('keydown', handleEscape)
    return () => {
      document.removeEventListener('mousedown', handleOutsideClick)
      document.removeEventListener('keydown', handleEscape)
    }
  }, [isOpen])

  useEffect(() => {
    if (!isOpen) return
    const updateMenuPosition = () => {
      if (!buttonRef.current || typeof window === 'undefined') return
      const rect = buttonRef.current.getBoundingClientRect()
      const viewportWidth = window.innerWidth
      const viewportHeight = window.innerHeight
      const width = Math.max(rect.width, 220)
      const left = Math.max(8, Math.min(rect.left, viewportWidth - width - 8))
      const top = Math.min(rect.bottom + 6, Math.max(8, viewportHeight - 180))
      const maxHeight = Math.max(160, viewportHeight - top - 12)

      setMenuStyle({
        left: `${left}px`,
        top: `${top}px`,
        width: `${width}px`,
        maxHeight: `${maxHeight}px`
      })
    }
    updateMenuPosition()
    window.addEventListener('resize', updateMenuPosition)
    window.addEventListener('scroll', updateMenuPosition, true)
    return () => {
      window.removeEventListener('resize', updateMenuPosition)
      window.removeEventListener('scroll', updateMenuPosition, true)
    }
  }, [isOpen])

  const toggleValue = (value) => {
    const current = Array.isArray(selectedValues) ? selectedValues : []
    if (current.includes(value)) {
      onChange(current.filter((item) => item !== value))
      return
    }
    onChange([...current, value])
  }

  const selectedCount = selectedValues.length
  const buttonLabel = selectedCount === 0 ? label : selectedCount === 1 ? selectedValues[0] : `${selectedCount} выбрано`
  const title = selectedCount <= 1 ? buttonLabel : `${label}: ${selectedValues.join(', ')}`

  const menu =
    isOpen && typeof document !== 'undefined'
      ? createPortal(
          <div ref={menuRef} className="filter-dropdown-menu" style={menuStyle}>
            <div className="filter-dropdown-menu-head">
              <span>{label}</span>
              {selectedCount > 0 && (
                <button
                  type="button"
                  className="filter-dropdown-clear"
                  onClick={() => {
                    onChange([])
                  }}
                >
                  Очистить
                </button>
              )}
            </div>
            <div className="filter-dropdown-search-wrap">
              <input
                type="text"
                className="filter-dropdown-search-input"
                value={searchQuery}
                placeholder={`Поиск ${label.toLowerCase()}...`}
                autoComplete="off"
                spellCheck={false}
                onChange={(event) => setSearchQuery(event.target.value)}
              />
            </div>
            <div className="filter-dropdown-options">
              {visibleOptions.length === 0 ? (
                <p className="filter-dropdown-empty">Нет вариантов</p>
              ) : (
                visibleOptions.map((option) => (
                  <label key={option} className="filter-dropdown-option">
                    <input type="checkbox" checked={selectedSet.has(option)} onChange={() => toggleValue(option)} />
                    <span>{option}</span>
                  </label>
                ))
              )}
            </div>
          </div>,
          document.body
        )
      : null

  return (
    <div className="filter-dropdown" ref={containerRef}>
      <button
        ref={buttonRef}
        type="button"
        className={`filter-dropdown-btn ${selectedCount > 0 ? 'has-selection' : ''}`}
        aria-expanded={isOpen}
        aria-haspopup="listbox"
        title={title}
        onClick={() => setIsOpen((prev) => !prev)}
      >
        <span className="filter-dropdown-text">{buttonLabel}</span>
        <span className="arrow">{isOpen ? '▲' : '▼'}</span>
      </button>
      {menu}
    </div>
  )
}

function SearchIcon() {
  return (
    <svg className="search-icon" viewBox="0 0 24 24" aria-hidden="true">
      <path d="M10.5 3a7.5 7.5 0 0 1 5.95 12.07l4.24 4.23a1 1 0 1 1-1.41 1.42l-4.24-4.24A7.5 7.5 0 1 1 10.5 3zm0 2a5.5 5.5 0 1 0 0 11 5.5 5.5 0 0 0 0-11z" />
    </svg>
  )
}

function TreeNode({ node, depth, expanded, onToggle, onSelect, selectedGuids, forceOpen = false }) {
  const hasChildren = Array.isArray(node.children) && node.children.length > 0
  const hasSubjects = Array.isArray(node.subjects) && node.subjects.length > 0
  const hasExpandable = hasChildren || hasSubjects
  const isOpen = forceOpen || Boolean(expanded[node.guid])
  const isSelected = selectedGuids.includes(node.guid)

  return (
    <div className="tree-node" style={{ paddingLeft: depth * 16 }}>
      <div className="tree-row">
        {hasExpandable && !forceOpen ? (
          <button className="expand-btn" onClick={() => onToggle(node.guid)} type="button">
            {isOpen ? '−' : '+'}
          </button>
        ) : (
          <span className="expand-placeholder" />
        )}
        <label className="tree-label">
          <input type="checkbox" checked={isSelected} onChange={() => onSelect({ type: 'niche', guid: node.guid, name: node.name })} />
          <span>{node.name}</span>
        </label>
      </div>

      {hasExpandable && isOpen && (
        <div className="tree-children">
          {hasChildren &&
            node.children.map((child) => (
              <TreeNode
                key={child.guid}
                node={child}
                depth={depth + 1}
                expanded={expanded}
                onToggle={onToggle}
                onSelect={onSelect}
                selectedGuids={selectedGuids}
                forceOpen={forceOpen}
              />
            ))}
          {hasSubjects &&
            node.subjects.map((subject) => (
              <div key={subject.guid} className="tree-node subject" style={{ paddingLeft: (depth + 1) * 16 }}>
                <div className="tree-row">
                  <span className="expand-placeholder" />
                  <label className="tree-label">
                    <input
                      type="checkbox"
                      checked={selectedGuids.includes(subject.guid)}
                      onChange={() => onSelect({ type: 'subject', guid: subject.guid, name: subject.name })}
                    />
                    <span>{subject.name}</span>
                  </label>
                </div>
              </div>
            ))}
        </div>
      )}
    </div>
  )
}

function MainCategoryNode({ node, depth, path, expanded, onToggle, selectedCategoryIds, onSelectCategory }) {
  const hasChildren = Array.isArray(node.children) && node.children.length > 0
  const isOpen = Boolean(expanded[node.id])
  const currentPath = [...path, node.name]
  const isSelected = selectedCategoryIds.includes(node.id)
  const toggleSelection = () => onSelectCategory(node, currentPath)

  return (
    <div className="main-tree-node" style={{ paddingLeft: depth * 16 }}>
      <div className="main-tree-row" onClick={toggleSelection}>
        {hasChildren ? (
          <button
            className="expand-btn"
            onClick={(e) => {
              e.stopPropagation()
              onToggle(node.id)
            }}
            type="button"
          >
            {isOpen ? '−' : '+'}
          </button>
        ) : (
          <span className="expand-placeholder" />
        )}
        <label
          className="main-tree-label"
          onClick={(e) => {
            e.stopPropagation()
          }}
        >
          <input type="checkbox" checked={isSelected} onChange={toggleSelection} />
          <span className="main-tree-name">{node.name}</span>
        </label>
      </div>

      {hasChildren && isOpen && (
        <div className="main-tree-children">
          {node.children.map((child) => (
            <MainCategoryNode
              key={child.id}
              node={child}
              depth={depth + 1}
              path={currentPath}
              expanded={expanded}
              onToggle={onToggle}
              selectedCategoryIds={selectedCategoryIds}
              onSelectCategory={onSelectCategory}
            />
          ))}
        </div>
      )}
    </div>
  )
}

function MainProductCard({
  product,
  checked,
  selectable,
  onToggle,
  onViewDetails,
  onEdit,
  onDelete,
  editDisabled = false,
  deleteDisabled = false,
  editing = false,
  deleting = false
}) {
  const authors = product.authorNames?.join(', ') || product.authorCover || 'Неизвестно'
  const categoryText = Array.isArray(product.categoryPath) && product.categoryPath.length > 0 ? product.categoryPath.join(' / ') : 'Без категории'
  const handleToggle = () => {
    if (!selectable) return
    onToggle()
  }
  const handleKeyDown = (event) => {
    if (!selectable) return
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault()
      onToggle()
    }
  }

  return (
    <article
      className={`product-card ${selectable ? 'selectable' : ''} ${checked ? 'selected' : ''}`}
      onClick={handleToggle}
      onKeyDown={handleKeyDown}
      role={selectable ? 'checkbox' : undefined}
      aria-checked={selectable ? checked : undefined}
      tabIndex={selectable ? 0 : -1}
    >
      <label className={`product-check ${!selectable ? 'disabled' : ''}`} onClick={(event) => event.stopPropagation()}>
        <input type="checkbox" checked={checked} disabled={!selectable} onClick={(event) => event.stopPropagation()} onChange={onToggle} />
      </label>
      <button
        type="button"
        className="product-view-btn"
        aria-label="Просмотр деталей товара"
        title="Открыть детали"
        onClick={(event) => {
          event.stopPropagation()
          onViewDetails()
        }}
      >
        <EyeIcon />
      </button>
      <div className="cover-wrap">
        {product.coverUrl ? <img src={resolveImageUrl(product.coverUrl)} alt={product.name || ''} loading="lazy" /> : <div className="no-cover">Нет изображения</div>}
      </div>
      <div className="product-info">
        <h3>{product.name || 'Без названия'}</h3>
        <p className="author">{authors}</p>
        <p className="meta">{product.isbn || '-'}</p>
        <p className="meta">{categoryText}</p>
        <p className="meta">Кол-во: {formatMainMetric(product.quantity, 3)} · Цена: {formatMainMetric(product.price, 2)}</p>
        <div className="main-product-card-actions">
          <button
            type="button"
            className="btn table-btn"
            disabled={editDisabled}
            onClick={(event) => {
              event.stopPropagation()
              onEdit?.()
            }}
          >
            <EditIcon />
            {editing ? 'Сохранение...' : 'Изменить'}
          </button>
          <button
            type="button"
            className="btn table-btn danger"
            disabled={deleteDisabled}
            onClick={(event) => {
              event.stopPropagation()
              onDelete?.()
            }}
          >
            <TrashIcon />
            {deleting ? 'Удаление...' : 'Удалить'}
          </button>
        </div>
      </div>
    </article>
  )
}

function ProductCard({ product, checked, selectable, onToggle, onViewDetails, onDelete, deleteDisabled = false, deleting = false }) {
  const authors = product.authorNames?.join(', ') || product.authorCover || 'Неизвестно'
  const inMainProducts = Boolean(product.inMainProducts)
  const handleToggle = () => {
    if (!selectable) return
    onToggle()
  }
  const handleKeyDown = (e) => {
    if (!selectable) return
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      onToggle()
    }
  }

  return (
    <article
      className={`product-card ${selectable ? 'selectable' : ''} ${checked ? 'selected' : ''} ${inMainProducts ? 'in-main' : 'not-in-main'}`}
      onClick={handleToggle}
      onKeyDown={handleKeyDown}
      role={selectable ? 'checkbox' : undefined}
      aria-checked={selectable ? checked : undefined}
      tabIndex={selectable ? 0 : -1}
    >
      <label className={`product-check ${!selectable ? 'disabled' : ''}`} onClick={(e) => e.stopPropagation()}>
        <input type="checkbox" checked={checked} disabled={!selectable} onClick={(e) => e.stopPropagation()} onChange={onToggle} />
      </label>
      <button
        type="button"
        className="product-view-btn"
        aria-label="Просмотр деталей товара"
        title="Открыть детали"
        onClick={(e) => {
          e.stopPropagation()
          onViewDetails()
        }}
      >
        <EyeIcon />
      </button>
      <div className="cover-wrap">
        {product.coverUrl ? <img src={resolveImageUrl(product.coverUrl)} alt={product.name || ''} loading="lazy" /> : <div className="no-cover">Нет изображения</div>}
      </div>
      <div className="product-info">
        <h3>{product.name || 'Без названия'}</h3>
        <p className="author">{authors}</p>
        <p className="meta">{product.subjectName || product.serieName || '-'}</p>
        <span className={`main-indicator ${inMainProducts ? 'yes' : 'no'}`}>
          <span className="main-indicator-dot" />
          {inMainProducts ? 'В основном' : 'Не в основном'}
        </span>
        {product.ageRestriction && <span className="age-badge">{product.ageRestriction}</span>}
        <div className="product-card-actions">
          <button
            type="button"
            className="product-delete-btn"
            disabled={deleteDisabled}
            onClick={(event) => {
              event.stopPropagation()
              onDelete?.()
            }}
          >
            {deleting ? 'Удаление...' : 'Удалить'}
          </button>
        </div>
      </div>
    </article>
  )
}

function ProductListRow({ product, checked, selectable, onToggle, onViewDetails, onDelete, deleteDisabled = false, deleting = false }) {
  const authors = product.authorNames?.join(', ') || product.authorCover || 'Неизвестно'
  const inMainProducts = Boolean(product.inMainProducts)
  const handleToggle = () => {
    if (!selectable) return
    onToggle()
  }
  const handleKeyDown = (e) => {
    if (!selectable) return
    if (e.key === 'Enter' || e.key === ' ') {
      e.preventDefault()
      onToggle()
    }
  }

  return (
    <article
      className={`product-row ${selectable ? 'selectable' : ''} ${checked ? 'selected' : ''} ${inMainProducts ? 'in-main' : 'not-in-main'}`}
      onClick={handleToggle}
      onKeyDown={handleKeyDown}
      role={selectable ? 'checkbox' : undefined}
      aria-checked={selectable ? checked : undefined}
      tabIndex={selectable ? 0 : -1}
    >
      <div className="product-row-check" onClick={(e) => e.stopPropagation()}>
        <input type="checkbox" checked={checked} disabled={!selectable} onClick={(e) => e.stopPropagation()} onChange={onToggle} />
      </div>
      <div className="product-row-cover">
        {product.coverUrl ? <img src={resolveImageUrl(product.coverUrl)} alt={product.name || ''} loading="lazy" /> : <div className="no-cover">Нет изображения</div>}
      </div>
      <div className="product-row-body">
        <h3>{product.name || 'Без названия'}</h3>
        <p>{authors}</p>
        <p>{product.subjectName || product.serieName || '-'}</p>
      </div>
      <div className="product-row-side">
        <button
          type="button"
          className="row-view-btn"
          onClick={(e) => {
            e.stopPropagation()
            onViewDetails()
          }}
        >
          <EyeIcon />
          Детали
        </button>
        <button
          type="button"
          className="row-view-btn danger"
          disabled={deleteDisabled}
          onClick={(e) => {
            e.stopPropagation()
            onDelete?.()
          }}
        >
          <TrashIcon />
          {deleting ? 'Удаление...' : 'Удалить'}
        </button>
        <span className={`main-indicator compact ${inMainProducts ? 'yes' : 'no'}`}>
          <span className="main-indicator-dot" />
          {inMainProducts ? 'В основном' : 'Не в основном'}
        </span>
        <div className="product-row-age">{product.ageRestriction || '-'}</div>
      </div>
    </article>
  )
}

const PRODUCT_FIELD_META = {
  id: { label: 'ID записи', hidden: true },
  sourceProductId: { label: 'ID исходного товара' },
  guid: { label: 'GUID товара', hidden: true },
  guidNom: { label: 'GUID номенклатуры', hidden: true },
  nomcode: { label: 'Внутренний код товара', hidden: true },
  sourceGuidNom: { label: 'GUID NOM источника' },
  sourceGuid: { label: 'GUID источника' },
  sourceNomcode: { label: 'NOMCODE источника' },
  name: { label: 'Название' },
  isbn: { label: 'ISBN' },
  isbnNormalized: { label: 'Нормализованный ISBN', hidden: true },
  authorCover: { label: 'Автор (обложка)' },
  authorNames: { label: 'Авторы' },
  annotation: { label: 'Аннотация' },
  subject: { label: 'Тема' },
  subjectName: { label: 'Тема' },
  niche: { label: 'Группа категорий' },
  nicheName: { label: 'Ниша' },
  brand: { label: 'Бренд' },
  brandName: { label: 'Бренд' },
  series: { label: 'Серия' },
  seriesName: { label: 'Серия' },
  publisher: { label: 'Издатель' },
  publisherName: { label: 'Издатель' },
  authorRefs: { label: 'Авторы' },
  tagRefs: { label: 'Теги' },
  genreRefs: { label: 'Жанры' },
  categoryId: { label: 'ID категории' },
  categoryPath: { label: 'Путь категории' },
  quantity: { label: 'Количество' },
  price: { label: 'Цена' },
  billzUpdatedAt: { label: 'Обновлено в Billz' },
  createdAt: { label: 'Дата создания' },
  pages: { label: 'Страницы' },
  format: { label: 'Формат' },
  paperType: { label: 'Тип бумаги' },
  bindingType: { label: 'Тип переплета' },
  ageRestriction: { label: 'Возрастное ограничение' },
  covers: { label: 'Все обложки' },
  coverUrl: { label: 'Главная обложка' },
  categoryIds: { label: 'Связанные ID категорий', hidden: true },
  inMainProducts: { label: 'Уже в основном каталоге' },
  syncedAt: { label: 'Синхронизировано', hidden: true },
  updatedAt: { label: 'Обновлено', hidden: true }
}

function isIdentifierLikeKey(key) {
  const normalized = String(key || '').trim().toLowerCase()
  if (!normalized) return false
  if (normalized === 'id' || normalized === '_id') return true
  if (normalized.includes('guid')) return true
  if (normalized.includes('nomcode')) return true
  if (normalized === 'code' || normalized.endsWith('_code') || normalized.endsWith('code')) return true
  if (normalized.endsWith('id') && normalized !== 'isbn') return true
  return false
}

function looksLikeIdentifierValue(value) {
  const text = String(value || '').trim()
  if (!text) return false
  if (/^[a-f\d]{24}$/i.test(text)) return true
  if (/^[a-f\d]{8}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{4}-[a-f\d]{12}$/i.test(text)) return true
  if (/^ITD[0-9A-Z]+$/i.test(text)) return true
  return false
}

function formatNestedEntry(nestedKey, nestedValue) {
  const formatted = formatDetailValue(nestedValue)
  if (formatted === '-') return null
  if (String(nestedKey || '').trim().toLowerCase() === 'name') return formatted
  return `${nestedKey}: ${formatted}`
}

function formatDetailValue(value) {
  if (value === null || value === undefined || value === '') return '-'

  const primitiveType = typeof value
  if (primitiveType === 'string') return looksLikeIdentifierValue(value) ? '-' : String(value)
  if (primitiveType === 'number') return String(value)
  if (primitiveType === 'boolean') return value ? 'да' : 'нет'

  if (Array.isArray(value)) {
    if (value.length === 0) return '-'
    if (value.every((item) => item === null || item === undefined || ['string', 'number', 'boolean'].includes(typeof item))) {
      const items = value.map((item) => formatDetailValue(item)).filter((item) => item !== '-')
      return items.length > 0 ? items.join(', ') : '-'
    }
    const items = value
      .map((item, index) => {
        if (!item || typeof item !== 'object') return formatDetailValue(item)
        const parts = Object.entries(item)
          .filter(([nestedKey, nestedValue]) => nestedValue !== null && nestedValue !== undefined && nestedValue !== '' && !isIdentifierLikeKey(nestedKey))
          .map(([nestedKey, nestedValue]) => formatNestedEntry(nestedKey, nestedValue))
          .filter(Boolean)
        return parts.length > 0 ? parts.join('; ') : null
      })
      .filter(Boolean)
    return items.length > 0 ? items.join(' | ') : '-'
  }

  if (primitiveType === 'object') {
    const entries = Object.entries(value)
      .filter(([nestedKey, nestedValue]) => nestedValue !== null && nestedValue !== undefined && nestedValue !== '' && !isIdentifierLikeKey(nestedKey))
      .map(([nestedKey, nestedValue]) => formatNestedEntry(nestedKey, nestedValue))
      .filter(Boolean)
    return entries.length > 0 ? entries.join('; ') : '-'
  }

  return String(value)
}

function buildProductDetailRows(product) {
  if (!product || typeof product !== 'object') return []

  const preferredOrder = [
    'name',
    'isbn',
    'sourceGuidNom',
    'sourceGuid',
    'sourceNomcode',
    'authorCover',
    'authorNames',
    'annotation',
    'price',
    'quantity',
    'categoryPath',
    'subject',
    'subjectName',
    'niche',
    'nicheName',
    'brand',
    'brandName',
    'series',
    'seriesName',
    'publisher',
    'publisherName',
    'authorRefs',
    'tagRefs',
    'genreRefs',
    'pages',
    'format',
    'paperType',
    'bindingType',
    'ageRestriction',
    'covers',
    'coverUrl',
    'inMainProducts',
    'billzUpdatedAt',
    'createdAt',
    'updatedAt'
  ]

  const allKeys = Object.keys(product)
  const orderedKeys = [...new Set([...preferredOrder, ...allKeys])]

  return orderedKeys
    .filter((key) => PRODUCT_FIELD_META[key] && !PRODUCT_FIELD_META[key].hidden)
    .map((key) => ({
      key,
      name: PRODUCT_FIELD_META[key].label || key,
      value: formatDetailValue(product[key])
    }))
}

function getProductImages(product) {
  const images = []
  const seen = new Set()

  const pushImage = (label, url) => {
    const safeUrl = typeof url === 'string' ? url.trim() : ''
    if (!safeUrl || seen.has(safeUrl)) return
    seen.add(safeUrl)
    images.push({ label, url: safeUrl })
  }

  if (product?.covers && typeof product.covers === 'object') {
    Object.entries(product.covers)
      .sort(([left], [right]) => left.localeCompare(right, undefined, { numeric: true }))
      .forEach(([coverKey, coverUrl], index) => pushImage(`Обложка ${index + 1} (${coverKey})`, coverUrl))
  }

  pushImage('Главная обложка', product?.coverUrl)
  return images
}

function ProductDetailsModal({ product, onClose }) {
  if (typeof document === 'undefined') return null

  const title = product?.name || 'Без названия'
  const authors = product?.authorNames?.join(', ') || product?.authorCover || '-'
  const detailRows = useMemo(() => buildProductDetailRows(product), [product])
  const images = useMemo(() => getProductImages(product), [product])
  const [activeImageIndex, setActiveImageIndex] = useState(0)
  const summaryGenres = useMemo(() => {
    if (Array.isArray(product?.genreNames) && product.genreNames.length > 0) return product.genreNames.join(', ')
    if (Array.isArray(product?.genreRefs) && product.genreRefs.length > 0) return product.genreRefs.map((item) => item.name).filter(Boolean).join(', ')
    return '-'
  }, [product])
  const summarySeries = useMemo(() => product?.serieName || product?.seriesName || '-', [product])
  const summaryCategory = useMemo(() => {
    if (Array.isArray(product?.categoryPath) && product.categoryPath.length > 0) return product.categoryPath.join(' / ')
    return '-'
  }, [product])

  useEffect(() => {
    setActiveImageIndex(0)
  }, [product?.id, product?.guid, product?.guidNom])

  const activeImage = images[activeImageIndex] || null
  const hasMultipleImages = images.length > 1

  const handlePrevImage = () => {
    if (images.length === 0) return
    setActiveImageIndex((prev) => (prev - 1 + images.length) % images.length)
  }

  const handleNextImage = () => {
    if (images.length === 0) return
    setActiveImageIndex((prev) => (prev + 1) % images.length)
  }

  return createPortal(
    <div
      className="details-modal-overlay"
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-label="Детали товара"
    >
      <div className="details-modal" onClick={(e) => e.stopPropagation()}>
        <div className="details-modal-header">
          <h3>{title}</h3>
          <button type="button" className="details-close-btn" onClick={onClose} aria-label="Закрыть детали">
            &times;
          </button>
        </div>

        <div className="details-modal-body">
          <div className="details-top">
            <div className="details-carousel">
              <div className="details-cover">
                {activeImage ? <img src={resolveImageUrl(activeImage.url)} alt={`${title} обложка`} /> : <div className="no-cover">Нет изображения</div>}
              </div>

              {hasMultipleImages && (
                <div className="details-carousel-controls">
                  <button type="button" onClick={handlePrevImage} aria-label="Предыдущее изображение">
                    Назад
                  </button>
                  <span>{activeImageIndex + 1} / {images.length}</span>
                  <button type="button" onClick={handleNextImage} aria-label="Следующее изображение">
                    Далее
                  </button>
                </div>
              )}

              {activeImage && <p className="details-image-label">{activeImage.label}</p>}
            </div>

            <div className="details-summary">
              <p><strong>Автор:</strong> {authors}</p>
              <p><strong>Серия:</strong> {summarySeries}</p>
              <p><strong>Тема:</strong> {product?.subjectName || '-'}</p>
              <p><strong>Издатель:</strong> {product?.publisherName || '-'}</p>
              <p><strong>Категория:</strong> {summaryCategory}</p>
              <p><strong>Жанр:</strong> {summaryGenres}</p>
              <p><strong>Возраст:</strong> {product?.ageRestriction || '-'}</p>
              <p><strong>ISBN:</strong> {product?.isbn || '-'}</p>
            </div>
          </div>

          <div className="details-fields">
            <h4>Полные данные о товаре</h4>
            <div className="details-fields-grid">
              {detailRows.map((row) => (
                <div key={row.key} className="details-field-row">
                  <div className="details-field-name">
                    <div>{row.name}</div>
                  </div>
                  <div className="details-field-value">{row.value}</div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </div>,
    document.body
  )
}

function MainProductFormModal({ title, submitLabel, form, statusMessage, categoryOptions, loading, uploadingImages, onChange, onSubmit, onClose, onUploadImages }) {
  if (typeof document === 'undefined') return null
  const [categoryInput, setCategoryInput] = useState('')
  const [categoryMenuOpen, setCategoryMenuOpen] = useState(false)
  const categoryInputRef = useRef(null)
  const imageInputRef = useRef(null)

  useEffect(() => {
    if (!form?.categoryId) {
      setCategoryInput('')
      return
    }
    const selected = categoryOptions.find((option) => option.id === form.categoryId)
    setCategoryInput(selected?.label || '')
  }, [form?.categoryId, categoryOptions])

  const filteredCategoryOptions = useMemo(() => {
    const query = String(categoryInput || '').trim().toLowerCase()
    if (!query) return categoryOptions.slice(0, 120)

    return categoryOptions.filter((option) => {
      const label = String(option?.label || '').toLowerCase()
      const pathParts = Array.isArray(option?.path) ? option.path.map((item) => String(item || '').toLowerCase()) : []
      if (label.includes(query)) return true
      return pathParts.some((part) => part.includes(query))
    })
  }, [categoryInput, categoryOptions])

  const hasTypedCategory = String(categoryInput || '').trim() !== ''
  const exactMatchedCategory = useMemo(() => {
    const query = String(categoryInput || '').trim().toLowerCase()
    if (!query) return null
    return categoryOptions.find((option) => String(option.label || '').trim().toLowerCase() === query) || null
  }, [categoryInput, categoryOptions])
  const invalidCategory = hasTypedCategory && !form?.categoryId
  const disableSubmit = loading || uploadingImages || invalidCategory || String(form?.name || '').trim() === ''
  const coverUrls = normalizeCoverUrls(form?.coverUrls)
  const uploadStatusText = String(statusMessage || '').trim()
  const statusIsError = /failed|error|invalid|unsupported|required|too large|not an image|ошиб|невер|обязат|слишком|изображен/i.test(uploadStatusText)

  const handleCategoryInputChange = (value) => {
    setCategoryInput(value)
    setCategoryMenuOpen(true)
    const query = String(value || '').trim().toLowerCase()
    if (!query) {
      onChange('categoryId', '')
      return
    }
    const match = categoryOptions.find((option) => String(option.label || '').trim().toLowerCase() === query)
    onChange('categoryId', match?.id || '')
  }

  const handleSelectCategory = (option) => {
    setCategoryInput(option.label)
    onChange('categoryId', option.id)
    setCategoryMenuOpen(false)
  }

  const handleSubmit = (event) => {
    if (invalidCategory) {
      event.preventDefault()
      return
    }
    onSubmit(event)
  }

  const handleUploadClick = () => {
    if (loading || uploadingImages) return
    imageInputRef.current?.click()
  }

  const handleImageInputChange = (event) => {
    const files = Array.from(event.target.files || [])
    event.target.value = ''
    if (files.length === 0) return
    onUploadImages?.(files)
  }

  const handleRemoveCover = (url) => {
    const next = coverUrls.filter((item) => item !== url)
    onChange('coverUrls', next)
    if (String(form?.coverUrl || '').trim() === url) {
      onChange('coverUrl', next[0] || '')
    }
  }

  const handleSetMainCover = (url) => {
    onChange('coverUrl', url)
    if (!coverUrls.includes(url)) {
      onChange('coverUrls', normalizeCoverUrls([url, ...coverUrls]))
    }
  }

  return createPortal(
    <div className="details-modal-overlay" onClick={onClose} role="dialog" aria-modal="true" aria-label={title || 'Форма основного товара'}>
      <div className="details-modal create-main-modal" onClick={(e) => e.stopPropagation()}>
        <div className="details-modal-header">
          <h3>{title || 'Основной товар'}</h3>
          <button type="button" className="details-close-btn" onClick={onClose} aria-label="Закрыть форму товара" disabled={loading || uploadingImages}>
            &times;
          </button>
        </div>

        <div className="details-modal-body">
          <form className="main-product-form" onSubmit={handleSubmit}>
            <label className="main-product-field full">
              <span>Категория</span>
              <input
                ref={categoryInputRef}
                type="text"
                value={categoryInput}
                onChange={(e) => handleCategoryInputChange(e.target.value)}
                onFocus={() => setCategoryMenuOpen(true)}
                onBlur={() => {
                  setTimeout(() => setCategoryMenuOpen(false), 120)
                }}
                placeholder="Введите название категории..."
              />
              {categoryMenuOpen && (
                <div className="category-suggest">
                  <button
                    type="button"
                    className={`category-suggest-item ${!hasTypedCategory ? 'active' : ''}`}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => {
                      setCategoryInput('')
                      onChange('categoryId', '')
                      setCategoryMenuOpen(false)
                    }}
                  >
                    Без категории
                  </button>
                  {filteredCategoryOptions.slice(0, 120).map((option) => (
                    <button
                      key={option.id}
                      type="button"
                      className={`category-suggest-item ${form?.categoryId === option.id ? 'active' : ''}`}
                      onMouseDown={(e) => e.preventDefault()}
                      onClick={() => handleSelectCategory(option)}
                    >
                      {option.label}
                    </button>
                  ))}
                  {filteredCategoryOptions.length === 0 && hasTypedCategory && (
                    <div className="category-suggest-empty">Совпадений нет</div>
                  )}
                </div>
              )}
              {invalidCategory && <small className="main-product-field-error">Выберите существующую категорию из списка или очистите поле.</small>}
              {exactMatchedCategory && form?.categoryId && (
                <small className="main-product-field-hint">Выбрано: {exactMatchedCategory.label}</small>
              )}
            </label>

            <label className="main-product-field full">
              <span>Название *</span>
              <input type="text" value={form.name} onChange={(e) => onChange('name', e.target.value)} required />
            </label>

            <label className="main-product-field">
              <span>ISBN</span>
              <input type="text" value={form.isbn} onChange={(e) => onChange('isbn', e.target.value)} />
            </label>

            <label className="main-product-field full">
              <span>Авторы (через запятую)</span>
              <input type="text" value={form.authorNames} onChange={(e) => onChange('authorNames', e.target.value)} placeholder="Автор 1, Автор 2" />
            </label>

            <label className="main-product-field full">
              <span>Теги (через запятую)</span>
              <input type="text" value={form.tagNames} onChange={(e) => onChange('tagNames', e.target.value)} placeholder="Тег 1, Тег 2" />
            </label>

            <label className="main-product-field full">
              <span>Жанры (через запятую)</span>
              <input type="text" value={form.genreNames} onChange={(e) => onChange('genreNames', e.target.value)} placeholder="Жанр 1, Жанр 2" />
            </label>

            <label className="main-product-field">
              <span>Автор на обложке</span>
              <input type="text" value={form.authorCover} onChange={(e) => onChange('authorCover', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>URL обложки</span>
              <input type="text" value={form.coverUrl} onChange={(e) => onChange('coverUrl', e.target.value)} />
              <small className="main-product-field-hint">Путь или URL главного изображения (поддерживаются `/uploads/...` и `https://...`).</small>
            </label>

            <label className="main-product-field full">
              <span>Загрузить изображения (несколько)</span>
              <div className="main-product-upload-row">
                <button type="button" className="btn" onClick={handleUploadClick} disabled={loading || uploadingImages}>
                  {uploadingImages ? 'Загрузка...' : 'Загрузить изображения'}
                </button>
                <small className="main-product-field-hint">Поддерживаются JPG, PNG, WEBP, GIF, SVG.</small>
                {uploadStatusText && (
                  <small className={`main-product-upload-status ${statusIsError ? 'error' : ''}`}>
                    {uploadStatusText}
                  </small>
                )}
              </div>
              <input
                ref={imageInputRef}
                className="hidden-file-input"
                type="file"
                accept="image/*"
                multiple
                onChange={handleImageInputChange}
              />
              {coverUrls.length > 0 && (
                <div className="uploaded-images-list">
                  {coverUrls.map((url) => {
                    const isMain = String(form?.coverUrl || '').trim() === url
                    return (
                      <div key={url} className="uploaded-image-item">
                        <div className="uploaded-image-meta">
                          <a className="uploaded-image-preview" href={resolveImageUrl(url)} target="_blank" rel="noreferrer" title="Открыть изображение">
                            <img src={resolveImageUrl(url)} alt="Предпросмотр загруженного" loading="lazy" />
                          </a>
                          <a href={resolveImageUrl(url)} target="_blank" rel="noreferrer">{url}</a>
                        </div>
                        <div className="uploaded-image-actions">
                          <button
                            type="button"
                            className="btn table-btn"
                            disabled={loading || uploadingImages || isMain}
                            onClick={() => handleSetMainCover(url)}
                          >
                            {isMain ? 'Главное' : 'Сделать главным'}
                          </button>
                          <button
                            type="button"
                            className="btn table-btn danger"
                            disabled={loading || uploadingImages}
                            onClick={() => handleRemoveCover(url)}
                          >
                            Удалить
                          </button>
                        </div>
                      </div>
                    )
                  })}
                </div>
              )}
            </label>

            <label className="main-product-field">
              <span>Возрастное ограничение</span>
              <input type="text" value={form.ageRestriction} onChange={(e) => onChange('ageRestriction', e.target.value)} placeholder="16+" />
            </label>

            <label className="main-product-field">
              <span>Страницы</span>
              <input type="number" min="0" step="1" value={form.pages} onChange={(e) => onChange('pages', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Формат</span>
              <input type="text" value={form.format} onChange={(e) => onChange('format', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Тип бумаги</span>
              <input type="text" value={form.paperType} onChange={(e) => onChange('paperType', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Тип переплета</span>
              <input type="text" value={form.bindingType} onChange={(e) => onChange('bindingType', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Тема</span>
              <input type="text" value={form.subjectName} onChange={(e) => onChange('subjectName', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Ниша</span>
              <input type="text" value={form.nicheName} onChange={(e) => onChange('nicheName', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Бренд</span>
              <input type="text" value={form.brandName} onChange={(e) => onChange('brandName', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Серия</span>
              <input type="text" value={form.seriesName} onChange={(e) => onChange('seriesName', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Издатель</span>
              <input type="text" value={form.publisherName} onChange={(e) => onChange('publisherName', e.target.value)} />
            </label>

            <label className="main-product-field full">
              <span>Путь категории</span>
              <input
                type="text"
                value={form.categoryPath}
                onChange={(e) => onChange('categoryPath', e.target.value)}
                placeholder="Категория / Подкатегория / ..."
              />
              <small className="main-product-field-hint">Можно вводить через ` / ` или запятую.</small>
            </label>

            <label className="main-product-field">
              <span>Количество</span>
              <input type="number" step="0.001" value={form.quantity} onChange={(e) => onChange('quantity', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Цена</span>
              <input type="number" step="0.01" value={form.price} onChange={(e) => onChange('price', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Исходный GUID NOM</span>
              <input type="text" value={form.sourceGuidNom} onChange={(e) => onChange('sourceGuidNom', e.target.value)} />
            </label>

            <label className="main-product-field">
              <span>Исходный GUID</span>
              <input type="text" value={form.sourceGuid} onChange={(e) => onChange('sourceGuid', e.target.value)} />
            </label>

            <label className="main-product-field full">
              <span>Исходный NOMCODE</span>
              <input type="text" value={form.sourceNomcode} onChange={(e) => onChange('sourceNomcode', e.target.value)} />
            </label>

            <label className="main-product-field full">
              <span>Авторы (refs JSON)</span>
              <textarea
                value={form.authorRefsJson}
                onChange={(e) => onChange('authorRefsJson', e.target.value)}
                rows={4}
                placeholder='[{"guid":"...","name":"..."}]'
              />
            </label>

            <label className="main-product-field full">
              <span>Теги (refs JSON)</span>
              <textarea
                value={form.tagRefsJson}
                onChange={(e) => onChange('tagRefsJson', e.target.value)}
                rows={4}
                placeholder='[{"guid":"...","name":"..."}]'
              />
            </label>

            <label className="main-product-field full">
              <span>Жанры (refs JSON)</span>
              <textarea
                value={form.genreRefsJson}
                onChange={(e) => onChange('genreRefsJson', e.target.value)}
                rows={4}
                placeholder='[{"guid":"...","name":"..."}]'
              />
            </label>

            <label className="main-product-field full">
              <span>Аннотация</span>
              <textarea value={form.annotation} onChange={(e) => onChange('annotation', e.target.value)} rows={4} />
            </label>

            <div className="main-product-actions">
              <button type="button" className="btn" onClick={onClose} disabled={loading || uploadingImages}>
                Отмена
              </button>
              <button type="submit" className="btn primary" disabled={disableSubmit}>
                {loading ? 'Сохранение...' : submitLabel || 'Сохранить товар'}
              </button>
            </div>
          </form>
        </div>
      </div>
    </div>,
    document.body
  )
}

function EditIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M16.8 3.2a2.7 2.7 0 0 1 3.8 3.8l-9.9 9.9-4.4.6.6-4.4zm2.4 2.4a1 1 0 0 0-1.4-1.4L8.1 14l-.3 2.1 2.1-.3zM5 19h14a1 1 0 1 1 0 2H5a1 1 0 1 1 0-2z" />
    </svg>
  )
}

function EyeIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 5c5.5 0 9.5 4.7 10.8 6.5a.8.8 0 0 1 0 1C21.5 14.3 17.5 19 12 19S2.5 14.3 1.2 12.5a.8.8 0 0 1 0-1C2.5 9.7 6.5 5 12 5zm0 2C8.3 7 5.2 10 3.3 12c1.9 2 5 5 8.7 5s6.8-3 8.7-5c-1.9-2-5-5-8.7-5zm0 2.5a2.5 2.5 0 1 1 0 5 2.5 2.5 0 0 1 0-5z" />
    </svg>
  )
}

function TrashIcon() {
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M9 3a1 1 0 0 0-1 1v1H5a1 1 0 1 0 0 2h1v11a3 3 0 0 0 3 3h6a3 3 0 0 0 3-3V7h1a1 1 0 1 0 0-2h-3V4a1 1 0 0 0-1-1zm1 2V5h4V5zM8 7h8v11a1 1 0 0 1-1 1H9a1 1 0 0 1-1-1zm3 2a1 1 0 0 0-1 1v6a1 1 0 1 0 2 0v-6a1 1 0 0 0-1-1m3 0a1 1 0 0 0-1 1v6a1 1 0 1 0 2 0v-6a1 1 0 0 0-1-1" />
    </svg>
  )
}

export default App
