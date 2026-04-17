import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useParserStore } from '../store/parserStore'

export default function ProductTable() {
  const { t } = useTranslation()
  const { records, detectedFields, currentRun, exportCurrentRunToJson } = useParserStore()
  const [error, setError] = useState('')
  const [exportSuccess, setExportSuccess] = useState('')
  const [isExporting, setIsExporting] = useState(false)

  const canExport = !!currentRun && currentRun.status !== 'running' && currentRun.parsed_products > 0

  const getErrorMessage = (err: any, fallback: string) => {
    if (!err) return fallback
    if (typeof err === 'string') return err
    if (typeof err.message === 'string' && err.message.trim()) return err.message
    if (typeof err.error === 'string' && err.error.trim()) return err.error
    return fallback
  }

  const handleExport = async () => {
    if (!canExport || isExporting) return

    setError('')
    setExportSuccess('')
    setIsExporting(true)

    try {
      const path = await exportCurrentRunToJson()
      if (path) {
        setExportSuccess(t('review.export.success', { path }))
      }
    } catch (err: any) {
      setError(getErrorMessage(err, t('review.export.failed')))
    } finally {
      setIsExporting(false)
    }
  }

  if (records.length === 0) {
    return (
      <div className="product-table">
        <h2>{t('review.title')}</h2>
        {canExport && (
          <div className="review-actions">
            <button
              type="button"
              className="btn-secondary"
              onClick={handleExport}
              disabled={isExporting}
            >
              {isExporting ? t('review.export.exporting') : t('review.export.button')}
            </button>
          </div>
        )}
        {error && <div className="error-message">{error}</div>}
        {exportSuccess && <div className="success-message">{exportSuccess}</div>}
        <p>{t('review.noProducts')}</p>
      </div>
    )
  }

  const truncate = (text: string, maxLen: number = 60) => {
    if (text.length <= maxLen) return text
    return text.substring(0, maxLen) + '...'
  }

  return (
    <div className="product-table">
      <h2>{t('review.title')}</h2>
      {canExport && (
        <div className="review-actions">
          <button
            type="button"
            className="btn-secondary"
            onClick={handleExport}
            disabled={isExporting}
          >
            {isExporting ? t('review.export.exporting') : t('review.export.button')}
          </button>
        </div>
      )}
      {error && <div className="error-message">{error}</div>}
      {exportSuccess && <div className="success-message">{exportSuccess}</div>}
      <p>{t('review.showing', { count: records.length })}</p>

      <div className="table-wrapper">
        <table>
          <thead>
            <tr>
              {detectedFields.map((field) => (
                <th key={field}>{field}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {records.map((record) => (
              <tr key={record.id}>
                {detectedFields.map((field) => {
                  const value = record.data[field]
                  const displayValue =
                    typeof value === 'object'
                      ? JSON.stringify(value)
                      : String(value || '')

                  return (
                    <td key={field} title={displayValue}>
                      {truncate(displayValue)}
                    </td>
                  )
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
