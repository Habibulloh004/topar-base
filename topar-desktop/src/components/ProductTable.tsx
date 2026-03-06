import { useTranslation } from 'react-i18next'
import { useParserStore } from '../store/parserStore'

export default function ProductTable() {
  const { t } = useTranslation()
  const { records, detectedFields } = useParserStore()

  if (records.length === 0) {
    return (
      <div className="product-table">
        <h2>{t('review.title')}</h2>
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
