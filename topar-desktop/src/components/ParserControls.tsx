import { useState } from 'react'
import { useTranslation } from 'react-i18next'
import { useParserStore } from '../store/parserStore'

export default function ParserControls() {
  const { t } = useTranslation()
  const { startParsing, progress } = useParserStore()

  const [sourceUrl, setSourceUrl] = useState('')
  const [limitInput, setLimitInput] = useState('')
  const [workersInput, setWorkersInput] = useState('1')
  const [requestsPerSecInput, setRequestsPerSecInput] = useState('3')
  const [error, setError] = useState('')

  const getErrorMessage = (err: any, fallback: string) => {
    if (!err) return fallback
    if (typeof err === 'string') return err
    if (typeof err.message === 'string' && err.message.trim()) return err.message
    if (typeof err.error === 'string' && err.error.trim()) return err.error
    return fallback
  }

  const parseIntegerInput = (value: string): number | null => {
    const normalized = value.trim()
    if (!normalized) return null
    if (!/^\d+$/.test(normalized)) return Number.NaN
    return Number(normalized)
  }

  const parseRpsInput = (value: string): number | null => {
    const normalized = value.trim().replace(',', '.')
    if (!normalized) return null
    if (!/^\d+(\.\d+)?$/.test(normalized)) return Number.NaN
    return Number(normalized)
  }

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')

    if (!sourceUrl.trim()) {
      setError(t('parse.errors.url'))
      return
    }

    const parsedLimit = parseIntegerInput(limitInput)
    const parsedWorkers = parseIntegerInput(workersInput)
    const parsedRequestsPerSec = parseRpsInput(requestsPerSecInput)

    if (parsedLimit !== null && (!Number.isFinite(parsedLimit) || parsedLimit < 0)) {
      setError(t('parse.limitHint'))
      return
    }
    if (parsedWorkers !== null && (!Number.isFinite(parsedWorkers) || parsedWorkers < 1 || parsedWorkers > 4)) {
      setError(t('parse.workersHint'))
      return
    }
    if (parsedRequestsPerSec !== null && (!Number.isFinite(parsedRequestsPerSec) || parsedRequestsPerSec < 1 || parsedRequestsPerSec > 20)) {
      setError(t('parse.requestsPerSecHint'))
      return
    }

    try {
      await startParsing({
        source_url: sourceUrl,
        limit: parsedLimit ?? undefined,
        workers: parsedWorkers ?? undefined,
        requests_per_sec: parsedRequestsPerSec ?? undefined,
      })
    } catch (err: any) {
      setError(getErrorMessage(err, t('parse.errors.failed')))
    }
  }

  const isRunning = progress.status === 'running'

  return (
    <div className="parser-controls">
      <h2>{t('parse.title')}</h2>

      <form onSubmit={handleSubmit} className="parse-form">
        <div className="form-group">
          <label htmlFor="sourceUrl">{t('parse.sourceUrl')}</label>
          <input
            id="sourceUrl"
            type="url"
            value={sourceUrl}
            onChange={(e) => setSourceUrl(e.target.value)}
            placeholder="https://example.com/products"
            disabled={isRunning}
            required
          />
          <span className="hint">{t('parse.sourceUrlHint')}</span>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label htmlFor="limit">{t('parse.limit')}</label>
            <input
              id="limit"
              type="number"
              value={limitInput}
              onChange={(e) => setLimitInput(e.target.value)}
              placeholder={t('parse.limitHint')}
              disabled={isRunning}
              min="0"
            />
            <span className="hint">{t('parse.limitHint')}</span>
          </div>

          <div className="form-group">
            <label htmlFor="workers">{t('parse.workers')}</label>
            <input
              id="workers"
              type="number"
              value={workersInput}
              onChange={(e) => setWorkersInput(e.target.value)}
              disabled={isRunning}
              min="1"
              max="4"
              required
            />
            <span className="hint">{t('parse.workersHint')}</span>
          </div>

          <div className="form-group">
            <label htmlFor="rps">{t('parse.requestsPerSec')}</label>
            <input
              id="rps"
              type="number"
              step="0.1"
              value={requestsPerSecInput}
              onChange={(e) => setRequestsPerSecInput(e.target.value)}
              disabled={isRunning}
              min="1"
              max="20"
              required
            />
            <span className="hint">{t('parse.requestsPerSecHint')}</span>
          </div>
        </div>

        {error && <div className="error-message">{error}</div>}

        <button type="submit" className="btn-primary" disabled={isRunning}>
          {isRunning ? `${t('parse.progress.running')}...` : t('parse.startParsing')}
        </button>
      </form>

      {isRunning && (
        <div className="progress-section">
          <h3>{t('parse.progress.title')}</h3>
          <div className="progress-bar">
            <div
              className="progress-fill"
              style={{ width: `${progress.progress_percent}%` }}
            />
          </div>
          <div className="progress-stats">
            <div>
              <strong>{t('parse.progress.discovered')}:</strong> {progress.discovered_urls}
            </div>
            <div>
              <strong>{t('parse.progress.parsed')}:</strong> {progress.parsed_products}
            </div>
            <div>
              <strong>{t('parse.progress.retries')}:</strong> {progress.rate_limit_retries}
            </div>
            {progress.current_url && (
              <div className="current-url">
                <strong>{t('parse.progress.currentUrl')}:</strong> <span title={progress.current_url}>{progress.current_url.substring(0, 80)}...</span>
              </div>
            )}
          </div>
        </div>
      )}

      {progress.status === 'finished' && (
        <div className="success-message">
          {t('parse.progress.finished')}! {progress.parsed_products} / {progress.discovered_urls}
        </div>
      )}

      {progress.status === 'failed' && (
        <div className="error-message">
          {t('parse.progress.failed')}: {progress.error || t('parse.errors.failed')}
        </div>
      )}
    </div>
  )
}
