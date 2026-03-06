import { useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { invoke } from '@tauri-apps/api/tauri'
import { useParserStore } from '../store/parserStore'

export default function SyncPanel() {
  const { t } = useTranslation()
  const { currentRun, mappingRules } = useParserStore()

  const [mappingName, setMappingName] = useState('')
  const [saveMapping, setSaveMapping] = useState(false)

  const [comparing, setComparing] = useState(false)
  const [syncing, setSyncing] = useState(false)
  const [comparisonResult, setComparisonResult] = useState<any>(null)
  const [syncResult, setSyncResult] = useState<any>(null)
  const [error, setError] = useState('')
  const stopRequestedRef = useRef(false)

  const getErrorMessage = (err: any, fallback: string) => {
    if (!err) return fallback
    if (typeof err === 'string') return err
    if (typeof err.message === 'string' && err.message.trim()) return err.message
    if (typeof err.error === 'string' && err.error.trim()) return err.error
    return fallback
  }

  const handleCompare = async () => {
    if (!currentRun) {
      console.log('No current run available')
      return
    }

    console.log('Starting comparison...', { run_id: currentRun.id, rulesCount: Object.keys(mappingRules).length })
    setComparing(true)
    setError('')

    try {
      const result = await invoke('compare_with_remote', {
        request: {
          run_id: currentRun.id,
          rules: mappingRules,
        },
      })

      console.log('Comparison result:', result)
      setComparisonResult(result)
    } catch (err: any) {
      console.error('Comparison error:', err)
      setError(err.message || t('sync.errors.compareFailed'))
    } finally {
      setComparing(false)
    }
  }

  const handleSync = async () => {
    if (!currentRun) {
      console.log('No current run available')
      return
    }

    console.log('Starting sync...', {
      run_id: currentRun.id,
      rulesCount: Object.keys(mappingRules).length,
      sync_to_eksmo: true,
      sync_to_main: true
    })
    setSyncing(true)
    setError('')
    setSyncResult(null) // Clear previous result
    stopRequestedRef.current = false

    try {
      const result = await invoke('sync_to_database', {
        request: {
          run_id: currentRun.id,
          rules: mappingRules,
          mapping_name: mappingName,
          save_mapping: saveMapping,
          sync_to_eksmo: true,
          sync_to_main: true,
        },
      })

      if (stopRequestedRef.current) {
        return
      }

      console.log('Sync result:', result)
      setSyncResult(result)
    } catch (err: any) {
      if (stopRequestedRef.current) {
        return
      }
      console.error('Sync error:', err)
      setError(getErrorMessage(err, t('sync.errors.syncFailed')))
    } finally {
      setSyncing(false)
      stopRequestedRef.current = false
    }
  }

  const handleStopSync = async () => {
    stopRequestedRef.current = true
    try {
      await invoke('stop_sync')
    } catch (err) {
      console.warn('Stop sync command failed:', err)
    } finally {
      setSyncing(false)
      setComparing(false)
      setError('')
      setComparisonResult(null)
      setSyncResult(null)
      setSaveMapping(false)
      setMappingName('')
    }
  }

  return (
    <div className="sync-panel">
      <h2>{t('sync.title')}</h2>

      <div className="sync-options">
        <label>
          <input
            type="checkbox"
            checked={saveMapping}
            onChange={(e) => setSaveMapping(e.target.checked)}
          />
          {t('sync.saveMapping')}
        </label>

        {saveMapping && (
          <input
            className="mapping-name-input"
            type="text"
            placeholder={t('sync.mappingName')}
            value={mappingName}
            onChange={(e) => setMappingName(e.target.value)}
          />
        )}
      </div>

      <div className="sync-actions">
        <button
          onClick={handleCompare}
          className="btn-secondary"
          disabled={comparing || syncing}
        >
          {comparing ? `${t('sync.compare')}...` : t('sync.compare')}
        </button>

        <button
          onClick={handleSync}
          className="btn-primary"
          disabled={syncing || comparing}
        >
          {syncing ? `${t('sync.syncNow')}...` : t('sync.syncNow')}
        </button>

        {syncing && (
          <button
            onClick={handleStopSync}
            className="btn-danger"
          >
            {t('sync.stop')}
          </button>
        )}
      </div>

      {error && <div className="error-message">{error}</div>}

      {comparisonResult && (
        <div className="comparison-result">
          <h3>{t('sync.comparison.title')}</h3>
          <div className="stats-grid">
            <div className="stat">
              <div className="stat-value">{comparisonResult.total}</div>
              <div className="stat-label">{t('sync.comparison.total')}</div>
            </div>
            <div className="stat stat-new">
              <div className="stat-value">{comparisonResult.new_count}</div>
              <div className="stat-label">{t('sync.comparison.new')}</div>
            </div>
            <div className="stat stat-changed">
              <div className="stat-value">{comparisonResult.changed_count}</div>
              <div className="stat-label">{t('sync.comparison.changed')}</div>
            </div>
            <div className="stat stat-unchanged">
              <div className="stat-value">{comparisonResult.unchanged_count}</div>
              <div className="stat-label">{t('sync.comparison.unchanged')}</div>
            </div>
          </div>
        </div>
      )}

      {syncResult && (
        <div className="sync-result">
          <h3>{t('sync.result.title')}</h3>
          <div className="stats-grid">
            <div className="stat">
              <div className="stat-value">{syncResult.total_records}</div>
              <div className="stat-label">{t('sync.result.totalRecords')}</div>
            </div>
            <div className="stat stat-new">
              <div className="stat-value">{syncResult.new_count}</div>
              <div className="stat-label">{t('sync.result.newCount')}</div>
            </div>
            <div className="stat stat-changed">
              <div className="stat-value">{syncResult.updated_count}</div>
              <div className="stat-label">{t('sync.result.updatedCount')}</div>
            </div>
            <div className="stat stat-unchanged">
              <div className="stat-value">{syncResult.unchanged_count}</div>
              <div className="stat-label">{t('sync.result.unchangedCount')}</div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
