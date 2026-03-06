import { useState, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import { useParserStore } from '../store/parserStore'

const SCHEMA = {
  eksmo: [
    { key: 'guidNom', description: 'Unique identifier (stable)' },
    { key: 'guid', description: 'GUID' },
    { key: 'nomcode', description: 'Barcode/Nomcode' },
    { key: 'isbn', description: 'ISBN' },
    { key: 'name', description: 'Product name' },
    { key: 'annotation', description: 'Description' },
    { key: 'authorCover', description: 'Author cover name' },
    { key: 'coverUrl', description: 'Cover image URL' },
  ],
  main: [
    { key: 'sourceGuidNom', description: 'Source GUID Nom' },
    { key: 'sourceGuid', description: 'Source GUID' },
    { key: 'sourceNomcode', description: 'Source nomcode' },
    { key: 'isbn', description: 'ISBN' },
    { key: 'name', description: 'Product name' },
    { key: 'authorCover', description: 'Author cover' },
    { key: 'annotation', description: 'Description' },
    { key: 'coverUrl', description: 'Cover URL' },
  ],
}

const FIELD_ALIASES: Record<string, string[]> = {
  barcode: ['barcode', 'gtin', 'gtin13', 'gtin14', 'sku', 'isbn'],
  nomcode: ['nomcode', 'barcode', 'gtin', 'sku'],
  isbn: ['isbn', 'gtin', 'gtin13'],
  name: ['name', 'title'],
  annotation: ['annotation', 'description'],
  coverUrl: ['cover', 'image', 'coverurl', 'img'],
}

export default function FieldMapping() {
  const { t } = useTranslation()
  const { detectedFields, mappingRules, updateMappingRule, saveMappingProfile } = useParserStore()
  const [mappingName, setMappingName] = useState('')
  const [status, setStatus] = useState('')

  // Auto-fill mappings on mount
  useEffect(() => {
    if (detectedFields.length > 0 && Object.keys(mappingRules).length === 0) {
      autoFillMappings()
    }
  }, [detectedFields])

  const autoFillMappings = () => {
    const normalizedFields = new Map(
      detectedFields.map((f) => [normalizeKey(f), f])
    )

    // Try to auto-map fields
    for (const prefix of ['eksmo', 'main']) {
      for (const field of SCHEMA[prefix as keyof typeof SCHEMA]) {
        const target = `${prefix}.${field.key}`

        // Check if already mapped
        if (mappingRules[target]?.source || mappingRules[target]?.constant) {
          continue
        }

        const aliases = FIELD_ALIASES[field.key] || [field.key]
        let match = ''

        for (const alias of aliases) {
          const normalized = normalizeKey(alias)
          if (normalizedFields.has(normalized)) {
            match = normalizedFields.get(normalized)!
            break
          }

          // Try partial match
          for (const sourceField of detectedFields) {
            if (normalizeKey(sourceField).includes(normalized)) {
              match = sourceField
              break
            }
          }

          if (match) break
        }

        if (match) {
          updateMappingRule(target, { source: match, constant: '' })
        }
      }
    }
  }

  const normalizeKey = (key: string): string => {
    return key.toLowerCase().replace(/[^a-z0-9]/g, '')
  }

  const handleSaveMapping = async () => {
    if (!mappingName.trim()) {
      setStatus(t('mapping.errors.saveFailed'))
      return
    }

    try {
      await saveMappingProfile(mappingName)
      setStatus(`${t('mapping.saveProfile')}: "${mappingName}"`)
    } catch (err: any) {
      setStatus(`${t('mapping.errors.saveFailed')}: ${err.message}`)
    }
  }

  if (detectedFields.length === 0) {
    return (
      <div className="field-mapping">
        <h2>{t('mapping.title')}</h2>
        <p className="error-message">{t('mapping.noFields')}</p>
      </div>
    )
  }

  return (
    <div className="field-mapping">
      <h2>{t('mapping.title')}</h2>

      <div className="mapping-info">
        <p>{t('mapping.detected')}: {detectedFields.length}</p>
      </div>

      <div className="mapping-columns">
        {(['eksmo', 'main'] as const).map((prefix) => (
          <div key={prefix} className="mapping-column">
            <h3>{prefix === 'eksmo' ? 'Eksmo' : 'Main'}</h3>

            {SCHEMA[prefix].map((field) => {
              const target = `${prefix}.${field.key}`
              const rule = mappingRules[target] || { source: '', constant: '' }

              return (
                <div key={target} className="mapping-row">
                  <label title={field.description}>{field.key}</label>

                  <div className="mapping-controls">
                    <select
                      value={rule.source}
                      onChange={(e) =>
                        updateMappingRule(target, {
                          ...rule,
                          source: e.target.value,
                        })
                      }
                    >
                      <option value="">({t('mapping.source')})</option>
                      {detectedFields.map((f) => (
                        <option key={f} value={f}>
                          {f}
                        </option>
                      ))}
                    </select>

                    <input
                      type="text"
                      placeholder={t('mapping.constant')}
                      value={rule.constant}
                      onChange={(e) =>
                        updateMappingRule(target, {
                          ...rule,
                          constant: e.target.value,
                        })
                      }
                    />
                  </div>
                </div>
              )
            })}
          </div>
        ))}
      </div>

      <div className="mapping-actions">
        <input
          type="text"
          placeholder={t('mapping.profileName')}
          value={mappingName}
          onChange={(e) => setMappingName(e.target.value)}
        />
        <button onClick={handleSaveMapping} className="btn-secondary">
          {t('mapping.saveProfile')}
        </button>
      </div>

      {status && <div className="status-message">{status}</div>}
    </div>
  )
}
