import { useState, useEffect } from 'react'
import { useTranslation } from 'react-i18next'
import ParserControls from './components/ParserControls'
import FieldMapping from './components/FieldMapping'
import ProductTable from './components/ProductTable'
import SyncPanel from './components/SyncPanel'
import { useParserStore } from './store/parserStore'

function App() {
  const { t, i18n } = useTranslation()
  const { currentRun, mappingRules, progress, reset } = useParserStore()
  const [activeTab, setActiveTab] = useState<'parse' | 'map' | 'review' | 'sync'>('parse')

  // Auto-advance tabs based on state
  useEffect(() => {
    if (progress.status === 'finished' && activeTab === 'parse') {
      setActiveTab('map')
    }
  }, [progress.status, activeTab])

  const handleNewParse = () => {
    reset()
    setActiveTab('parse')
  }

  const toggleLanguage = () => {
    i18n.changeLanguage(i18n.language === 'en' ? 'ru' : 'en')
  }

  return (
    <div className="app">
      <header className="app-header">
        <div className="header-content">
          <div>
            <h1>{t('app.title')}</h1>
            <p className="subtitle">{t('app.subtitle')}</p>
          </div>
          <div className="header-actions">
            <button className="lang-switcher" onClick={toggleLanguage}>
              {i18n.language === 'en' ? 'RU' : 'EN'}
            </button>
            {currentRun && (
              <button className="new-parse-btn" onClick={handleNewParse}>
                {t('app.newParse')}
              </button>
            )}
          </div>
        </div>
      </header>

      <nav className="tabs">
        <button
          className={activeTab === 'parse' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('parse')}
        >
          {t('tabs.parse')}
        </button>
        <button
          className={activeTab === 'map' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('map')}
          disabled={!currentRun}
        >
          {t('tabs.map')}
        </button>
        <button
          className={activeTab === 'review' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('review')}
          disabled={!currentRun}
        >
          {t('tabs.review')}
        </button>
        <button
          className={activeTab === 'sync' ? 'tab active' : 'tab'}
          onClick={() => setActiveTab('sync')}
          disabled={!currentRun || Object.keys(mappingRules).length === 0}
        >
          {t('tabs.sync')}
        </button>
      </nav>

      <main className="app-main">
        {activeTab === 'parse' && <ParserControls />}
        {activeTab === 'map' && <FieldMapping />}
        {activeTab === 'review' && <ProductTable />}
        {activeTab === 'sync' && <SyncPanel />}
      </main>
    </div>
  )
}

export default App
