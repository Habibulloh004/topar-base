import { create } from 'zustand'
import { invoke } from '@tauri-apps/api/tauri'

let progressPollInterval: ReturnType<typeof setInterval> | null = null
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

// Types
export interface Run {
  id: string
  source_url: string
  limit_count: number
  workers: number
  requests_per_sec: number
  discovered_urls: number
  parsed_products: number
  rate_limit_retries: number
  status: 'running' | 'finished' | 'failed'
  error?: string
  detected_fields: string[]
  created_at: string
  finished_at?: string
}

export interface ParsedRecord {
  id: string
  run_id: string
  source_url: string
  data: Record<string, any>
  created_at: string
}

export interface MappingRule {
  source: string
  constant: string
}

export interface ParserProgress {
  status: 'idle' | 'running' | 'finished' | 'failed'
  current_url?: string
  discovered_urls: number
  parsed_products: number
  rate_limit_retries: number
  error?: string
  progress_percent: number
}

interface ParserState {
  // Data
  currentRun?: Run
  records: ParsedRecord[]
  detectedFields: string[]
  mappingRules: Record<string, MappingRule>
  progress: ParserProgress

  // Actions
  startParsing: (request: {
    source_url: string
    limit?: number
    workers?: number
    requests_per_sec?: number
  }) => Promise<void>
  stopParsing: () => Promise<void>
  loadRun: (runId: string) => Promise<void>
  pollProgress: () => Promise<void>
  updateMappingRule: (target: string, rule: MappingRule) => void
  saveMappingProfile: (name: string) => Promise<void>
  loadMappingProfile: (profileId: string) => Promise<void>
  reset: () => void
}

export const useParserStore = create<ParserState>((set, get) => ({
  // Initial state
  records: [],
  detectedFields: [],
  mappingRules: {},
  progress: {
    status: 'idle',
    discovered_urls: 0,
    parsed_products: 0,
    rate_limit_retries: 0,
    progress_percent: 0,
  },

  // Start parsing
  startParsing: async (request) => {
    try {
      if (progressPollInterval) {
        clearInterval(progressPollInterval)
        progressPollInterval = null
      }

      set({
        currentRun: undefined,
        records: [],
        detectedFields: [],
        mappingRules: {},
        progress: {
          status: 'running',
          discovered_urls: 0,
          parsed_products: 0,
          rate_limit_retries: 0,
          progress_percent: 0,
        },
      })

      const run: Run = await invoke('start_parsing', { request })
      set({ currentRun: run })

      // Start polling progress
      progressPollInterval = setInterval(async () => {
        await get().pollProgress()

        const { progress } = get()
        if (progress.status === 'finished' || progress.status === 'failed') {
          if (progressPollInterval) {
            clearInterval(progressPollInterval)
            progressPollInterval = null
          }

          // Load run snapshot for both successful and cancelled/failed runs.
          await get().loadRun(run.id)
        }
      }, 1000)
    } catch (error) {
      set((state) => ({
        progress: {
          ...state.progress,
          status: 'failed',
        },
      }))
      console.error('Failed to start parsing:', error)
      throw error
    }
  },

  // Stop current parsing process but keep already parsed data from current run.
  stopParsing: async () => {
    const runId = get().currentRun?.id
    try {
      await invoke('stop_parsing')
    } catch (error) {
      console.error('Failed to stop parsing:', error)
      throw error
    } finally {
      if (progressPollInterval) {
        clearInterval(progressPollInterval)
        progressPollInterval = null
      }
    }

    // Wait briefly until backend marks run finished/failed and persists partial records.
    for (let attempt = 0; attempt < 25; attempt++) {
      await get().pollProgress()
      if (get().progress.status !== 'running') break
      await sleep(120)
    }

    if (runId) {
      await get().loadRun(runId)
    }
  },

  // Load run with records
  loadRun: async (runId) => {
    try {
      const response: {
        run: Run
        records: ParsedRecord[]
        total_records: number
      } = await invoke('get_run_with_records', {
        runId,
        limit: 100,
        offset: 0,
      })

      set({
        currentRun: response.run,
        records: response.records,
        detectedFields: response.run.detected_fields || [],
      })
    } catch (error) {
      console.error('Failed to load run:', error)
      throw error
    }
  },

  // Poll parsing progress
  pollProgress: async () => {
    try {
      const progress: ParserProgress = await invoke('get_parser_status')
      set({ progress })
    } catch (error) {
      console.error('Failed to poll progress:', error)
    }
  },

  // Update mapping rule
  updateMappingRule: (target, rule) => {
    set((state) => ({
      mappingRules: {
        ...state.mappingRules,
        [target]: rule,
      },
    }))
  },

  // Save mapping profile
  saveMappingProfile: async (name) => {
    try {
      const { mappingRules } = get()
      await invoke('save_mapping_profile', {
        request: {
          name,
          rules: mappingRules,
        },
      })
    } catch (error) {
      console.error('Failed to save mapping profile:', error)
      throw error
    }
  },

  // Load mapping profile
  loadMappingProfile: async (profileId) => {
    try {
      const profile: {
        id: string
        name: string
        rules: Record<string, MappingRule>
      } = await invoke('get_mapping_profile', { profileId })

      set({ mappingRules: profile.rules })
    } catch (error) {
      console.error('Failed to load mapping profile:', error)
      throw error
    }
  },

  // Reset state
  reset: () => {
    if (progressPollInterval) {
      clearInterval(progressPollInterval)
      progressPollInterval = null
    }

    set({
      currentRun: undefined,
      records: [],
      detectedFields: [],
      mappingRules: {},
      progress: {
        status: 'idle',
        discovered_urls: 0,
        parsed_products: 0,
        rate_limit_retries: 0,
        progress_percent: 0,
      },
    })
  },
}))
