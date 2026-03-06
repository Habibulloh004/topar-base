import { create } from 'zustand'
import { invoke } from '@tauri-apps/api/tauri'

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
      const run: Run = await invoke('start_parsing', { request })
      set({ currentRun: run })

      // Start polling progress
      const pollInterval = setInterval(async () => {
        await get().pollProgress()

        const { progress } = get()
        if (progress.status === 'finished' || progress.status === 'failed') {
          clearInterval(pollInterval)

          // Load full results
          if (progress.status === 'finished') {
            await get().loadRun(run.id)
          }
        }
      }, 1000)
    } catch (error) {
      console.error('Failed to start parsing:', error)
      throw error
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
