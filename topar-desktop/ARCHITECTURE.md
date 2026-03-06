# Topar Desktop - Architecture Overview

## Design Philosophy

The Topar Desktop Parser was designed with these principles:

1. **Local-first**: All parsing happens locally, data stored in SQLite before syncing
2. **Non-invasive**: Completely isolated in `topar-desktop/` folder, doesn't touch existing code
3. **Reusable logic**: Replicates existing Topar parsing patterns and field mappings
4. **Production-ready**: Built for real daily usage, not just a demo
5. **Cross-platform**: Single codebase for macOS, Windows, Linux

## Technology Stack

### Why Tauri over Electron?

| Aspect | Tauri | Electron | Winner |
|--------|-------|----------|--------|
| **Bundle size** | ~5MB | ~150MB | Tauri (30x smaller) |
| **Memory usage** | ~50-100MB | ~300-500MB | Tauri (3-5x lighter) |
| **Startup time** | ~1s | ~3-5s | Tauri (3-5x faster) |
| **Security** | Rust backend isolation | Node.js exposed to frontend | Tauri |
| **Updates** | Smaller delta updates | Full bundle updates | Tauri |
| **Native feel** | System webview | Bundled Chromium | Tauri |

**Decision**: Tauri is the clear winner for a desktop parser that needs to run locally, handle large datasets, and distribute to users.

### Why Python for Parsing?

| Aspect | Python + Playwright | Go + Colly | Node.js + Puppeteer |
|--------|-------------------|------------|---------------------|
| **Browser automation** | Excellent (Playwright) | None (static HTML only) | Good (Puppeteer) |
| **Parsing libraries** | Excellent (BS4, lxml) | Good (goquery) | Good (cheerio) |
| **JS rendering** | Full support | None | Full support |
| **Async performance** | Good | Excellent | Good |
| **Ease of debugging** | Excellent | Good | Good |
| **Ecosystem** | Vast scraping tools | Growing | Good |

**Decision**: Python + Playwright provides the best balance of power, ease of use, and compatibility with complex sites requiring JS rendering.

### Why SQLite?

| Feature | SQLite | PostgreSQL | MongoDB |
|---------|--------|------------|---------|
| **Setup** | Zero config | Install + configure server | Install + configure server |
| **Size** | ~1MB library | ~50MB server | ~500MB server |
| **Speed (local)** | Excellent | Good (network overhead) | Good (network overhead) |
| **Transactions** | ACID compliant | ACID compliant | Eventually consistent |
| **Portability** | Single file | Needs server | Needs server |
| **Backup** | Copy file | pg_dump | mongodump |

**Decision**: SQLite is perfect for local desktop storage with zero configuration and single-file portability.

## Component Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Tauri Desktop App                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────────────────────────────────────────────┐  │
│  │            React Frontend (TypeScript)                │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐     │  │
│  │  │  Parser    │  │   Field    │  │  Product   │     │  │
│  │  │  Controls  │  │  Mapping   │  │   Table    │     │  │
│  │  └────────────┘  └────────────┘  └────────────┘     │  │
│  │  ┌────────────┐  ┌────────────────────────────┐     │  │
│  │  │   Sync     │  │    Zustand Store           │     │  │
│  │  │   Panel    │  │    (State Management)      │     │  │
│  │  └────────────┘  └────────────────────────────┘     │  │
│  └──────────────────────────────────────────────────────┘  │
│                           │                                  │
│                           │ Tauri IPC Commands               │
│                           ▼                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │            Rust Backend (Tauri Core)                  │  │
│  │  ┌──────────────────────────────────────────────┐   │  │
│  │  │  Commands Layer (Tauri Commands)             │   │  │
│  │  │  - database.rs  - parser.rs  - sync.rs      │   │  │
│  │  └──────────────────────────────────────────────┘   │  │
│  │  ┌──────────────────────────────────────────────┐   │  │
│  │  │  Business Logic Layer                         │   │  │
│  │  │  ┌────────────┐  ┌────────────┐  ┌────────┐ │   │  │
│  │  │  │  Parser    │  │    Sync    │  │  DB    │ │   │  │
│  │  │  │  Engine    │  │  Service   │  │ Ops    │ │   │  │
│  │  │  └────────────┘  └────────────┘  └────────┘ │   │  │
│  │  └──────────────────────────────────────────────┘   │  │
│  │  ┌──────────────────────────────────────────────┐   │  │
│  │  │  Data Layer (SQLite)                          │   │  │
│  │  │  - runs  - records  - mappings  - sync_logs │   │  │
│  │  └──────────────────────────────────────────────┘   │  │
│  └──────────────────────────────────────────────────────┘  │
│                           │                                  │
│                           │ Process Spawn                    │
│                           ▼                                  │
│  ┌──────────────────────────────────────────────────────┐  │
│  │         Python Parser Engine (Subprocess)             │  │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐     │  │
│  │  │  Crawler   │→ │   Parser   │→ │ Extractor  │     │  │
│  │  │ (Sitemap/  │  │ (Playwright)│  │(BS4/lxml)  │     │  │
│  │  │  Crawl)    │  │            │  │            │     │  │
│  │  └────────────┘  └────────────┘  └────────────┘     │  │
│  │         │                                              │  │
│  │         │ JSON over stdout/stderr                      │  │
│  │         ▼                                              │  │
│  │    Progress updates → Rust backend → React UI         │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                               │
│                           │ HTTP Client (Sync)                │
│                           ▼                                  │
└───────────────────────────┼──────────────────────────────────┘
                            │
                            ▼
            ┌───────────────────────────────┐
            │   Topar Backend API            │
            │   (Go + MongoDB)               │
            │   - /parser-app/runs/:id/sync │
            │   - /parser-app/mappings      │
            └───────────────────────────────┘
```

## Data Flow

### 1. Parsing Flow

```
User Input (URL)
    → React Form
    → Tauri Command (start_parsing)
    → Rust spawns Python process
    → Python: Discover URLs (sitemap/crawl)
    → Python: Parse products (Playwright + workers)
    → Python: Extract fields (BeautifulSoup)
    → Python: Send progress updates via JSON stdout
    → Rust: Receives progress, updates state
    → React: Polls progress, updates UI
    → Python: Sends final results via JSON
    → Rust: Saves records to SQLite in batches
    → React: Shows completion message
```

### 2. Field Mapping Flow

```
Parsed fields from DB
    → React: Display detected fields
    → React: Auto-fill mappings using aliases
    → User: Adjust mappings manually
    → Zustand Store: Store mappings
    → User: Save mapping profile (optional)
    → Tauri Command (save_mapping_profile)
    → Rust: Save to SQLite
```

### 3. Comparison Flow

```
User clicks "Compare"
    → Tauri Command (compare_with_remote)
    → Rust: Load local records from SQLite
    → Rust: Apply mapping rules
    → Rust: Extract identifiers (guidNom, guid, isbn)
    → Rust: Fetch remote products via HTTP
    → Rust: Compare field-by-field
    → Rust: Categorize (new, changed, unchanged)
    → React: Display comparison results
```

### 4. Sync Flow

```
User clicks "Sync"
    → Tauri Command (sync_to_database)
    → Rust: Create sync log entry
    → Rust: Build sync request with mapping rules
    → Rust: HTTP POST to backend /parser-app/runs/:id/sync
    → Backend: Processes records
    → Backend: Upserts to MongoDB (eksmo + main collections)
    → Backend: Returns sync results
    → Rust: Updates sync log with results
    → React: Display sync statistics
```

## Database Schema

### SQLite (Local Storage)

```sql
runs                     -- Parsing run metadata
├── id (PK)
├── source_url
├── discovered_urls
├── parsed_products
├── detected_fields (JSON array)
└── status

records                  -- Parsed product data
├── id (PK)
├── run_id (FK → runs.id)
├── source_url
└── data (JSON blob)

mapping_profiles         -- Saved field mappings
├── id (PK)
├── name
└── rules (JSON blob)

sync_logs               -- Sync history
├── id (PK)
├── run_id (FK → runs.id)
├── new_count
├── updated_count
└── unchanged_count

config                  -- App configuration
├── key (PK)
└── value
```

### MongoDB (Remote Backend)

```
eksmo_products          -- All product collection
├── _id
├── guidNom (UNIQUE)    -- Primary identifier
├── guid
├── nomcode
├── isbn
├── name
├── annotation
└── ... (all product fields)

main_products           -- Main catalog collection
├── _id
├── sourceGuidNom (UNIQUE)
├── sourceGuid
├── isbn
└── ... (denormalized fields)

parser_runs             -- Parsing metadata (from backend)
parser_records          -- Raw parsed data (from backend)
```

## Communication Protocols

### 1. Tauri IPC (Frontend ↔ Rust)

Uses Tauri's built-in IPC system:

```typescript
// Frontend calls Rust command
const result = await invoke('start_parsing', {
  request: { source_url, limit, workers }
})

// Rust command handler
#[tauri::command]
async fn start_parsing(request: ParseRequest) -> Result<Run, String> {
  // Implementation
}
```

**Why Tauri IPC?**
- Type-safe (Rust ↔ TypeScript)
- Serialization handled automatically (serde)
- Async support out of the box
- Secure by default (no Node.js exposure)

### 2. JSON over stdout (Rust ↔ Python)

Python subprocess writes JSON to stdout:

```python
# Python sends progress
print(json.dumps({
  "type": "progress",
  "event": "product_parsed",
  "data": {"total": 42}
}), flush=True)
```

```rust
// Rust reads and parses JSON
let reader = BufReader::new(stdout);
for line in reader.lines() {
  let msg: ParserMessage = serde_json::from_str(&line)?;
  // Handle message
}
```

**Why JSON over stdout?**
- Simple and reliable
- Language-agnostic
- Line-buffered (no partial messages)
- Easy to debug (can run Python script manually)

### 3. HTTP REST (Rust ↔ Backend)

Uses reqwest HTTP client:

```rust
let response = client
  .post(&format!("{}/parser-app/runs/{}/sync", base_url, run_id))
  .json(&request)
  .send()
  .await?;
```

**Why HTTP REST?**
- Reuses existing backend API
- No need for MongoDB driver in desktop app
- Works with remote backends
- Maintains business logic in backend

## Error Handling Strategy

### Frontend (React)

```typescript
try {
  await startParsing(request)
} catch (err: any) {
  // User-friendly error messages
  setError(err.message || 'Failed to start parsing')
}
```

### Rust Backend

```rust
use anyhow::{Context, Result};

pub fn parse() -> Result<Data> {
  let data = operation()
    .context("Failed to perform operation")?;
  Ok(data)
}
```

Returns error strings to frontend for display.

### Python Parser

```python
try:
  result = parse_url(url)
except Exception as e:
  log_error(str(e), {"type": type(e).__name__})
  sys.exit(1)
```

Sends errors via JSON to Rust, which stores in database.

## Performance Optimizations

### 1. Concurrent Parsing

- **Worker pool**: 1-40 concurrent workers (default: 6)
- **Rate limiting**: Configurable requests/sec (default: 1.2)
- **Connection pooling**: Playwright reuses browser contexts
- **Batch inserts**: 250 records per SQLite transaction

### 2. Memory Management

- **Streaming results**: Python sends results incrementally, not all at once
- **Batch processing**: Records inserted in batches to limit memory
- **WAL mode**: SQLite uses Write-Ahead Logging for better concurrency
- **Arc<Mutex>**: Rust uses atomic reference counting for shared state

### 3. Database Optimization

```sql
-- Indexes for fast queries
CREATE INDEX idx_records_run_id ON records(run_id);
CREATE INDEX idx_runs_status ON runs(status, created_at);

-- WAL mode for better write performance
PRAGMA journal_mode = WAL;

-- Foreign keys for referential integrity
PRAGMA foreign_keys = ON;
```

## Security Considerations

### 1. Tauri Security Model

- **No Node.js**: Backend is Rust, no Node.js runtime
- **Allowlist**: Only specific Tauri APIs enabled (dialog, notification)
- **No arbitrary code execution**: Frontend can't run arbitrary Rust code
- **Sandboxing**: Python subprocess runs isolated

### 2. Data Security

- **Local-first**: Data never leaves machine until explicit sync
- **SQLite security**: Database file has OS-level permissions
- **No credentials in code**: Backend URL stored in config, not hardcoded
- **HTTPS**: Sync uses HTTPS for remote backend

### 3. Input Validation

- **URL validation**: Source URLs validated before parsing
- **Numeric ranges**: Workers (1-40), RPS (0.2-20) enforced
- **SQL injection**: Prevented by parameterized queries
- **XSS**: React escapes HTML by default

## Extension Points

### Adding New Field

1. **Update schema** in `FieldMapping.tsx`:
   ```typescript
   { key: 'newField', description: 'New field' }
   ```

2. **Add to Python extractor** in `extractor.py`:
   ```python
   if 'newFieldSelector' in item:
     self.data['newField'] = item['newFieldSelector']
   ```

3. **Update backend models** (if syncing new field)

### Adding New Parser Strategy

1. **Create new extractor** in `parser_engine/`:
   ```python
   class CustomExtractor(FieldExtractor):
     def extract(self):
       # Custom extraction logic
   ```

2. **Register in parser**:
   ```python
   extractor = CustomExtractor(content, url)
   ```

### Adding New Sync Target

1. **Create new sync client** in `src-tauri/src/sync/`:
   ```rust
   pub struct CustomSyncClient { /* ... */ }
   ```

2. **Add Tauri command**:
   ```rust
   #[tauri::command]
   async fn sync_to_custom() { /* ... */ }
   ```

3. **Add UI in React**:
   ```typescript
   <button onClick={() => invoke('sync_to_custom')}>
     Sync to Custom
   </button>
   ```

## Comparison with Web Version

| Feature | Web Version | Desktop Version |
|---------|-------------|-----------------|
| **Deployment** | Server + Nginx | User's machine |
| **Parsing location** | Server (Go backend) | Local (Python subprocess) |
| **Storage** | MongoDB directly | SQLite → MongoDB (sync) |
| **Pagination** | Manual (single page) | Automatic (all pages) |
| **Offline mode** | No | Yes (parse offline, sync later) |
| **Review before sync** | No | Yes (compare + review) |
| **Field mapping** | Web UI | Desktop UI (same UX) |
| **Performance** | Network dependent | Local, faster |
| **Distribution** | Single deployment | Per-user installation |

## Future Enhancements

### Phase 2 Ideas

1. **Scheduling**: Cron-like scheduled parsing
2. **Incremental parsing**: Only parse new/changed products
3. **Multiple profiles**: Switch between different backends
4. **Export**: Export to CSV/Excel before syncing
5. **Diff visualization**: Visual field-by-field comparison
6. **Proxy support**: Route requests through proxy
7. **Browser selection**: Choose Chrome/Firefox/WebKit
8. **Auto-updates**: Self-updating via Tauri updater
9. **Plugins**: Custom field extractors via plugins
10. **Cloud backup**: Backup SQLite to cloud storage

## Lessons Learned

### What Worked Well

1. **Tauri choice**: Small bundle, fast, native feel
2. **Python for parsing**: Ecosystem is unmatched
3. **Local-first approach**: Users trust local storage
4. **SQLite**: Zero config, just works
5. **Type safety**: Rust + TypeScript caught many bugs early

### What Could Be Improved

1. **Python distribution**: Requires Python installed on user's machine
   - Future: Bundle Python runtime with app (PyOxidizer)
2. **First compile time**: 5-10 minutes for Rust compilation
   - Future: Pre-built binaries for releases
3. **Error messages**: Some Rust errors are too technical
   - Future: Better error translation for users

### Architecture Decisions

**Q: Why not embed Python in Rust binary?**
A: PyO3 exists but has platform-specific issues. Subprocess is simpler and more reliable.

**Q: Why not use Go for everything?**
A: Go's scraping ecosystem lacks full browser automation like Playwright.

**Q: Why not direct MongoDB access?**
A: Reusing backend API ensures business logic consistency and allows remote backends.

**Q: Why not Electron?**
A: Bundle size (30x larger) and memory usage (3-5x higher) were dealbreakers.

## Maintenance

### Adding Dependencies

**Node/React**:
```bash
npm install <package>
```

**Rust**:
Edit `src-tauri/Cargo.toml`:
```toml
[dependencies]
new-crate = "1.0"
```

**Python**:
Edit `src-tauri/parser_engine/requirements.txt`:
```
new-package==1.0.0
```

### Updating Dependencies

```bash
# Node
npm update

# Rust
cargo update

# Python
pip install --upgrade -r requirements.txt
```

### Running Tests

```bash
# Frontend
npm test

# Rust
cd src-tauri && cargo test

# Python
cd src-tauri/parser_engine
python -m pytest
```

---

**This architecture document is a living document. Update it as the system evolves.**
