# Topar Desktop Parser

A standalone desktop application for parsing, managing, and syncing product data locally before syncing to the main Topar database.

## Architecture

This desktop app is built with:

- **Frontend**: React + TypeScript + Vite (modern, fast UI)
- **Desktop Shell**: Tauri (Rust-based, lightweight, ~5MB vs Electron's ~150MB)
- **Parser Engine**: Python 3.11+ with Playwright (reliable browser automation)
- **Local Database**: SQLite (embedded, zero-config, perfect for desktop)
- **Sync Client**: HTTP client to Topar backend API

### Why Tauri over Electron?

1. **Size**: ~5MB bundle vs 150MB+ with Electron (doesn't bundle Chromium)
2. **Performance**: Lower memory usage, faster startup, uses system webview
3. **Security**: Built-in security by default, no Node.js exposure to frontend
4. **Modern**: Native Rust backend for concurrent parsing operations
5. **Cross-platform**: Single codebase for macOS, Windows, Linux

### Why Python for Parsing?

1. **Playwright**: Best-in-class browser automation with full JS rendering
2. **BeautifulSoup + lxml**: Excellent HTML parsing libraries
3. **Existing ecosystem**: Can leverage Python's rich scraping ecosystem
4. **Easy debugging**: Python is more accessible than Go for parsing logic
5. **Compatibility**: Can reuse parsing patterns from existing Topar logic

## Features

### Core Functionality

1. **Multi-page Parsing**
   - Parses ALL pagination pages automatically
   - Configurable workers (1-40 concurrent requests)
   - Rate limiting (requests per second control)
   - Automatic retry on rate limit (429) responses
   - Progress tracking with real-time updates

2. **Local Storage First**
   - All parsed data stored in SQLite first
   - Review data before syncing
   - Offline capability
   - Fast queries and filtering

3. **Smart Comparison**
   - Compare local data with remote database
   - Detect: new products, updated fields, unchanged items
   - Field-by-field diff visualization
   - Skip unchanged products during sync

4. **Manual Sync Control**
   - Explicit sync button - nothing happens automatically
   - Preview changes before applying
   - Incremental sync (only changed data)
   - Sync result logs and statistics

5. **Field Mapping**
   - Visual mapping interface (source field → target field)
   - Auto-detection with field aliases
   - Save/load mapping profiles
   - Support for constant values

## Project Structure

```
topar-desktop/
├── src-tauri/                    # Rust backend (Tauri app)
│   ├── src/
│   │   ├── main.rs              # Entry point
│   │   ├── commands/            # Tauri commands (IPC)
│   │   │   ├── mod.rs
│   │   │   ├── parser.rs        # Parsing control commands
│   │   │   ├── database.rs      # Local DB operations
│   │   │   └── sync.rs          # Remote sync commands
│   │   ├── db/                  # SQLite database layer
│   │   │   ├── mod.rs
│   │   │   ├── schema.rs        # Table definitions
│   │   │   └── operations.rs   # CRUD operations
│   │   ├── parser/              # Parser orchestration
│   │   │   ├── mod.rs
│   │   │   ├── engine.rs        # Python process management
│   │   │   └── progress.rs     # Progress tracking
│   │   └── sync/                # Sync service
│   │       ├── mod.rs
│   │       ├── client.rs        # HTTP client for backend API
│   │       └── compare.rs      # Comparison logic
│   ├── parser_engine/           # Python parsing engine
│   │   ├── main.py              # CLI entry point
│   │   ├── parser.py            # Core parsing logic
│   │   ├── crawler.py           # URL discovery (sitemap/crawl)
│   │   ├── extractor.py         # Field extraction
│   │   └── requirements.txt     # Python dependencies
│   ├── Cargo.toml               # Rust dependencies
│   ├── tauri.conf.json          # Tauri configuration
│   └── build.rs                 # Build script
├── src/                         # React frontend
│   ├── App.tsx                  # Main app component
│   ├── components/              # UI components
│   │   ├── ParserControls.tsx  # Parse form & controls
│   │   ├── FieldMapping.tsx    # Mapping interface
│   │   ├── ProductTable.tsx    # Parsed products view
│   │   ├── ComparisonView.tsx  # Diff visualization
│   │   ├── SyncPanel.tsx       # Sync controls & logs
│   │   └── ProgressBar.tsx     # Progress indicator
│   ├── hooks/                   # React hooks
│   │   ├── useParser.ts        # Parser state management
│   │   ├── useDatabase.ts      # Local DB queries
│   │   └── useSync.ts          # Sync operations
│   ├── types/                   # TypeScript types
│   │   └── index.ts
│   ├── utils/                   # Utilities
│   │   ├── api.ts              # Tauri command wrappers
│   │   └── formatting.ts       # Display helpers
│   ├── main.tsx                # React entry point
│   └── styles.css              # Global styles
├── package.json                # Node dependencies
├── tsconfig.json               # TypeScript config
├── vite.config.ts              # Vite config
└── README.md                   # This file
```

## Installation

### Prerequisites

1. **Node.js 18+**: [Download](https://nodejs.org/)
2. **Rust 1.70+**: [Install rustup](https://rustup.rs/)
3. **Python 3.11+**: [Download](https://www.python.org/downloads/)
4. **System dependencies**:

   **macOS**:
   ```bash
   xcode-select --install
   ```

   **Linux (Ubuntu/Debian)**:
   ```bash
   sudo apt update
   sudo apt install libwebkit2gtk-4.0-dev \
       build-essential \
       curl \
       wget \
       file \
       libssl-dev \
       libgtk-3-dev \
       libayatana-appindicator3-dev \
       librsvg2-dev \
       python3-dev
   ```

   **Windows**:
   - Install [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
   - Install [WebView2](https://developer.microsoft.com/en-us/microsoft-edge/webview2/)

### Setup

1. **Navigate to desktop app directory**:
   ```bash
   cd topar/topar-desktop
   ```

2. **Install Node dependencies**:
   ```bash
   npm install
   ```

3. **Install Python dependencies**:
   ```bash
   cd src-tauri/parser_engine
   python3 -m pip install -r requirements.txt
   playwright install chromium  # Install browser
   cd ../..
   ```

4. **Build Rust dependencies** (first time only):
   ```bash
   npm run tauri build
   ```

## Usage

### Development Mode

Run the app in development mode with hot-reload:

```bash
npm run tauri dev
```

This will:
- Start Vite dev server (React frontend)
- Launch Tauri app with Rust backend
- Enable hot-reload for both frontend and backend changes

### Production Build

Build the production app:

```bash
npm run tauri build
```

The compiled app will be in:
- **macOS**: `src-tauri/target/release/bundle/dmg/`
- **Linux**: `src-tauri/target/release/bundle/deb/` or `appimage/`
- **Windows**: `src-tauri/target/release/bundle/msi/`

### Configuration

On first launch, configure the app:

1. **Backend URL**: Set the Topar backend API URL (e.g., `http://localhost:8090`)
2. **MongoDB URI**: For direct sync mode (optional, can use backend API instead)
3. **Rate Limits**: Default workers and requests/sec

Settings are stored in SQLite database at:
- **macOS**: `~/Library/Application Support/com.topar.desktop/`
- **Linux**: `~/.local/share/topar-desktop/`
- **Windows**: `C:\Users\<user>\AppData\Roaming\com.topar.desktop\`

## Workflow

### 1. Parse Products

1. Enter source URL (e.g., `https://example.com/products`)
2. Configure parsing options:
   - **Limit**: Max products to parse (0 = unlimited)
   - **Workers**: Concurrent requests (1-40, default 6)
   - **Rate**: Requests per second (0.2-20, default 1.2)
3. Click **Run Parsing**

The parser will:
- Discover URLs from sitemap (or crawl if needed)
- Parse all pages with pagination support
- Extract product fields automatically
- Store raw data in local SQLite database
- Show real-time progress

### 2. Review Parsed Data

After parsing completes:
- View all parsed products in table
- See detected fields and their values
- Search and filter products
- Check for parsing errors

### 3. Map Fields

1. The app auto-detects field mappings using aliases
2. Review and adjust mappings:
   - Select source field from dropdown
   - Or enter constant value
   - Map to either "All" (eksmo) or "Main" collections
3. Save mapping profile for reuse

### 4. Compare with Database

Before syncing, click **Compare with Database**:

The app will:
- Fetch existing products from MongoDB by GUID/nomcode/ISBN
- Compare field-by-field
- Categorize as: NEW, CHANGED, or UNCHANGED
- Show diff summary:
  - New products: `X`
  - Updated products: `Y` (shows which fields changed)
  - Unchanged products: `Z` (will be skipped)

### 5. Sync to Database

1. Review comparison results
2. Check sync options:
   - [ ] Sync to All (eksmo_products)
   - [ ] Sync to Main (main_products)
3. Click **Sync to Database**

The app will:
- Upload only NEW and CHANGED products
- Skip unchanged products
- Use batch upsert operations (250 products/batch)
- Show sync progress and results

### 6. View Logs

All operations are logged:
- Parsing progress and errors
- Field mapping changes
- Comparison results
- Sync statistics
- Error details

Logs can be exported or cleared.

## Key Features Detail

### Pagination Support

Unlike the web version which requires manual page handling, this desktop app:
- Automatically detects pagination links
- Follows "next page" links
- Stops when no more pages found
- Handles both numbered pages and infinite scroll

### Rate Limiting & Retries

Respects server rate limits:
- Configurable requests/sec throttling
- Automatic exponential backoff on 429 responses
- Max retry attempts configurable
- Shows retry count in UI

### Field Mapping Aliases

Auto-mapping supports common field name variations:

```typescript
barcode: ["barcode", "gtin", "gtin13", "gtin14", "sku", "isbn"]
nomcode: ["nomcode", "barcode", "gtin", "sku"]
isbn: ["isbn", "gtin", "gtin13"]
name: ["name", "title", "product_name"]
annotation: ["annotation", "description", "desc"]
coverUrl: ["cover", "image", "coverurl", "img_url"]
price: ["price", "offers.price", "product_price"]
```

### Local Database Schema

SQLite tables:

```sql
-- Parsing runs metadata
CREATE TABLE runs (
    id TEXT PRIMARY KEY,
    source_url TEXT NOT NULL,
    limit INTEGER,
    workers INTEGER,
    requests_per_sec REAL,
    discovered_urls INTEGER,
    parsed_products INTEGER,
    rate_limit_retries INTEGER,
    status TEXT,  -- 'running' | 'finished' | 'failed'
    error TEXT,
    detected_fields TEXT,  -- JSON array
    created_at DATETIME,
    finished_at DATETIME
);

-- Parsed product records
CREATE TABLE records (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES runs(id),
    source_url TEXT,
    data TEXT,  -- JSON blob with all fields
    created_at DATETIME
);

-- Field mapping profiles
CREATE TABLE mapping_profiles (
    id TEXT PRIMARY KEY,
    name TEXT,
    rules TEXT,  -- JSON blob
    created_at DATETIME,
    updated_at DATETIME
);

-- Sync history
CREATE TABLE sync_logs (
    id TEXT PRIMARY KEY,
    run_id TEXT REFERENCES runs(id),
    total_records INTEGER,
    new_count INTEGER,
    updated_count INTEGER,
    unchanged_count INTEGER,
    failed_count INTEGER,
    sync_to_eksmo BOOLEAN,
    sync_to_main BOOLEAN,
    started_at DATETIME,
    finished_at DATETIME,
    error TEXT
);

-- App configuration
CREATE TABLE config (
    key TEXT PRIMARY KEY,
    value TEXT
);
```

### Comparison Algorithm

Field-by-field comparison:

```
For each parsed product:
  1. Try to find existing product by:
     - sourceGuidNom (primary)
     - sourceGuid (fallback)
     - sourceNomcode (fallback)
     - isbn (last resort)

  2. If not found: Mark as NEW

  3. If found: Compare each mapped field:
     - If any field differs: Mark as CHANGED (record which fields)
     - If all fields match: Mark as UNCHANGED (skip sync)
```

### Sync Strategy

Incremental sync to minimize database writes:

1. **New products**: Full insert
2. **Changed products**: Upsert with updated fields only
3. **Unchanged products**: Skip entirely (no database operation)

Batch operations:
- Process 250 products per batch
- Show progress per batch
- Continue on partial failures

## Troubleshooting

### Python parser fails to start

```bash
# Verify Python installation
python3 --version

# Reinstall dependencies
cd src-tauri/parser_engine
python3 -m pip install --upgrade -r requirements.txt
playwright install chromium
```

### Playwright browser not found

```bash
# Install Chromium browser
cd src-tauri/parser_engine
playwright install chromium

# Or use system Chrome
playwright install chrome
```

### Rust build fails

```bash
# Update Rust
rustup update

# Clean and rebuild
cd src-tauri
cargo clean
cargo build
```

### Port already in use

If dev server fails to start:
- Check if port 1420 is in use
- Or edit `vite.config.ts` to use different port

### Database locked

If SQLite database is locked:
- Close any other instances of the app
- Delete `.db-wal` and `.db-shm` files if present
- Restart the app

## Architecture Decisions

### Why SQLite over PostgreSQL/MySQL?

- **Zero configuration**: No database server to install/configure
- **Embedded**: Single file, portable
- **Fast**: Excellent for local desktop apps
- **Reliable**: ACID compliant, production-grade
- **Small**: ~1MB library

### Why Playwright over Puppeteer?

- **Better API**: More modern, cleaner
- **Multi-browser**: Supports Chromium, Firefox, WebKit
- **Better waits**: Smarter wait mechanisms
- **Auto-wait**: Waits for elements automatically
- **Maintained**: Active development by Microsoft

### Why React over Vue (like dashboard)?

- **Consistency**: Both web and desktop UIs similar workflow
- **TypeScript**: Better type safety with Tauri commands
- **Ecosystem**: More Tauri examples use React
- **Developer preference**: More common in desktop apps

### Sync Strategy: API vs Direct MongoDB

This app uses **Backend API** for sync by default:

**Pros**:
- Reuses existing business logic
- Maintains data integrity rules
- Handles authentication/authorization
- No need to bundle MongoDB driver
- Works with remote backends

**Alternative**: Direct MongoDB connection available for offline mode:
- Requires MongoDB URI configuration
- Useful for fully offline workflows
- Must replicate business logic

## Performance Considerations

### Memory Usage

- **Tauri**: ~50-100MB base memory
- **SQLite**: ~10MB for 100k products
- **Python parser**: ~150MB per worker
- **Total**: ~500MB with 6 workers (vs 1GB+ with Electron)

### Parsing Speed

With default settings (6 workers, 1.2 req/sec):
- **~7 products/second**
- **~420 products/minute**
- **~25k products/hour**

Increase workers and rate for faster parsing:
- 20 workers at 10 req/sec: ~200 products/sec
- Respect target site's rate limits!

### Database Performance

SQLite can handle:
- **Millions of products** without issues
- **Sub-millisecond queries** with indexes
- **Thousands of inserts/sec** in batch mode

## Contributing

This desktop app is isolated in the `topar-desktop/` folder and doesn't affect existing Topar code.

To modify:
1. Frontend (React): Edit files in `src/`
2. Backend (Rust): Edit files in `src-tauri/src/`
3. Parser (Python): Edit files in `src-tauri/parser_engine/`

Hot-reload works in dev mode for all three layers.

## License

Same license as main Topar project.

## Support

For issues or questions:
1. Check this README
2. Check Tauri docs: https://tauri.app/
3. Check Playwright docs: https://playwright.dev/python/
4. Open an issue in the repository
