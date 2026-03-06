# Topar Desktop - Quick Setup Guide

This guide will help you get the Topar Desktop Parser up and running in under 10 minutes.

## Prerequisites Installation

### 1. Node.js (Required)

**macOS** (using Homebrew):
```bash
brew install node
```

**macOS/Linux/Windows** (using official installer):
Download from [nodejs.org](https://nodejs.org/) - Use LTS version (18+)

Verify installation:
```bash
node --version  # Should show v18.0.0 or higher
npm --version   # Should show 9.0.0 or higher
```

### 2. Rust (Required)

**All platforms**:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

After installation, restart your terminal and verify:
```bash
rustc --version  # Should show 1.70.0 or higher
cargo --version  # Should show 1.70.0 or higher
```

### 3. Python 3.11+ (Required)

**macOS**:
```bash
brew install python@3.11
```

**Linux (Ubuntu/Debian)**:
```bash
sudo apt update
sudo apt install python3.11 python3-pip
```

**Windows**:
Download from [python.org](https://www.python.org/downloads/)

Verify:
```bash
python3 --version  # Should show 3.11.0 or higher
```

### 4. System Dependencies

**macOS**:
```bash
xcode-select --install
```

**Linux (Ubuntu/Debian)**:
```bash
sudo apt update
sudo apt install libwebkit2gtk-4.0-dev build-essential curl wget \
  libssl-dev libgtk-3-dev libayatana-appindicator3-dev librsvg2-dev
```

**Windows**:
- Install [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)
- Install [WebView2](https://developer.microsoft.com/microsoft-edge/webview2/)

## Project Setup

### Step 1: Navigate to Project

```bash
cd /path/to/topar/topar-desktop
```

### Step 2: Install Node Dependencies

```bash
npm install
```

This installs:
- React + TypeScript
- Tauri
- Zustand (state management)
- Other frontend dependencies

Expected time: 2-3 minutes

### Step 3: Install Python Dependencies

```bash
cd src-tauri/parser_engine
python3 -m pip install -r requirements.txt
```

This installs:
- Playwright (browser automation)
- BeautifulSoup4 (HTML parsing)
- lxml, requests, etc.

Expected time: 1-2 minutes

### Step 4: Install Playwright Browser

```bash
playwright install chromium
```

This downloads the Chromium browser for headless parsing.

Expected time: 1 minute (downloads ~170MB)

### Step 5: Return to Project Root

```bash
cd ../..  # Back to topar-desktop/
```

## Running the App

### Development Mode

Run the app with hot-reload enabled:

```bash
npm run tauri:dev
```

This will:
1. Start the Vite dev server (React frontend)
2. Compile the Rust backend
3. Launch the desktop app

**First run**: Takes 5-10 minutes to compile Rust dependencies
**Subsequent runs**: Takes 10-30 seconds

The app window should open automatically.

### Building for Production

To create a standalone distributable app:

```bash
npm run tauri:build
```

Expected time: 5-10 minutes

The built app will be in `src-tauri/target/release/bundle/`:

- **macOS**: `dmg/Topar Desktop Parser.dmg`
- **Linux**: `deb/topar-desktop_1.0.0_amd64.deb` or `appimage/topar-desktop_1.0.0_amd64.AppImage`
- **Windows**: `msi/Topar Desktop Parser_1.0.0_x64_en-US.msi`

## First Launch Configuration

When you first launch the app:

1. **Configure Backend URL** (if syncing to remote):
   - The app stores this in: Settings → Backend URL
   - Default: `http://localhost:8090`
   - For remote backend: `https://your-domain.com/api`

2. **Test Parsing**:
   - Enter a test URL (e.g., sitemap or product page)
   - Set Limit to `10` for testing
   - Click "Run Parsing"

## Troubleshooting

### Issue: `python3: command not found`

**macOS/Linux**:
```bash
# Check if python3 is installed
which python3

# If not found, install it
brew install python@3.11  # macOS
sudo apt install python3  # Linux
```

**Windows**:
- Make sure Python is added to PATH during installation
- Or use full path: `C:\Python311\python.exe`

### Issue: `Playwright browser not found`

```bash
cd src-tauri/parser_engine
playwright install chromium
```

### Issue: Rust compilation errors

```bash
# Update Rust
rustup update

# Clean and rebuild
cd src-tauri
cargo clean
cargo build
```

### Issue: `error: linker 'cc' not found` (Linux)

```bash
sudo apt install build-essential
```

### Issue: Port 1420 already in use

Edit `vite.config.ts`:
```typescript
server: {
  port: 1421,  // Change to different port
  strictPort: true,
}
```

### Issue: SQLite database locked

- Close all instances of the app
- Delete temporary files:
  ```bash
  # macOS
  rm ~/Library/Application\ Support/com.topar.desktop/*.db-wal
  rm ~/Library/Application\ Support/com.topar.desktop/*.db-shm

  # Linux
  rm ~/.local/share/topar-desktop/*.db-wal
  rm ~/.local/share/topar-desktop/*.db-shm
  ```

### Issue: Parser fails with import errors

Make sure you're in the correct directory when installing Python packages:
```bash
cd topar-desktop/src-tauri/parser_engine
python3 -m pip install --upgrade -r requirements.txt
```

## Usage Workflow

### 1. Parse Products

1. Open the app
2. Go to "Parse" tab
3. Enter source URL (sitemap or product page)
4. Configure options:
   - **Limit**: 0 for unlimited, or specific number
   - **Workers**: 6 (default) - increase for faster parsing
   - **Requests/sec**: 1.2 (default) - adjust based on site's rate limits
5. Click "Run Parsing"
6. Monitor progress in real-time

### 2. Map Fields

1. After parsing completes, go to "Map Fields" tab
2. Review auto-detected field mappings
3. Adjust mappings as needed:
   - Select source field from dropdown
   - Or enter constant value
4. Map to both "All" (eksmo) and "Main" products
5. Optionally save mapping profile for reuse

### 3. Review Data

1. Go to "Review" tab
2. Browse parsed products in table format
3. Verify data quality
4. Check for missing or incorrect fields

### 4. Sync to Database

1. Go to "Sync" tab
2. Configure sync options:
   - ☑ Sync to All (eksmo_products)
   - ☑ Sync to Main (main_products)
   - ☑ Save mapping profile (optional)
3. Click "Compare with Database" to see what will change
4. Review comparison results:
   - New products
   - Changed products
   - Unchanged products (will be skipped)
5. Click "Sync to Database"
6. Wait for sync to complete
7. Review sync results

## Performance Tips

### Faster Parsing

- Increase workers: `10-20` (if target site allows)
- Increase rate limit: `5-10` requests/sec
- Use limit for testing: `10-100` products

### Memory Usage

- Default (6 workers): ~500MB RAM
- High (20 workers): ~1-2GB RAM
- For large datasets (100k+ products): Use batches with limit

### Disk Space

- SQLite database: ~10MB per 10k products
- Parsed data: ~100KB per 10k products
- Python/Playwright cache: ~500MB

## File Locations

### Application Data

**macOS**:
- Database: `~/Library/Application Support/com.topar.desktop/topar.db`
- Logs: `~/Library/Logs/com.topar.desktop/`

**Linux**:
- Database: `~/.local/share/topar-desktop/topar.db`
- Logs: `~/.local/share/topar-desktop/logs/`

**Windows**:
- Database: `C:\Users\<user>\AppData\Roaming\com.topar.desktop\topar.db`
- Logs: `C:\Users\<user>\AppData\Roaming\com.topar.desktop\logs\`

### Development Files

- Source code: `topar-desktop/src/` (React)
- Rust backend: `topar-desktop/src-tauri/src/`
- Python parser: `topar-desktop/src-tauri/parser_engine/`
- Database: Local app data directory (see above)

## Next Steps

1. **Configure Backend URL**: Set your Topar backend URL in the app
2. **Test with Small Dataset**: Parse 10-100 products first
3. **Review Mappings**: Ensure field mappings are correct
4. **Test Sync**: Sync a small batch to test integration
5. **Scale Up**: Once verified, parse larger datasets

## Getting Help

- Check [README.md](./README.md) for detailed documentation
- Review error messages in the app
- Check logs in application data directory
- Inspect SQLite database with: `sqlite3 topar.db`

## Uninstalling

### Remove Application

**macOS**:
- Drag app from Applications to Trash
- Or: `rm -rf /Applications/Topar\ Desktop\ Parser.app`

**Linux**:
```bash
sudo apt remove topar-desktop  # If installed via .deb
# Or delete AppImage file
```

**Windows**:
- Use Windows "Add or Remove Programs"
- Or run uninstaller from Start Menu

### Remove Application Data

**macOS**:
```bash
rm -rf ~/Library/Application\ Support/com.topar.desktop
rm -rf ~/Library/Logs/com.topar.desktop
```

**Linux**:
```bash
rm -rf ~/.local/share/topar-desktop
```

**Windows**:
```
rmdir /s %APPDATA%\com.topar.desktop
```

## Updating

To update to a new version:

1. Pull latest code:
   ```bash
   cd topar-desktop
   git pull
   ```

2. Update dependencies:
   ```bash
   npm install
   cd src-tauri/parser_engine
   python3 -m pip install --upgrade -r requirements.txt
   cd ../..
   ```

3. Rebuild:
   ```bash
   npm run tauri:build
   ```

Your data (SQLite database) will be preserved during updates.

---

**Need help?** Check the main [README.md](./README.md) or open an issue in the repository.
