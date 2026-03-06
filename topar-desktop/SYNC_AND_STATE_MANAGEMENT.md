# Sync and State Management

**Date**: March 6, 2026  
**Status**: ✅ **LOCAL SYNC WORKING**

## What's Fixed

### ✅ Local Database Sync

The sync functionality now works **completely locally** without requiring a remote backend:

1. **Compare Function** - Compares current run with previous runs
   - Uses local SQLite database
   - Compares by `source_url` as unique identifier
   - Shows: new, changed, and unchanged products

2. **Sync Function** - Marks records as synced
   - All data already in local database
   - Creates sync log for tracking
   - Saves mapping profiles if requested
   - No remote backend required

### How It Works

#### Compare with Previous Run

```
Current Run (Run A)
   ↓
Compare with most recent previous run (Run B)
   ↓
For each product:
   - If source_url exists in Run B:
     - Same data → "Unchanged"
     - Different data → "Changed"
   - If source_url NOT in Run B:
     - Mark as "New"
   ↓
Return counts: new, changed, unchanged
```

**Example**:
- Run 1: Parse 10 products → All marked as "new"
- Run 2: Parse same URL again → Compare with Run 1
  - 8 products unchanged
  - 2 products changed (price updated)
  - 0 new products

#### Sync to Database

Since everything is already in the local SQLite database:

1. **Get records** from current run
2. **Save mapping profile** (if requested)
3. **Create sync log** with:
   - Total records
   - All marked as "new" (local mode)
   - Success status
4. **Return success** response

All data is **already persisted** in:
```
~/Library/Application Support/com.topar.desktop/topar.db
```

## State Management

### Current State Flow

```
Parse Tab
   ↓
Run Parsing → Save to DB → Status: "running"
   ↓
Parsing Complete → Update Run → Status: "finished"
   ↓
Auto-switch to "Map Fields" tab
   ↓
User maps fields
   ↓
Go to "Review" tab → View products
   ↓
Go to "Sync" tab → Compare & Sync
   ↓
Need to parse again?
   → PROBLEM: No clear way to go back to Parse tab
```

### What's Needed: Clear State & Return to Parse

You need a way to:

1. **Clear current run state**
2. **Reset progress**
3. **Return to Parse tab**
4. **Start fresh parsing**

### Recommended Solution

Add a "New Parse" or "Clear & Parse Again" button that:

1. **Clears frontend state**:
   ```typescript
   // In parserStore.ts
   clearAndReset: () => {
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
   }
   ```

2. **Navigate to Parse tab**:
   ```typescript
   // In App.tsx or component
   const handleNewParse = () => {
     useParserStore.getState().clearAndReset()
     setActiveTab('parse')
   }
   ```

3. **Add button to UI** in Sync tab or header:
   ```tsx
   <button onClick={handleNewParse}>
     New Parse
   </button>
   ```

## Database Schema

### Tables Used for Sync

1. **runs** - Parsing run metadata
   - id, source_url, workers, status, etc.
   - Used for: Finding previous runs to compare

2. **records** - Individual product records
   - id, run_id, source_url, data, created_at
   - Used for: Storing parsed products, comparing changes

3. **mapping_profiles** - Saved field mappings
   - id, name, rules, created_at, updated_at
   - Used for: Saving/loading mapping configurations

4. **sync_logs** - Sync operation history
   - id, run_id, new_count, updated_count, started_at, etc.
   - Used for: Tracking sync operations and results

5. **config** - App configuration
   - key, value, updated_at
   - Used for: Backend URL (for future remote sync)

## Testing Sync Functionality

### Test Scenario 1: First Parse

1. **Parse URL** with limit=10
2. **Go to Sync tab**
3. **Click "Compare with Database"**
4. **Expected**: All 10 marked as "new"
5. **Click "Sync to Database"**
6. **Expected**: Success with 10 new records

### Test Scenario 2: Re-parse Same URL

1. **Clear state** (needs "New Parse" button)
2. **Parse same URL** again
3. **Go to Sync tab**
4. **Click "Compare"**
5. **Expected**: All 10 marked as "unchanged" (if data same)
6. **If prices changed**: Some marked as "changed"

### Test Scenario 3: Parse Different Products

1. **Parse different URL**
2. **Compare**
3. **Expected**: All marked as "new"

## UI Flow Improvements Needed

### Current Issues

1. ❌ **No way to start new parse** after finishing one
2. ❌ **State persists** between parses
3. ❌ **Can't go back to Parse tab** easily

### Recommended Fixes

#### Option 1: Add "New Parse" Button

**Location**: Top header (always visible)

```tsx
<header className="app-header">
  <h1>Topar Desktop Parser</h1>
  {currentRun && (
    <button onClick={clearAndStartNewParse} className="btn-secondary">
      New Parse
    </button>
  )}
</header>
```

#### Option 2: Add to Sync Tab

**Location**: After sync completes

```tsx
{syncComplete && (
  <div className="sync-complete">
    <h3>Sync Complete!</h3>
    <p>{totalRecords} records synced successfully</p>
    <button onClick={startNewParse} className="btn-primary">
      Parse More Products
    </button>
  </div>
)}
```

#### Option 3: Make Tabs Always Accessible

Remove auto-navigation and let users click Parse tab anytime:

```tsx
// Remove or modify this in App.tsx:
useEffect(() => {
  if (progress.status === 'finished' && activeTab === 'parse') {
    // Don't auto-switch, let user decide
    // setActiveTab('map')
  }
}, [progress.status, activeTab])
```

## Implementation Steps

### 1. Add Clear Function to Store

```typescript
// src/store/parserStore.ts
export const useParserStore = create<ParserState>((set, get) => ({
  // ... existing code ...
  
  clearAndReset: () => {
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
        error: undefined,
      },
    })
  },
}))
```

### 2. Add "New Parse" Button

```typescript
// src/App.tsx
function App() {
  const { currentRun, clearAndReset } = useParserStore()
  
  const handleNewParse = () => {
    if (confirm('Start a new parse? Current data will remain in database.')) {
      clearAndReset()
      setActiveTab('parse')
    }
  }
  
  return (
    <div className="app">
      <header className="app-header">
        <h1>Topar Desktop Parser</h1>
        {currentRun && (
          <button onClick={handleNewParse} className="btn-new-parse">
            🔄 New Parse
          </button>
        )}
      </header>
      {/* ... rest of app ... */}
    </div>
  )
}
```

### 3. Add CSS Styling

```css
/* src/styles.css */
.btn-new-parse {
  padding: 8px 16px;
  background: #4CAF50;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-weight: 500;
}

.btn-new-parse:hover {
  background: #45a049;
}
```

## Summary

### ✅ What Works Now

- **Local sync** - Compare and sync with local database
- **Progress tracking** - Real-time updates during parsing
- **Data persistence** - All data saved in SQLite
- **Mapping profiles** - Can save and load field mappings
- **Sync logs** - History of all sync operations

### ⏳ What's Needed

- **Clear state function** - Reset between parses
- **New Parse button** - Easy way to start over
- **Better navigation** - Go back to Parse tab anytime

### 🔮 Future Enhancements

- **Remote backend sync** - When backend is ready
- **Export functionality** - Export to CSV/JSON
- **Scheduling** - Auto-parse on schedule
- **Multi-source parsing** - Queue multiple URLs

---

**App Status**: Running (PID 11701)  
**Sync**: Working locally  
**Next Step**: Add "New Parse" button for better UX
