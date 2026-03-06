pub mod operations;
pub mod schema;

use anyhow::Result;
use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;

/// Thread-safe database connection wrapper
#[derive(Clone)]
pub struct Database {
    conn: Arc<Mutex<Connection>>,
}

impl Database {
    /// Initialize database connection and create tables if needed
    pub fn new(db_path: PathBuf) -> Result<Self> {
        let conn = Connection::open(db_path)?;

        // Enable foreign keys
        conn.pragma_update(None, "foreign_keys", "ON")?;

        // Enable WAL mode for better concurrent performance
        conn.pragma_update(None, "journal_mode", "WAL")?;

        let db = Database {
            conn: Arc::new(Mutex::new(conn)),
        };

        // Create all tables
        schema::create_tables(&db)?;

        Ok(db)
    }

    /// Get a locked connection for executing queries
    pub fn with_conn<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&mut Connection) -> Result<T>,
    {
        let mut conn = self.conn.lock();
        f(&mut conn)
    }
}
