use anyhow::Context;
use bincode::Options;
use compact_str::CompactString;
use flume::{Receiver, Sender};
use jiff::Timestamp;
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{cell::RefCell, fs, mem, sync::Arc};

pub const ARTICLES_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("articles");
const METADATA_TABLE: TableDefinition<&str, u64> = TableDefinition::new("metadata");
const DOWNLOADED_URLS_TABLE: TableDefinition<&str, ()> = TableDefinition::new("downloaded_urls");

/// Temporary database written to during ingestion, which
/// does not contain resolved IDs and links.
#[derive(Clone)]
pub struct TempDb {
    db: Arc<Database>,
    _dropper: Arc<Dropper>,
}

impl TempDb {
    pub fn open() -> anyhow::Result<(Self, Receiver<()>)> {
        fs::create_dir_all("data").ok();
        let db = Arc::new(
            Database::builder()
                .set_cache_size(1024 * 1024 * 1024)
                .set_repair_callback(|s| {
                    tracing::warn!("repair progress: {:.2}", s.progress());
                })
                .create("data/temp-db")?,
        );
        let (on_shutdown, on_shutown_rx) = flume::bounded(1);
        Ok((
            Self {
                db,
                _dropper: Arc::new(Dropper(on_shutdown)),
            },
            on_shutown_rx,
        ))
    }

    pub fn insert_article_batch(
        &self,
        articles: impl IntoIterator<Item = TempArticle>,
    ) -> anyhow::Result<()> {
        let mut kv_pairs = Vec::new();
        let mut uncompressed_buf = Vec::new();
        let mut compressed_buf = Vec::new();
        for article in articles {
            bincode::options().serialize_into(&mut uncompressed_buf, &article)?;

            thread_local! {
                static COMPRESSOR: RefCell<zstd::bulk::Compressor<'static>> = RefCell::new(zstd::bulk::Compressor::new(3).unwrap());
            }
            compressed_buf.reserve(uncompressed_buf.len() * 2);
            COMPRESSOR.with(|cell| {
                cell.borrow_mut()
                    .compress_to_buffer(&uncompressed_buf, &mut compressed_buf)
                    .unwrap();
            });

            kv_pairs.push((article.title.clone(), mem::take(&mut compressed_buf)));

            uncompressed_buf.clear();
            compressed_buf.clear();
        }

        let mut tx = self.db.begin_write()?;
        tx.set_durability(redb::Durability::Eventual);

        let mut articles_table = tx.open_table(ARTICLES_TABLE)?;

        for (title, data) in kv_pairs {
            articles_table
                .insert(title.as_bytes(), data.as_slice())
                .with_context(|| format!("failed to insert article {}", title))?;
        }
        drop(articles_table);
        tx.commit()?;
        Ok(())
    }

    pub fn mark_url_downloaded(&self, url: &str, bytes_read: u64) -> anyhow::Result<()> {
        let tx = self.db.begin_write()?;
        tx.open_table(DOWNLOADED_URLS_TABLE)?.insert(url, ())?;
        let old_bytes_read = tx
            .open_table(METADATA_TABLE)?
            .get("bytes_read")?
            .map(|val| val.value())
            .unwrap_or(0);
        let bytes_read = bytes_read + old_bytes_read;
        tx.open_table(METADATA_TABLE)?
            .insert("bytes_read", bytes_read)?;

        tx.commit()?;
        Ok(())
    }

    pub fn bytes_read(&self) -> anyhow::Result<u64> {
        match self
            .db
            .begin_read()?
            .open_table(METADATA_TABLE)?
            .get("bytes_read")?
        {
            Some(s) => Ok(s.value()),
            None => Ok(0),
        }
    }

    pub fn has_downloaded_url(&self, url: &str) -> anyhow::Result<bool> {
        if self
            .db
            .begin_read()?
            .open_table(DOWNLOADED_URLS_TABLE)
            .is_err()
        {
            return Ok(false);
        }
        Ok(self
            .db
            .begin_read()?
            .open_table(DOWNLOADED_URLS_TABLE)?
            .get(url)?
            .is_some())
    }

    pub fn db(&self) -> &Database {
        &self.db
    }

    pub fn close(self) -> anyhow::Result<()> {
        Ok(())
    }
}

#[derive(Clone)]
struct Dropper(Sender<()>);

impl Drop for Dropper {
    fn drop(&mut self) {
        self.0.send(()).ok();
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TempArticle {
    pub title: CompactString,
    pub revisions: Vec<TempArticleRevision>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TempArticleRevision {
    pub user_id: i64,
    pub timestamp: Timestamp,
    /// Links stored as article titles. Later resolved
    /// to IDs after all articles are ingested.
    pub links: Vec<CompactString>,
}
