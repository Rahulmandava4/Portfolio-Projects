//! Ingestion of dump data into a temporary embedded
//! database provided by `fjall`.
//!
//! Aggressively parallelized and pipelined to speed up
//! ingestion of the huge dump dataset.

use crate::ingest::temp_db::TempDb;
use bytes::Bytes;
use reqwest::Client;
use sevenz_rust2::{Password, SevenZReader};
use std::io::{BufRead, BufReader, Cursor, Read};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};
use tokio::signal::ctrl_c;
use tokio::sync::Semaphore;
use tokio::{select, task};

mod index;
mod parser;
pub mod temp_db;

/// Maximum number of parallel HTTP requests to the dump
/// download mirror.
const MAX_HTTP_PARALLELISM: usize = 2;

const DOWNLOADED_CHUNK_BUFFER_SIZE: usize = 2;

const NUM_INGEST_WORKERS: usize = 32;

pub async fn ingest(temp_db: TempDb) -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(10))
        .build()?;

    tracing::info!("fetching chunk URLs from index");
    let mut chunk_urls = index::get_download_urls(&client).await?;
    chunk_urls.retain(|url| {
        if temp_db.has_downloaded_url(url).unwrap() {
            tracing::debug!("skipping already downloaded URL {url}");
            false
        } else {
            true
        }
    });

    if chunk_urls.is_empty() {
        tracing::info!("nothing to download");
        return Ok(());
    }

    tracing::info!("{} chunks left to ingest", chunk_urls.len());

    let (chunks_tx, chunks) = flume::bounded(DOWNLOADED_CHUNK_BUFFER_SIZE);
    let mut task_handles = Vec::new();
    task_handles.push(task::spawn(async move {
        if let Err(e) = run_chunk_downloader(chunk_urls, &client, chunks_tx).await {
            tracing::error!("chunk downloader failed: {e:?}");
        }
    }));

    let mut thread_handles = Vec::new();
    for _ in 0..NUM_INGEST_WORKERS {
        thread_handles.push(thread::spawn({
            let chunks = chunks.clone();
            let temp_db = temp_db.clone();
            move || {
                if let Err(e) = run_ingest_worker(&chunks, &temp_db) {
                    tracing::error!("ingest worker failed: {e:?}");
                }
            }
        }));
    }

    let ctrl_c = ctrl_c();

    let completion = async move {
        for task in task_handles {
            task.await.unwrap();
        }
        for thread in thread_handles {
            tokio::task::spawn_blocking(move || {
                thread.join().unwrap();
            })
            .await
            .unwrap();
        }
    };

    select! {
        _ = ctrl_c => {
            tracing::info!("SIGINT, exiting");
        },
        _ = completion => {
            tracing::info!("done");
        },
    }

    let bytes_read = temp_db.bytes_read()?;
    tracing::info!(
        "data processed so far: {:.2} TiB",
        bytes_read as f64 / 1024f64.powi(4)
    );

    temp_db.close()?;
    Ok(())
}

struct DownloadedChunk {
    archive_bytes: Bytes,
    url: String,
}

async fn run_chunk_downloader(
    chunk_urls: Vec<String>,
    client: &Client,
    sender: flume::Sender<DownloadedChunk>,
) -> anyhow::Result<()> {
    let semaphore = Arc::new(Semaphore::new(MAX_HTTP_PARALLELISM));

    for chunk_url in chunk_urls {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let task_handle = task::spawn({
            let client = client.clone();
            let sender = sender.clone();
            async move {
                tracing::info!("downloading {chunk_url}");
                let bytes = client.get(&chunk_url).send().await?.bytes().await?;
                sender
                    .send_async(DownloadedChunk {
                        archive_bytes: bytes,
                        url: chunk_url,
                    })
                    .await
                    .ok();

                // ensure permit is only dropped after the task finishes
                drop(permit);

                Result::<_, anyhow::Error>::Ok(())
            }
        });
        task::spawn(async move {
            if let Err(e) = task_handle.await.unwrap() {
                tracing::error!("failed to download a chunk: {e:?}");
            }
        });
    }

    Ok(())
}

fn run_ingest_worker(
    chunks: &flume::Receiver<DownloadedChunk>,
    temp_db: &TempDb,
) -> anyhow::Result<()> {
    for chunk in chunks {
        tracing::info!("ingesting chunk {}", chunk.url);
        let mut reader = Cursor::new(chunk.archive_bytes.as_ref());
        let mut archive = SevenZReader::new(&mut reader, Password::empty())?;
        if archive.archive().files.len() != 1 {
            tracing::warn!("more than one file");
        }

        archive.for_each_entries(|_, reader| {
            let mut bytes_read = 0;
            let xml = quick_xml::Reader::from_reader(BufReader::new(TrackingReader {
                reader,
                bytes_read: &mut bytes_read,
            }));

            let mut batch = Vec::new();
            parser::parse(xml, |article| {
                // skip special articles
                if !article.title.contains(':') {
                    batch.push(article);
                    if batch.len() >= 4096 {
                        temp_db.insert_article_batch(batch.drain(..))?;
                    }
                }
                Ok(())
            })
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            if !batch.is_empty() {
                temp_db
                    .insert_article_batch(batch.drain(..))
                    .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            }

            temp_db
                .mark_url_downloaded(&chunk.url, bytes_read)
                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
            tracing::info!("ingested chunk {}", chunk.url);
            Ok(false)
        })?;
    }
    Ok(())
}

struct TrackingReader<'a, R> {
    reader: R,
    bytes_read: &'a mut u64,
}

impl<R: Read> Read for TrackingReader<'_, R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.reader.read(buf).map(|n| {
            *self.bytes_read += n as u64;
            n
        })
    }
}

impl<R: BufRead> BufRead for TrackingReader<'_, R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        self.reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.reader.consume(amt);
        *self.bytes_read += amt as u64;
    }
}
