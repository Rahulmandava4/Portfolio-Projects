use crate::ingest::temp_db::{ARTICLES_TABLE, TempArticle, TempDb};
use arrow::array::{Int64Builder, RecordBatch, StringBuilder, TimestampSecondBuilder};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use bincode::Options;
use compact_str::CompactString;
use dashmap::DashMap;
use foldhash::{HashMap, HashSet};
use jiff::Timestamp;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use pbr::ProgressBar;
use rayon::prelude::*;
use redb::ReadableTableMetadata;
use std::cell::{Cell, RefCell};
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Instant;
use zstd::bulk::Decompressor;

type ArticleIdTable = DashMap<CompactString, i64, foldhash::fast::RandomState>;

pub fn postprocess_to_parquet(temp_db: &TempDb) -> anyhow::Result<()> {
    let start = Instant::now();
    let batch_size = 16384;

    let articles_file = File::create("data/articles.parquet")?;
    let links_file = File::create("data/links.parquet")?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
        .build();

    let mut articles_writer = ArrowWriter::try_new(
        articles_file,
        SchemaRef::new(articles_schema()),
        Some(props.clone()),
    )?;
    let mut links_writer = ArrowWriter::try_new(
        links_file,
        SchemaRef::new(links_schema()),
        Some(props.clone()),
    )?;

    let (data_tx, data_rx) = flume::bounded(4);

    for i in 0..32 {
        thread::spawn({
            let temp_db = temp_db.clone();
            let data_tx = data_tx.clone();
            move || {
                let tx = temp_db.db().begin_read().unwrap();
                let articles_table = tx.open_table(ARTICLES_TABLE).unwrap();

                let start = u8::MAX / 32 * i;
                let mut iter = if i == 31 {
                    articles_table.range((&[start][..])..).unwrap()
                } else {
                    let end = u8::MAX / 32 * (i + 1);
                    articles_table.range((&[start][..])..(&[end][..])).unwrap()
                };

                loop {
                    let mut done = false;
                    let mut batch = Vec::with_capacity(batch_size);
                    while batch.len() < batch_size {
                        let Some(next) = iter.next() else {
                            done = true;
                            break;
                        };
                        batch.push(next.unwrap().1.value().to_vec());
                    }

                    data_tx.send(batch).unwrap();

                    if done {
                        break;
                    }
                }
            }
        });
    }

    drop(data_tx);

    let article_id_table: ArticleIdTable = DashMap::default();
    let next_article_id = AtomicU64::new(0);
    let num_articles = Arc::new(AtomicU64::new(0));
    let num_links = Arc::new(AtomicU64::new(0));

    let mut pbr = ProgressBar::new(
        temp_db
            .db()
            .begin_read()?
            .open_table(ARTICLES_TABLE)?
            .len()?,
    );

    let (delta_encoded_batch_tx, delta_encoded_batch_rx) =
        flume::bounded::<Vec<DeltaEncodedArticle>>(2);
    let writer_thread = thread::spawn({
        let num_articles = num_articles.clone();
        let num_links = num_links.clone();
        move || {
            for delta_encoded in delta_encoded_batch_rx {
                let mut article_ids = Int64Builder::with_capacity(delta_encoded.len());
                let mut article_titles =
                    StringBuilder::with_capacity(delta_encoded.len(), delta_encoded.len() * 32);

                let mut link_src_articles = Int64Builder::with_capacity(delta_encoded.len());
                let mut link_dst_articles = Int64Builder::with_capacity(delta_encoded.len());
                let mut link_created_ats =
                    TimestampSecondBuilder::with_capacity(delta_encoded.len());
                let mut link_deleted_ats =
                    TimestampSecondBuilder::with_capacity(delta_encoded.len());
                let mut link_created_by_users = Int64Builder::with_capacity(delta_encoded.len());
                let mut link_deleted_by_users = Int64Builder::with_capacity(delta_encoded.len());

                for delta_encoded in delta_encoded {
                    article_ids.append_value(delta_encoded.id);
                    article_titles.append_value(delta_encoded.title.to_string());
                    num_articles.fetch_add(1, Ordering::Relaxed);
                    num_links.fetch_add(delta_encoded.links.len() as u64, Ordering::Relaxed);

                    for link in delta_encoded.links {
                        link_src_articles.append_value(delta_encoded.id);
                        link_dst_articles.append_value(link.dst_article);
                        link_created_ats.append_value(link.created_at.as_second());
                        link_created_by_users.append_value(link.created_by_user);
                        match link.removed_at {
                            Some(r) => link_deleted_ats.append_value(r.as_second()),
                            None => link_deleted_ats.append_null(),
                        }
                        match link.deleted_by_user {
                            Some(u) => link_deleted_by_users.append_value(u),
                            None => link_deleted_by_users.append_null(),
                        }
                    }
                }

                let article_batch = RecordBatch::try_new(
                    SchemaRef::new(articles_schema()),
                    vec![
                        Arc::new(article_ids.finish()),
                        Arc::new(article_titles.finish()),
                    ],
                )
                .unwrap();
                let links_batch = RecordBatch::try_new(
                    SchemaRef::new(links_schema()),
                    vec![
                        Arc::new(link_src_articles.finish()),
                        Arc::new(link_dst_articles.finish()),
                        Arc::new(link_created_ats.finish()),
                        Arc::new(link_created_by_users.finish()),
                        Arc::new(link_deleted_ats.finish()),
                        Arc::new(link_deleted_by_users.finish()),
                    ],
                )
                .unwrap();
                articles_writer.write(&article_batch).unwrap();
                links_writer.write(&links_batch).unwrap();
            }

            articles_writer.close().unwrap();
            links_writer.close().unwrap();
        }
    });

    for batch_data in data_rx {
        let delta_encoded = batch_data.par_iter()
            .filter_map(|data| {
                thread_local! {
                    static DECOMPRESSOR: RefCell<Decompressor<'static>> = RefCell::new(Decompressor::new().unwrap());
                }
                thread_local! {
                    static DECOMPRESS_BUF: Cell<Vec<u8>> = Cell::new(Vec::new());
                }
                let mut uncompressed_data = DECOMPRESS_BUF.with(|c| c.take());
                DECOMPRESSOR.with(|d| {
                    let size = Decompressor::upper_bound(&data).unwrap();
                    uncompressed_data.reserve(size);
                    d.borrow_mut().decompress_to_buffer(&data, &mut uncompressed_data).unwrap();
                });
                let article: TempArticle = bincode::options().deserialize(&uncompressed_data).unwrap();
                uncompressed_data.clear();
                uncompressed_data.shrink_to(256 * 1024 * 1024);

                if !article.title.is_empty() && article.title.chars().next().unwrap().is_ascii_alphanumeric() {
                DECOMPRESS_BUF.with(move |c| c.set(uncompressed_data));
                Some(delta_encode(article, &article_id_table, &next_article_id))
                } else { None }
            }).collect::<Vec<_>>();
        delta_encoded_batch_tx.send(delta_encoded).unwrap();
        pbr.add(batch_data.len() as u64);
    }

    drop(delta_encoded_batch_tx);
    writer_thread.join().unwrap();

    tracing::info!(
        "{}M articles, {}M links",
        num_articles.load(Ordering::Relaxed) / 1_000_000,
        num_links.load(Ordering::Relaxed) / 1_000_000
    );
    tracing::info!("finished in {:.2?}", start.elapsed());

    Ok(())
}

fn articles_schema() -> Schema {
    Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("title", DataType::Utf8, false),
    ])
}

fn links_schema() -> Schema {
    Schema::new(vec![
        Field::new("src_article", DataType::Int64, false),
        Field::new("dst_article", DataType::Int64, false),
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new("created_by_user", DataType::Int64, false),
        Field::new(
            "removed_at",
            DataType::Timestamp(TimeUnit::Second, None),
            true,
        ),
        Field::new("removed_by_user", DataType::Int64, true),
    ])
}

struct DeltaEncodedArticle {
    title: CompactString,
    id: i64,
    links: Vec<DeltaEncodedLink>,
}

struct DeltaEncodedLink {
    dst_article: i64,
    created_by_user: i64,
    deleted_by_user: Option<i64>,
    created_at: Timestamp,
    removed_at: Option<Timestamp>,
}

#[derive(Default)]
struct CachedStructures {
    link_indexes: HashMap<i64, usize>,
    current_links: HashSet<i64>,
    this_revision_links: HashSet<i64>,
}

fn delta_encode(
    mut article: TempArticle,
    id_table: &ArticleIdTable,
    next_id: &AtomicU64,
) -> DeltaEncodedArticle {
    thread_local! {
        static CACHED_STRUCTURES: Cell<CachedStructures> = Cell::new(CachedStructures::default());
    }

    let CachedStructures {
        mut link_indexes,
        mut current_links,
        mut this_revision_links,
    } = CACHED_STRUCTURES.with(|c| c.take());

    let title = normalize_title(&article.title);
    let id = *id_table
        .entry(title.clone())
        .or_insert_with(|| next_id.fetch_add(1, Ordering::Relaxed) as i64);

    let mut links = Vec::<DeltaEncodedLink>::new();

    article.revisions.sort_unstable_by_key(|rev| rev.timestamp);
    for revision in article.revisions {
        for link in &revision.links {
            let dst_id = *id_table
                .entry(normalize_title(link))
                .or_insert_with(|| next_id.fetch_add(1, Ordering::Relaxed) as i64);
            if current_links.insert(dst_id) {
                links.push(DeltaEncodedLink {
                    dst_article: dst_id,
                    created_at: revision.timestamp,
                    removed_at: None,
                    created_by_user: revision.user_id,
                    deleted_by_user: None,
                });
                link_indexes.insert(dst_id, links.len() - 1);
            }
            this_revision_links.insert(dst_id);
        }

        current_links.retain(|&dst_id| {
            if !this_revision_links.contains(&dst_id) {
                links[link_indexes[&dst_id]].removed_at = Some(revision.timestamp);
                links[link_indexes[&dst_id]].deleted_by_user = Some(revision.user_id);
                false
            } else {
                true
            }
        });
        this_revision_links.clear();
    }

    link_indexes.clear();
    link_indexes.shrink_to(16384);
    current_links.clear();
    current_links.shrink_to(4096);
    this_revision_links.clear();
    this_revision_links.shrink_to(4096);
    CACHED_STRUCTURES.with(move |c| {
        c.set(CachedStructures {
            link_indexes,
            current_links,
            this_revision_links,
        })
    });

    if links.len() > 1_000_000 {
        dbg!(&article.title);
    }

    DeltaEncodedArticle { title, id, links }
}

fn normalize_title(title: &str) -> CompactString {
    CompactString::from_str_to_lowercase(title.trim())
}
