use crate::cache::Cache;
use crate::connection::{ConnectionDefinition, ConnectionRuntime};
use crate::database::{Database, DatabaseValue};
use crate::definition::CacheDefinition;
use crate::error::{CacheError, ErrorCode, Result};
use std::fs;
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const QUERY_COUNT: usize = 1_000;
const DRAIN_WAIT: Duration = Duration::from_secs(10);

pub fn run(root: &Path) -> Result<()> {
    let connection_config = std::env::var("AB_CACHE_CONNECTION")
        .unwrap_or_else(|_| "config/connection.postgresql.json".to_owned());
    let cache_config = std::env::var("AB_CACHE_CONFIG")
        .unwrap_or_else(|_| "config/cache.user-purpose.json".to_owned());
    let connection_json = read(root, &connection_config)?;
    let cache_json = read(root, &cache_config)?;
    let connection_definition = ConnectionDefinition::from_json(&connection_json)?;
    let base_cache_definition = CacheDefinition::from_json(&cache_json)?;
    let queue_directory = root.join("test/data/queue");
    fs::create_dir_all(&queue_directory).map_err(|error| {
        CacheError::new(
            ErrorCode::QueueDirectoryInvalid,
            format!("failed to create Queue directory: {error}"),
        )
    })?;
    let connection = Arc::new(Database::open(&connection_definition, &queue_directory)?);
    let keys = (1..=QUERY_COUNT)
        .map(|number| format!("user{number:05}"))
        .collect::<Vec<_>>();
    let mut caches = Vec::with_capacity(20);

    println!("PURPOSE TEST - ON_DEMAND cold load and retention drain");
    println!("  initial preload     : disabled");
    println!("  queries per case    : {QUERY_COUNT}");
    println!("  retention           : TIMESTAMP / 10 seconds");

    for threads in 1..=20 {
        let mut definition = base_cache_definition.clone();
        definition.cache_id = format!("user_purpose_cache_{threads}");
        let cache = Arc::new(Cache::connected(definition, Arc::clone(&connection))?);
        let initial = cache.status_data()?;
        if initial.record_count != 0 {
            return Err(CacheError::new(
                ErrorCode::InvalidStateTransition,
                "ON_DEMAND Cache did not start empty",
            ));
        }

        let metrics = measure_load(
            &cache,
            &connection,
            &base_cache_definition.select.query,
            &keys,
            threads,
        )?;
        let status = cache.status_data()?;
        if status.record_count != QUERY_COUNT || metrics.matched_records != QUERY_COUNT {
            return Err(CacheError::new(
                ErrorCode::RecordDecodingFailed,
                format!(
                    "{threads} Thread Case expected {QUERY_COUNT} records, loaded {} and matched {}",
                    status.record_count, metrics.matched_records
                ),
            ));
        }
        print_case(threads, &metrics, status.memory_bytes);
        caches.push(cache);
    }

    println!("DRAIN TEST");
    println!("  caches              : {}", caches.len());
    println!("  records before wait : {}", QUERY_COUNT * caches.len());
    println!("  wait                : 10 seconds");
    thread::sleep(DRAIN_WAIT);

    let mut removed = 0usize;
    for cache in &caches {
        removed = removed.checked_add(cache.drain(0)?).ok_or_else(|| {
            CacheError::new(ErrorCode::InternalError, "drained record count overflow")
        })?;
    }
    let remaining = caches.iter().try_fold(0usize, |total, cache| {
        total
            .checked_add(cache.status_data()?.record_count)
            .ok_or_else(|| CacheError::new(ErrorCode::InternalError, "record count overflow"))
    })?;
    let expected = QUERY_COUNT * caches.len();
    if removed != expected || remaining != 0 {
        return Err(CacheError::new(
            ErrorCode::InvalidStateTransition,
            format!("drain expected {expected} removals, removed {removed}, remaining {remaining}"),
        ));
    }
    println!("  removed records     : {removed}");
    println!("  remaining records   : {remaining}");
    println!("  result              : PASS");
    Ok(())
}

struct LoadMetrics {
    wall_elapsed: Duration,
    database_samples: Vec<Duration>,
    cache_samples: Vec<Duration>,
    matched_records: usize,
}

impl LoadMetrics {
    fn cache_average(&self) -> Duration {
        average(&self.cache_samples)
    }
}

fn measure_load(
    cache: &Arc<Cache>,
    connection: &Arc<ConnectionRuntime>,
    select_query: &str,
    keys: &[String],
    threads: usize,
) -> Result<LoadMetrics> {
    let start = Arc::new(Barrier::new(threads + 1));
    let (wall_elapsed, results) = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for thread_index in 0..threads {
            let cache = Arc::clone(cache);
            let connection = Arc::clone(connection);
            let start = Arc::clone(&start);
            let begin = QUERY_COUNT * thread_index / threads;
            let end = QUERY_COUNT * (thread_index + 1) / threads;
            let thread_keys = &keys[begin..end];
            handles.push(scope.spawn(move || {
                let mut database_samples = Vec::with_capacity(thread_keys.len());
                let mut cache_samples = Vec::with_capacity(thread_keys.len());
                let mut matched = 0usize;
                start.wait();
                for key in thread_keys {
                    let value = DatabaseValue::from(key.clone());
                    let database_started = Instant::now();
                    let records = connection.select(select_query, std::slice::from_ref(&value))?;
                    database_samples.push(database_started.elapsed());

                    let cache_started = Instant::now();
                    let loaded = cache.benchmark_load_records(records)?;
                    cache_samples.push(cache_started.elapsed());
                    matched += loaded;
                }
                Ok::<_, CacheError>((database_samples, cache_samples, matched))
            }));
        }
        let wall_started = Instant::now();
        start.wait();
        let results = handles
            .into_iter()
            .map(|handle| {
                handle.join().map_err(|_| {
                    CacheError::new(ErrorCode::InternalError, "ON_DEMAND query Thread panicked")
                })?
            })
            .collect::<Result<Vec<_>>>()?;
        Ok::<_, CacheError>((wall_started.elapsed(), results))
    })?;

    let mut database_samples = Vec::with_capacity(QUERY_COUNT);
    let mut cache_samples = Vec::with_capacity(QUERY_COUNT);
    let mut matched_records = 0usize;
    for (mut thread_database, mut thread_cache, matched) in results {
        database_samples.append(&mut thread_database);
        cache_samples.append(&mut thread_cache);
        matched_records += matched;
    }
    if database_samples.len() != QUERY_COUNT || cache_samples.len() != QUERY_COUNT {
        return Err(CacheError::new(
            ErrorCode::InternalError,
            "ON_DEMAND test did not execute exactly 1,000 queries",
        ));
    }
    Ok(LoadMetrics {
        wall_elapsed,
        database_samples,
        cache_samples,
        matched_records,
    })
}

fn print_case(threads: usize, metrics: &LoadMetrics, memory_bytes: usize) {
    println!("CASE {threads} - ON_DEMAND load ({threads} Thread)");
    println!("  initial records     : 0");
    println!("  total records       : {}", metrics.cache_samples.len());
    println!("  loaded records      : {}", metrics.matched_records);
    println!(
        "  pipeline wall       : {}",
        milliseconds(metrics.wall_elapsed)
    );
    println!(
        "  DB fetch average    : {}/record",
        milliseconds(average(&metrics.database_samples))
    );
    println!(
        "  Cache load average  : {}/record",
        milliseconds(metrics.cache_average())
    );
    println!(
        "  Cache load minimum  : {}/record",
        milliseconds(minimum(&metrics.cache_samples))
    );
    println!(
        "  Cache load maximum  : {}/record",
        milliseconds(maximum(&metrics.cache_samples))
    );
    println!("  cache memory        : {memory_bytes} bytes");
}

fn average(samples: &[Duration]) -> Duration {
    samples.iter().copied().sum::<Duration>() / samples.len() as u32
}

fn minimum(samples: &[Duration]) -> Duration {
    samples.iter().copied().min().unwrap_or_default()
}

fn maximum(samples: &[Duration]) -> Duration {
    samples.iter().copied().max().unwrap_or_default()
}

fn milliseconds(value: Duration) -> String {
    format!("{:.6} ms", value.as_secs_f64() * 1_000.0)
}

fn read(root: &Path, relative: &str) -> Result<String> {
    fs::read_to_string(root.join(relative)).map_err(|error| {
        CacheError::new(
            ErrorCode::InvalidArgument,
            format!("failed to read {relative}: {error}"),
        )
    })
}
