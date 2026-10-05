use crate::cache::Cache;
use crate::connection::{ConnectionDefinition, ConnectionRuntime};
use crate::database::{Database, DatabaseRecord, DatabaseValue};
use crate::definition::CacheDefinition;
use crate::error::{CacheError, ErrorCode, Result};
use std::fs;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const USER_COUNT: usize = 100_000;
const RANDOM_QUERY_COUNT: usize = 100;

pub fn run(root: &Path) -> Result<()> {
    let record_bytes = benchmark_record_bytes()?;
    let connection_config = std::env::var("AB_CACHE_CONNECTION")
        .unwrap_or_else(|_| "config/connection.postgresql.json".to_owned());
    let connection_json = read(root, &connection_config)?;
    let cache_config = std::env::var("AB_CACHE_CONFIG")
        .unwrap_or_else(|_| "config/cache.user-benchmark.json".to_owned());
    let cache_json = read(root, &cache_config)?;
    let connection_definition = ConnectionDefinition::from_json(&connection_json)?;
    let cache_definition = CacheDefinition::from_json(&cache_json)?;
    let queue_directory = root.join("test/data/queue");
    fs::create_dir_all(&queue_directory).map_err(|error| {
        CacheError::new(
            ErrorCode::QueueDirectoryInvalid,
            format!("failed to create Queue directory: {error}"),
        )
    })?;
    let connection = Arc::new(Database::open(&connection_definition, &queue_directory)?);
    let cache = Arc::new(Cache::connected(cache_definition, Arc::clone(&connection))?);

    let resident_before_preload = process_resident_bytes()?;
    let preload_started = Instant::now();
    let loaded = cache.preload()?;
    let preload_elapsed = preload_started.elapsed();
    let resident_after_preload = process_resident_bytes()?;
    let resident_increase = resident_after_preload.saturating_sub(resident_before_preload);
    let rss_per_record = resident_increase as f64 / loaded as f64;
    let projected_million_bytes = rss_per_record * 1_000_000.0;
    if loaded != USER_COUNT || cache.record_count()? != USER_COUNT {
        return Err(CacheError::new(
            ErrorCode::RecordDecodingFailed,
            format!("expected {USER_COUNT} users, loaded {loaded}"),
        ));
    }

    let keys = benchmark_keys(20 * RANDOM_QUERY_COUNT);
    let query = std::env::var("AB_CACHE_BENCHMARK_QUERY").unwrap_or_else(|_| {
        "SELECT id, user_id, display_name, status FROM public.users WHERE user_id = $1".to_owned()
    });

    println!("CASE 2 - Cache preload");
    println!("  records            : {loaded}");
    println!("  pool size          : {}", connection.pool_size()?);
    println!("  select connections : {}", connection.select_pool_size()?);
    println!("  write connections  : 1");
    println!("  elapsed            : {}", duration(preload_elapsed));
    println!("  RSS before preload : {resident_before_preload} bytes");
    println!("  RSS after preload  : {resident_after_preload} bytes");
    println!("  RSS increase       : {resident_increase} bytes");
    println!("  record size        : {record_bytes} bytes/record");
    println!("  RSS per record     : {rss_per_record:.3} bytes/record");
    println!(
        "  RSS / record size  : {:.2}x",
        rss_per_record / record_bytes as f64
    );
    println!(
        "  memory efficiency  : {:.2}%",
        record_bytes as f64 / rss_per_record * 100.0
    );
    println!(
        "  memory overhead    : {:.2}%",
        (rss_per_record - record_bytes as f64) / rss_per_record * 100.0
    );
    println!(
        "  projected 1M RSS   : {:.3} MiB / {:.3} GiB",
        projected_million_bytes / (1024.0 * 1024.0),
        projected_million_bytes / (1024.0 * 1024.0 * 1024.0)
    );
    if std::env::var_os("AB_CACHE_MEMORY_ONLY").is_some() {
        print_cache_status(&cache)?;
        return Ok(());
    }
    let mut case_number = 3;
    for threads in [1, 10, 20] {
        let query_count = threads * RANDOM_QUERY_COUNT;
        let test_keys = &keys[..query_count];
        let cache_metrics = measure_cache(&cache, test_keys, threads)?;
        print_result(case_number, "Cache random lookup", threads, &cache_metrics);
        case_number += 1;
        let database_metrics = measure_database(&connection, &query, test_keys, threads)?;
        print_result(
            case_number,
            "Database random query",
            threads,
            &database_metrics,
        );
        case_number += 1;
        println!(
            "  Database / Cache ratio: {:.2}x",
            database_metrics.average().as_secs_f64() / cache_metrics.average().as_secs_f64()
        );
    }
    print_pool_matrix(
        &cache,
        &connection_definition,
        &queue_directory,
        &query,
        &keys,
    )?;
    print_cache_status(&cache)?;
    Ok(())
}

fn print_cache_status(cache: &Cache) -> Result<()> {
    let status = cache.status_data()?;
    println!("Cache status");
    println!("  record count       : {}", status.record_count);
    println!("  memory bytes       : {}", status.memory_bytes);
    println!(
        "  memory MiB         : {:.3}",
        status.memory_bytes as f64 / (1024.0 * 1024.0)
    );
    Ok(())
}

fn print_pool_matrix(
    cache: &Arc<Cache>,
    base_definition: &ConnectionDefinition,
    queue_directory: &Path,
    query: &str,
    keys: &[String],
) -> Result<()> {
    println!("CASE 9 - Thread count / pool_size matrix");
    println!("  WRITE Connection is always 1");
    println!("  All elapsed values are ms (millisecond)");
    println!("  Cache and Database use the same query keys");
    for threads in [1, 10, 20] {
        let test_keys = &keys[..threads * RANDOM_QUERY_COUNT];
        for pool_size in 2..=21 {
            let mut definition = base_definition.clone();
            definition.pool_size = pool_size;
            let connection = Arc::new(Database::open(&definition, queue_directory)?);
            let cache_metrics = measure_cache(cache, test_keys, threads)?;
            let database_metrics = measure_database(&connection, query, test_keys, threads)?;
            println!(
                "  Threads={threads} Pool={pool_size} SELECT={} Queries={}",
                pool_size - 1,
                database_metrics.samples.len(),
            );
            println!(
                "    Cache   wall={:.6} average={:.6} minimum={:.6} maximum={:.6}",
                milliseconds_value(cache_metrics.wall_elapsed),
                milliseconds_value(cache_metrics.average()),
                milliseconds_value(cache_metrics.minimum()),
                milliseconds_value(cache_metrics.maximum()),
            );
            println!(
                "    Database wall={:.6} average={:.6} minimum={:.6} maximum={:.6}",
                milliseconds_value(database_metrics.wall_elapsed),
                milliseconds_value(database_metrics.average()),
                milliseconds_value(database_metrics.minimum()),
                milliseconds_value(database_metrics.maximum()),
            );
            println!(
                "    Database / Cache ratio={:.2}x",
                database_metrics.average().as_secs_f64() / cache_metrics.average().as_secs_f64(),
            );
        }
    }
    Ok(())
}

struct QueryMetrics {
    wall_elapsed: Duration,
    samples: Vec<Duration>,
    matched_records: usize,
}

impl QueryMetrics {
    fn average(&self) -> Duration {
        self.samples.iter().copied().sum::<Duration>() / self.samples.len() as u32
    }

    fn minimum(&self) -> Duration {
        self.samples.iter().copied().min().unwrap_or_default()
    }

    fn maximum(&self) -> Duration {
        self.samples.iter().copied().max().unwrap_or_default()
    }
}

fn measure_cache(cache: &Arc<Cache>, keys: &[String], threads: usize) -> Result<QueryMetrics> {
    let start = Arc::new(Barrier::new(threads + 1));
    let (wall_elapsed, results) = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for thread_index in 0..threads {
            let cache = Arc::clone(cache);
            let start = Arc::clone(&start);
            let thread_keys =
                &keys[thread_index * RANDOM_QUERY_COUNT..(thread_index + 1) * RANDOM_QUERY_COUNT];
            handles.push(scope.spawn(move || {
                let mut samples = Vec::with_capacity(RANDOM_QUERY_COUNT);
                let mut matched = 0;
                start.wait();
                for key in thread_keys {
                    let started = Instant::now();
                    let input = DatabaseRecord::from([(
                        "user_id".to_owned(),
                        DatabaseValue::from(key.clone()),
                    )]);
                    let records = cache.query_record(&input)?;
                    for index in 0..records.len() {
                        let record = records.record(index)?;
                        std::hint::black_box(record.first().copied());
                        std::hint::black_box(record.last().copied());
                        matched += 1;
                    }
                    samples.push(started.elapsed());
                }
                Ok::<_, CacheError>((samples, matched))
            }));
        }
        let wall_started = Instant::now();
        start.wait();
        let results = handles
            .into_iter()
            .map(|handle| {
                handle.join().map_err(|_| {
                    CacheError::new(ErrorCode::InternalError, "Cache query Thread panicked")
                })?
            })
            .collect::<Result<Vec<_>>>()?;
        Ok::<_, CacheError>((wall_started.elapsed(), results))
    })?;
    merge_metrics(wall_elapsed, results)
}

fn measure_database(
    connection: &Arc<ConnectionRuntime>,
    query: &str,
    keys: &[String],
    threads: usize,
) -> Result<QueryMetrics> {
    let start = Arc::new(Barrier::new(threads + 1));
    let (wall_elapsed, results) = thread::scope(|scope| {
        let mut handles = Vec::with_capacity(threads);
        for thread_index in 0..threads {
            let connection = Arc::clone(connection);
            let start = Arc::clone(&start);
            let thread_keys =
                &keys[thread_index * RANDOM_QUERY_COUNT..(thread_index + 1) * RANDOM_QUERY_COUNT];
            handles.push(scope.spawn(move || {
                let mut samples = Vec::with_capacity(RANDOM_QUERY_COUNT);
                let mut matched = 0;
                start.wait();
                for key in thread_keys {
                    let started = Instant::now();
                    let records = connection.select(query, &[DatabaseValue::from(key.clone())])?;
                    samples.push(started.elapsed());
                    matched += records.len();
                }
                Ok::<_, CacheError>((samples, matched))
            }));
        }
        let wall_started = Instant::now();
        start.wait();
        let results = handles
            .into_iter()
            .map(|handle| {
                handle.join().map_err(|_| {
                    CacheError::new(ErrorCode::InternalError, "Database query Thread panicked")
                })?
            })
            .collect::<Result<Vec<_>>>()?;
        Ok::<_, CacheError>((wall_started.elapsed(), results))
    })?;
    merge_metrics(wall_elapsed, results)
}

fn merge_metrics(
    wall_elapsed: Duration,
    results: Vec<(Vec<Duration>, usize)>,
) -> Result<QueryMetrics> {
    let mut samples = Vec::new();
    let mut matched_records = 0;
    for (mut thread_samples, matched) in results {
        samples.append(&mut thread_samples);
        matched_records += matched;
    }
    if samples.is_empty() || matched_records != samples.len() {
        return Err(CacheError::new(
            ErrorCode::RecordDecodingFailed,
            "query benchmark returned an unexpected record count",
        ));
    }
    Ok(QueryMetrics {
        wall_elapsed,
        samples,
        matched_records,
    })
}

fn read(root: &Path, relative: &str) -> Result<String> {
    fs::read_to_string(root.join(relative)).map_err(|error| {
        CacheError::new(
            ErrorCode::InvalidArgument,
            format!("failed to read {relative}: {error}"),
        )
    })
}

fn process_resident_bytes() -> Result<usize> {
    let output = Command::new("ps")
        .args(["-o", "rss=", "-p", &std::process::id().to_string()])
        .output()
        .map_err(|error| {
            CacheError::new(
                ErrorCode::InternalError,
                format!("failed to measure process RSS: {error}"),
            )
        })?;
    if !output.status.success() {
        return Err(CacheError::new(
            ErrorCode::InternalError,
            "ps failed while measuring process RSS",
        ));
    }
    let kibibytes = String::from_utf8(output.stdout)
        .map_err(|error| {
            CacheError::new(
                ErrorCode::InternalError,
                format!("process RSS output is not UTF-8: {error}"),
            )
        })?
        .trim()
        .parse::<usize>()
        .map_err(|error| {
            CacheError::new(
                ErrorCode::InternalError,
                format!("process RSS output is invalid: {error}"),
            )
        })?;
    kibibytes.checked_mul(1024).ok_or_else(|| {
        CacheError::new(
            ErrorCode::InternalError,
            "process RSS byte count overflowed",
        )
    })
}

fn benchmark_record_bytes() -> Result<usize> {
    let value = std::env::var("AB_CACHE_RECORD_BYTES").unwrap_or_else(|_| "156".to_owned());
    value.parse::<usize>().map_err(|error| {
        CacheError::new(
            ErrorCode::InvalidArgument,
            format!("AB_CACHE_RECORD_BYTES is invalid: {error}"),
        )
    })
}

fn benchmark_keys(count: usize) -> Vec<String> {
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    (0..count)
        .map(|_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let number = (state % USER_COUNT as u64) + 1;
            format!("user{number:05}")
        })
        .collect()
}

fn print_result(case_number: usize, name: &str, threads: usize, metrics: &QueryMetrics) {
    println!("CASE {case_number} - {name} ({threads} Thread)");
    println!("  threads            : {threads}");
    println!("  queries per thread : {RANDOM_QUERY_COUNT}");
    println!("  total queries      : {}", metrics.samples.len());
    println!("  matched records    : {}", metrics.matched_records);
    println!("  wall elapsed       : {}", duration(metrics.wall_elapsed));
    println!(
        "  average            : {} ms/query",
        milliseconds(metrics.average())
    );
    println!(
        "  minimum            : {} ms/query",
        milliseconds(metrics.minimum())
    );
    println!(
        "  maximum            : {} ms/query",
        milliseconds(metrics.maximum())
    );
}

fn duration(value: Duration) -> String {
    format!("{:.3} ms", value.as_secs_f64() * 1_000.0)
}

fn milliseconds(value: Duration) -> String {
    format!("{:.6}", value.as_secs_f64() * 1_000.0)
}

fn milliseconds_value(value: Duration) -> f64 {
    value.as_secs_f64() * 1_000.0
}
