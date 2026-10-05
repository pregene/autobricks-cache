//! Durable FIFO used by a Connection's database worker.

use crate::error::{CacheError, ErrorCode, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

const VERSION: u32 = 1;
const LENGTH_BYTES: usize = 4;
const HEADER_BYTES: usize = 36;
const MAX_OBJECT_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Default)]
struct Header {
    version: u32,
    read_position: u64,
    write_position: u64,
    file_size: u64,
    item_count: u64,
}

struct State {
    header_file: File,
    body_file: File,
    header: Header,
}

/// Disk-backed FIFO. `peek` does not remove an item; `pop` is called only
/// after the database worker has completed that item successfully.
pub struct Queue {
    name: String,
    state: Mutex<State>,
}

impl Queue {
    pub fn open(queue_directory: &Path, queue_name: &str) -> Result<Self> {
        validate_queue_name(queue_name)?;
        let metadata = queue_directory.metadata().map_err(|error| {
            CacheError::new(
                ErrorCode::QueueDirectoryInvalid,
                format!("failed to inspect Queue directory: {error}"),
            )
        })?;
        if !metadata.is_dir() {
            return Err(CacheError::new(
                ErrorCode::QueueDirectoryInvalid,
                "Queue directory is not a directory",
            ));
        }

        let header_path = queue_directory.join(format!("{queue_name}.header"));
        let body_path = queue_directory.join(format!("{queue_name}.body"));
        let mut state = State {
            header_file: open_file(&header_path)?,
            body_file: open_file(&body_path)?,
            header: Header::default(),
        };
        state.header = load_header(&mut state.header_file)?;
        validate_state(&state)?;

        Ok(Self {
            name: queue_name.to_owned(),
            state: Mutex::new(state),
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn push(&self, object: &[u8]) -> Result<()> {
        if object.is_empty() || object.len() > MAX_OBJECT_BYTES {
            return Err(CacheError::new(
                ErrorCode::QueueObjectSizeInvalid,
                "Queue object size is invalid",
            ));
        }
        let length = u32::try_from(object.len()).map_err(|_| {
            CacheError::new(
                ErrorCode::QueueObjectSizeInvalid,
                "Queue object length exceeds u32",
            )
        })?;
        let mut state = lock_state(&self.state)?;
        let previous = state.header;
        let write_position = previous.write_position;

        state
            .body_file
            .seek(SeekFrom::Start(write_position))
            .and_then(|_| state.body_file.write_all(&length.to_be_bytes()))
            .and_then(|_| state.body_file.write_all(object))
            .and_then(|_| state.body_file.sync_data())
            .map_err(|error| queue_io("append Queue object", error))?;

        let record_size = (LENGTH_BYTES as u64)
            .checked_add(u64::from(length))
            .ok_or_else(position_overflow)?;
        state.header.write_position = previous
            .write_position
            .checked_add(record_size)
            .ok_or_else(position_overflow)?;
        state.header.file_size = previous.file_size.max(state.header.write_position);
        state.header.item_count = previous
            .item_count
            .checked_add(1)
            .ok_or_else(position_overflow)?;

        if let Err(error) = store_header(&mut state) {
            state.header = previous;
            let _ = state.body_file.set_len(previous.file_size);
            return Err(error);
        }
        Ok(())
    }

    pub fn peek(&self) -> Result<Option<Vec<u8>>> {
        let mut state = lock_state(&self.state)?;
        if state.header.item_count == 0 {
            return Ok(None);
        }
        let read_position = state.header.read_position;
        let write_position = state.header.write_position;
        let (_, object) = read_object(&mut state.body_file, read_position, write_position)?;
        Ok(Some(object))
    }

    pub fn pop(&self) -> Result<()> {
        let mut state = lock_state(&self.state)?;
        if state.header.item_count == 0 {
            return Err(CacheError::new(ErrorCode::QueueEmpty, "Queue is empty"));
        }
        let read_position = state.header.read_position;
        let write_position = state.header.write_position;
        let (next_position, _) = read_object(&mut state.body_file, read_position, write_position)?;
        state.header.read_position = next_position;
        state.header.item_count = state
            .header
            .item_count
            .checked_sub(1)
            .ok_or_else(position_overflow)?;

        if state.header.item_count == 0 {
            state.header = Header {
                version: VERSION,
                file_size: state.header.file_size,
                ..Header::default()
            };
        }
        store_header(&mut state)
    }

    pub fn count(&self) -> Result<u64> {
        Ok(lock_state(&self.state)?.header.item_count)
    }
}

fn validate_queue_name(name: &str) -> Result<()> {
    if name.is_empty() || name == "." || name == ".." || name.contains('/') || name.contains('\\') {
        return Err(CacheError::new(
            ErrorCode::QueueNameInvalid,
            "Queue name is invalid",
        ));
    }
    Ok(())
}

fn lock_state(state: &Mutex<State>) -> Result<std::sync::MutexGuard<'_, State>> {
    state
        .lock()
        .map_err(|_| CacheError::new(ErrorCode::LockPoisoned, "Queue lock is unavailable"))
}

fn read_object(file: &mut File, start: u64, boundary: u64) -> Result<(u64, Vec<u8>)> {
    file.seek(SeekFrom::Start(start))
        .map_err(|error| queue_io("seek Queue body", error))?;
    let mut length = [0_u8; LENGTH_BYTES];
    file.read_exact(&mut length)
        .map_err(|error| queue_io("read Queue object length", error))?;
    let length = u32::from_be_bytes(length) as usize;
    if !(1..=MAX_OBJECT_BYTES).contains(&length) {
        return Err(CacheError::new(
            ErrorCode::QueueBodyInvalid,
            "Queue object length is invalid",
        ));
    }
    let end = start
        .checked_add(LENGTH_BYTES as u64)
        .and_then(|value| value.checked_add(length as u64))
        .filter(|value| *value <= boundary)
        .ok_or_else(|| {
            CacheError::new(
                ErrorCode::QueueBodyInvalid,
                "Queue object exceeds recorded boundary",
            )
        })?;
    let mut object = vec![0_u8; length];
    file.read_exact(&mut object)
        .map_err(|error| queue_io("read Queue object", error))?;
    Ok((end, object))
}

fn load_header(file: &mut File) -> Result<Header> {
    let size = file
        .metadata()
        .map_err(|error| queue_io("inspect Queue header", error))?
        .len();
    if size == 0 {
        return Ok(Header {
            version: VERSION,
            ..Header::default()
        });
    }
    if size != HEADER_BYTES as u64 {
        return Err(CacheError::new(
            ErrorCode::QueueHeaderInvalid,
            "Queue header size is invalid",
        ));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|error| queue_io("seek Queue header", error))?;
    let mut bytes = [0_u8; HEADER_BYTES];
    file.read_exact(&mut bytes)
        .map_err(|error| queue_io("read Queue header", error))?;
    let header = Header {
        version: read_u32(&bytes, 0)?,
        read_position: read_u64(&bytes, 4)?,
        write_position: read_u64(&bytes, 12)?,
        file_size: read_u64(&bytes, 20)?,
        item_count: read_u64(&bytes, 28)?,
    };
    if header.version != VERSION {
        return Err(CacheError::new(
            ErrorCode::QueueHeaderVersionUnsupported,
            "Queue header version is unsupported",
        ));
    }
    Ok(header)
}

fn store_header(state: &mut State) -> Result<()> {
    let mut bytes = [0_u8; HEADER_BYTES];
    bytes[0..4].copy_from_slice(&state.header.version.to_be_bytes());
    bytes[4..12].copy_from_slice(&state.header.read_position.to_be_bytes());
    bytes[12..20].copy_from_slice(&state.header.write_position.to_be_bytes());
    bytes[20..28].copy_from_slice(&state.header.file_size.to_be_bytes());
    bytes[28..36].copy_from_slice(&state.header.item_count.to_be_bytes());
    state
        .header_file
        .seek(SeekFrom::Start(0))
        .and_then(|_| state.header_file.write_all(&bytes))
        .and_then(|_| state.header_file.set_len(HEADER_BYTES as u64))
        .and_then(|_| state.header_file.sync_data())
        .map_err(|error| queue_io("store Queue header", error))
}

fn validate_state(state: &State) -> Result<()> {
    let actual = state
        .body_file
        .metadata()
        .map_err(|error| queue_io("inspect Queue body", error))?
        .len();
    if state.header.read_position > state.header.write_position
        || state.header.write_position > state.header.file_size
        || actual != state.header.file_size
        || (state.header.item_count == 0
            && (state.header.read_position != 0 || state.header.write_position != 0))
        || (state.header.item_count > 0
            && state.header.read_position >= state.header.write_position)
    {
        return Err(CacheError::new(
            ErrorCode::QueueHeaderInvalid,
            "Queue header and body are inconsistent",
        ));
    }
    Ok(())
}

fn open_file(path: &Path) -> Result<File> {
    if path
        .symlink_metadata()
        .is_ok_and(|metadata| metadata.file_type().is_symlink())
    {
        return Err(CacheError::new(
            ErrorCode::QueueFileSecurityInvalid,
            "Queue path must not be a symbolic link",
        ));
    }

    let mut options = OpenOptions::new();
    options.create(true).read(true).write(true);
    #[cfg(unix)]
    options.mode(0o600);
    let file = options.open(path).map_err(|error| {
        CacheError::new(
            ErrorCode::QueueFileOpenFailed,
            format!("failed to open Queue file: {error}"),
        )
    })?;
    let metadata = file.metadata().map_err(|error| {
        CacheError::new(
            ErrorCode::QueueFileOpenFailed,
            format!("failed to inspect Queue file: {error}"),
        )
    })?;
    if !metadata.is_file() {
        return Err(CacheError::new(
            ErrorCode::QueueFileSecurityInvalid,
            "Queue path is not a regular file",
        ));
    }
    #[cfg(unix)]
    if metadata.permissions().mode() & 0o777 != 0o600 {
        return Err(CacheError::new(
            ErrorCode::QueueFileSecurityInvalid,
            "Queue file permissions must be 0600",
        ));
    }
    Ok(file)
}

fn read_u32(bytes: &[u8], start: usize) -> Result<u32> {
    let end = start.checked_add(4).ok_or_else(position_overflow)?;
    let value = bytes.get(start..end).ok_or_else(|| {
        CacheError::new(ErrorCode::QueueHeaderInvalid, "Queue header u32 is missing")
    })?;
    Ok(u32::from_be_bytes(value.try_into().map_err(|_| {
        CacheError::new(ErrorCode::QueueHeaderInvalid, "Queue header u32 is invalid")
    })?))
}

fn read_u64(bytes: &[u8], start: usize) -> Result<u64> {
    let end = start.checked_add(8).ok_or_else(position_overflow)?;
    let value = bytes.get(start..end).ok_or_else(|| {
        CacheError::new(ErrorCode::QueueHeaderInvalid, "Queue header u64 is missing")
    })?;
    Ok(u64::from_be_bytes(value.try_into().map_err(|_| {
        CacheError::new(ErrorCode::QueueHeaderInvalid, "Queue header u64 is invalid")
    })?))
}

fn position_overflow() -> CacheError {
    CacheError::new(
        ErrorCode::QueuePositionOverflow,
        "Queue position or item count overflow",
    )
}

fn queue_io(action: &str, error: std::io::Error) -> CacheError {
    CacheError::new(
        ErrorCode::QueueIoFailed,
        format!("failed to {action}: {error}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn test_directory(name: &str) -> std::path::PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "autobricks-cache-{name}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        directory
    }

    #[test]
    fn restores_fifo_after_reopen() {
        let directory = test_directory("queue-reopen");
        let queue = Queue::open(&directory, "database-write").unwrap();
        queue.push(b"one").unwrap();
        queue.push(b"two").unwrap();
        assert_eq!(queue.count().unwrap(), 2);
        assert_eq!(queue.peek().unwrap().unwrap(), b"one");
        drop(queue);

        let queue = Queue::open(&directory, "database-write").unwrap();
        assert_eq!(queue.peek().unwrap().unwrap(), b"one");
        queue.pop().unwrap();
        assert_eq!(queue.peek().unwrap().unwrap(), b"two");
        queue.pop().unwrap();
        assert_eq!(queue.count().unwrap(), 0);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_unsafe_queue_name() {
        let directory = test_directory("queue-name");
        let error = Queue::open(&directory, "../escape").err().unwrap();
        assert_eq!(error.code(), ErrorCode::QueueNameInvalid);
        std::fs::remove_dir_all(directory).unwrap();
    }
}
