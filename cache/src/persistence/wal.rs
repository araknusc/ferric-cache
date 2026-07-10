use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, BufReader, Read};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::Mutex;
use tokio::time::{interval, Interval};
use bytes::{Bytes, BytesMut, Buf, BufMut};

use crate::protocol::Command;
use super::{WALSyncPolicy, PersistenceConfig};

#[derive(Debug)]
pub enum WALError {
    IoError(std::io::Error),
    SerializationError(String),
    CorruptedEntry(String),
}

impl From<std::io::Error> for WALError {
    fn from(err: std::io::Error) -> Self {
        Self::IoError(err)
    }
}

impl std::fmt::Display for WALError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(err) => write!(f, "WAL IO error: {}", err),
            Self::SerializationError(msg) => write!(f, "WAL serialization error: {}", msg),
            Self::CorruptedEntry(msg) => write!(f, "WAL corrupted entry: {}", msg),
        }
    }
}

impl std::error::Error for WALError {}

#[derive(Debug, Clone)]
pub struct WALEntry {
    pub sequence: u64,
    pub timestamp: u64,
    pub command: Command,
}

impl WALEntry {
    pub fn new(sequence: u64, command: Command) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;

        Self {
            sequence,
            timestamp,
            command,
        }
    }

    pub fn serialize(&self) -> Result<Bytes, WALError> {
        let mut buf = BytesMut::new();

        // Entry format: [SEQUENCE:8][TIMESTAMP:8][CMD_SIZE:4][COMMAND][CHECKSUM:4]
        buf.put_u64(self.sequence);
        buf.put_u64(self.timestamp);

        // Serialize command
        let cmd_bytes = self.serialize_command()?;
        buf.put_u32(cmd_bytes.len() as u32);
        buf.put(cmd_bytes);

        // Add simple checksum (CRC32)
        let checksum = crc32fast::hash(&buf[..]);
        buf.put_u32(checksum);

        Ok(buf.freeze())
    }

    pub fn deserialize(mut data: Bytes) -> Result<Self, WALError> {
        if data.len() < 24 {  // 8 + 8 + 4 + 4 minimum
            return Err(WALError::CorruptedEntry("Entry too short".to_string()));
        }

        // Extract checksum first
        let checksum_offset = data.len() - 4;
        let expected_checksum = u32::from_be_bytes([
            data[checksum_offset],
            data[checksum_offset + 1],
            data[checksum_offset + 2],
            data[checksum_offset + 3],
        ]);

        // Verify checksum
        let actual_checksum = crc32fast::hash(&data[..checksum_offset]);
        if actual_checksum != expected_checksum {
            return Err(WALError::CorruptedEntry("Checksum mismatch".to_string()));
        }

        // Parse entry
        let sequence = data.get_u64();
        let timestamp = data.get_u64();
        let cmd_size = data.get_u32() as usize;

        if data.len() < cmd_size + 4 {  // +4 for checksum
            return Err(WALError::CorruptedEntry("Invalid command size".to_string()));
        }

        let cmd_data = data.split_to(cmd_size);
        let command = Self::deserialize_command(cmd_data)?;

        Ok(Self {
            sequence,
            timestamp,
            command,
        })
    }

    fn serialize_command(&self) -> Result<Bytes, WALError> {
        bincode::serialize(&self.command)
            .map(Bytes::from)
            .map_err(|e| WALError::SerializationError(e.to_string()))
    }

    fn deserialize_command(data: Bytes) -> Result<Command, WALError> {
        bincode::deserialize(&data)
            .map_err(|e| WALError::CorruptedEntry(format!("bincode decode failed: {}", e)))
    }
}

pub struct WriteAheadLog {
    writer: Arc<Mutex<BufWriter<File>>>,
    sequence: AtomicU64,
    sync_policy: WALSyncPolicy,
    sync_interval: Option<Interval>,
    file_path: String,
    current_size: AtomicU64,
    max_size: u64,
    backup_count: u32,
}

impl WriteAheadLog {
    pub async fn new(config: &PersistenceConfig) -> Result<Self, WALError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.wal_path)?;

        let current_size = file.metadata()?.len();
        let writer = Arc::new(Mutex::new(BufWriter::new(file)));

        let sync_interval = match config.wal_sync_policy {
            WALSyncPolicy::EverySecond => Some(interval(Duration::from_secs(1))),
            _ => None,
        };

        // Read existing WAL (active + retained segments) to determine the next
        // sequence number, so sequences keep increasing across rotations.
        let sequence = Self::read_last_sequence(config).await?;

        Ok(Self {
            writer,
            sequence: AtomicU64::new(sequence),
            sync_policy: config.wal_sync_policy.clone(),
            sync_interval,
            file_path: config.wal_path.clone(),
            current_size: AtomicU64::new(current_size),
            max_size: config.max_wal_size_bytes,
            backup_count: config.wal_backup_count,
        })
    }

    /// Path of rotated backup segment `n` (1 = most recent).
    fn segment_path(&self, n: u32) -> String {
        format!("{}.{}", self.file_path, n)
    }

    /// How many rotated segments to retain — at least one, so rotation can
    /// never discard the only durable copy of unsnapshotted writes.
    fn segments_to_keep(&self) -> u32 {
        self.backup_count.max(1)
    }

    /// Existing WAL files in replay order (oldest first): highest-numbered
    /// backup segment down to `.1`, then the active file.
    fn files_in_replay_order(&self) -> Vec<String> {
        let keep = self.segments_to_keep();
        let mut files = Vec::new();
        for n in (1..=keep).rev() {
            let seg = self.segment_path(n);
            if Path::new(&seg).exists() {
                files.push(seg);
            }
        }
        if Path::new(&self.file_path).exists() {
            files.push(self.file_path.clone());
        }
        files
    }

    pub async fn append_command(&self, cmd: &Command) -> Result<u64, WALError> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst);
        let entry = WALEntry::new(seq, cmd.clone());
        let entry_bytes = entry.serialize()?;

        {
            let mut writer = self.writer.lock().await;
            writer.write_all(&entry_bytes)?;

            // Sync based on policy
            match self.sync_policy {
                WALSyncPolicy::Always => {
                    writer.flush()?;
                },
                _ => {
                    // Will be synced by background task or manual call
                }
            }
        }

        // Update size tracking
        self.current_size.fetch_add(entry_bytes.len() as u64, Ordering::Relaxed);

        // Check if WAL needs rotation
        if self.should_rotate() {
            if let Err(e) = self.rotate().await {
                eprintln!("WAL rotation failed: {}", e);
                // Continue operation even if rotation fails
            }
        }

        Ok(seq)
    }

    pub async fn sync(&self) -> Result<(), WALError> {
        let mut writer = self.writer.lock().await;
        writer.flush()?;
        Ok(())
    }

    pub async fn replay<F>(&self, mut callback: F) -> Result<u64, WALError>
    where
        F: FnMut(&WALEntry) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        // Replay retained backup segments (oldest first) and then the active
        // WAL, so writes preserved across a size-triggered rotation are not
        // lost on restart.
        let mut entries_replayed = 0;
        for path in self.files_in_replay_order() {
            entries_replayed += Self::replay_file(&path, &mut callback)?;
        }
        Ok(entries_replayed)
    }

    fn replay_file<F>(path: &str, callback: &mut F) -> Result<u64, WALError>
    where
        F: FnMut(&WALEntry) -> Result<(), Box<dyn std::error::Error + Send + Sync>>,
    {
        if !Path::new(path).exists() {
            return Ok(0);
        }

        let file = File::open(path)?;
        let mut reader = BufReader::new(file);
        let mut entries_replayed = 0;

        loop {
            // Try to read entry header (16 bytes: sequence + timestamp + size)
            let mut header = [0u8; 20]; // 8 + 8 + 4
            match reader.read_exact(&mut header) {
                Ok(_) => {},
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(WALError::IoError(e)),
            }

            let sequence = u64::from_be_bytes([
                header[0], header[1], header[2], header[3],
                header[4], header[5], header[6], header[7],
            ]);
            let timestamp = u64::from_be_bytes([
                header[8], header[9], header[10], header[11],
                header[12], header[13], header[14], header[15],
            ]);
            let cmd_size = u32::from_be_bytes([
                header[16], header[17], header[18], header[19],
            ]);

            // Read command data + checksum
            let mut entry_data = vec![0u8; cmd_size as usize + 4]; // +4 for checksum
            reader.read_exact(&mut entry_data)?;

            // Reconstruct full entry for parsing
            let mut full_entry = BytesMut::with_capacity(20 + entry_data.len());
            full_entry.put_slice(&header);
            full_entry.put_slice(&entry_data);

            match WALEntry::deserialize(full_entry.freeze()) {
                Ok(entry) => {
                    if let Err(e) = callback(&entry) {
                        eprintln!("Error replaying entry {}: {}", sequence, e);
                        // Continue with next entry rather than failing
                    }
                    entries_replayed += 1;
                },
                Err(e) => {
                    eprintln!("Corrupted WAL entry at sequence {}: {}", sequence, e);
                    // Skip corrupted entry and continue
                }
            }
        }

        Ok(entries_replayed)
    }

    pub fn should_rotate(&self) -> bool {
        self.current_size.load(Ordering::Relaxed) >= self.max_size
    }

    pub async fn rotate(&self) -> Result<(), WALError> {
        println!("WAL rotation triggered - size limit reached");

        let keep = self.segments_to_keep();

        // Serialize the whole rotation under the writer lock so no append can
        // race the file swap.
        let mut writer = self.writer.lock().await;
        writer.flush()?;

        // Age out segments: drop the oldest, shift `.k` → `.k+1`.
        let oldest = self.segment_path(keep);
        if Path::new(&oldest).exists() {
            std::fs::remove_file(&oldest)?;
        }
        for n in (1..keep).rev() {
            let from = self.segment_path(n);
            if Path::new(&from).exists() {
                std::fs::rename(&from, self.segment_path(n + 1))?;
            }
        }

        // Preserve the active WAL as segment `.1`. We copy rather than rename
        // because the active file still has an open append handle (renaming an
        // open file is rejected on Windows); the copy captures the flushed
        // bytes. Then truncate the active file in place.
        std::fs::copy(&self.file_path, self.segment_path(1))?;
        let new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.file_path)?;
        *writer = BufWriter::new(new_file);
        drop(writer);

        self.current_size.store(0, Ordering::Relaxed);
        println!(
            "WAL rotated: previous log preserved as {} ({} segment(s) retained)",
            self.segment_path(1), keep
        );
        Ok(())
    }

    pub async fn truncate_after_snapshot(&self) -> Result<(), WALError> {
        // After a successful snapshot, we can truncate the WAL since
        // the snapshot contains the complete state
        println!("Truncating WAL after successful snapshot");

        // Create new empty WAL file
        let new_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.file_path)?;

        // Replace writer
        {
            let mut writer = self.writer.lock().await;
            *writer = BufWriter::new(new_file);
        }

        self.current_size.store(0, Ordering::Relaxed);
        Ok(())
    }

    async fn read_last_sequence(config: &PersistenceConfig) -> Result<u64, WALError> {
        // Scan the active WAL and every retained backup segment for the highest
        // sequence number seen.
        let keep = config.wal_backup_count.max(1);
        let mut paths = vec![config.wal_path.clone()];
        for n in 1..=keep {
            paths.push(format!("{}.{}", config.wal_path, n));
        }

        let mut last_sequence = 0;
        let mut found_any = false;

        for path in paths {
            if !Path::new(&path).exists() {
                continue;
            }
            let file = File::open(&path)?;
            let mut reader = BufReader::new(file);

            loop {
                let mut header = [0u8; 20];
                match reader.read_exact(&mut header) {
                    Ok(_) => {},
                    Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(WALError::IoError(e)),
                }

                let sequence = u64::from_be_bytes([
                    header[0], header[1], header[2], header[3],
                    header[4], header[5], header[6], header[7],
                ]);
                last_sequence = sequence.max(last_sequence);
                found_any = true;

                let cmd_size = u32::from_be_bytes([
                    header[16], header[17], header[18], header[19],
                ]);

                // Skip the command data and checksum
                let mut skip_data = vec![0u8; cmd_size as usize + 4];
                reader.read_exact(&mut skip_data)?;
            }
        }

        // Return the next sequence number to use
        if found_any {
            Ok(last_sequence + 1) // Continue from last sequence
        } else {
            Ok(0) // Empty file, start from 0
        }
    }

    pub async fn start_sync_task(wal: Arc<Self>) {
        if let WALSyncPolicy::EverySecond = wal.sync_policy {
            tokio::spawn(async move {
                let mut interval = interval(Duration::from_secs(1));
                loop {
                    interval.tick().await;
                    if let Err(e) = wal.sync().await {
                        eprintln!("WAL sync error: {}", e);
                    }
                }
            });
        }
    }
}