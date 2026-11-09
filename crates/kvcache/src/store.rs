// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The per-node store: one writer per drive, an in-memory index of every
//! entry this node holds, rebuilt from segment files at open.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};

use crate::aligned::AlignedBuf;
use crate::error::{KvError, KvResult};
use crate::format::{check_key, entry_footprint};
use crate::segment::{SegmentFile, SegmentId, SegmentWriter, WriterOptions, scan_segment, segment_file_name};

const INDEX_SHARDS: usize = 64;
const SEGMENT_SUFFIX: &str = ".seg";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOptions {
    pub segment_bytes: u64,
    pub direct_io: bool,
    pub sync_on_seal: bool,
}

impl StoreOptions {
    fn writer(&self) -> WriterOptions {
        WriterOptions {
            segment_bytes: self.segment_bytes,
            direct_io: self.direct_io,
            sync_on_seal: self.sync_on_seal,
        }
    }
}

/// Where an entry lives. Holding it keeps the segment file open.
#[derive(Debug, Clone)]
struct Location {
    drive: u16,
    segment: Arc<SegmentFile>,
    offset: u64,
    value_len: u64,
    value_crc: u32,
    write_time_ms: u64,
}

/// What a lookup reports about an entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EntryInfo {
    pub drive: u16,
    pub segment: SegmentId,
    pub value_len: u64,
    pub value_crc: u32,
    pub write_time_ms: u64,
}

/// A value read from a segment: an aligned buffer and the value's length.
pub struct Value {
    buf: AlignedBuf,
    len: usize,
}

impl Value {
    pub fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PutOutcome {
    Written,
    /// The key was already stored; nothing was written.
    Exists,
}

/// A segment as the evictor sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentStat {
    pub drive: u16,
    pub id: SegmentId,
    pub sealed: bool,
    /// Write time of the segment's newest entry (unix ms), or its creation.
    pub newest_write_ms: u64,
    pub entries: usize,
    /// Bytes of entries still in the index.
    pub live_bytes: u64,
}

#[derive(Debug)]
struct SegmentInfo {
    file: Arc<SegmentFile>,
    sealed: bool,
    newest_write_ms: u64,
    keys: Vec<String>,
    live_bytes: u64,
}

#[derive(Debug)]
struct DriveState {
    dir: PathBuf,
    writer: Mutex<Option<SegmentWriter>>,
    segments: RwLock<BTreeMap<SegmentId, SegmentInfo>>,
    next_id: Mutex<SegmentId>,
}

/// What open found on disk.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    pub segments: usize,
    /// Unsealed segments found (and sealed) at open.
    pub resealed: usize,
    /// Segment files that could not be read and were removed.
    pub removed: usize,
    pub entries: usize,
    /// Entries skipped because an older segment already held the key.
    pub duplicates: usize,
}

pub struct KvStore {
    options: StoreOptions,
    drives: Vec<DriveState>,
    index: Vec<RwLock<HashMap<String, Location>>>,
}

fn shard_of(key: &str) -> usize {
    crate::format::crc32c(key.as_bytes()) as usize % INDEX_SHARDS
}

fn parse_segment_id(name: &str) -> Option<SegmentId> {
    let hex = name.strip_suffix(SEGMENT_SUFFIX)?;
    if hex.len() != 16 {
        return None;
    }
    SegmentId::from_str_radix(hex, 16).ok()
}

impl KvStore {
    /// Opens the store over `drive_dirs` (index = drive number), creating the
    /// directories, and rebuilds the index from their segment files.
    /// Unsealed segments are cut at their first torn entry and sealed; older
    /// segments win when two hold the same key.
    pub fn open(options: StoreOptions, drive_dirs: Vec<PathBuf>, now_ms: u64) -> KvResult<(Self, RecoveryReport)> {
        if drive_dirs.len() > usize::from(u16::MAX) {
            return Err(KvError::Corrupt("too many drives".into()));
        }
        let store = Self {
            options,
            drives: drive_dirs
                .into_iter()
                .map(|dir| DriveState {
                    dir,
                    writer: Mutex::new(None),
                    segments: RwLock::new(BTreeMap::new()),
                    next_id: Mutex::new(1),
                })
                .collect(),
            index: (0..INDEX_SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
        };
        let mut report = RecoveryReport::default();
        for drive in 0..store.drives.len() {
            store.recover_drive(drive as u16, now_ms, &mut report)?;
        }
        Ok((store, report))
    }

    fn recover_drive(&self, drive: u16, now_ms: u64, report: &mut RecoveryReport) -> KvResult<()> {
        let state = &self.drives[usize::from(drive)];
        std::fs::create_dir_all(&state.dir)?;
        let mut ids: Vec<SegmentId> = std::fs::read_dir(&state.dir)?
            .filter_map(|entry| entry.ok())
            .filter_map(|entry| parse_segment_id(&entry.file_name().to_string_lossy()))
            .collect();
        ids.sort_unstable();
        *state.next_id.lock() = ids.last().map_or(1, |id| id + 1);

        for id in ids {
            let path = state.dir.join(segment_file_name(id));
            let scanned = match scan_segment(&path, id, self.options.direct_io) {
                Ok(scanned) => scanned,
                Err(KvError::Corrupt(_)) => {
                    std::fs::remove_file(&path)?;
                    report.removed += 1;
                    continue;
                }
                Err(err) => return Err(err),
            };
            if !scanned.sealed {
                let mut writer = SegmentWriter::resume(&scanned, &path, self.options.writer())?;
                writer.seal(now_ms)?;
                report.resealed += 1;
            }
            let file = Arc::new(SegmentFile::open(&path, id, self.options.direct_io)?);
            let mut info = SegmentInfo {
                file: Arc::clone(&file),
                sealed: true,
                newest_write_ms: 0,
                keys: Vec::with_capacity(scanned.entries.len()),
                live_bytes: 0,
            };
            for (offset, meta) in scanned.entries {
                info.newest_write_ms = info.newest_write_ms.max(meta.write_time_ms);
                let location = Location {
                    drive,
                    segment: Arc::clone(&file),
                    offset,
                    value_len: meta.value_len,
                    value_crc: meta.value_crc,
                    write_time_ms: meta.write_time_ms,
                };
                if self.insert_if_absent(&meta.key, location) {
                    info.live_bytes += entry_footprint(meta.value_len);
                    info.keys.push(meta.key);
                    report.entries += 1;
                } else {
                    report.duplicates += 1;
                }
            }
            if info.newest_write_ms == 0 {
                // An empty segment ages from when it was sealed.
                info.newest_write_ms = scanned.sealed_time_ms.unwrap_or(now_ms);
            }
            state.segments.write().insert(id, info);
            report.segments += 1;
        }
        Ok(())
    }

    fn insert_if_absent(&self, key: &str, location: Location) -> bool {
        let mut shard = self.index[shard_of(key)].write();
        if shard.contains_key(key) {
            return false;
        }
        shard.insert(key.to_owned(), location);
        true
    }

    fn locate(&self, key: &str) -> Option<Location> {
        self.index[shard_of(key)].read().get(key).cloned()
    }

    pub fn drive_count(&self) -> usize {
        self.drives.len()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.index[shard_of(key)].read().contains_key(key)
    }

    /// Stores `value` under `key` on `drive` unless the key is already stored.
    pub fn put(&self, drive: u16, key: &str, value: &[u8], now_ms: u64) -> KvResult<PutOutcome> {
        check_key(key)?;
        let state = self
            .drives
            .get(usize::from(drive))
            .ok_or_else(|| KvError::Corrupt(format!("no drive {drive}")))?;
        if self.contains(key) {
            return Ok(PutOutcome::Exists);
        }
        if !SegmentWriter::fits_empty(key, value.len() as u64, self.options.segment_bytes) {
            return Err(KvError::ValueTooLarge {
                len: value.len() as u64,
                segment_bytes: self.options.segment_bytes,
            });
        }

        let mut writer_slot = state.writer.lock();
        // Re-check under the drive's writer lock: a concurrent put of the
        // same key on this drive may have finished meanwhile.
        if self.contains(key) {
            return Ok(PutOutcome::Exists);
        }
        loop {
            if writer_slot.is_none() {
                let id = {
                    let mut next = state.next_id.lock();
                    let id = *next;
                    *next += 1;
                    id
                };
                let writer = SegmentWriter::create(&state.dir, id, self.options.writer())?;
                let file = Arc::new(SegmentFile::open(writer.path(), id, self.options.direct_io)?);
                state.segments.write().insert(
                    id,
                    SegmentInfo {
                        file,
                        sealed: false,
                        newest_write_ms: now_ms,
                        keys: Vec::new(),
                        live_bytes: 0,
                    },
                );
                *writer_slot = Some(writer);
            }
            let writer = writer_slot.as_mut().expect("writer was just installed");
            match writer.append(key, value, now_ms)? {
                Some(offset) => {
                    let id = writer.id();
                    let file = Arc::clone(&state.segments.read().get(&id).expect("open segment is registered").file);
                    let location = Location {
                        drive,
                        segment: file,
                        offset,
                        value_len: value.len() as u64,
                        value_crc: writer.entries().last().expect("entry was just appended").1.value_crc,
                        write_time_ms: now_ms,
                    };
                    let inserted = self.insert_if_absent(key, location);
                    let mut segments = state.segments.write();
                    let info = segments.get_mut(&id).expect("open segment is registered");
                    info.newest_write_ms = info.newest_write_ms.max(now_ms);
                    if inserted {
                        info.keys.push(key.to_owned());
                        info.live_bytes += entry_footprint(value.len() as u64);
                    }
                    return Ok(if inserted { PutOutcome::Written } else { PutOutcome::Exists });
                }
                None => {
                    // Full: seal it and start the next segment.
                    let mut full = writer_slot.take().expect("writer is present");
                    full.seal(now_ms)?;
                    if let Some(info) = state.segments.write().get_mut(&full.id()) {
                        info.sealed = true;
                    }
                }
            }
        }
    }

    /// Reads `key`. A value that fails its checksum is removed from the index
    /// and reported as a miss.
    pub fn get(&self, key: &str) -> KvResult<Option<Value>> {
        let Some(location) = self.locate(key) else {
            return Ok(None);
        };
        match location
            .segment
            .read_value(location.offset, location.value_len, location.value_crc)
        {
            Ok(buf) => Ok(Some(Value {
                buf,
                len: location.value_len as usize,
            })),
            Err(KvError::ChecksumMismatch) => {
                self.forget(key, &location);
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    fn forget(&self, key: &str, location: &Location) {
        let mut shard = self.index[shard_of(key)].write();
        if shard
            .get(key)
            .is_some_and(|current| Arc::ptr_eq(&current.segment, &location.segment) && current.offset == location.offset)
        {
            shard.remove(key);
            drop(shard);
            if let Some(info) = self.drives[usize::from(location.drive)]
                .segments
                .write()
                .get_mut(&location.segment.id())
            {
                info.live_bytes = info.live_bytes.saturating_sub(entry_footprint(location.value_len));
            }
        }
    }

    pub fn head(&self, key: &str) -> Option<EntryInfo> {
        self.locate(key).map(|location| EntryInfo {
            drive: location.drive,
            segment: location.segment.id(),
            value_len: location.value_len,
            value_crc: location.value_crc,
            write_time_ms: location.write_time_ms,
        })
    }

    /// Removes `key` from the index; its bytes are reclaimed with its segment.
    pub fn delete(&self, key: &str) -> bool {
        match self.locate(key) {
            Some(location) => {
                self.forget(key, &location);
                true
            }
            None => false,
        }
    }

    /// How many leading `keys` are stored.
    pub fn prefix_query<'a>(&self, keys: impl IntoIterator<Item = &'a str>) -> usize {
        keys.into_iter().take_while(|key| self.contains(key)).count()
    }

    /// Up to `max` stored keys with `prefix`, after `start_after`, sorted.
    /// Best effort: concurrent writes may or may not appear.
    pub fn list(&self, prefix: &str, start_after: Option<&str>, max: usize) -> Vec<(String, EntryInfo)> {
        let mut keys: Vec<(String, EntryInfo)> = Vec::new();
        for shard in &self.index {
            for (key, location) in shard.read().iter() {
                if key.starts_with(prefix) && start_after.is_none_or(|after| key.as_str() > after) {
                    keys.push((
                        key.clone(),
                        EntryInfo {
                            drive: location.drive,
                            segment: location.segment.id(),
                            value_len: location.value_len,
                            value_crc: location.value_crc,
                            write_time_ms: location.write_time_ms,
                        },
                    ));
                }
            }
        }
        keys.sort_by(|left, right| left.0.cmp(&right.0));
        keys.truncate(max);
        keys
    }

    /// Number of keys in the index.
    pub fn len(&self) -> usize {
        self.index.iter().map(|shard| shard.read().len()).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Seals every drive's open segment (shutdown, or ageing out the open
    /// segment so it can be evicted).
    pub fn seal_open_segments(&self, now_ms: u64) -> KvResult<()> {
        for state in &self.drives {
            let mut writer_slot = state.writer.lock();
            if let Some(mut writer) = writer_slot.take() {
                writer.seal(now_ms)?;
                if let Some(info) = state.segments.write().get_mut(&writer.id()) {
                    info.sealed = true;
                }
            }
        }
        Ok(())
    }

    /// Every segment on `drive`, oldest id first.
    pub fn segments(&self, drive: u16) -> Vec<SegmentStat> {
        self.drives
            .get(usize::from(drive))
            .map(|state| {
                state
                    .segments
                    .read()
                    .iter()
                    .map(|(id, info)| SegmentStat {
                        drive,
                        id: *id,
                        sealed: info.sealed,
                        newest_write_ms: info.newest_write_ms,
                        entries: info.keys.len(),
                        live_bytes: info.live_bytes,
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Drops a sealed segment: its keys leave the index and its file is
    /// unlinked. Reads already holding the segment finish normally; the
    /// space is returned when the last of them ends. Returns the number of
    /// keys removed.
    pub fn drop_segment(&self, drive: u16, id: SegmentId) -> KvResult<usize> {
        let state = self
            .drives
            .get(usize::from(drive))
            .ok_or_else(|| KvError::Corrupt(format!("no drive {drive}")))?;
        let Some(info) = state.segments.write().remove(&id) else {
            return Ok(0);
        };
        if !info.sealed {
            // The open segment is sealed (and dropped) only by its writer.
            state.segments.write().insert(id, info);
            return Err(KvError::Corrupt(format!("segment {id} is still open")));
        }
        let mut removed = 0;
        for key in &info.keys {
            let mut shard = self.index[shard_of(key)].write();
            if shard
                .get(key)
                .is_some_and(|location| Arc::ptr_eq(&location.segment, &info.file))
            {
                shard.remove(key);
                removed += 1;
            }
        }
        match std::fs::remove_file(info.file.path()) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => return Err(err.into()),
        }
        Ok(removed)
    }

    /// The directory of `drive`.
    pub fn drive_dir(&self, drive: u16) -> Option<&Path> {
        self.drives.get(usize::from(drive)).map(|state| state.dir.as_path())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    const SEGMENT: u64 = 256 * 1024;

    fn options() -> StoreOptions {
        StoreOptions {
            segment_bytes: SEGMENT,
            direct_io: false,
            sync_on_seal: false,
        }
    }

    fn value(seed: u8, len: usize) -> Vec<u8> {
        (0..len).map(|i| seed.wrapping_add((i % 241) as u8)).collect()
    }

    fn open(dirs: &[PathBuf]) -> (KvStore, RecoveryReport) {
        KvStore::open(options(), dirs.to_vec(), 1_000).expect("open")
    }

    fn drives(root: &Path, count: usize) -> Vec<PathBuf> {
        (0..count).map(|i| root.join(format!("d{i}"))).collect()
    }

    #[test]
    fn puts_read_back_and_writes_of_existing_keys_are_noops() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 2));
        assert_eq!(store.put(0, "a", &value(1, 10_000), 5).expect("put"), PutOutcome::Written);
        assert_eq!(store.put(1, "b", &value(2, 3), 6).expect("put"), PutOutcome::Written);
        let offset_before = store.segments(0)[0].live_bytes;
        assert_eq!(store.put(0, "a", &value(9, 10_000), 7).expect("put"), PutOutcome::Exists);
        assert_eq!(store.segments(0)[0].live_bytes, offset_before, "an existing key writes nothing");

        assert_eq!(store.get("a").expect("get").expect("hit").as_slice(), &value(1, 10_000)[..]);
        assert_eq!(store.get("b").expect("get").expect("hit").as_slice(), &value(2, 3)[..]);
        assert!(store.get("missing").expect("get").is_none());
        assert_eq!(store.head("b").map(|info| (info.drive, info.value_len)), Some((1, 3)));

        assert!(store.delete("a"));
        assert!(!store.delete("a"));
        assert!(store.get("a").expect("get").is_none());
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn segments_rotate_when_full_and_old_segments_stay_readable() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        for i in 0..20u8 {
            store
                .put(0, &format!("k{i:02}"), &value(i, 40_000), u64::from(i))
                .expect("put");
        }
        let segments = store.segments(0);
        assert!(segments.len() >= 4, "{segments:?}");
        assert!(segments[..segments.len() - 1].iter().all(|segment| segment.sealed));
        for i in 0..20u8 {
            let read = store.get(&format!("k{i:02}")).expect("get").expect("hit");
            assert_eq!(read.as_slice(), &value(i, 40_000)[..]);
        }
        assert_eq!(store.prefix_query(["k00", "k01", "k02", "zz", "k03"]), 3);
        let listed: Vec<_> = store.list("k1", Some("k12"), 3).into_iter().map(|(key, _)| key).collect();
        assert_eq!(listed, ["k13", "k14", "k15"]);
    }

    #[test]
    fn reopen_rebuilds_the_index_and_drops_a_torn_tail() {
        let root = tempfile::tempdir().expect("tempdir");
        let dirs = drives(root.path(), 2);
        {
            let (store, _) = open(&dirs);
            for i in 0..12u8 {
                store
                    .put(u16::from(i % 2), &format!("k{i:02}"), &value(i, 30_000), 100 + u64::from(i))
                    .expect("put");
            }
            // Crash: open segments are left unsealed. Tear the last entry of drive 1.
            let last = store.head("k11").expect("present");
            let path = store.drive_dir(1).expect("dir").join(segment_file_name(last.segment));
            let file = std::fs::OpenOptions::new().write(true).open(path).expect("open");
            let location = store.locate("k11").expect("present");
            std::os::unix::fs::FileExt::write_all_at(&file, &[0xEE; 64], location.offset + 4096 + 10).expect("tear");
        }

        let (store, report) = open(&dirs);
        assert_eq!(report.entries, 11);
        assert!(report.resealed >= 2, "{report:?}");
        assert_eq!(store.len(), 11);
        assert!(store.get("k11").expect("get").is_none(), "the torn entry is gone");
        for i in 0..11u8 {
            let read = store.get(&format!("k{i:02}")).expect("get").expect("hit");
            assert_eq!(read.as_slice(), &value(i, 30_000)[..]);
        }
        // New writes go to new segments after the recovered ones.
        store.put(0, "after", b"x", 500).expect("put");
        let ids: Vec<_> = store.segments(0).iter().map(|segment| segment.id).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));

        let (reopened, report) = open(&dirs);
        assert_eq!(report.resealed, 1, "only the segment written after recovery was open");
        assert_eq!(reopened.len(), 12);
    }

    #[test]
    fn a_corrupt_value_becomes_a_miss_and_leaves_the_index() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        store.put(0, "k", &value(4, 20_000), 1).expect("put");
        let location = store.locate("k").expect("present");
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(location.segment.path())
            .expect("open");
        std::os::unix::fs::FileExt::write_all_at(&file, &[0x11], location.offset + 4096 + 777).expect("flip");

        assert!(store.get("k").expect("get").is_none());
        assert!(!store.contains("k"));
    }

    #[test]
    fn dropping_a_segment_does_not_break_reads_in_flight() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        for i in 0..10u8 {
            store.put(0, &format!("k{i}"), &value(i, 40_000), u64::from(i)).expect("put");
        }
        let first = store.segments(0)[0].clone();
        assert!(first.sealed);
        let in_flight = store.locate("k0").expect("present");

        let removed = store.drop_segment(0, first.id).expect("drop");
        assert_eq!(removed, first.entries);
        assert!(store.get("k0").expect("get").is_none());
        assert!(!in_flight.segment.path().exists(), "the file is unlinked");
        // The read that located the entry before the drop still completes.
        let read = in_flight
            .segment
            .read_value(in_flight.offset, in_flight.value_len, in_flight.value_crc)
            .expect("read after unlink");
        assert_eq!(&read[..40_000], &value(0, 40_000)[..]);

        let open_segment = store.segments(0).last().cloned().expect("open segment");
        assert!(!open_segment.sealed);
        assert!(store.drop_segment(0, open_segment.id).is_err(), "the open segment cannot be dropped");
        assert_eq!(store.drop_segment(0, first.id).expect("again"), 0);
    }

    #[test]
    fn concurrent_puts_of_one_key_store_it_once() {
        let root = tempfile::tempdir().expect("tempdir");
        let (store, _) = open(&drives(root.path(), 1));
        let store = Arc::new(store);
        let outcomes: Vec<PutOutcome> = std::thread::scope(|scope| {
            // Start every writer before joining any of them.
            let mut handles = Vec::new();
            for i in 0..8 {
                let store = Arc::clone(&store);
                handles.push(scope.spawn(move || store.put(0, "same", &value(i, 5_000), 1).expect("put")));
            }
            handles.into_iter().map(|handle| handle.join().expect("join")).collect()
        });
        assert_eq!(outcomes.iter().filter(|outcome| **outcome == PutOutcome::Written).count(), 1);
        assert_eq!(store.segments(0)[0].entries, 1);
    }
}
