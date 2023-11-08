use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeMap;
use std::io::Result;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Condvar, Mutex};
use thinp::io_engine::*;

use crate::byte_types::*;
use crate::lru::*;

//-------------------------------------------------------------------------

// All blocks begin with this header.  The is automatically calculated by
// the cache just before it writes the block to disk.  Similarly the checksum
// will be verified by the cache when it reads the block from disk.
pub struct BlockHeader {
    pub loc: u32,
    pub kind: u32,
    pub sum: u32,
}

pub const BLOCK_HEADER_SIZE: usize = 16;
pub const BLOCK_PAYLOAD_SIZE: usize = 4096 - BLOCK_HEADER_SIZE;

pub fn read_block_header<R: Read>(r: &mut R) -> Result<BlockHeader> {
    let loc = r.read_u32::<LittleEndian>()?;
    let kind = r.read_u32::<LittleEndian>()?;
    let _padding = r.read_u32::<LittleEndian>()?;
    let sum = r.read_u32::<LittleEndian>()?;

    Ok(BlockHeader { loc, kind, sum })
}

pub fn write_block_header<W: Write>(w: &mut W, hdr: BlockHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.loc)?;
    w.write_u32::<LittleEndian>(hdr.kind)?;
    w.write_u32::<LittleEndian>(0)?;
    w.write_u32::<LittleEndian>(hdr.sum)?;

    Ok(())
}

//-------------------------------------------------------------------------

enum LockState {
    Unlocked,
    Read(usize),
    Write,
}

struct EntryInner {
    lock: LockState,
    dirty: bool,
    block: Block,
}

struct CacheEntry {
    inner: Mutex<EntryInner>,
    cond: Condvar,
}

impl CacheEntry {
    fn new_read(block: Block) -> CacheEntry {
        CacheEntry {
            inner: Mutex::new(EntryInner {
                lock: LockState::Read(1),
                dirty: false,
                block,
            }),
            cond: Condvar::new(),
        }
    }

    fn new_write(block: Block) -> CacheEntry {
        CacheEntry {
            inner: Mutex::new(EntryInner {
                lock: LockState::Write,
                dirty: true,
                block,
            }),
            cond: Condvar::new(),
        }
    }

    fn is_held(&self) -> bool {
        use LockState::*;

        let inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => false,
            Read(_) => true,
            Write => true,
        }
    }

    fn is_dirty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.dirty
    }

    // Returns true on success, if false you will need to wait for the lock
    fn read_lock(&self) -> bool {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                inner.lock = Read(1);
                true
            }
            Read(n) => {
                inner.lock = Read(n + 1);
                true
            }
            Write => false,
        }
    }

    // Returns true on success, if false you will need to wait for the lock
    fn write_lock(&self) -> bool {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                inner.lock = Write;
                inner.dirty = true;
                true
            }
            Read(_) => false,
            Write => false,
        }
    }

    fn unlock(&self) {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                panic!("Unlocking an unlocked block");
            }
            Read(1) => {
                inner.lock = Unlocked;
            }
            Read(n) => {
                inner.lock = Read(n - 1);
            }
            Write => {
                inner.lock = Unlocked;
            }
        }
        self.cond.notify_all();
    }
}

//-------------------------------------------------------------------------

enum LockResult {
    Locked(Arc<CacheEntry>),
    Busy(Arc<CacheEntry>),
}

struct MetadataCacheInner {
    nr_blocks: u32,
    nr_held: usize,
    engine: SyncIoEngine,

    // The LRU lists only contain blocks that are not currently locked.
    lru: LRU,
    cache: BTreeMap<u32, Arc<CacheEntry>>,
}

impl MetadataCacheInner {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self> {
        let engine = SyncIoEngine::new(path, true)?;
        let nr_blocks = engine.get_nr_blocks() as u32;
        Ok(Self {
            nr_blocks,
            nr_held: 0,
            engine,
            lru: LRU::with_capacity(capacity),
            cache: BTreeMap::new(),
        })
    }

    pub fn nr_blocks(&self) -> u32 {
        self.nr_blocks
    }

    pub fn nr_held(&self) -> usize {
        self.nr_held
    }

    fn insert_lru_(&mut self, loc: u32) -> Result<()> {
        match self.lru.push(loc) {
            PushResult::AlreadyPresent => {
                panic!("AlreadyPresent")
            }
            PushResult::Added => {
                // Nothing
            }
            PushResult::AddAndEvict(old) => {
                let old_entry = self.cache.remove(&old).unwrap();
                if old_entry.is_dirty() {
                    self.writeback_(&old_entry)?;
                }
            }
        }

        Ok(())
    }

    fn remove_lru_(&mut self, loc: u32) {
        self.lru.remove(loc);
    }

    fn verify_(&self, _block: &Block) -> Result<()> {
        todo!();
    }

    fn prep_(&self, _block: &mut [u8]) {
        todo!();
    }

    fn read_(&mut self, loc: u32) -> Result<Block> {
        let block = self.engine.read(loc as u64)?;
        self.verify_(&block)?;
        Ok(block)
    }

    fn writeback_(&self, entry: &CacheEntry) -> Result<()> {
        let inner = entry.inner.lock().unwrap();
        let mut data = inner.block.get_data();
        self.prep_(&mut data);
        self.engine.write(&inner.block)?;
        Ok(())
    }

    fn unlock(&mut self, loc: u32) -> Result<()> {
        let entry = self.cache.get_mut(&loc).unwrap();
        entry.unlock();
        self.insert_lru_(loc)?;
        Ok(())
    }

    // Returns true on success
    pub fn read_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.read_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let entry = Arc::new(CacheEntry::new_read(self.read_(loc)?));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    pub fn write_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.write_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let entry = Arc::new(CacheEntry::new_write(self.read_(loc)?));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    ///! Write lock and zero the data (avoids reading the block)
    pub fn zero_lock(&mut self, loc: u32) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.write_lock() {
                let inner = entry.inner.lock().unwrap();
                let mut data = inner.block.get_data();
                unsafe {
                    std::ptr::write_bytes(&mut data, 0, BLOCK_SIZE);
                }
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let block = Block::zeroed(loc as u64);
            let entry = Arc::new(CacheEntry::new_write(block));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    /// Writeback all dirty blocks
    pub fn flush(&mut self) {
        let mut writebacks = Vec::new();
        for (loc, entry) in &self.cache {
            if entry.is_dirty() {
                writebacks.push(*loc);
            }
        }

        for loc in writebacks {
            if let Some(entry) = self.cache.get_mut(&loc).cloned() {
                self.writeback_(&entry).expect("flush: writeback failed");
            } else {
                panic!("flush: block disappeared");
            }
        }
    }
}

//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct ReadProxy<'a> {
    pub loc: u32,

    cache: &'a MetadataCache,
    begin: usize,
    end: usize,
    entry: Arc<CacheEntry>,
}

impl<'a> ReadProxy<'a> {
    pub fn loc(&self) -> u32 {
        self.loc
    }
}

impl<'a> Drop for ReadProxy<'a> {
    fn drop(&mut self) {
        self.cache.unlock_(self.loc);
    }
}

impl<'a> Readable for ReadProxy<'a> {
    fn r(&self) -> &[u8] {
        let inner = self.entry.inner.lock().unwrap();
        &inner.block.get_data()[self.begin..self.end]
    }

    fn split_at(&self, offset: usize) -> (Self, Self) {
        assert!(offset < (self.begin - self.end));
        (
            Self {
                loc: self.loc,
                cache: self.cache,
                begin: self.begin,
                end: offset,
                entry: self.entry.clone(),
            },
            Self {
                loc: self.loc,
                cache: self.cache,
                begin: offset,
                end: self.end,
                entry: self.entry.clone(),
            },
        )
    }
}

//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct WriteProxy<'a> {
    pub inner: ReadProxy<'a>,
}

impl<'a> WriteProxy<'a> {
    pub fn loc(&self) -> u32 {
        self.inner.loc
    }
}

impl<'a> Readable for WriteProxy<'a> {
    fn r(&self) -> &[u8] {
        self.inner.r()
    }

    fn split_at(&self, loc: usize) -> (Self, Self) {
        let (a, b) = self.inner.split_at(loc);
        (Self { inner: a }, Self { inner: b })
    }
}

impl<'a> Writeable for WriteProxy<'a> {
    fn rw(&mut self) -> &mut [u8] {
        let inner = self.inner.entry.inner.lock().unwrap();
        &mut inner.block.get_data()[self.inner.begin..self.inner.end]
    }
}

//-------------------------------------------------------------------------

pub struct MetadataCache {
    inner: Mutex<MetadataCacheInner>,
}

impl MetadataCache {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self> {
        let inner = MetadataCacheInner::new(path, capacity)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    pub fn nr_blocks(&self) -> u32 {
        let inner = self.inner.lock().unwrap();
        inner.nr_blocks()
    }

    pub fn nr_held(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.nr_held()
    }

    pub fn read_lock(&self, loc: u32) -> Result<ReadProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.read_lock(loc)? {
                Locked(entry) => {
                    return Ok(ReadProxy {
                        loc,
                        cache: self,
                        begin: 0,
                        end: BLOCK_SIZE,
                        entry,
                    })
                }
                Busy(entry) => self.wait_on_entry_(&*entry),
            }
        }
    }

    pub fn write_lock(&self, loc: u32) -> Result<WriteProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.write_lock(loc)? {
                Locked(entry) => {
                    return Ok(WriteProxy {
                        inner: ReadProxy {
                            loc,
                            cache: self,
                            begin: 0,
                            end: BLOCK_SIZE,
                            entry,
                        },
                    })
                }
                Busy(entry) => self.wait_on_entry_(&*entry),
            }
        }
    }

    ///! Write lock and zero the data (avoids reading the block)
    pub fn zero_lock(&self, loc: u32) -> Result<WriteProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.zero_lock(loc)? {
                Locked(entry) => {
                    return Ok(WriteProxy {
                        inner: ReadProxy {
                            loc,
                            cache: self,
                            begin: 0,
                            end: BLOCK_SIZE,
                            entry,
                        },
                    })
                }
                Busy(entry) => self.wait_on_entry_(&*entry),
            }
        }
    }

    /// Writeback all dirty blocks
    pub fn flush(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.flush();
    }

    // for use by the proxies only
    fn unlock_(&self, loc: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.unlock(loc).expect("unlock failed");
    }

    // Do not call this with the top level cache lock held
    fn wait_on_entry_(&self, entry: &CacheEntry) {
        let inner = entry.inner.lock().unwrap();
        entry.cond.wait(inner);
    }
}
