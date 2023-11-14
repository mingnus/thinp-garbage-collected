use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use crc32c::crc32c;
use linked_hash_map::*;
use std::collections::BTreeMap;
use std::io::{self, Result};
use std::io::{Read, Write};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::ThreadId;
use thinp::io_engine::*;

use crate::byte_types::*;

//-------------------------------------------------------------------------

#[derive(Clone, Eq, PartialEq)]
pub struct Kind(pub u32);

// All blocks begin with this header.  The is automatically calculated by
// the cache just before it writes the block to disk.  Similarly the checksum
// will be verified by the cache when it reads the block from disk.
pub struct BlockHeader {
    pub sum: u32,
    pub loc: u32,
    pub kind: Kind,
}

pub const BLOCK_HEADER_SIZE: usize = 16;
pub const BLOCK_PAYLOAD_SIZE: usize = 4096 - BLOCK_HEADER_SIZE;

pub fn read_block_header<R: Read>(r: &mut R) -> Result<BlockHeader> {
    let sum = r.read_u32::<LittleEndian>()?;
    let loc = r.read_u32::<LittleEndian>()?;
    let kind = Kind(r.read_u32::<LittleEndian>()?);
    let _padding = r.read_u32::<LittleEndian>()?;

    Ok(BlockHeader { loc, kind, sum })
}

pub fn write_block_header<W: Write>(w: &mut W, hdr: &BlockHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.sum)?;
    w.write_u32::<LittleEndian>(hdr.loc)?;
    w.write_u32::<LittleEndian>(hdr.kind.0)?;
    w.write_u32::<LittleEndian>(0)?;

    Ok(())
}

fn checksum_(data: &[u8]) -> u32 {
    crc32c(&data[4..]) ^ 0xffffffff
}

fn fail_(msg: String) -> Result<()> {
    Err(io::Error::new(io::ErrorKind::Other, msg))
}

fn get_tid_() -> ThreadId {
    std::thread::current().id()
}

//-------------------------------------------------------------------------

#[derive(Eq, PartialEq)]
enum LockState {
    Unlocked,
    Read(usize),

    // We record the thread id so we can spot dead locks
    Write(ThreadId),
}

struct EntryInner {
    lock: LockState,
    dirty: bool,
    block: Block,
    kind: Kind,
}

struct CacheEntry {
    inner: Mutex<EntryInner>,
    cond: Condvar,
}

impl CacheEntry {
    fn new_read(block: Block, kind: &Kind) -> CacheEntry {
        CacheEntry {
            inner: Mutex::new(EntryInner {
                lock: LockState::Read(1),
                dirty: false,
                block,
                kind: kind.clone(),
            }),
            cond: Condvar::new(),
        }
    }

    fn new_write(block: Block, kind: &Kind) -> CacheEntry {
        CacheEntry {
            inner: Mutex::new(EntryInner {
                lock: LockState::Write(get_tid_()),
                dirty: true,
                block,
                kind: kind.clone(),
            }),
            cond: Condvar::new(),
        }
    }

    fn is_dirty(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.dirty
    }

    fn clear_dirty(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.dirty = false
    }

    fn is_held(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.lock != LockState::Unlocked
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
            Write(_tid) => false,
        }
    }

    // Returns true on success, if false you will need to wait for the lock
    fn write_lock(&self) -> bool {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                inner.lock = Write(get_tid_());
                inner.dirty = true;
                true
            }
            Read(_) => false,
            Write(tid) => {
                if tid == get_tid_() {
                    panic!("thread attempting to lock block {} twice", inner.block.loc);
                }
                false
            }
        }
    }

    fn unlock(&self) {
        use LockState::*;

        let mut inner = self.inner.lock().unwrap();
        match inner.lock {
            Unlocked => {
                panic!("Unlocking an unlocked block {}", inner.block.loc);
            }
            Read(1) => {
                inner.lock = Unlocked;
            }
            Read(n) => {
                inner.lock = Read(n - 1);
            }
            Write(tid) => {
                assert!(tid == get_tid_());
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

#[derive(Debug, PartialEq, Eq)]
pub enum PushResult {
    AlreadyPresent,
    Added,
    AddAndEvict(u32),
}

struct MetadataCacheInner {
    nr_blocks: u32,
    nr_held: usize,
    capacity: usize,
    engine: Arc<dyn IoEngine>,

    // The LRU lists only contain blocks that are not currently locked.
    lru: LinkedHashMap<u32, u32>,
    cache: BTreeMap<u32, Arc<CacheEntry>>,
}
impl MetadataCacheInner {
    pub fn new(engine: Arc<dyn IoEngine>, capacity: usize) -> Result<Self> {
        let nr_blocks = engine.get_nr_blocks() as u32;
        Ok(Self {
            nr_blocks,
            nr_held: 0,
            capacity,
            engine,
            lru: LinkedHashMap::new(),
            cache: BTreeMap::new(),
        })
    }

    pub fn nr_blocks(&self) -> u32 {
        self.nr_blocks
    }

    pub fn nr_held(&self) -> usize {
        self.nr_held
    }

    pub fn residency(&self) -> usize {
        self.lru.len()
    }

    fn lru_push_(&mut self, loc: u32) -> PushResult {
        use PushResult::*;

        if self.lru.contains_key(&loc) {
            AlreadyPresent
        } else if self.lru.len() < self.capacity {
            self.lru.insert(loc, loc);
            Added
        } else {
            let old = self.lru.pop_front().unwrap();
            self.lru.insert(loc, loc);
            AddAndEvict(old.1)
        }
    }

    fn insert_lru_(&mut self, loc: u32) -> Result<()> {
        match self.lru_push_(loc) {
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
        self.lru.remove(&loc);
    }

    fn verify_(&self, block: &Block, kind: &Kind) -> Result<()> {
        let mut r = std::io::Cursor::new(block.get_data());
        let hdr = read_block_header(&mut r)?;

        if hdr.loc != block.loc as u32 {
            return fail_(format!(
                "verify failed: block.loc {} != hdr.loc {}",
                block.loc, hdr.loc
            ));
        }

        if hdr.kind != *kind {
            return fail_(format!(
                "verify failed: hdr.kind {} != kind {}",
                hdr.kind.0, kind.0
            ));
        }

        Ok(())
    }

    fn prep_(&self, block: &Block, kind: &Kind) -> Result<()> {
        let hdr = BlockHeader {
            sum: 0,
            loc: block.loc as u32,
            kind: kind.clone(),
        };

        let mut w = std::io::Cursor::new(block.get_data());
        write_block_header(&mut w, &hdr)?;

        let sum = checksum_(&block.get_data()[4..]);
        let mut w = std::io::Cursor::new(block.get_data());
        w.write_u32::<LittleEndian>(sum)?;

        Ok(())
    }

    fn read_(&mut self, loc: u32, kind: &Kind) -> Result<Block> {
        let block = self.engine.read(loc as u64)?;
        self.verify_(&block, kind)?;
        Ok(block)
    }

    fn writeback_(&self, entry: &CacheEntry) -> Result<()> {
        let inner = entry.inner.lock().unwrap();
        self.prep_(&inner.block, &inner.kind)?;
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
    pub fn read_lock(&mut self, loc: u32, kind: &Kind) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.read_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let entry = Arc::new(CacheEntry::new_read(self.read_(loc, kind)?, kind));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    pub fn write_lock(&mut self, loc: u32, kind: &Kind) -> Result<LockResult> {
        use LockResult::*;

        if let Some(entry) = self.cache.get_mut(&loc).cloned() {
            if entry.write_lock() {
                self.remove_lru_(loc);
                Ok(Locked(entry.clone()))
            } else {
                Ok(Busy(entry.clone()))
            }
        } else {
            let entry = Arc::new(CacheEntry::new_write(self.read_(loc, kind)?, kind));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    ///! Write lock and zero the data (avoids reading the block)
    pub fn zero_lock(&mut self, loc: u32, kind: &Kind) -> Result<LockResult> {
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
            let entry = Arc::new(CacheEntry::new_write(block, kind));
            self.cache.insert(loc, entry.clone());
            Ok(Locked(entry.clone()))
        }
    }

    /// Writeback all dirty blocks
    pub fn flush(&mut self) -> Result<()> {
        for (_loc, entry) in &self.cache {
            if !entry.is_held() && entry.is_dirty() {
                self.writeback_(&*entry)?;
                entry.clear_dirty();
            }
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------

// FIXME: I don't think the proxies should expose the block header
#[derive(Clone)]
pub struct ReadProxy_ {
    pub loc: u32,
    cache: Arc<MetadataCache>,
    entry: Arc<CacheEntry>,
}

impl Drop for ReadProxy_ {
    fn drop(&mut self) {
        self.cache.unlock_(self.loc);
    }
}

#[derive(Clone)]
pub struct ReadProxy {
    proxy: Arc<ReadProxy_>,
    begin: usize,
    end: usize,
}

impl ReadProxy {
    pub fn loc(&self) -> u32 {
        self.proxy.loc
    }
}

impl Readable for ReadProxy {
    fn r(&self) -> &[u8] {
        let inner = self.proxy.entry.inner.lock().unwrap();
        &inner.block.get_data()[self.begin..self.end]
    }

    // FIXME: should split_at consume self?
    fn split_at(&self, offset: usize) -> (Self, Self) {
        assert!(offset < (self.end - self.begin));
        (
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin,
                end: self.begin + offset,
            },
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin + offset,
                end: self.end,
            },
        )
    }
}

//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct WriteProxy_ {
    pub loc: u32,
    cache: Arc<MetadataCache>,
    entry: Arc<CacheEntry>,
}

impl Drop for WriteProxy_ {
    fn drop(&mut self) {
        self.cache.unlock_(self.loc);
    }
}

#[derive(Clone)]
pub struct WriteProxy {
    proxy: Arc<WriteProxy_>,
    begin: usize,
    end: usize,
}

impl WriteProxy {
    pub fn loc(&self) -> u32 {
        self.proxy.loc
    }
}

impl Readable for WriteProxy {
    fn r(&self) -> &[u8] {
        let inner = self.proxy.entry.inner.lock().unwrap();
        &inner.block.get_data()[self.begin..self.end]
    }

    fn split_at(&self, offset: usize) -> (Self, Self) {
        assert!(offset < (self.end - self.begin));
        (
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin,
                end: self.begin + offset,
            },
            Self {
                proxy: self.proxy.clone(),
                begin: self.begin + offset,
                end: self.end,
            },
        )
    }
}

impl Writeable for WriteProxy {
    fn rw(&mut self) -> &mut [u8] {
        let inner = self.proxy.entry.inner.lock().unwrap();
        &mut inner.block.get_data()[self.begin..self.end]
    }
}

//-------------------------------------------------------------------------

pub struct MetadataCache {
    inner: Mutex<MetadataCacheInner>,
}

impl Drop for MetadataCache {
    fn drop(&mut self) {
        self.flush()
            .expect("flush failed when dropping metadata cache");
    }
}

impl MetadataCache {
    pub fn new(engine: Arc<dyn IoEngine>, capacity: usize) -> Result<Self> {
        let inner = MetadataCacheInner::new(engine, capacity)?;
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

    pub fn residency(&self) -> usize {
        let inner = self.inner.lock().unwrap();
        inner.residency()
    }

    pub fn read_lock(self: &Arc<Self>, loc: u32, kind: &Kind) -> Result<ReadProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.read_lock(loc, kind)? {
                Locked(entry) => {
                    let proxy_ = ReadProxy_ {
                        loc,
                        cache: self.clone(),
                        entry: entry.clone(),
                    };

                    let proxy = ReadProxy {
                        proxy: Arc::new(proxy_),
                        begin: 0,
                        end: BLOCK_SIZE,
                    };

                    return Ok(proxy);
                }
                Busy(entry) => self.wait_on_entry_(&*entry),
            }
        }
    }

    pub fn write_lock(self: &Arc<Self>, loc: u32, kind: &Kind) -> Result<WriteProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.write_lock(loc, kind)? {
                Locked(entry) => {
                    let proxy_ = WriteProxy_ {
                        loc,
                        cache: self.clone(),
                        entry: entry.clone(),
                    };

                    let proxy = WriteProxy {
                        proxy: Arc::new(proxy_),
                        begin: 0,
                        end: BLOCK_SIZE,
                    };

                    return Ok(proxy);
                }
                Busy(entry) => self.wait_on_entry_(&*entry),
            }
        }
    }

    ///! Write lock and zero the data (avoids reading the block)
    pub fn zero_lock(self: &Arc<Self>, loc: u32, kind: &Kind) -> Result<WriteProxy> {
        use LockResult::*;

        let mut inner = self.inner.lock().unwrap();

        loop {
            match inner.zero_lock(loc, kind)? {
                Locked(entry) => {
                    let proxy_ = WriteProxy_ {
                        loc,
                        cache: self.clone(),
                        entry: entry.clone(),
                    };

                    let proxy = WriteProxy {
                        proxy: Arc::new(proxy_),
                        begin: 0,
                        end: BLOCK_SIZE,
                    };

                    return Ok(proxy);
                }
                Busy(entry) => self.wait_on_entry_(&*entry),
            }
        }
    }

    /// Writeback all dirty blocks
    pub fn flush(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.flush()
    }

    // for use by the proxies only
    fn unlock_(&self, loc: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.unlock(loc).expect("unlock failed");
    }

    // Do not call this with the top level cache lock held
    fn wait_on_entry_(&self, entry: &CacheEntry) {
        let inner = entry.inner.lock().unwrap();
        let _ = entry.cond.wait(inner);
    }
}

//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::core::*;
    use anyhow::{ensure, Result};
    use std::io;
    use std::sync::Arc;

    fn stamp(data: &mut [u8], byte: u8) -> Result<()> {
        let len = data.len();
        let mut w = io::Cursor::new(data);

        // FIXME: there must be a std function for this
        for _ in 0..len {
            w.write_u8(byte)?;
        }

        Ok(())
    }

    fn verify(data: &[u8], byte: u8) {
        for i in BLOCK_HEADER_SIZE..data.len() {
            assert!(data[i] == byte);
        }
    }

    fn mk_engine(nr_blocks: u32) -> Arc<dyn IoEngine> {
        Arc::new(CoreIoEngine::new(nr_blocks as u64))
    }

    #[test]
    fn test_create() -> Result<()> {
        let engine = mk_engine(16);
        let _cache = Arc::new(MetadataCache::new(engine, 16)?);
        Ok(())
    }

    #[test]
    fn test_new_block() -> Result<()> {
        let engine = mk_engine(16);
        let cache = Arc::new(MetadataCache::new(engine, 16)?);
        let mut wp = cache.zero_lock(0, &Kind(17))?;
        stamp(wp.rw(), 21)?;
        drop(wp);

        cache.flush()?;

        let rp = cache.read_lock(0, &Kind(17))?;

        let data = rp.r();
        verify(data, 21);

        Ok(())
    }

    #[test]
    fn test_rolling_writes() -> Result<()> {
        let nr_blocks = 1024u32;
        let engine = mk_engine(nr_blocks);

        {
            const CACHE_SIZE: usize = 16;
            let cache = Arc::new(MetadataCache::new(engine.clone(), CACHE_SIZE)?);

            for i in 0..nr_blocks {
                let mut wp = cache.zero_lock(i, &Kind(17))?;
                stamp(wp.rw(), i as u8)?;
                ensure!(cache.residency() <= CACHE_SIZE);
            }
        }

        {
            let cache = Arc::new(MetadataCache::new(engine, 16)?);

            for i in 0..nr_blocks {
                let rp = cache.read_lock(i, &Kind(17))?;
                verify(rp.r(), i as u8);
            }
        }

        Ok(())
    }

    #[test]
    fn test_write_twice() -> Result<()> {
        let nr_blocks = 1024u32;
        let engine = mk_engine(nr_blocks);

        {
            const CACHE_SIZE: usize = 16;
            let cache = Arc::new(MetadataCache::new(engine.clone(), CACHE_SIZE)?);

            for i in 0..nr_blocks {
                let mut wp = cache.zero_lock(i, &Kind(17))?;
                stamp(wp.rw(), i as u8)?;
                ensure!(cache.residency() <= CACHE_SIZE);
            }
        }

        {
            const CACHE_SIZE: usize = 16;
            let cache = Arc::new(MetadataCache::new(engine.clone(), CACHE_SIZE)?);

            for i in 0..nr_blocks {
                let mut wp = cache.zero_lock(i, &Kind(17))?;
                stamp(wp.rw(), (i * 3) as u8)?;
                ensure!(cache.residency() <= CACHE_SIZE);
            }
        }

        {
            let cache = Arc::new(MetadataCache::new(engine, 16)?);

            for i in 0..nr_blocks {
                let rp = cache.read_lock(i, &Kind(17))?;
                verify(rp.r(), (i * 3) as u8);
            }
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------
