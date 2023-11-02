use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use roaring::*;
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

#[derive(Clone)]
pub struct ReadProxy {
    pub loc: u32,
    block: Arc<Block>,
}

impl Drop for ReadProxy {
    fn drop(&mut self) {
        todo!();
    }
}

impl std::ops::Deref for ReadProxy {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.block.get_data()
    }
}

impl Readable for ReadProxy {
    fn r(&self) -> &[u8] {
        self.block.get_data()
    }

    fn split_at(&self, loc: usize) -> (ReadProxy, ReadProxy) {
        todo!();
    }
}

//-------------------------------------------------------------------------

#[derive(Clone)]
pub struct WriteProxy {
    pub loc: u32,
    block: Arc<Block>,
}

impl Drop for WriteProxy {
    fn drop(&mut self) {
        todo!();
    }
}

impl std::ops::Deref for WriteProxy {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.block.get_data()
    }
}

impl std::ops::DerefMut for WriteProxy {
    fn deref_mut(&mut self) -> &mut [u8] {
        self.block.get_data()
    }
}

impl Readable for WriteProxy {
    fn r(&self) -> &[u8] {
        self.block.get_data()
    }

    fn split_at(&self, loc: usize) -> (WriteProxy, WriteProxy) {
        todo!();
    }
}

impl Writeable for WriteProxy {
    fn rw(&mut self) -> &mut [u8] {
        self.block.get_data()
    }
}

//-------------------------------------------------------------------------

enum LockState {
    Unlocked,
    Read(usize),
    Write,
}

struct CacheEntry {
    lock: Mutex<LockState>,
    cond: Condvar,
    dirty: bool,
    block: Arc<Block>,
}

impl CacheEntry {
    fn new_read(block: Arc<Block>) -> CacheEntry {
        CacheEntry {
            lock: Mutex::new(LockState::Read(1)),
            cond: Condvar::new(),
            dirty: false,
            block,
        }
    }

    fn new_write(block: Arc<Block>) -> CacheEntry {
        CacheEntry {
            lock: Mutex::new(LockState::Write),
            cond: Condvar::new(),
            dirty: false,
            block,
        }
    }

    fn read_lock(&mut self) {
        use LockState::*;

        let mut lock = self.lock.lock().unwrap();
        loop {
            match *lock {
                Unlocked => {
                    *lock = Read(1);
                    break;
                }
                Read(n) => {
                    *lock = Read(n + 1);
                    break;
                }
                Write => {
                    lock = self.cond.wait(lock).unwrap();
                }
            }
        }
    }

    fn write_lock(&mut self) {
        use LockState::*;

        let mut lock = self.lock.lock().unwrap();
        loop {
            match *lock {
                Unlocked => {
                    *lock = Write;
                    self.dirty = true;
                    break;
                }
                Read(_) => {
                    lock = self.cond.wait(lock).unwrap();
                }
                Write => {
                    lock = self.cond.wait(lock).unwrap();
                }
            }
        }
    }

    fn unlock(&mut self) {
        use LockState::*;

        let mut lock = self.lock.lock().unwrap();
        loop {
            match *lock {
                Unlocked => {
                    panic!("Unlocking an unlocked block");
                }
                Read(1) => {
                    *lock = Unlocked;
                    break;
                }
                Read(n) => {
                    *lock = Read(n - 1);
                    break;
                }
                Write => {
                    *lock = Unlocked;
                    break;
                }
            }
        }
        self.cond.notify_all();
    }
}

//-------------------------------------------------------------------------

struct MetadataCacheInner {
    nr_blocks: u32,
    free_blocks: RoaringBitmap,
    engine: SyncIoEngine,

    lru: LRU,
    cache: BTreeMap<u32, CacheEntry>,
}

impl MetadataCacheInner {
    pub fn new<P: AsRef<Path>>(path: P, capacity: usize) -> Result<Self> {
        let engine = SyncIoEngine::new(path, true)?;
        let nr_blocks = engine.get_nr_blocks() as u32;
        Ok(Self {
            nr_blocks,
            free_blocks: RoaringBitmap::new(),
            engine,
            lru: LRU::with_capacity(capacity),
            cache: BTreeMap::new(),
        })
    }

    pub fn nr_blocks(&self) -> u32 {
        self.nr_blocks
    }

    pub fn nr_held(&self) -> usize {
        self.cache.len()
    }

    fn lookup_(&mut self, loc: u32) -> Option<&mut CacheEntry> {
        if let Some(entry) = self.cache.get_mut(&loc) {
            self.lru.hit(loc);
            return Some(entry);
        }

        None
    }

    fn insert_(&mut self, loc: u32, entry: CacheEntry) {
        use PushResult::*;

        match self.lru.push(loc) {
            AlreadyPresent => {
                panic!("AlreadyPresent")
            }
            Added => {
                self.cache.insert(loc, entry);
            }
            AddAndEvict(old) => {
                let mut old_entry = self.cache.remove(&old).unwrap();
                if old_entry.dirty {
                    self.writeback_(&mut old_entry);
                }
                self.cache.insert(loc, entry);
            }
        }
    }

    fn verify_(&self, _block: &Block) -> Result<()> {
        todo!();
    }

    fn prep_(&self, _block: &mut [u8]) {
        todo!();
    }

    fn read_(&mut self, loc: u32) -> Result<Arc<Block>> {
        let block = self.engine.read(loc as u64)?;
        self.verify_(&block)?;
        Ok(Arc::new(block))
    }

    fn writeback_(&mut self, entry: &mut CacheEntry) -> Result<()> {
        // FIXME: verify this entry is not locked.
        let mut data = entry.block.get_data();
        self.prep_(&mut data);
        self.engine.write(entry.block.as_ref())?;
        Ok(())
    }

    fn writeback_loc_(&mut self, loc: u32) -> Result<()> {
        let entry = self.cache.get_mut(&loc).unwrap();
        let block = entry.block.clone();
        drop(entry); // to lose the mutable borrow

        let mut data = block.get_data();
        self.prep_(&mut data);
        self.engine.write(block.as_ref())?;

        Ok(())
    }

    fn unlock_(&mut self, loc: u32) {
        self.cache.get_mut(&loc).unwrap().unlock();
    }

    pub fn read_lock(&mut self, loc: u32) -> Result<ReadProxy> {
        if let Some(entry) = self.lookup_(loc) {
            entry.read_lock();
            return Ok(ReadProxy {
                loc,
                block: entry.block.clone(),
            });
        } else {
            let entry = CacheEntry::new_read(self.read_(loc)?);
            let block = entry.block.clone();
            self.insert_(loc, entry);
            return Ok(ReadProxy { loc, block });
        }
    }

    pub fn write_lock(&mut self, loc: u32) -> Result<WriteProxy> {
        if let Some(entry) = self.lookup_(loc) {
            entry.write_lock();
            return Ok(WriteProxy {
                loc,
                block: entry.block.clone(),
            });
        } else {
            let entry = CacheEntry::new_write(self.read_(loc)?);
            let block = entry.block.clone();
            self.insert_(loc, entry);
            return Ok(WriteProxy { loc, block });
        }
    }

    ///! Write lock and zero the data (avoids reading the block)
    pub fn zero_lock(&mut self, loc: u32) -> Result<WriteProxy> {
        if let Some(entry) = self.lookup_(loc) {
            entry.write_lock();
            let mut data = entry.block.get_data();
            unsafe {
                std::ptr::write_bytes(&mut data, 0, BLOCK_SIZE);
            }
            return Ok(WriteProxy {
                loc,
                block: entry.block.clone(),
            });
        } else {
            let block = Arc::new(Block::zeroed(loc as u64));
            let entry = CacheEntry::new_write(block.clone());
            self.insert_(loc, entry);
            return Ok(WriteProxy { loc, block });
        }
    }

    /// Writeback all dirty blocks
    pub fn flush(&mut self) {
        let mut writebacks = Vec::new();
        for (loc, entry) in &self.cache {
            if entry.dirty {
                writebacks.push(*loc);
            }
        }

        for loc in writebacks {
            self.writeback_loc_(loc);
        }
    }

    /*
    // For use by the garbage collector only.
    pub fn peek<F, T>(&mut self, loc: u32, op: F)
    with
        F: FnOnce(&mut [u8]) -> Result<T>,
    {
        todo!();

    }
    */
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
        let mut inner = self.inner.lock().unwrap();
        inner.read_lock(loc)
    }

    pub fn write_lock(&self, loc: u32) -> Result<WriteProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.write_lock(loc)
    }

    ///! Write lock and zero the data (avoids reading the block)
    pub fn zero_lock(&self, loc: u32) -> Result<WriteProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.zero_lock(loc)
    }

    /// Writeback all dirty blocks
    pub fn flush(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.flush();
    }

    /*
    // For use by the garbage collector only.
    pub fn peek<F, T>(&mut self, loc: u32, op: F)
    with
        F: FnOnce(&mut [u8]) -> Result<T>,
    {
        todo!();

    }
    */
}
