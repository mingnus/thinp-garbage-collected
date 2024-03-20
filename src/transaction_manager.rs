use anyhow::Result;
use std::collections::BTreeSet;

use crate::block_allocator::*;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::byte_types::*;

use std::sync::{Arc, Mutex};

//------------------------------------------------------------------------------

struct TransactionManager_ {
    // FIXME: I think both of these should support concurrent access
    allocator: Arc<Mutex<BlockAllocator>>,
    cache: Arc<MetadataCache>,
    shadows: BTreeSet<u32>,

    // While a transaction is in progress we must keep the superblock
    // locked to prevent an accidental commit.
    superblock: Option<WriteProxy>,
}

const SUPERBLOCK_LOC: u32 = 0;

impl TransactionManager_ {
    fn new(allocator: Arc<Mutex<BlockAllocator>>, cache: Arc<MetadataCache>) -> Self {
        let superblock = cache.zero_lock(SUPERBLOCK_LOC, &SUPERBLOCK_KIND).unwrap();
        Self {
            allocator,
            cache,
            shadows: BTreeSet::new(),
            superblock: Some(superblock),
        }
    }

    fn pre_commit(&mut self, roots: &[u32]) -> Result<()> {
        {
            let mut allocator = self.allocator.lock().unwrap();

            // quiesce the gc
            allocator.gc_quiesce();
            allocator.set_roots(roots);
        }

        // FIXME: flush the BlockAllocator

        // FIXME: check that only the superblock is held
        self.cache.flush()?;

        Ok(())
    }

    fn commit(&mut self) -> Result<()> {
        // writeback the superblock
        self.superblock = None;
        self.cache.flush()?;

        // set new roots ready for next gc
        // FIXME: finish

        // get superblock for next transaction
        self.superblock = Some(self.cache.write_lock(SUPERBLOCK_LOC, &SUPERBLOCK_KIND)?);

        // clear shadows
        self.shadows.clear();

        // resume the gc
        self.allocator.lock().unwrap().gc_resume();

        Ok(())
    }

    fn abort(&mut self) {}

    fn superblock(&mut self) -> &WriteProxy {
        self.superblock.as_ref().unwrap()
    }

    fn read(&self, loc: u32, kind: &Kind) -> Result<ReadProxy> {
        let b = self.cache.read_lock(loc, kind)?;
        Ok(b)
    }

    fn new_block(&mut self, kind: &Kind) -> Result<WriteProxy> {
        if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata()? {
            let b = self.cache.zero_lock(loc, kind)?;
            self.shadows.insert(loc);
            Ok(b)
        } else {
            // FIXME: I think we need our own error type to distinguish
            // between io error and out of metadata blocks.
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }

    fn shadow(&mut self, old_loc: u32, kind: &Kind) -> Result<WriteProxy> {
        if self.shadows.contains(&old_loc) {
            Ok(self.cache.write_lock(old_loc, kind)?)
        } else if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata()? {
            eprintln!("shadowing {}", old_loc);
            let old = self.cache.read_lock(old_loc, kind)?;
            let mut new = self.cache.zero_lock(loc, kind)?;
            self.shadows.insert(loc);

            // We're careful not to touch the block header
            (&mut new.rw()[BLOCK_HEADER_SIZE..]).copy_from_slice(&old.r()[BLOCK_HEADER_SIZE..]);
            Ok(new)
        } else {
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }

    fn inc_ref(&mut self, loc: u32) {
        self.shadows.remove(&loc);
    }
}

//------------------------------------------------------------------------------

pub struct TransactionManager {
    inner: Mutex<TransactionManager_>,
}

impl TransactionManager {
    pub fn new(allocator: Arc<Mutex<BlockAllocator>>, cache: Arc<MetadataCache>) -> Self {
        Self {
            inner: Mutex::new(TransactionManager_::new(allocator, cache)),
        }
    }

    pub fn pre_commit(&self, roots: &[u32]) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.pre_commit(roots)
    }

    pub fn commit(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.commit()
    }

    pub fn abort(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.abort()
    }

    pub fn superblock(&self) -> WriteProxy {
        let mut inner = self.inner.lock().unwrap();
        inner.superblock().clone()
    }

    pub fn read(&self, loc: u32, kind: &Kind) -> Result<ReadProxy> {
        let inner = self.inner.lock().unwrap();
        inner.read(loc, kind)
    }

    pub fn new_block(&self, kind: &Kind) -> Result<WriteProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.new_block(kind)
    }

    pub fn shadow(&self, loc: u32, kind: &Kind) -> Result<WriteProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.shadow(loc, kind)
    }

    pub fn inc_ref(&self, loc: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.inc_ref(loc)
    }
}

//------------------------------------------------------------------------------
