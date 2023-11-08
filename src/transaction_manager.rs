use anyhow::Result;
use std::collections::BTreeSet;

use crate::block_allocator::*;
use crate::block_cache::*;
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
        let superblock = cache.write_lock(SUPERBLOCK_LOC).unwrap();
        Self {
            allocator,
            cache,
            shadows: BTreeSet::new(),
            superblock: Some(superblock),
        }
    }

    fn commit_transaction(&mut self) -> Result<()> {
        // quiesce the gc
        self.allocator.lock().unwrap().gc_quiesce();

        // FIXME: check that only the superblock is held
        self.cache.flush();

        // writeback the superblock
        self.superblock = None;
        self.cache.flush();

        // set new roots ready for next gc
        // FIXME: finish

        // get superblock for next transaction
        self.superblock = Some(self.cache.write_lock(SUPERBLOCK_LOC)?);

        // clear shadows
        self.shadows.clear();

        // resume the gc
        self.allocator.lock().unwrap().gc_resume();

        Ok(())
    }

    fn abort_transaction(&mut self) {}

    fn superblock(&mut self) -> &WriteProxy {
        self.superblock.as_ref().unwrap()
    }

    fn read(&self, loc: u32) -> Result<ReadProxy> {
        let b = self.cache.read_lock(loc)?;
        Ok(b)
    }

    fn new_block(&mut self) -> Result<WriteProxy> {
        if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata() {
            let b = self.cache.zero_lock(loc)?;
            self.shadows.insert(loc);
            Ok(b)
        } else {
            // FIXME: I think we need our own error type to distinguish
            // between io error and out of metadata blocks.
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }

    fn shadow(&mut self, loc: u32) -> Result<WriteProxy> {
        if self.shadows.contains(&loc) {
            Ok(self.cache.write_lock(loc)?)
        } else if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata() {
            let old = self.cache.read_lock(loc)?;
            let mut new = self.cache.zero_lock(loc)?;
            self.shadows.insert(loc);
            new.rw().copy_from_slice(old.r());
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

    pub fn commit_transaction(&self) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.commit_transaction()
    }

    pub fn abort_transaction(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.abort_transaction()
    }

    pub fn superblock(&self) -> WriteProxy {
        let mut inner = self.inner.lock().unwrap();
        inner.superblock().clone()
    }

    pub fn read(&self, loc: u32) -> Result<ReadProxy> {
        let inner = self.inner.lock().unwrap();
        inner.read(loc)
    }

    pub fn new_block(&self) -> Result<WriteProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.new_block()
    }

    pub fn shadow(&self, loc: u32) -> Result<WriteProxy> {
        let mut inner = self.inner.lock().unwrap();
        inner.shadow(loc)
    }

    pub fn inc_ref(&self, loc: u32) {
        let mut inner = self.inner.lock().unwrap();
        inner.inc_ref(loc)
    }
}

//------------------------------------------------------------------------------
