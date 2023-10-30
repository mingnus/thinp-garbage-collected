use anyhow::Result;
use std::collections::BTreeSet;

use crate::block_allocator::*;
use crate::block_cache::*;

use std::sync::{Arc, Mutex};

pub struct TransactionManager {
    // FIXME: I think both of these should support concurrent access
    allocator: Arc<Mutex<BlockAllocator>>,
    cache: Arc<Mutex<MetadataCache>>,
    shadows: BTreeSet<u32>,

    // While a transaction is in progress we must keep the superblock
    // locked to prevent an accidental commit.
    superblock: Option<WriteProxy>,
}

const SUPERBLOCK_LOC: u32 = 0;

impl TransactionManager {
    pub fn new(allocator: Arc<Mutex<BlockAllocator>>, cache: Arc<Mutex<MetadataCache>>) -> Self {
        let superblock = cache.lock().unwrap().write_lock(SUPERBLOCK_LOC).unwrap();
        Self {
            allocator,
            cache,
            shadows: BTreeSet::new(),
            superblock: Some(superblock),
        }
    }

    pub fn commit_transaction(&mut self, superblock: WriteProxy) {
        // quiesce the gc
        self.allocator.lock().unwrap().gc_quiesce();

        // writeback all dirty metadata except the superblock
        let mut cache = self.cache.lock().unwrap();
        // FIXME: check that only the superblock is held
        cache.flush();

        // writeback the superblock
        self.superblock = None;
        cache.flush();

        // set new roots ready for next gc
        todo!();

        // get superblock for next transaction

        // clear shadows

        // resume the gc
        self.allocator.lock().unwrap().gc_resume();

        todo!();
    }

    pub fn abort_transaction(&mut self) {
        todo!();
    }

    pub fn superblock(&mut self) -> &WriteProxy {
        self.superblock.as_ref().unwrap()
    }

    pub fn read(&mut self, loc: u32) -> Result<ReadProxy> {
        let b = self.cache.lock().unwrap().read_lock(loc)?;
        Ok(b)
    }

    pub fn new_block(&mut self) -> Result<WriteProxy> {
        if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata() {
            let b = self.cache.lock().unwrap().zero_lock(loc)?;
            self.shadows.insert(loc);
            Ok(b)
        } else {
            // FIXME: I think we need our own error type to distinguish
            // between io error and out of metadata blocks.
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }

    pub fn shadow(&mut self, loc: u32) -> Result<WriteProxy> {
        if self.shadows.contains(&loc) {
            Ok(self.cache.lock().unwrap().write_lock(loc)?)
        } else if let Some(loc) = self.allocator.lock().unwrap().allocate_metadata() {
            let mut cache = self.cache.lock().unwrap();
            let old = cache.read_lock(loc)?;
            let mut new = cache.zero_lock(loc)?;
            self.shadows.insert(loc);
            new.as_mut().copy_from_slice(old.as_ref());
            Ok(new)
        } else {
            Err(anyhow::anyhow!("out of metadata blocks"))
        }
    }

    pub fn inc_ref(&mut self, loc: u32) {
        self.shadows.remove(&loc);
    }
}
