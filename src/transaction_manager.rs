use crate::block_manager::*;
use crate::block_allocator::*;

use std::sync::{Arc, Mutex, RwLock};

struct TransactionManager {
    allocator: Arc<Mutex<BlockAllocator>>,
    cache: Arc<Mutex<BlockCache>>,
}

const SUPERBLOCK_LOC: u64 = 0;

impl TransactionManager {
    pub fn new(allocator: Arc<Mutex<BlockAllocator>>, cache: Arc<Mutex<BlockCache>>) -> TransactionManager {
        TransactionManager {
            allocator,
            cache,
        }
    }

    pub fn commit_transaction(&mut self, superblock: WriteProxy) {
        todo!();
    }

    pub fn abort_transaction(&mut self) {
        todo!();
    }

    pub fn superblock(&mut self) -> Arc<RwLock<WriteProxy>> {
        todo!();
    }

    pub fn read(&mut self, loc: u64) -> ReadProxy {
        todo!();
    }

    pub fn new_block(&mut self) -> WriteProxy {
        todo!();
    }

    pub fn shadow(&mut self, loc: u64) -> WriteProxy {
        todo!();
    }

    pub fn inc_ref(&mut self, loc: u64) {
        todo!();
    }

    pub fn add_root(&mut self, loc: u64) {
        todo!();
    }
}