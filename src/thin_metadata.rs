use anyhow::Result;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::block_allocator::BlockAllocator;
use crate::block_cache::*;
// use crate::extent_allocator::ExtentAllocator;
use crate::transaction_manager::TransactionManager;

//-------------------------------------------------------------------------

type ThinID = u64;

struct Pool {
    // data_extent_allocator: ExtentAllocator,
    allocator: BlockAllocator, // This manages both metadata and data blocks.
    cache: Arc<MetadataCache>,
    tm: TransactionManager,
}

struct ThinDevice {
    pool: Arc<Mutex<Pool>>,
    id: ThinID,
}

impl Pool {
    pub fn new<P: AsRef<Path>>(_path: P, _data_block_size: u64, _format: bool) -> Self {
        todo!();
    }

    pub fn open<P: AsRef<Path>>(_path: P, _data_block_size: u64) -> Self {
        todo!();
    }

    pub fn create_thin(&mut self, _dev: ThinID) -> Result<()> {
        todo!();
    }

    pub fn create_snap(&mut self, _dev: ThinID, _origin: ThinID) -> Result<()> {
        todo!();
    }

    pub fn delete_thin(&mut self, _dev: ThinID) -> Result<()> {
        todo!();
    }

    pub fn commit(&mut self) -> Result<()> {
        todo!();
    }

    pub fn abort(&mut self) -> Result<()> {
        todo!();
    }

    pub fn transation_id(&self) -> u64 {
        todo!();
    }

    pub fn set_transaction_id(&mut self, _old_id: u64, _id: u64) -> Result<()> {
        todo!();
    }

    pub fn reserve_metadata_snap(&mut self) -> Result<()> {
        todo!();
    }

    pub fn release_metadata_snap(&mut self) -> Result<()> {
        todo!();
    }

    pub fn get_metadata_snap(&self) -> Result<u64> {
        todo!();
    }

    pub fn open_thin(&self, _dev: ThinID) -> Result<ThinDevice> {
        todo!();
    }

    pub fn changed_this_transaction(&self) -> bool {
        todo!();
    }

    pub fn nr_free_data_blocks(&self) -> Result<u64> {
        todo!();
    }

    pub fn nr_free_metadata_blocks(&self) -> Result<u64> {
        todo!();
    }

    pub fn metadata_dev_size(&self) -> Result<u64> {
        todo!();
    }

    pub fn data_dev_size(&self) -> Result<u64> {
        todo!();
    }

    pub fn resize_data_dev(&mut self, _new_size: u64) -> Result<()> {
        todo!();
    }

    pub fn resize_metadata_dev(&mut self, _new_size: u64) -> Result<()> {
        todo!();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        todo!();
    }
}

struct LookupResult {
    block: u64,
    shared: bool,
}

struct RangeResult {
    thin_begin: u64,
    thin_end: u64,
    pool_begin: u64,
    maybe_shared: bool,
}

impl ThinDevice {
    pub fn get_id(&self) -> ThinID {
        todo!();
    }

    pub fn find_block(&self, _block: u64) -> Result<Option<LookupResult>> {
        todo!();
    }

    pub fn find_mapped_range(&self, _search: Range<u64>) -> Result<Option<RangeResult>> {
        todo!();
    }

    pub fn alloc_data_block(&mut self) -> Result<u64> {
        todo!();
    }

    pub fn insert_data_block(&mut self, _block: u64, _data_block: u64) -> Result<()> {
        todo!();
    }

    pub fn remove_range(&mut self, _thin_blocks: Range<u64>) -> Result<()> {
        todo!();
    }

    pub fn changed_this_transaction(&self) -> bool {
        todo!();
    }

    pub fn aborted_with_changes(&self) -> bool {
        todo!();
    }

    pub fn highest_mapped(&self) -> Result<Option<u64>> {
        todo!();
    }

    pub fn mapping_count(&self) -> Result<u64> {
        todo!();
    }
}
