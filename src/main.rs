use anyhow::Result;
use std::sync::{Arc, Mutex};
use thinp::io_engine::*;

use thinp_garbage_collected::block_allocator::*;
use thinp_garbage_collected::block_cache::*;
use thinp_garbage_collected::core::*;
use thinp_garbage_collected::mtree::*;
use thinp_garbage_collected::transaction_manager::*;

fn main() -> Result<()> {
    let engine: Arc<dyn IoEngine> = Arc::new(CoreIoEngine::new(1024));
    let cache = Arc::new(MetadataCache::new(engine, 16)?);
    let allocator = Arc::new(Mutex::new(BlockAllocator::new(cache.clone(), 100)?));
    let tm = Arc::new(TransactionManager::new(allocator, cache));
    let mappings = MTree::empty_tree(tm)?;

    println!("created empty tree at {}", mappings.root());
    Ok(())
}
