use anyhow::Result;
use std::sync::{Arc, Mutex};

use thinp_garbage_collected::block_allocator::*;
use thinp_garbage_collected::block_cache::*;
use thinp_garbage_collected::btree::*;
use thinp_garbage_collected::transaction_manager::*;

fn main() -> Result<()> {
    let metadata = "./metadata.bin";
    let cache = Arc::new(MetadataCache::new(metadata, 16)?);
    let allocator = Arc::new(Mutex::new(BlockAllocator::new(cache.clone(), 100)));
    let tm = Arc::new(Mutex::new(TransactionManager::new(allocator, cache)));
    let tree = BTree::empty_tree(tm)?;

    println!("created empty tree at {}", tree.root());
    Ok(())
}
