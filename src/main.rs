use anyhow::Result;
use std::sync::{Arc, Mutex};
use thinp::io_engine::*;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

use thinp_garbage_collected::block_allocator::*;
use thinp_garbage_collected::block_cache::*;
use thinp_garbage_collected::core::*;
use thinp_garbage_collected::mtree::*;
use thinp_garbage_collected::transaction_manager::*;

//-------------------------------------------------------------------------

fn main() -> Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting defualt subscriber failed");

    const SUPERBLOCK_LOCATION: u32 = 0;
    let engine: Arc<dyn IoEngine> = Arc::new(CoreIoEngine::new(1024));
    let cache = Arc::new(MetadataCache::new(engine, 16)?);
    let allocator = Arc::new(Mutex::new(BlockAllocator::new(
        cache.clone(),
        100,
        SUPERBLOCK_LOCATION,
    )?));
    let tm = Arc::new(TransactionManager::new(allocator, cache));
    let _mappings = MTree::empty_tree(tm, ReferenceContext::ThinId(0))?;

    info!("created empty tree");
    Ok(())
}

//-------------------------------------------------------------------------
