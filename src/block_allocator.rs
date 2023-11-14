use std::collections::{BTreeMap, VecDeque};
use std::ops::Range;
use std::sync::Arc;

use crate::bitset::*;
use crate::block_cache::*;

//-------------------------------------------------------------------------

pub enum BlockRef {
    Metadata(u32),
    Data(u64),
}

// Metadata blocks
pub trait MetadataOps {
    fn refs(&self, data: &[u8]) -> Vec<BlockRef>;
}

pub struct BlockRegister {
    kinds: BTreeMap<u32, Box<dyn MetadataOps>>,
}

impl BlockRegister {
    pub fn new() -> BlockRegister {
        BlockRegister {
            kinds: BTreeMap::new(),
        }
    }

    pub fn add(&mut self, kind: u32, ops: Box<dyn MetadataOps>) {
        self.kinds.insert(kind, ops);
    }
}

pub struct GCState {
    seen_metadata: Bitset,
    seen_data: Bitset,
    queue: VecDeque<BlockRef>,
}

pub enum GCProgress {
    Incomplete,
    Complete,
}

// FIXME: Instead of RoaringBitmap we should use an on disk bitmap
#[allow(dead_code)]
pub struct BlockAllocator {
    nr_data_blocks: u64,

    metadata_cache: Arc<MetadataCache>,

    // Blocks that are used by _this_ allocator to provide persistence.
    // As such they can be considered 'outside' the transaction.
    reserved_metadata: Bitset,

    allocated_metadata: Bitset,
    allocated_data: Bitset,

    block_register: BlockRegister,
}

// Manages both metadata and data blocks.
impl BlockAllocator {
    pub fn new(metadata_cache: Arc<MetadataCache>, nr_data_blocks: u64) -> BlockAllocator {
        let nr_metadata_blocks = metadata_cache.nr_blocks();

        BlockAllocator {
            nr_data_blocks,
            metadata_cache,
            reserved_metadata: Bitset::new(nr_metadata_blocks as u64),
            allocated_metadata: Bitset::new(nr_metadata_blocks as u64),
            allocated_data: Bitset::new(nr_data_blocks),
            block_register: BlockRegister::new(),
        }
    }

    pub fn allocate_metadata(&mut self) -> Option<u32> {
        self.allocated_metadata.set_first_clear().map(|x| x as u32)
    }

    pub fn allocate_metadata_specific(&mut self, loc: u32) {
        self.allocated_metadata.set(loc as u64);
    }

    pub fn allocate_data(&mut self, region: &Range<u64>) -> Option<u64> {
        self.allocated_data.set_first_clear_in_range(region)
    }

    fn refs(&self, _block: u32) -> Vec<BlockRef> {
        todo!();

        /*
        let data = self.metadata_cache.read_lock(block).unwrap();
        let kind = self.block_register.kinds.get(&block).unwrap();
        kind.refs(&data)
        */
    }

    pub fn gc_quiesce(&mut self) {
        // FIXME: finish
    }

    pub fn gc_resume(&mut self) {
        // FIXME: finish
    }

    pub fn gc_begin<I>(&mut self, roots: I) -> GCState
    where
        I: IntoIterator<Item = u32>,
    {
        let seen_metadata = self.reserved_metadata.clone();
        let seen_data = Bitset::new(self.nr_data_blocks);

        let mut queue: VecDeque<BlockRef> = VecDeque::new();

        // Prepare the queue with the roots.
        for root in roots {
            queue.push_back(BlockRef::Metadata(root));
        }

        GCState {
            seen_metadata,
            seen_data,
            queue,
        }
    }

    pub fn gc_step(&mut self, state: &mut GCState, nr_nodes: usize) -> GCProgress {
        // Traverse the graph.
        for _ in 0..nr_nodes {
            match state.queue.pop_front() {
                Some(BlockRef::Metadata(block)) => {
                    if state.seen_metadata.test_and_set(block as u64) {
                        state.queue.extend(self.refs(block));
                    }
                }
                Some(BlockRef::Data(block)) => {
                    state.seen_data.set(block);
                }
                None => {
                    break;
                }
            }
        }

        if state.queue.is_empty() {
            GCProgress::Complete
        } else {
            GCProgress::Incomplete
        }
    }
}

//-------------------------------------------------------------------------
