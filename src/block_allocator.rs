use anyhow::{anyhow, Result};
use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use crate::bitset::*;
use crate::block_cache::*;
use crate::byte_types::*;

// cyclic dependencies
use crate::btree::*;

//-------------------------------------------------------------------------

pub enum BlockRef {
    Metadata(u32),
    Data(u64),
}

pub struct GCState {
    queue: VecDeque<BlockRef>,
}

#[derive(PartialEq, Eq)]
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
    seen_metadata: Bitset,
    seen_data: Bitset,

    roots: Vec<u32>,
}

// Manages both metadata and data blocks.
impl BlockAllocator {
    pub fn new(
        metadata_cache: Arc<MetadataCache>,
        nr_data_blocks: u64,
        sb_location: u32,
    ) -> Result<BlockAllocator> {
        let nr_metadata_blocks = metadata_cache.nr_blocks();
        let mut reserved_metadata =
            Bitset::bootstrap(metadata_cache.clone(), nr_metadata_blocks, sb_location)?;

        let mut allocated_metadata = Self::create_bitset(
            &metadata_cache,
            &mut reserved_metadata,
            nr_metadata_blocks as u64,
        )?;
        let allocated_data =
            Self::create_bitset(&metadata_cache, &mut reserved_metadata, nr_data_blocks)?;

        let seen_metadata = Self::create_bitset(
            &metadata_cache,
            &mut reserved_metadata,
            nr_metadata_blocks as u64,
        )?;
        let seen_data =
            Self::create_bitset(&metadata_cache, &mut reserved_metadata, nr_data_blocks)?;

        allocated_metadata.copy_bits(&reserved_metadata)?;

        Ok(BlockAllocator {
            nr_data_blocks,
            metadata_cache,
            reserved_metadata,
            allocated_metadata,
            allocated_data,
            seen_metadata,
            seen_data,
            roots: Vec::new(),
        })
    }

    fn create_bitset(
        metadata_cache: &Arc<MetadataCache>,
        reserved_metadata: &mut Bitset,
        nr_bits: u64,
    ) -> Result<Bitset> {
        let blocks_required = Bitset::blocks_required(nr_bits)?;
        let bitset_blocks = (0..blocks_required)
            .map(|_| {
                if let Ok(Some(b)) = reserved_metadata.set_first_clear() {
                    Ok(b as u32)
                } else {
                    Err(anyhow!("insufficient metadata space"))
                }
            })
            .collect::<Result<Vec<u32>>>()?;

        Bitset::new(metadata_cache.clone(), &bitset_blocks, nr_bits)
    }

    pub fn reserve_metadata(&mut self, b: u32) -> Result<()> {
        self.allocated_metadata.set(b as u64)?;
        self.reserved_metadata.set(b as u64)?;
        Ok(())
    }

    pub fn set_roots(&mut self, roots: &[u32]) {
        self.roots = roots.iter().cloned().collect();
    }

    pub fn allocate_metadata(&mut self) -> Result<Option<u32>> {
        let r = self.allocated_metadata.set_first_clear()?.map(|x| x as u32);
        if r.is_some() {
            Ok(r)
        } else {
            self.gc()?;
            Ok(self.allocated_metadata.set_first_clear()?.map(|x| x as u32))
        }
    }

    pub fn allocate_data(&mut self, region: &Range<u64>) -> Result<Option<u64>> {
        self.allocated_data.set_first_clear_in_range(region)
    }

    fn refs(&self, block: u32, queue: &mut VecDeque<BlockRef>) -> Result<()> {
        use crate::block_kinds::*;

        let b = self.metadata_cache.clone().gc_lock(block)?;

        let mut cursor = std::io::Cursor::new(b.r());
        let hdr = read_block_header(&mut cursor)?;

        if hdr.kind == BNODE_KIND {
            btree_refs(&b, queue);
        }

        Ok(())
    }

    pub fn gc(&mut self) -> Result<()> {
        eprintln!("starting gc");
        let mut state = self.gc_begin()?;

        loop {
            eprintln!("step");
            if self.gc_step(&mut state, 1)? == GCProgress::Complete {
                break;
            }
        }

        std::mem::swap(&mut self.allocated_metadata, &mut self.seen_metadata);
        std::mem::swap(&mut self.allocated_data, &mut self.seen_data);
        eprintln!("completed gc");

        Ok(())
    }

    pub fn gc_quiesce(&mut self) {
        // FIXME: finish
    }

    pub fn gc_resume(&mut self) {
        // FIXME: finish
    }

    pub fn gc_begin(&mut self) -> Result<GCState> {
        self.seen_metadata.copy_bits(&self.reserved_metadata)?;
        self.seen_data.clear_all()?;

        let mut queue: VecDeque<BlockRef> = VecDeque::new();

        // Prepare the queue with the roots.
        for root in &self.roots {
            queue.push_back(BlockRef::Metadata(*root));
        }

        Ok(GCState { queue })
    }

    pub fn gc_step(&mut self, state: &mut GCState, nr_nodes: usize) -> Result<GCProgress> {
        // Traverse the graph.
        for _ in 0..nr_nodes {
            match state.queue.pop_front() {
                Some(BlockRef::Metadata(block)) => {
                    eprintln!("gc examining block {}", block);
                    if self.seen_metadata.test_and_set(block as u64)? {
                        self.refs(block, &mut state.queue)?;
                    }
                }
                Some(BlockRef::Data(block)) => {
                    self.seen_data.set(block)?;
                }
                None => {
                    break;
                }
            }
        }

        if state.queue.is_empty() {
            Ok(GCProgress::Complete)
        } else {
            Ok(GCProgress::Incomplete)
        }
    }
}

//-------------------------------------------------------------------------
