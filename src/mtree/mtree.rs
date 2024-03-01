use anyhow::{ensure, Result};
use std::sync::Arc;

use crate::block_cache::MetadataBlock;
use crate::block_kinds::*;
use crate::byte_types::*;
use crate::mtree::diff::*;
use crate::mtree::index::*;
use crate::mtree::node::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockInfo {
    pub loc: u32,
    pub nr_entries: u32,
    pub kbegin: u32,
    pub kend: u32,
}

pub struct MTree {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
    index: Index,
}

impl MTree {
    pub fn empty_tree(tm: Arc<TransactionManager>, context: ReferenceContext) -> Result<Self> {
        let index = Index::new(tm.clone(), context)?;
        Ok(Self { tm, context, index })
    }

    pub fn root(&self) -> u32 {
        self.index.root()
    }

    /// Lookup mappings in the range [kbegin, kend)
    /// FIXME: should we return an interator?
    pub fn lookup(&self, kbegin: u32, kend: u32) -> Result<Vec<(u32, Mapping)>> {
        let locs = self.index.lookup(kbegin, kend)?;

        let mut results = Vec::new();

        let mut first = true;
        for (_, loc) in &locs {
            let b = self.tm.read(*loc, &MNODE_KIND)?;
            let n = r_node(b);

            if first {
                first = false;
                let mut m_idx = n.keys.bsearch(&kbegin);

                if m_idx < 0 {
                    m_idx = 0;
                }

                read_mappings(&n, m_idx as usize, kend, &mut results)?;
            } else {
                read_mappings(&n, 0, kend, &mut results)?;
            }
        }

        Ok(results)
    }

    // Get the key ranges for the given nodes.  This only estimates based on the
    // keys in the nodes.  Other than the final node, it doesn't read the node contents.
    fn get_ranges(&self, kvs: &[KeyValue]) -> Result<Vec<(u32, u32)>> {
        let mut results = Vec::new();

        if kvs.is_empty() {
            return Ok(results);
        }

        for i in 0..kvs.len() - 1 {
            let begin = kvs[i].0;
            let end = kvs[i + 1].0;
            results.push((begin, end));
        }

        // We need to read the last node to get the last key
        let (last_key, last_loc) = kvs.last().unwrap();
        let b = self.tm.read(*last_loc, &MNODE_KIND)?;
        let n = r_node(b);
        if let Some(end) = n.last_key() {
            results.push((*last_key, end));
        }

        Ok(results)
    }

    /// Remove mappings in the range [kbegin, kend), returns the new set of nodes.
    fn remove_(&self, kvs: &[KeyValue], kbegin: u32, kend: u32) -> Result<Vec<KeyValue>> {
        let mut results = Vec::new();
        let ranges = self.get_ranges(kvs)?;

        for i in 0..ranges.len() {
            let r = ranges[i];
            if r.0 >= kbegin && r.1 <= kend {
                // The entire node is within the range
                continue;
            }

            if r.1 <= kbegin || r.0 >= kend {
                // The node is entirely outside the range
                results.push(kvs[i]);
                continue;
            }

            // Shadow the node and remove the range
            let b = self.tm.shadow(self.context, kvs[i].1, &MNODE_KIND)?;
            let mut n = w_node(b);

            remove_range(&mut n, kbegin, kend);

            if n.nr_entries.get() > 0 {
                results.push((n.keys.get(0), n.loc));
            }
        }

        Ok(results)
    }

    /// Returns the number of entries in the node at the given location
    fn nr_in_node(&self, loc: MetadataBlock) -> Result<usize> {
        let b = self.tm.read(loc, &MNODE_KIND)?;
        let n = r_node(b);
        Ok(n.nr_entries.get() as usize)
    }

    /// Merge two nodes if possible, otherwise return the original nodes.
    fn merge_nodes(&self, left: KeyValue, right: KeyValue) -> Result<Vec<KeyValue>> {
        let mut results = Vec::new();

        let nr_left = self.nr_in_node(left.1)?;
        let nr_right = self.nr_in_node(right.1)?;

        if nr_left + nr_right < MAX_ENTRIES {
            let lb = self.tm.shadow(self.context, left.1, &MNODE_KIND)?;
            let mut ln = w_node(lb);
            let mut rn = w_node(self.tm.shadow(self.context, right.1, &MNODE_KIND)?);
            let (keys, mappings) = rn.shift_left(nr_right);
            ln.append(&keys, &mappings);
            results.push((ln.keys.get(0), ln.loc));
        } else {
            results.push(left);
            results.push(right);
        }

        Ok(results)
    }

    fn apply_edits(&mut self, old: &[KeyValue], new: &[KeyValue]) -> Result<()> {
        let edits = calc_edits(&old, &new)?;
        eprintln!("edits = {:?}", edits);

        // FIXME: finish
        /*
        for e in edits {
            match e {
                Edit::RemoveRange(kbegin, kend) => self.index.remove(kbegin, kend)?,
                Edit::Insert(kv) => self.index.insert(kv)?,
            }
        }
        */

        Ok(())
    }

    pub fn remove(&mut self, kbegin: u32, kend: u32) -> Result<()> {
        eprintln!("remove_range({}, {})", kbegin, kend);
        let old = self.index.lookup(kbegin, kend)?;

        if old.is_empty() {
            return Ok(());
        }

        eprintln!("untrimmed infos: {:?}", old);
        let new = self.remove_(&old, kbegin, kend)?;
        eprintln!("trimmed infos: {:?}", new);

        self.apply_edits(&old, &new)?;

        Ok(())
    }

    fn alloc_node(&mut self) -> Result<WNode> {
        let b = self.tm.new_block(self.context, &MNODE_KIND)?;
        init_node(b)
    }

    fn info_from_node<R: Readable>(n: &Node<R>) -> BlockInfo {
        BlockInfo {
            loc: n.loc,
            nr_entries: n.nr_entries.get(),
            kbegin: n.first_key().unwrap(),
            kend: n.last_key().unwrap(),
        }
    }

    pub fn insert(&mut self, kbegin: u32, m: &Mapping) -> Result<()> {
        let kend = kbegin + m.len as u32;
        let old = self.index.lookup_extra(kbegin, kend)?;
        let mut new = self.remove_(&old, kbegin, kend)?;

        if new.is_empty() {
            // Create a new node
            let mut n = self.alloc_node()?;
            n.insert_at(0, kbegin, *m)?;
            new.push((n.keys.get(0), n.loc));
        } else {
            ensure!(new.len() <= 2);
            let (_key, loc) = new[0];

            let left = self.tm.shadow(self.context, loc, &MNODE_KIND)?;
            let mut left_n = w_node(left);
            let nr_entries = left_n.nr_entries.get();

            // Inserting a new mapping can consume 0, 1 or 2 spaces in a node.  We
            // don't want to split a node unless we absolutely need to.  But we also have
            // to examine the node quite closely to work out how much space we need.
            if nr_entries >= MAX_ENTRIES as u32 - 2 {
                eprintln!("splitting");
                todo!();

                /*
                // FIXME: factor out

                let right = self.tm.new_block(&MNODE_KIND)?;
                let mut right_n = w_node(right);
                let nr_right = nr_entries / 2;

                let (ks, ms) = left_n.remove_right(nr_right as usize);
                let r_first_key = ks.get(0).unwrap();
                right_n.prepend(&ks, &ms);

                new.insert(1, (*r_first_key, right_n.loc));
                let r_info = Self::info_from_node(&right_n);
                self.index.insert(&[l_info, r_info])?;

                // FIXME: inefficient
                // retry from the beginning
                drop(left_n);
                drop(right_n);
                return self.insert(kbegin, m);
                */
            }

            let idx = left_n.keys.bsearch(&kbegin);

            // bsearch will have returned the lower bound.  We know there are
            // no overlapping mappings since we remove the range at the start of
            // the insert.  So we can just add one to get the required insert
            // position.
            let idx = (idx + 1) as usize;

            left_n.insert_at(idx as usize, kbegin, *m)?;
            new[0] = (left_n.first_key().unwrap(), left_n.loc);
        }

        self.apply_edits(&old, &new)?;
        Ok(())
    }
}

//-------------------------------------------------------------------------

#[cfg(test)]
mod mtree {
    use super::*;
    use crate::block_allocator::*;
    use crate::block_cache::*;
    use crate::core::*;
    use anyhow::{ensure, Result};
    use rand::prelude::*;
    use rand_chacha::ChaCha20Rng;
    use std::sync::{Arc, Mutex};
    use thinp::io_engine::*;

    use ReferenceContext::*;

    fn mk_engine(nr_blocks: u32) -> Arc<dyn IoEngine> {
        Arc::new(CoreIoEngine::new(nr_blocks as u64))
    }

    fn mk_allocator(
        cache: Arc<MetadataCache>,
        nr_data_blocks: u64,
    ) -> Result<Arc<Mutex<BlockAllocator>>> {
        const SUPERBLOCK_LOCATION: u32 = 0;
        let mut allocator = BlockAllocator::new(cache, nr_data_blocks, SUPERBLOCK_LOCATION)?;
        allocator.reserve_metadata(0)?; // reserve the superblock
        Ok(Arc::new(Mutex::new(allocator)))
    }

    #[allow(dead_code)]
    struct Fixture {
        engine: Arc<dyn IoEngine>,
        cache: Arc<MetadataCache>,
        allocator: Arc<Mutex<BlockAllocator>>,
        tm: Arc<TransactionManager>,
        tree: MTree,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
            let allocator = mk_allocator(cache.clone(), nr_data_blocks)?;
            let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));
            let tree = MTree::empty_tree(tm.clone(), Force)?;

            Ok(Self {
                engine,
                cache,
                allocator,
                tm,
                tree,
            })
        }

        fn check(&self) -> Result<u32> {
            todo!();
            // self.tree.check()
        }

        fn lookup(&self, kbegin: u32, kend: u32) -> Result<Vec<(u32, Mapping)>> {
            self.tree.lookup(kbegin, kend)
        }

        fn insert(&mut self, kbegin: u32, m: &Mapping) -> Result<()> {
            self.tree.insert(kbegin, m)
        }

        fn remove(&mut self, kbegin: u32, kend: u32) -> Result<()> {
            self.tree.remove(kbegin, kend)
        }

        fn commit(&mut self) -> Result<()> {
            let roots = vec![self.tree.root()];
            self.tm.commit(&roots)
        }
    }

    #[test]
    fn empty() -> Result<()> {
        const NR_BLOCKS: u32 = 1024;
        const NR_DATA_BLOCKS: u64 = 102400;

        let mut fix = Fixture::new(NR_BLOCKS, NR_DATA_BLOCKS)?;
        fix.commit()?;

        Ok(())
    }

    #[test]
    fn insert_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let m = Mapping {
            data_begin: 123,
            len: 16,
            time: 0,
        };

        fix.insert(100, &m)?;
        fix.commit()?; // FIXME: shouldn't be needed

        let pairs = fix.lookup(0, 5000)?;
        ensure!(pairs.len() == 1);
        ensure!(pairs[0] == (100, m));

        Ok(())
    }

    //------------------------

    fn gen_non_overlapping_mappings(count: usize) -> Vec<(u32, Mapping)> {
        let mut rng = ChaCha20Rng::seed_from_u64(1);

        let mut lens = Vec::new();
        for _ in 0..count {
            let len = rng.gen_range(16..128);
            lens.push(len);
        }

        let mut results: Vec<(u32, Mapping)> = Vec::new();
        let mut kbegin = 123;
        let mut dbegin = 0;

        for len in lens {
            let m = Mapping {
                data_begin: dbegin,
                len,
                time: 0,
            };
            results.push((kbegin, m));

            kbegin += len as u32;
            dbegin += len as u32;
        }

        results
    }

    #[test]
    fn insert_many_non_overlapping() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let count = 10000;
        let mappings = gen_non_overlapping_mappings(count);

        for (kbegin, m) in &mappings {
            fix.insert(*kbegin, m)?;
        }
        fix.commit()?; // FIXME: shouldn't be needed

        let high_key = mappings.last().unwrap().0 + 1;
        let m2 = fix.lookup(0, high_key)?;
        ensure!(m2 == mappings);

        Ok(())
    }

    //------------------------

    fn insert_entries(fix: &mut Fixture, ranges: &[(u32, u32)]) -> Result<()> {
        let mut data_alloc = 1000;
        for (kbegin, kend) in ranges {
            let m = Mapping {
                data_begin: data_alloc,
                len: (*kend - *kbegin) as u16,
                time: 0,
            };
            eprintln!(">>> fix.insert({}, {})", kbegin, kend);
            fix.insert(*kbegin, &m)?;
            eprintln!("<<< fix.insert({}, {})", kbegin, kend);
            data_alloc += *kend - *kbegin;
        }

        Ok(())
    }

    #[test]
    fn insert_overlapping() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        insert_entries(&mut fix, &[(100, 200), (50, 150)])?;
        fix.commit()?; // FIXME: shouldn't be needed

        let pairs = fix.lookup(0, 5000)?;
        eprintln!("pairs = {:?}", pairs);
        ensure!(pairs.len() == 2);
        ensure!(pairs[0].0 == 50);
        ensure!(pairs[1].0 == 150);

        Ok(())
    }
}

//-------------------------------------------------------------------------
