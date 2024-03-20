use anyhow::Result;
use std::sync::Arc;

use crate::block_kinds::*;
use crate::byte_types::*;
use crate::mtree::index::*;
use crate::mtree::node::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct MTree {
    tm: Arc<TransactionManager>,
    index: Index,
}

impl MTree {
    pub fn empty_tree(tm: Arc<TransactionManager>) -> Result<Self> {
        let index = Index::new();
        Ok(Self { tm, index })
    }

    pub fn root(&self) -> u32 {
        self.index.root()
    }

    pub fn lookup(&self, kbegin: u32, kend: u32) -> Result<Vec<(u32, Mapping)>> {
        let infos = self.index.lookup(kbegin, kend)?;

        let mut results = Vec::new();

        let mut first = true;
        for info in infos {
            let b = self.tm.read(info.loc, &MNODE_KIND)?;
            let n = r_node(b);

            if first {
                first = false;
                eprintln!("nr entries = {}", n.keys.len());
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

    // FIXME: this also modifies the underlying nodes.  Rename.
    fn trim_infos(&self, infos: &[BlockInfo], kbegin: u32, kend: u32) -> Result<Vec<BlockInfo>> {
        eprintln!("trim_infos");
        let mut results = Vec::new();

        for info in infos {
            let b = self.tm.shadow(info.loc, &MNODE_KIND)?;
            let mut n = w_node(b);

            remove_range(&mut n, kbegin, kend);

            let nr_entries = n.nr_entries.get();
            if nr_entries > 0 {
                let idx = nr_entries as usize - 1;
                results.push(BlockInfo {
                    loc: n.loc,
                    nr_entries,
                    kbegin: n.keys.get(0),
                    kend: n.keys.get(idx) + n.mappings.get(idx).len as u32,
                })
            }
        }

        Ok(results)
    }

    fn coalesce_infos(&self, left: &BlockInfo, right: &BlockInfo) -> Result<Vec<BlockInfo>> {
        let mut results = Vec::new();

        if left.nr_entries + right.nr_entries < MAX_ENTRIES as u32 {
            let lb = self.tm.shadow(left.loc, &MNODE_KIND)?;
            let loc = lb.loc();
            let mut ln = w_node(lb);
            let mut rn = w_node(self.tm.shadow(right.loc, &MNODE_KIND)?);
            let (keys, mappings) = rn.shift_left(right.nr_entries as usize);
            ln.append(&keys, &mappings);
            results.push(BlockInfo {
                loc,
                nr_entries: left.nr_entries + right.nr_entries,
                kbegin: left.kbegin,
                kend: right.kend,
            });
        } else {
            results.push(left.clone());
            results.push(right.clone());
        }

        Ok(results)
    }

    pub fn remove(&mut self, kbegin: u32, kend: u32) -> Result<()> {
        eprintln!("remove_range({}, {})", kbegin, kend);
        let (_idx_begin, infos) = self.index.remove(kbegin, kend)?;

        if infos.is_empty() {
            eprintln!("no infos");
            return Ok(());
        }

        eprintln!("untrimmed infos: {:?}", infos);
        let infos = self.trim_infos(&infos, kbegin, kend)?;
        eprintln!("trimmed infos: {:?}", infos);

        match infos.len() {
            0 => Ok(()),
            1 => self.index.insert(&infos),
            2 => {
                let infos = self.coalesce_infos(&infos[0], &infos[1])?;
                self.index.insert(&infos)
            }
            _ => panic!("shouldn't be more than 2 nodes"),
        }
    }

    fn alloc_node(&mut self) -> Result<WNode> {
        let b = self.tm.new_block(&MNODE_KIND)?;
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

        self.remove(kbegin, kend)?;

        match self.index.info_for_insert(kbegin, kbegin + m.len as u32)? {
            InfoResult::New(_info_idx) => {
                eprintln!("empty branch");
                // Create a new node
                let mut n = self.alloc_node()?;
                n.insert_at(0, kbegin, *m)?;

                let info = BlockInfo {
                    loc: n.loc,
                    nr_entries: 1,
                    kbegin,
                    kend: kbegin + m.len as u32,
                };
                let infos = [info];
                // FIXME: we know where this should be inserted
                self.index.insert(&infos)?;
            }
            InfoResult::Update(_info_idx, mut info) => {
                eprintln!("non empty branch");
                let left = self.tm.shadow(info.loc, &MNODE_KIND)?;
                let mut left_n = w_node(left);
                let nr_entries = left_n.nr_entries.get();

                // Inserting a new mapping can consume 0, 1 or 2 spaces in a node.  We
                // don't want to split a node if we absolutely need to.  But we also have
                // to examine the node quite closely to work out how much space we need.

                if nr_entries == MAX_ENTRIES as u32 {
                    eprintln!("splitting");
                    // FIXME: factor out

                    let right = self.tm.new_block(&MNODE_KIND)?;
                    let mut right_n = w_node(right);
                    let nr_right = nr_entries / 2;

                    let (ks, ms) = left_n.remove_right(nr_right as usize);
                    right_n.prepend(&ks, &ms);

                    // Rejig infos
                    let l_info = Self::info_from_node(&left_n);
                    let r_info = Self::info_from_node(&right_n);
                    self.index.insert(&[l_info, r_info])?;

                    // FIXME: inefficient
                    // retry from the beginning
                    drop(left_n);
                    drop(right_n);
                    return self.insert(kbegin, m);
                } else {
                    let idx = left_n.keys.bsearch(&kbegin);
                    eprintln!("bsearch returned: {}, searching for {}", idx, kbegin);

                    // bsearch will have returned the lower bound.  We know there are
                    // no overlapping mappings since we remove the range at the start of
                    // the insert.  So we can just add one to get the required insert
                    // position.
                    let idx = (idx + 1) as usize;

                    left_n.insert_at(idx as usize, kbegin, *m)?;
                    info.nr_entries += 1;
                    info.kbegin = left_n.keys.get(0);
                    let last_idx = info.nr_entries as usize - 1;
                    info.kend =
                        left_n.keys.get(last_idx) + left_n.mappings.get(last_idx).len as u32;
                    eprintln!("m = {:?}", left_n.mappings.get(last_idx));
                    eprintln!("info = {:?}", info);

                    let infos = [info];
                    // FIXME: again, we already know where this should be inserted
                    self.index.insert(&infos)?;
                }
            }
        }

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
            let tree = MTree::empty_tree(tm.clone())?;

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
            self.tm.pre_commit(&roots)?;
            self.tm.commit()
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
