use anyhow::{ensure, Result};
use std::sync::Arc;

use crate::block_cache::MetadataBlock;
use crate::btree::btree::*;
use crate::mtree::diff::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct Index {
    tree: BTree<u32>,
}

impl Index {
    pub fn new(tm: Arc<TransactionManager>, context: ReferenceContext) -> Result<Self> {
        let tree = BTree::empty_tree(tm, context)?;
        Ok(Self { tree })
    }

    pub fn root(&self) -> u32 {
        self.tree.root()
    }

    fn get_cursor(&self, kbegin: u32) -> Result<Cursor<u32>> {
        self.tree.cursor(kbegin)
    }

    // Returns a the index entries within the range [kbegin, kend)
    pub fn lookup(&self, kbegin: u32, kend: u32) -> Result<Vec<KeyValue>> {
        let mut cursor = self.get_cursor(kbegin)?;

        let mut results = Vec::new();
        loop {
            if let Some(kv) = cursor.get()? {
                if kv.0 >= kend {
                    break;
                }

                results.push(kv.clone());
            }
            cursor.next_entry()?;
        }

        Ok(results)
    }

    // Returns a range of index entries within the range [kbegin, kend).  In addition
    // it will ensure the first entry has a mapping below kbegin, and the last entry
    // has a mapping above kend.  This is useful if you're replacing a range since it
    // guarantees you have an entry to insert into.
    pub fn lookup_extra(&self, kbegin: u32, kend: u32) -> Result<Vec<KeyValue>> {
        let mut cursor = self.get_cursor(kbegin)?;
        let mut results = Vec::new();

        // do we need to back up?
        if let Some(kv) = cursor.get()? {
            if kv.0 == kbegin && !cursor.is_first() {
                cursor.prev_entry()?;
            }
        }

        loop {
            if let Some(kv) = cursor.get()? {
                if kv.0 > kend {
                    results.push(kv.clone());
                    break;
                }
                results.push(kv.clone());
            }
            cursor.next_entry()?;
        }

        Ok(results)
    }

    pub fn replace(&mut self, old: &[KeyValue], new: &[KeyValue]) -> Result<()> {
        /*
                let edits = calc_edits(old, new)?;
                for edit in edits {
                    match edit {
                        Edit::RemoveRange(kbegin, Some(kend)) => {
                            self.tree.remove_range(kbegin, kend)?;
                        }
                        Edit::Insert((key, value)) => {
                            self.tree.insert(key, &value)?;
                        }
                    }
                }
        */
        Ok(())
    }

    /// Inserts a new sequence of infos.  The infos must be in sequence
    /// and must not overlap with themselves, or the infos already in the
    /// index.
    pub fn insert_many(&mut self, new: &[KeyValue]) -> Result<()> {
        for (key, value) in new {
            self.tree.insert(*key, value)?;
        }

        Ok(())
    }

    pub fn insert(&mut self, new: KeyValue) -> Result<()> {
        self.tree.insert(new.0, &new.1)?;
        Ok(())
    }

    pub fn remove(&mut self, kbegin: u32, kend: u32) -> Result<()> {
        self.tree.remove_range(kbegin, kend)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_allocator::*;
    use crate::block_cache::*;
    use crate::core::*;
    use anyhow::Result;
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
        let allocator = BlockAllocator::new(cache, nr_data_blocks, SUPERBLOCK_LOCATION)?;
        Ok(Arc::new(Mutex::new(allocator)))
    }

    #[allow(dead_code)]
    struct Fixture {
        engine: Arc<dyn IoEngine>,
        cache: Arc<MetadataCache>,
        allocator: Arc<Mutex<BlockAllocator>>,
        tm: Arc<TransactionManager>,
        index: Index,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
            let allocator = mk_allocator(cache.clone(), nr_data_blocks)?;
            let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));
            let index = Index::new(tm.clone(), ReferenceContext::ThinId(0))?;

            Ok(Self {
                engine,
                cache,
                allocator,
                tm,
                index,
            })
        }
    }

    /*
    #[test]
    fn empty() -> Result<()> {
        let mut _fix = Fixture::new(16, 1024)?;
        Ok(())
    }

    #[test]
    fn insert() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let infos = [
            BlockInfo {
                loc: 0,
                nr_entries: 12,
                kbegin: 0,
                kend: 100,
            },
            BlockInfo {
                loc: 1,
                nr_entries: 123,
                kbegin: 200,
                kend: 300,
            },
        ];

        index.insert(&infos)?;
        let infos2 = index.lookup(0, 1000)?;

        ensure!(infos == *infos2);
        Ok(())
    }

    #[test]
    fn inserts_must_be_in_sequence() -> Result<()> {
        let mut index = Index::new();
        let infos = [
            BlockInfo {
                loc: 0,
                nr_entries: 12,
                kbegin: 200,
                kend: 300,
            },
            BlockInfo {
                loc: 1,
                nr_entries: 123,
                kbegin: 0,
                kend: 100,
            },
        ];

        ensure!(index.insert(&infos).is_err());
        Ok(())
    }

    #[test]
    fn inserts_must_not_self_overlap() -> Result<()> {
        let mut index = Index::new();
        let infos = [
            BlockInfo {
                loc: 0,
                nr_entries: 12,
                kbegin: 200,
                kend: 300,
            },
            BlockInfo {
                loc: 1,
                nr_entries: 123,
                kbegin: 250,
                kend: 300,
            },
        ];

        ensure!(index.insert(&infos).is_err());
        Ok(())
    }

    #[test]
    fn inserts_must_not_overlap_preexisting() -> Result<()> {
        let mut index = Index::new();
        let infos = [
            BlockInfo {
                loc: 0,
                nr_entries: 12,
                kbegin: 0,
                kend: 100,
            },
            BlockInfo {
                loc: 1,
                nr_entries: 123,
                kbegin: 200,
                kend: 300,
            },
        ];

        index.insert(&infos)?;

        let infos = [BlockInfo {
            loc: 3,
            nr_entries: 13,
            kbegin: 50,
            kend: 200,
        }];
        ensure!(index.insert(&infos).is_err());

        let infos = [BlockInfo {
            loc: 4,
            nr_entries: 13,
            kbegin: 225,
            kend: 275,
        }];
        ensure!(index.insert(&infos).is_err());

        let infos = [BlockInfo {
            loc: 4,
            nr_entries: 13,
            kbegin: 225,
            kend: 500,
        }];
        ensure!(index.insert(&infos).is_err());

        Ok(())
    }

    #[test]
    fn find_empty() -> Result<()> {
        let mut index = Index::new();
        let r = index.info_for_insert(100, 200)?;
        ensure!(r == InfoResult::New(0));
        Ok(())
    }

    #[test]
    fn find_prior() -> Result<()> {
        let mut index = Index::new();
        let infos = [BlockInfo {
            loc: 0,
            nr_entries: 12,
            kbegin: 100,
            kend: 200,
        }];

        index.insert(&infos)?;
        let r = index.info_for_insert(25, 75)?;
        ensure!(r == InfoResult::Update(0, infos[0].clone()));
        Ok(())
    }

    #[test]
    fn find_post() -> Result<()> {
        let mut index = Index::new();
        let infos = [BlockInfo {
            loc: 0,
            nr_entries: 12,
            kbegin: 100,
            kend: 200,
        }];

        index.insert(&infos)?;
        let r = index.info_for_insert(225, 275)?;
        ensure!(r == InfoResult::Update(0, infos[0].clone()));
        Ok(())
    }

    #[test]
    fn find_between() -> Result<()> {
        let mut index = Index::new();
        let infos = [
            BlockInfo {
                loc: 0,
                nr_entries: 12,
                kbegin: 100,
                kend: 200,
            },
            BlockInfo {
                loc: 1,
                nr_entries: 123,
                kbegin: 500,
                kend: 1000,
            },
        ];

        index.insert(&infos)?;
        let r = index.info_for_insert(300, 400)?;
        ensure!(r == InfoResult::Update(0, infos[0].clone()));
        Ok(())
    }

    #[test]
    fn remove_overlapping() -> Result<()> {
        let mut index = Index::new();
        let infos = [
            BlockInfo {
                loc: 0,
                nr_entries: 12,
                kbegin: 100,
                kend: 200,
            },
            BlockInfo {
                loc: 1,
                nr_entries: 123,
                kbegin: 500,
                kend: 1000,
            },
        ];

        index.insert(&infos)?;
        let (_idx, infos) = index.remove(150, 550)?;
        ensure!(infos.len() == 2);

        Ok(())
    }
    */
}

//-------------------------------------------------------------------------
