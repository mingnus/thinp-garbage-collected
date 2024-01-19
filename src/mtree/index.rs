use anyhow::{ensure, Result};

//-------------------------------------------------------------------------

// FIXME: An in core implementation to get the interface right
// FIXME: Very slow

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlockInfo {
    pub loc: u32,
    pub nr_entries: u32,
    pub kbegin: u32,

    // FIXME: do we really need this?  it means we have to update the info in the btree
    // very frequently.
    pub kend: u32,
}

pub struct Index {
    // FIXME: rename to infos
    nodes: Vec<BlockInfo>,
}

#[derive(Eq, PartialEq)]
pub enum InfoResult {
    Update(usize, BlockInfo),
    New(usize),
}

impl Index {
    pub fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub fn root(&self) -> u32 {
        // FIXME: should be the root of the btree that holds the index
        0
    }

    /// Returns the index range for infos that overlap with the given key range.
    fn get_range(&self, kbegin: u32, kend: u32) -> Result<(usize, usize)> {
        let len = self.nodes.len();
        let mut begin = len;

        for i in 0..len {
            let info = &self.nodes[i];
            if info.kend > kbegin {
                begin = i;
                break;
            }
        }

        let mut end = len;
        for i in begin..len {
            let info = &self.nodes[i];
            if info.kbegin >= kend {
                end = i;
                break;
            }
        }

        Ok((begin, end))
    }

    // Returns a (begin, end) pair of indexes to the block infos
    pub fn lookup(&self, kbegin: u32, kend: u32) -> Result<&[BlockInfo]> {
        let (begin, end) = self.get_range(kbegin, kend)?;
        Ok(&self.nodes[begin..end])
    }

    fn insert_at_(&mut self, idx: usize, infos: &[BlockInfo]) -> Result<()> {
        for info in infos.iter().rev() {
            self.nodes.insert(idx, *info);
        }

        Ok(())
    }

    fn overlaps_(infos: &[BlockInfo]) -> bool {
        let mut kend = 0;
        for info in infos {
            if info.kbegin < kend {
                return true;
            }
            assert!(info.kbegin < info.kend);
            kend = info.kend;
        }

        false
    }

    /// Inserts a new sequence of infos.  The infos must be in sequence
    /// and must not overlap with themselves, or the infos already in the
    /// index.
    pub fn insert(&mut self, infos: &[BlockInfo]) -> Result<()> {
        // Validate
        ensure!(!Self::overlaps_(infos));

        // Calculate where to insert
        let idx = self
            .nodes
            .partition_point(|&info| info.kend < infos[0].kbegin);

        // check for overlap at start
        if idx > 0 {
            let prior = &self.nodes[idx - 1];
            ensure!(prior.kend <= infos[0].kbegin);
        }

        // Check for overlap at end
        if idx < self.nodes.len() {
            let next = &self.nodes[idx];
            ensure!(next.kbegin >= infos.last().unwrap().kend);
        }

        self.insert_at_(idx, infos)
    }

    // FIXME: remove
    /// Removes infos that _overlap_ the given key range
    pub fn remove(&mut self, kbegin: u32, kend: u32) -> Result<(usize, Vec<BlockInfo>)> {
        let mut results = Vec::new();
        let (idx_begin, idx_end) = self.get_range(kbegin, kend)?;
        for _ in idx_begin..idx_end {
            results.push(self.nodes.remove(idx_begin));
        }

        Ok((idx_begin, results))
    }

    /// Finds and removes and info where we can insert the given key range.  You will
    /// need to re-insert the info once the mapping block has been updated.
    pub fn info_for_insert(&mut self, kbegin: u32, kend: u32) -> Result<InfoResult> {
        if self.nodes.is_empty() {
            return Ok(InfoResult::New(0));
        }

        // FIXME: we only need idx_begin
        let (mut idx_begin, _) = self.get_range(kbegin, kend)?;
        eprintln!("idx_begin = {}", idx_begin);
        if (idx_begin >= self.nodes.len()) || (self.nodes[idx_begin].kbegin > kend && idx_begin > 0)
        {
            // FIXME: if prior is full consider the next info anyway
            eprintln!("decrementing");
            idx_begin -= 1;
        }

        let info = self.nodes.remove(idx_begin);
        return Ok(InfoResult::Update(idx_begin, info));
    }

    pub fn dump(&self) {
        eprintln!("infos: {:?}", self.nodes);
    }
}

#[cfg(test)]
mod index {
    use super::*;

    #[test]
    fn empty() -> Result<()> {
        let _ = Index::new();
        Ok(())
    }

    #[test]
    fn insert() -> Result<()> {
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
}

//-------------------------------------------------------------------------
