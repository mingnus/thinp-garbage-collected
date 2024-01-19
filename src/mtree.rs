use anyhow::{ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{BTreeSet, VecDeque};
use std::io::{self, Read, Write};
use std::mem::size_of;
use std::ops::Range;
use std::sync::Arc;

use crate::block_allocator::BlockRef;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

const TIME_BITS: usize = 20;
const LEN_BITS: usize = 12;
const MAPPING_MAX_LEN: usize = 1 << LEN_BITS;
const MAPPING_MAX_TIME: usize = 1 << TIME_BITS;

//-------------------------------------------------------------------------

/// `TrimOp` is an enum representing the different operations that can be performed when trimming a range.
///
/// Variants:
/// * `Noop`: No operation is performed.
/// * `RemoveAll`: The entire range is removed.
/// * `RemoveCenter(u32, u32)`: A sub-range from the center of the range is removed. The two `u32` values
///                             represent the start and end of the sub-range.
/// * `RemoveFront(u32)`: The front part of the range up to the specified `u32` value is removed.
/// * `RemoveBack(u32)`: The back part of the range starting from the specified `u32` value is removed.
#[derive(Eq, PartialEq)]
enum TrimOp {
    Noop,
    RemoveAll,
    RemoveCenter(u32, u32),
    RemoveFront(u32),
    RemoveBack(u32),
}

fn trim(origin: Range<u32>, trim: Range<u32>) -> TrimOp {
    use TrimOp::*;

    if trim.start <= origin.start {
        if trim.end <= origin.start {
            Noop
        } else if trim.end < origin.end {
            RemoveFront(trim.end)
        } else {
            RemoveAll
        }
    } else {
        if trim.end < origin.end {
            RemoveCenter(trim.start, trim.end)
        } else if trim.start < origin.end {
            RemoveBack(trim.start)
        } else {
            Noop
        }
    }
}

#[cfg(test)]
mod trim {
    use super::*;
    use TrimOp::*;

    #[test]
    fn noop() -> Result<()> {
        ensure!(trim(100..200, 25..75) == Noop);
        ensure!(trim(100..200, 25..100) == Noop);
        ensure!(trim(100..200, 200..275) == Noop);
        ensure!(trim(100..200, 225..275) == Noop);

        Ok(())
    }

    #[test]
    fn remove_all() -> Result<()> {
        ensure!(trim(100..200, 50..250) == RemoveAll);
        ensure!(trim(100..200, 50..200) == RemoveAll);
        ensure!(trim(100..200, 100..250) == RemoveAll);
        ensure!(trim(100..200, 100..200) == RemoveAll);
        Ok(())
    }

    #[test]
    fn remove_front() -> Result<()> {
        ensure!(trim(100..200, 50..150) == RemoveFront(150));
        ensure!(trim(100..200, 100..150) == RemoveFront(150));

        Ok(())
    }

    #[test]
    fn remove_center() -> Result<()> {
        ensure!(trim(100..200, 125..175) == RemoveCenter(125, 175));
        Ok(())
    }

    #[test]
    fn remove_back() -> Result<()> {
        ensure!(trim(100..200, 150..250) == RemoveBack(150));
        ensure!(trim(100..200, 150..200) == RemoveBack(150));

        Ok(())
    }
}

//-------------------------------------------------------------------------

// FIXME: An in core implementation to get the interface right
// FIXME: Very slow

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BlockInfo {
    loc: u32,
    nr_entries: u32,
    kbegin: u32,

    // FIXME: do we really need this?  it means we have to update the info in the btree
    // very frequently.
    kend: u32,
}

struct Index {
    // FIXME: rename to infos
    nodes: Vec<BlockInfo>,
}

#[derive(Eq, PartialEq)]
enum InfoResult {
    Update(usize, BlockInfo),
    New(usize),
}

impl Index {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    fn root(&self) -> u32 {
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
    fn lookup(&self, kbegin: u32, kend: u32) -> Result<&[BlockInfo]> {
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
    fn insert(&mut self, infos: &[BlockInfo]) -> Result<()> {
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
    fn remove(&mut self, kbegin: u32, kend: u32) -> Result<(usize, Vec<BlockInfo>)> {
        let mut results = Vec::new();
        let (idx_begin, idx_end) = self.get_range(kbegin, kend)?;
        for _ in idx_begin..idx_end {
            results.push(self.nodes.remove(idx_begin));
        }

        Ok((idx_begin, results))
    }

    /// Finds and removes and info where we can insert the given key range.  You will
    /// need to re-insert the info once the mapping block has been updated.
    fn info_for_insert(&mut self, kbegin: u32, kend: u32) -> Result<InfoResult> {
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

    fn dump(&self) {
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

#[derive(Copy, Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct Mapping {
    data_begin: u32,
    len: u16,
    time: u32,
}

impl Serializable for Mapping {
    fn packed_len() -> usize {
        // Change MAX_ENTRIES if you change the size of this
        8
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        assert!(self.len < MAPPING_MAX_LEN as u16);
        assert!(self.time < MAPPING_MAX_TIME as u32);
        let len_time = ((self.len as u32) << TIME_BITS as u32) | self.time;

        w.write_u32::<LittleEndian>(self.data_begin)?;
        w.write_u32::<LittleEndian>(len_time)?;
        Ok(())
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let data_begin = r.read_u32::<LittleEndian>()?;
        let len_time = r.read_u32::<LittleEndian>()?;

        let len = len_time >> TIME_BITS;
        let time = len_time & ((1 << TIME_BITS) - 1);

        Ok(Self {
            data_begin,
            len: len as u16,
            time: time as u32,
        })
    }
}

//-------------------------------------------------------------------------

const MAX_ENTRIES: usize = (BLOCK_PAYLOAD_SIZE - (4)) / (size_of::<Mapping>() + size_of::<u32>());

struct NodeHeader {
    nr_entries: u32,
}

fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    Ok(())
}

//----------------------------------

struct Node<Data> {
    // cached copy, doesn't get written to disk
    loc: u32,

    nr_entries: U32<Data>,

    keys: PArray<u32, Data>,
    mappings: PArray<Mapping, Data>,
}

impl<Data: Readable> Node<Data> {
    fn new(loc: u32, data: Data) -> Self {
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        let (nr_entries, data) = data.split_at(4);
        let (keys, mappings) = data.split_at(MAX_ENTRIES * 4);

        let nr_entries = U32::new(nr_entries);
        let keys = PArray::new(keys, nr_entries.get() as usize);
        let mappings = PArray::new(mappings, nr_entries.get() as usize);

        Self {
            loc,
            nr_entries,
            keys,
            mappings,
        }
    }

    fn free_space(&self) -> usize {
        todo!();
    }

    fn first_key(&self) -> Option<u32> {
        if self.keys.len() == 0 {
            None
        } else {
            Some(self.keys.get(0))
        }
    }
}

impl<Data: Writeable> Node<Data> {
    fn insert_at(&mut self, idx: usize, key: u32, value: Mapping) {
        self.keys.insert_at(idx, &key);
        self.mappings.insert_at(idx, &value);
        self.nr_entries.inc(1);
    }

    fn remove_at(&mut self, idx: usize) {
        self.keys.remove_at(idx);
        self.mappings.remove_at(idx);
        self.nr_entries.dec(1);
    }

    /// Returns (keys, values) for the entries that have been lost
    fn shift_left(&mut self, count: usize) -> (Vec<u32>, Vec<Mapping>) {
        let keys = self.keys.shift_left(count);
        let mappings = self.mappings.shift_left(count);
        self.nr_entries.dec(count as u32);
        (keys, mappings)
    }

    fn prepend(&mut self, keys: &[u32], mappings: &[Mapping]) {
        assert!(keys.len() == mappings.len());
        self.keys.prepend(keys);
        self.mappings.prepend(mappings);
        self.nr_entries.inc(keys.len() as u32);
    }

    fn append(&mut self, keys: &[u32], mappings: &[Mapping]) {
        self.keys.append(keys);
        self.mappings.append(mappings);
        self.nr_entries.inc(keys.len() as u32);
    }

    fn remove_right(&mut self, count: usize) -> (Vec<u32>, Vec<Mapping>) {
        let keys = self.keys.remove_right(count);
        let mappings = self.mappings.remove_right(count);
        self.nr_entries.dec(count as u32);
        (keys, mappings)
    }
}

type RNode = Node<ReadProxy>;
type WNode = Node<WriteProxy>;

//-------------------------------------------------------------------------

fn w_node(block: WriteProxy) -> WNode {
    Node::new(block.loc(), block)
}

fn r_node(block: ReadProxy) -> RNode {
    Node::new(block.loc(), block)
}

fn init_node(mut block: WriteProxy, is_leaf: bool) -> Result<WNode> {
    todo!();

    /*
    let loc = block.loc();

    // initialise the block
    let mut w = std::io::Cursor::new(block.rw());
    let hdr = BlockHeader {
        loc,
        kind: BNODE_KIND,
        sum: 0,
    };
    write_block_header(&mut w, &hdr)?;

    write_node_header(
        &mut w,
        NodeHeader {
            flags: if is_leaf {
                BTreeFlags::Leaf
            } else {
                BTreeFlags::Internal
            } as u32,
            nr_entries: 0,
        },
    )?;
    drop(w);

    Ok(w_node(block))
    */
}

//-------------------------------------------------------------------------

pub struct MTree {
    tm: Arc<TransactionManager>,
    index: Index,
}

impl MTree {
    /*
    pub fn open_tree(tm: Arc<TransactionManager>, root: u32) -> Self {
        let tree = BTree::open_tree(tm.clone(), root);
        let index = Index {
            tm: tm.clone(),
            tree,
        };
        Self { tm, index }
    }
    */

    pub fn empty_tree(tm: Arc<TransactionManager>) -> Result<Self> {
        let tree = BTree::empty_tree(tm.clone())?;
        let index = Index::new();
        Ok(Self { tm, index })
    }

    pub fn root(&self) -> u32 {
        self.index.root()
    }

    fn read_mappings(
        &self,
        n: &RNode,
        idx: usize,
        kend: u32,
        results: &mut Vec<(u32, Mapping)>,
    ) -> Result<()> {
        for i in idx..n.keys.len() {
            let k = n.keys.get(i as usize);
            let m = n.mappings.get(i as usize);

            if k >= kend {
                break;
            }

            results.push((k, m));
        }

        Ok(())
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

                self.read_mappings(&n, m_idx as usize, kend, &mut results)?;
            } else {
                self.read_mappings(&n, 0, kend, &mut results)?;
            }
        }

        Ok(results)
    }

    // FIXME: what if we split a single entry into two and there's no space?
    fn n_remove_range(n: &mut WNode, kbegin: u32, kend: u32) {
        use TrimOp::*;

        // FIXME: slow, but hopefully correct
        let mut w_idx = 0;

        for r_idx in 0..n.nr_entries.get() as usize {
            let k = n.keys.get(r_idx);
            let mut m = n.mappings.get(r_idx);

            match trim(k..(k + m.len as u32), kbegin..kend) {
                Noop => {
                    n.keys.set(w_idx, &k);
                    n.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
                RemoveAll => { /* do nothing */ }
                RemoveCenter(start, end) => {
                    assert!(n.nr_entries.get() < MAX_ENTRIES as u32);

                    // write a fragment from before the trim range
                    n.keys.set(w_idx, &k);
                    m.len = (start - k) as u16;
                    n.mappings.set(w_idx, &m);
                    w_idx += 1;

                    // write a fragment from after the trim range
                    n.keys.set(w_idx, &end);
                    let delta = end - k;
                    m.data_begin += delta;
                    m.len -= delta as u16;
                    n.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
                RemoveFront(new_start) => {
                    n.keys.set(w_idx, &new_start);
                    let delta = new_start - k;
                    m.data_begin += delta;
                    m.len -= delta as u16;
                    n.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
                RemoveBack(new_end) => {
                    n.keys.set(w_idx, &k);
                    m.len -= ((k + m.len as u32) - new_end) as u16;
                    n.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
            }
        }
    }

    fn trim_infos(&self, infos: &[BlockInfo], kbegin: u32, kend: u32) -> Result<Vec<BlockInfo>> {
        eprintln!("trim_infos");
        let mut results = Vec::new();

        for info in infos {
            let b = self.tm.shadow(info.loc, &MNODE_KIND)?;
            let mut n = w_node(b);

            eprintln!(">>> n_remove_range");
            Self::n_remove_range(&mut n, kbegin, kend);
            eprintln!("<<< n_remove_range");
            let nr_entries = n.nr_entries.get();
            if nr_entries > 0 {
                results.push(BlockInfo {
                    loc: n.loc,
                    nr_entries,
                    kbegin: n.keys.get(0),
                    kend: n.keys.get(nr_entries as usize - 1),
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

    fn remove_range(&mut self, kbegin: u32, kend: u32) -> Result<()> {
        eprintln!("remove_range({}, {})", kbegin, kend);
        let (idx_begin, infos) = self.index.remove(kbegin, kend)?;

        if infos.is_empty() {
            eprintln!("no infos");
            return Ok(());
        }

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

    fn init_node(mut block: WriteProxy) -> Result<WNode> {
        let loc = block.loc();

        let mut w = std::io::Cursor::new(block.rw());
        let hdr = BlockHeader {
            loc,
            kind: MNODE_KIND,
            sum: 0,
        };
        write_block_header(&mut w, &hdr)?;

        write_node_header(&mut w, NodeHeader { nr_entries: 0 })?;
        drop(w);

        Ok(w_node(block))
    }

    fn alloc_node(&mut self) -> Result<WNode> {
        let b = self.tm.new_block(&MNODE_KIND)?;
        Self::init_node(b)
    }

    pub fn insert(&mut self, kbegin: u32, m: &Mapping) -> Result<()> {
        let kend = kbegin + m.len as u32;

        self.remove_range(kbegin, kend)?;

        match self.index.info_for_insert(kbegin, kbegin + m.len as u32)? {
            InfoResult::New(_info_idx) => {
                eprintln!("empty branch");
                // Create a new node
                let mut n = self.alloc_node()?;
                let loc = n.loc;
                n.insert_at(0, kbegin, *m);

                let info = BlockInfo {
                    loc,
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

                if nr_entries == MAX_ENTRIES as u32 {
                    eprintln!("splitting");
                    // split the node
                    todo!();
                } else {
                    for i in 0..left_n.keys.len() {
                        eprintln!("key[{}] = {}", i, left_n.keys.get(i));
                    }

                    let idx = left_n.keys.bsearch(&kbegin);
                    eprintln!("bsearch returned: {}, searching for {}", idx, kbegin);

                    // bsearch will have returned the lower bound.  We know there are
                    // no overlapping mappings since we remove the range at the start of
                    // the insert.  So we can just add one to get the required insert
                    // position.
                    let idx = (idx + 1) as usize;

                    left_n.insert_at(idx as usize, kbegin, *m);
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
        let mut allocator = BlockAllocator::new(cache, nr_data_blocks)?;
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
            self.tree.remove_range(kbegin, kend)
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

        let count = 10;
        let mappings = gen_non_overlapping_mappings(count);
        eprintln!("mappings: {:?}", mappings);

        for (kbegin, m) in mappings {
            eprintln!(">>> inserting: ({}, {:?})", kbegin, m);
            fix.insert(kbegin, &m)?;
            eprintln!("<<< inserting");
        }
        fix.commit()?; // FIXME: shouldn't be needed

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
