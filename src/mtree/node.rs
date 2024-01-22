use anyhow::{anyhow, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::mem::size_of;
use std::ops::Range;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

const TIME_BITS: usize = 20;
const LEN_BITS: usize = 12;
const MAPPING_MAX_LEN: usize = 1 << LEN_BITS;
const MAPPING_MAX_TIME: usize = 1 << TIME_BITS;

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
    use anyhow::ensure;
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

#[derive(Copy, Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
pub struct Mapping {
    pub data_begin: u32,
    pub len: u16,
    pub time: u32,
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

pub const MAX_ENTRIES: usize =
    (BLOCK_PAYLOAD_SIZE - (4)) / (size_of::<Mapping>() + size_of::<u32>());

pub struct NodeHeader {
    pub nr_entries: u32,
}

pub fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    Ok(())
}

pub struct Node<Data> {
    // cached copy, doesn't get written to disk
    pub loc: u32,

    pub nr_entries: U32<Data>,

    pub keys: PArray<u32, Data>,
    pub mappings: PArray<Mapping, Data>,
}

impl<Data: Readable> Node<Data> {
    pub fn new(loc: u32, data: Data) -> Self {
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

    pub fn free_space(&self) -> usize {
        todo!();
    }

    pub fn first_key(&self) -> Option<u32> {
        if self.keys.len() == 0 {
            None
        } else {
            Some(self.keys.get(0))
        }
    }

    pub fn dump(&self) {
        let nr_entries = self.nr_entries.get();
        eprintln!("Node: loc = {}, nr_entries = {}", self.loc, nr_entries);
        for i in 0..nr_entries as usize {
            eprintln!("    ({}, {:?})", self.keys.get(i), self.mappings.get(i));
        }
    }
}

impl<Data: Writeable> Node<Data> {
    pub fn insert_at(&mut self, idx: usize, key: u32, value: Mapping) -> Result<()> {
        if self.nr_entries.get() == MAX_ENTRIES as u32 {
            return Err(anyhow!("attempt to insert into full node"));
        }

        self.keys.insert_at(idx, &key);
        self.mappings.insert_at(idx, &value);
        self.nr_entries.inc(1);
        Ok(())
    }

    pub fn remove_at(&mut self, idx: usize) {
        self.keys.remove_at(idx);
        self.mappings.remove_at(idx);
        self.nr_entries.dec(1);
    }

    /// Returns (keys, values) for the entries that have been lost
    pub fn shift_left(&mut self, count: usize) -> (Vec<u32>, Vec<Mapping>) {
        let keys = self.keys.shift_left(count);
        let mappings = self.mappings.shift_left(count);
        self.nr_entries.dec(count as u32);
        (keys, mappings)
    }

    pub fn prepend(&mut self, keys: &[u32], mappings: &[Mapping]) {
        assert!(keys.len() == mappings.len());
        self.keys.prepend(keys);
        self.mappings.prepend(mappings);
        self.nr_entries.inc(keys.len() as u32);
    }

    pub fn append(&mut self, keys: &[u32], mappings: &[Mapping]) {
        self.keys.append(keys);
        self.mappings.append(mappings);
        self.nr_entries.inc(keys.len() as u32);
    }

    pub fn remove_right(&mut self, count: usize) -> (Vec<u32>, Vec<Mapping>) {
        let keys = self.keys.remove_right(count);
        let mappings = self.mappings.remove_right(count);
        self.nr_entries.dec(count as u32);
        (keys, mappings)
    }
}

pub type RNode = Node<ReadProxy>;
pub type WNode = Node<WriteProxy>;

pub fn w_node(block: WriteProxy) -> WNode {
    Node::new(block.loc(), block)
}

pub fn r_node(block: ReadProxy) -> RNode {
    Node::new(block.loc(), block)
}

pub fn init_node(mut block: WriteProxy) -> Result<WNode> {
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

// FIXME: what if we split a single entry into two and there's no space?
pub fn remove_range<W: Writeable>(n: &mut Node<W>, kbegin: u32, kend: u32) {
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

pub fn read_mappings<R: Readable>(
    n: &Node<R>,
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

//-------------------------------------------------------------------------

#[cfg(test)]
mod node {

    use super::*;
    use crate::block_allocator::*;
    use crate::core::*;
    use crate::transaction_manager::*;
    use anyhow::{ensure, Result};
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
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
            let allocator = mk_allocator(cache.clone(), nr_data_blocks)?;
            let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));

            Ok(Self {
                engine,
                cache,
                allocator,
                tm,
            })
        }
    }

    #[test]
    fn empty() -> Result<()> {
        let fix = Fixture::new(1024, 1024)?;
        let b = fix.tm.new_block(&MNODE_KIND)?;
        let n = Node::new(b.loc(), b);
        ensure!(n.nr_entries.get() == 0);

        Ok(())
    }

    #[test]
    fn insert() -> Result<()> {
        let fix = Fixture::new(1024, 1024)?;
        let b = fix.tm.new_block(&MNODE_KIND)?;
        let mut n = Node::new(b.loc(), b);

        let pairs = [
            (
                128,
                Mapping {
                    data_begin: 100,
                    len: 16,
                    time: 0,
                },
            ),
            (
                256,
                Mapping {
                    data_begin: 200,
                    len: 128,
                    time: 1,
                },
            ),
        ];

        for (k, m) in pairs.iter().rev() {
            n.insert_at(0, *k, *m)?;
        }

        let mut pairs2 = Vec::new();
        read_mappings(&n, 0, 100000, &mut pairs2)?;

        ensure!(&pairs[..] == &pairs2);

        Ok(())
    }

    #[test]
    fn insert_when_full() -> Result<()> {
        let fix = Fixture::new(1024, 1024)?;
        let b = fix.tm.new_block(&MNODE_KIND)?;
        let mut n = Node::new(b.loc(), b);

        for i in 0..MAX_ENTRIES {
            n.insert_at(
                i,
                i as u32,
                Mapping {
                    data_begin: i as u32,
                    len: 1,
                    time: 0,
                },
            )?;
        }

        ensure!(n
            .insert_at(
                0,
                0,
                Mapping {
                    data_begin: 0,
                    len: 1,
                    time: 0
                }
            )
            .is_err());

        Ok(())
    }
}

//-------------------------------------------------------------------------
