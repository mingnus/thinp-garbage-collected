use anyhow::Result;
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

    pub fn read_mappings(
        &self,
        idx: usize,
        kend: u32,
        results: &mut Vec<(u32, Mapping)>,
    ) -> Result<()> {
        for i in idx..self.keys.len() {
            let k = self.keys.get(i as usize);
            let m = self.mappings.get(i as usize);

            if k >= kend {
                break;
            }

            results.push((k, m));
        }

        Ok(())
    }
}

impl<Data: Writeable> Node<Data> {
    pub fn insert_at(&mut self, idx: usize, key: u32, value: Mapping) {
        self.keys.insert_at(idx, &key);
        self.mappings.insert_at(idx, &value);
        self.nr_entries.inc(1);
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

    // FIXME: what if we split a single entry into two and there's no space?
    pub fn remove_range(&mut self, kbegin: u32, kend: u32) {
        use TrimOp::*;

        // FIXME: slow, but hopefully correct
        let mut w_idx = 0;

        for r_idx in 0..self.nr_entries.get() as usize {
            let k = self.keys.get(r_idx);
            let mut m = self.mappings.get(r_idx);

            match trim(k..(k + m.len as u32), kbegin..kend) {
                Noop => {
                    self.keys.set(w_idx, &k);
                    self.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
                RemoveAll => { /* do nothing */ }
                RemoveCenter(start, end) => {
                    assert!(self.nr_entries.get() < MAX_ENTRIES as u32);

                    // write a fragment from before the trim range
                    self.keys.set(w_idx, &k);
                    m.len = (start - k) as u16;
                    self.mappings.set(w_idx, &m);
                    w_idx += 1;

                    // write a fragment from after the trim range
                    self.keys.set(w_idx, &end);
                    let delta = end - k;
                    m.data_begin += delta;
                    m.len -= delta as u16;
                    self.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
                RemoveFront(new_start) => {
                    self.keys.set(w_idx, &new_start);
                    let delta = new_start - k;
                    m.data_begin += delta;
                    m.len -= delta as u16;
                    self.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
                RemoveBack(new_end) => {
                    self.keys.set(w_idx, &k);
                    m.len -= ((k + m.len as u32) - new_end) as u16;
                    self.mappings.set(w_idx, &m);
                    w_idx += 1;
                }
            }
        }
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

//-------------------------------------------------------------------------
