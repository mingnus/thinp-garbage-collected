use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Write;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

pub const NODE_HEADER_SIZE: usize = 16;
pub const SPACE_THRESHOLD: usize = 8;

#[derive(Eq, PartialEq)]
pub enum BTreeFlags {
    Internal = 0,
    Leaf = 1,
}

// FIXME: we can pack this more
pub struct NodeHeader {
    flags: u32,
    nr_entries: u32,
    value_size: u16,
}

/// Writes the header of a node to a writer.
pub fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.flags)?;
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    w.write_u16::<LittleEndian>(hdr.value_size)?;

    // Pad out to a 64bit boundary
    w.write_u16::<LittleEndian>(0)?;
    w.write_u32::<LittleEndian>(0)?;

    Ok(())
}

// We need to read the flags to know what sort of node to instance.
pub fn read_flags(r: &[u8]) -> Result<BTreeFlags> {
    use BTreeFlags::*;

    let mut r = &r[BLOCK_HEADER_SIZE..];
    let flags = r.read_u32::<LittleEndian>()?;

    match flags {
        0 => Ok(Internal),
        1 => Ok(Leaf),
        _ => panic!("bad flags"),
    }
}

//-------------------------------------------------------------------------

#[allow(dead_code)]
pub struct Node<V: Serializable, Data: Readable> {
    // We cache a copy of the loc because the underlying proxy isn't available.
    // This doesn't get written to disk.
    pub loc: u32,

    pub flags: U32<Data>,
    pub nr_entries: U32<Data>,
    pub value_size: U16<Data>,

    pub keys: PArray<u32, Data>,
    pub values: PArray<V, Data>,
}

impl<V: Serializable, Data: Readable> Node<V, Data> {
    pub fn max_entries() -> usize {
        (BLOCK_PAYLOAD_SIZE - NODE_HEADER_SIZE)
            / (std::mem::size_of::<u32>() + std::mem::size_of::<V>())
    }

    pub fn new(loc: u32, data: Data) -> Self {
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        let (flags, data) = data.split_at(4);
        let (nr_entries, data) = data.split_at(4);
        let (value_size, data) = data.split_at(2);
        let (_padding, data) = data.split_at(6);
        let (keys, values) = data.split_at(Self::max_entries() * std::mem::size_of::<u32>());

        let flags = U32::new(flags);
        let nr_entries = U32::new(nr_entries);
        let value_size = U16::new(value_size);
        let keys = PArray::new(keys, nr_entries.get() as usize);
        let values = PArray::new(values, nr_entries.get() as usize);

        Self {
            loc,
            flags,
            nr_entries,
            value_size,
            keys,
            values,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.flags.get() == BTreeFlags::Leaf as u32
    }

    pub fn first_key(&self) -> Option<u32> {
        if self.keys.is_empty() {
            None
        } else {
            Some(self.keys.get(0))
        }
    }

    pub fn first(&self) -> Option<(u32, V)> {
        self.keys
            .first()
            .and_then(|k| self.values.first().map(|v| (k, v)))
    }

    pub fn last(&self) -> Option<(u32, V)> {
        self.keys
            .last()
            .and_then(|k| self.values.last().map(|v| (k, v)))
    }

    #[allow(dead_code)]
    pub fn nr_entries(&self) -> usize {
        self.nr_entries.get() as usize
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.nr_entries() == 0
    }
}

impl<V: Serializable, Data: Writeable> Node<V, Data> {
    pub fn insert_at(&mut self, idx: usize, key: u32, value: &V) {
        self.keys.insert_at(idx, &key);
        self.values.insert_at(idx, value);
        self.nr_entries.inc(1);
    }

    pub fn remove_at(&mut self, idx: usize) {
        self.keys.remove_at(idx);
        self.values.remove_at(idx);
        self.nr_entries.dec(1);
    }

    /// Returns (keys, values) for the entries that have been lost
    pub fn shift_left(&mut self, count: usize) -> (Vec<u32>, Vec<V>) {
        let keys = self.keys.shift_left(count);
        let values = self.values.shift_left(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
    }

    pub fn prepend(&mut self, keys: &[u32], values: &[V]) {
        assert!(keys.len() == values.len());
        self.keys.prepend(keys);
        self.values.prepend(values);
        self.nr_entries.inc(keys.len() as u32);
    }

    pub fn append(&mut self, keys: &[u32], values: &[V]) {
        self.keys.append(keys);
        self.values.append(values);
        self.nr_entries.inc(keys.len() as u32);
    }

    pub fn remove_right(&mut self, count: usize) -> (Vec<u32>, Vec<V>) {
        let keys = self.keys.remove_right(count);
        let values = self.values.remove_right(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
    }

    pub fn remove_from(&mut self, idx: usize) {
        self.keys.remove_from(idx);
        self.values.remove_from(idx);
        self.nr_entries.set(idx as u32);
    }
}

// FIXME: remove these, I don't think they add much now it's parameterised by V
pub type RNode<V> = Node<V, ReadProxy>;
pub type WNode<V> = Node<V, WriteProxy>;

//-------------------------------------------------------------------------

pub fn w_node<V: Serializable>(block: WriteProxy) -> WNode<V> {
    Node::new(block.loc(), block)
}

pub fn r_node<V: Serializable>(block: ReadProxy) -> RNode<V> {
    Node::new(block.loc(), block)
}

pub fn init_node<V: Serializable>(mut block: WriteProxy, is_leaf: bool) -> Result<WNode<V>> {
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
            value_size: V::packed_len() as u16,
        },
    )?;

    Ok(w_node(block))
}

//-------------------------------------------------------------------------
