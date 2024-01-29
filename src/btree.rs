use anyhow::{ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{BTreeSet, VecDeque};
use std::io::Write;
use std::sync::Arc;

use crate::block_allocator::BlockRef;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::spine::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

const NODE_HEADER_SIZE: usize = 16;
const SPACE_THRESHOLD: usize = 8;

#[derive(Eq, PartialEq)]
enum BTreeFlags {
    Internal = 0,
    Leaf = 1,
}

// FIXME: we can pack this more
struct NodeHeader {
    flags: u32,
    nr_entries: u32,
    value_size: u16,
}

fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.flags)?;
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    w.write_u16::<LittleEndian>(hdr.value_size)?;

    // Pad out to a 64bit boundary
    w.write_u16::<LittleEndian>(0)?;
    w.write_u32::<LittleEndian>(0)?;

    Ok(())
}

// We need to read the flags to know what sort of node to instance.
fn read_flags(r: &[u8]) -> Result<BTreeFlags> {
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

pub struct Node<V: Serializable, Data: Readable> {
    // We cache a copy of the loc because the underlying proxy isn't available.
    // This doesn't get written to disk.
    loc: u32,

    flags: U32<Data>,
    nr_entries: U32<Data>,
    value_size: U16<Data>,

    keys: PArray<u32, Data>,
    values: PArray<V, Data>,
}

impl<V: Serializable, Data: Readable> Node<V, Data> {
    fn max_entries() -> usize {
        (BLOCK_PAYLOAD_SIZE - NODE_HEADER_SIZE)
            / (std::mem::size_of::<u32>() + std::mem::size_of::<V>())
    }

    fn new(loc: u32, data: Data) -> Self {
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

    fn is_leaf(&self) -> bool {
        self.flags.get() == BTreeFlags::Leaf as u32
    }

    fn first_key(&self) -> Option<u32> {
        if self.keys.len() == 0 {
            None
        } else {
            Some(self.keys.get(0))
        }
    }
}

impl<V: Serializable, Data: Writeable> Node<V, Data> {
    fn insert_at(&mut self, idx: usize, key: u32, value: &V) {
        self.keys.insert_at(idx, &key);
        self.values.insert_at(idx, value);
        self.nr_entries.inc(1);
    }

    fn remove_at(&mut self, idx: usize) {
        self.keys.remove_at(idx);
        self.values.remove_at(idx);
        self.nr_entries.dec(1);
    }

    /// Returns (keys, values) for the entries that have been lost
    fn shift_left(&mut self, count: usize) -> (Vec<u32>, Vec<V>) {
        let keys = self.keys.shift_left(count);
        let values = self.values.shift_left(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
    }

    fn prepend(&mut self, keys: &[u32], values: &[V]) {
        assert!(keys.len() == values.len());
        self.keys.prepend(keys);
        self.values.prepend(values);
        self.nr_entries.inc(keys.len() as u32);
    }

    fn append(&mut self, keys: &[u32], values: &[V]) {
        self.keys.append(keys);
        self.values.append(values);
        self.nr_entries.inc(keys.len() as u32);
    }

    fn remove_right(&mut self, count: usize) -> (Vec<u32>, Vec<V>) {
        let keys = self.keys.remove_right(count);
        let values = self.values.remove_right(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
    }
}

// FIXME: remove these, I don't think they add much now it's parameterised by V
type RNode<V> = Node<V, ReadProxy>;
type WNode<V> = Node<V, WriteProxy>;

//-------------------------------------------------------------------------

fn w_node<V: Serializable>(block: WriteProxy) -> WNode<V> {
    Node::new(block.loc(), block)
}

fn r_node<V: Serializable>(block: ReadProxy) -> RNode<V> {
    Node::new(block.loc(), block)
}

fn init_node<V: Serializable>(mut block: WriteProxy, is_leaf: bool) -> Result<WNode<V>> {
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
    drop(w);

    Ok(w_node(block))
}

//-------------------------------------------------------------------------

// Insert and remove both have similarly named utilities, so we keep them
// in separate modules.
mod insert_utils {
    use super::*;

    fn redistribute2<V: Serializable>(left: &mut WNode<V>, right: &mut WNode<V>) {
        let nr_left = left.nr_entries.get() as usize;
        let nr_right = right.nr_entries.get() as usize;
        let total = nr_left + nr_right;
        let target_left = total / 2;

        if nr_left < target_left {
            // Move entries from right to left
            let nr_move = target_left - nr_left;
            let (keys, values) = right.shift_left(nr_move);
            left.append(&keys, &values);
        } else if nr_left > target_left {
            // Move entries from left to right
            let nr_move = nr_left - target_left;
            let (keys, values) = left.remove_right(nr_move);
            right.prepend(&keys, &values);
        }
    }

    /// Redistribute entries between three node.  Assumes the central
    /// node is empty.
    fn redistribute3<V: Serializable>(
        left: &mut WNode<V>,
        middle: &mut WNode<V>,
        right: &mut WNode<V>,
    ) {
        let nr_left = left.nr_entries.get() as usize;
        let nr_middle = middle.nr_entries.get() as usize;
        assert!(nr_middle == 0);
        let nr_right = right.nr_entries.get() as usize;

        let total = nr_left + nr_middle + nr_right;
        let target_left = total / 3;
        let target_middle = (total - target_left) / 2;

        if nr_left < target_left {
            // Move entries from right to left
            let nr_move = target_left - nr_left;
            let (keys, values) = right.shift_left(nr_move);
            left.append(&keys, &values);

            // Move entries from right to middle
            let nr_move = target_middle;
            let (keys, values) = right.shift_left(nr_move);
            middle.append(&keys, &values);
        } else if nr_left < (target_left + target_middle) {
            // Move entries from left to middle
            let nr_move = nr_left - target_left;
            let (keys, values) = left.remove_right(nr_move);
            middle.prepend(&keys, &values);

            // Move entries from right to middle
            let nr_move = target_middle - nr_move;
            let (keys, values) = right.shift_left(nr_move);
            middle.append(&keys, &values);
        } else {
            // Move entries from left to right
            let nr_move = nr_left - target_left - target_middle;
            let (keys, values) = left.remove_right(nr_move);
            right.prepend(&keys, &values);

            // Move entries from left to middle
            let nr_move = nr_middle;
            let (keys, values) = left.remove_right(nr_move);
            middle.prepend(&keys, &values);
        }
    }

    fn has_space_for_insert<NV: Serializable, Data: Readable>(node: &Node<NV, Data>) -> bool {
        node.nr_entries.get() < Node::<NV, Data>::max_entries() as u32
    }

    fn split_beneath<NV: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
        let mut new_parent = w_node::<NV>(spine.child());
        let nr_left = (new_parent.nr_entries.get() / 2) as usize;
        let (lkeys, lvalues) = new_parent.shift_left(nr_left);
        let (rkeys, rvalues) = new_parent.shift_left(new_parent.nr_entries.get() as usize);

        let mut left = init_node(spine.new_block()?, new_parent.is_leaf())?;
        left.append(&lkeys, &lvalues);

        let mut right = init_node(spine.new_block()?, new_parent.is_leaf())?;
        right.append(&rkeys, &rvalues);

        // setup the parent to point to the two new children
        let mut new_parent = w_node::<MetadataBlock>(spine.child());
        new_parent.flags.set(BTreeFlags::Internal as u32);
        assert!(new_parent.keys.len() == 0);
        assert!(new_parent.values.len() == 0);
        new_parent.append(&vec![lkeys[0], rkeys[0]], &vec![left.loc, right.loc]);

        // Choose the correct child in the spine
        let left_loc = left.loc;
        let right_loc = right.loc;
        drop(left);
        drop(right);

        if key < rkeys[0] {
            spine.push(left_loc)?;
        } else {
            spine.push(right_loc)?;
        }

        Ok(())
    }

    fn rebalance_left<V: Serializable>(
        spine: &mut Spine,
        parent_idx: usize,
        key: u32,
    ) -> Result<()> {
        let mut parent = w_node(spine.parent());
        let left_loc = parent.values.get(parent_idx - 1);
        let left_block = spine.shadow(left_loc)?;
        let mut left = w_node::<V>(left_block.clone());
        let mut right = w_node::<V>(spine.child());
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let first_key = right.first_key().unwrap();
        parent.keys.set(parent_idx, &first_key);

        // Choose the correct child in the spine
        if key < first_key {
            spine.replace_child(left_block);
        }

        Ok(())
    }

    fn rebalance_right<V: Serializable>(
        spine: &mut Spine,
        parent_idx: usize,
        key: u32,
    ) -> Result<()> {
        let mut parent = w_node(spine.parent());
        let right_loc = parent.values.get(parent_idx + 1);
        let right_block = spine.shadow(right_loc)?;
        let mut right = w_node::<V>(right_block.clone());
        let mut left = w_node::<V>(spine.child());
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let first_key = right.first_key().unwrap();
        parent.keys.set(parent_idx + 1, &first_key);

        // Choose the correct child in the spine
        if key >= first_key {
            spine.replace_child(right_block);
        }

        Ok(())
    }

    fn get_node_free_space<V: Serializable>(node: &RNode<V>) -> usize {
        Node::<V, ReadProxy>::max_entries() - node.nr_entries.get() as usize
    }

    fn get_loc_free_space<V: Serializable>(tm: &TransactionManager, loc: u32) -> usize {
        let block = tm.read(loc, &BNODE_KIND).unwrap();
        let node = r_node::<V>(block);
        get_node_free_space(&node)
    }

    fn split_into_two<V: Serializable>(spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut left = w_node::<V>(spine.child());
        let right_block = spine.new_block()?;
        let mut right = init_node(right_block.clone(), left.is_leaf())?;
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let mut parent = w_node(spine.parent());

        let first_key = right.first_key().unwrap();
        parent.keys.insert_at(idx + 1, &first_key);
        parent.values.insert_at(idx + 1, &right.loc);
        parent.nr_entries.inc(1);

        // Choose the correct child in the spine
        if key >= first_key {
            spine.replace_child(right_block);
        }

        Ok(())
    }

    fn split_into_three<V: Serializable>(spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node::<u32>(spine.parent());

        if idx == 0 {
            // There is no left sibling, so we rebalance 0, 1, 2
            let mut left = w_node::<V>(spine.child());
            let right_block = spine.shadow(parent.values.get(1))?;
            let mut right = w_node(right_block.clone());

            let middle_block = spine.new_block()?;
            let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

            redistribute3(&mut left, &mut middle, &mut right);

            // patch up the parent
            let r_first_key = right.first_key().unwrap();
            parent.keys.set(1, &r_first_key);

            let m_first_key = middle.first_key().unwrap();
            parent.insert_at(1, m_first_key, &middle.loc);

            if key >= r_first_key {
                spine.replace_child(right_block);
            } else if key >= m_first_key {
                spine.replace_child(middle_block);
            }
        } else {
            let left_block = spine.shadow(parent.values.get(idx - 1))?;
            let mut left = w_node::<V>(left_block.clone());
            let mut right = w_node::<V>(spine.child());

            let middle_block = spine.new_block()?;
            let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

            redistribute3(&mut left, &mut middle, &mut right);

            // patch up the parent
            let r_first_key = right.first_key().unwrap();
            parent.keys.set(idx, &r_first_key);

            let m_first_key = middle.first_key().unwrap();
            parent.insert_at(idx, m_first_key, &middle.loc);

            if key < m_first_key {
                spine.replace_child(left_block);
            } else if key < r_first_key {
                spine.replace_child(middle_block);
            }
        }

        Ok(())
    }

    fn rebalance_or_split<V: Serializable>(spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let parent = w_node::<u32>(spine.parent());
        let nr_parent = parent.nr_entries.get() as usize;

        // Is there a left sibling?
        if idx > 0 {
            let left_loc = parent.values.get(idx - 1);

            // Can we move some entries to it?
            if get_loc_free_space::<V>(spine.tm.as_ref(), left_loc) >= SPACE_THRESHOLD {
                return rebalance_left::<V>(spine, idx, key);
            }
        }

        // Is there a right sibling? If so, rebalance with it
        if idx < nr_parent - 1 {
            let right_loc = parent.values.get(idx + 1);

            // Can we move some entries to it?
            if get_loc_free_space::<V>(spine.tm.as_ref(), right_loc) >= SPACE_THRESHOLD {
                return rebalance_right::<V>(spine, idx, key);
            }
        }

        // We didn't manage to rebalance, so we need to split
        if idx == 0 || idx == nr_parent - 1 {
            // When inserting a sequence that is either monotonically
            // increasing or decreasing, it's better to split a single node into two.
            split_into_two::<V>(spine, idx, key)
        } else {
            split_into_three::<V>(spine, idx, key)
        }
    }

    fn ensure_space<NV: Serializable>(
        spine: &mut Spine,
        key: u32,
        idx: isize,
    ) -> Result<WNode<NV>> {
        let mut child = w_node::<NV>(spine.child());

        if !has_space_for_insert::<NV, WriteProxy>(&child) {
            if spine.is_top() {
                split_beneath::<NV>(spine, key)?;
            } else {
                rebalance_or_split::<NV>(spine, idx as usize, key)?;
            }

            child = w_node(spine.child());
        }

        Ok(child)
    }

    pub fn insert<V: Serializable>(spine: &mut Spine, key: u32, value: &V) -> Result<()> {
        let mut idx = 0isize;

        loop {
            let flags = read_flags(&mut spine.child().r())?;

            if flags == BTreeFlags::Internal {
                let mut child = ensure_space::<u32>(spine, key, idx)?;

                // FIXME: remove, just here whilst hunting a bug
                ensure!(child.nr_entries.get() > 0);
                idx = child.keys.bsearch(&key);

                if idx < 0 {
                    // adjust the keys as we go down the spine.
                    child.keys.set(0, &key);
                    idx = 0;
                }

                spine.push(child.values.get(idx as usize))?;

                // Patch up the parent
                let loc = spine.child().loc();
                let mut p = w_node::<u32>(spine.parent());
                p.values.set(idx as usize, &loc);
            } else {
                let mut child = ensure_space::<V>(spine, key, idx)?;

                idx = child.keys.bsearch(&key);

                if idx < 0 {
                    child.keys.insert_at(0, &key);
                    child.values.insert_at(0, &value);
                    child.nr_entries.inc(1);
                } else if idx as usize >= child.keys.len() {
                    // insert
                    child.keys.append_single(&key);
                    child.values.append_single(value);
                    child.nr_entries.inc(1);
                } else {
                    if child.keys.get(idx as usize) == key {
                        // overwrite
                        child.values.set(idx as usize, &value);
                    } else {
                        ensure!(child.keys.get(idx as usize) < key);
                        child.keys.insert_at(idx as usize + 1, &key);
                        child.values.insert_at(idx as usize + 1, &value);
                        child.nr_entries.inc(1);
                    }
                }

                break;
            }
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------

mod remove_utilities {
    use super::*;

    // Sometimes we need to remember the index that led to a particular
    // node.
    struct Child<V: Serializable> {
        index: usize,
        node: WNode<V>,
    }

    fn is_leaf(spine: &mut Spine, loc: MetadataBlock) -> Result<bool> {
        let b = spine.tm.read(loc, &BNODE_KIND)?;
        let flags = read_flags(b.r())?;

        Ok(flags == BTreeFlags::Leaf)
    }

    fn shift_<V: Serializable>(left: &mut WNode<V>, right: &mut WNode<V>, count: isize) {
        if count > 0 {
            let (keys, values) = left.remove_right(count as usize);
            right.prepend(&keys, &values);
        } else {
            let (keys, values) = right.shift_left((-count) as usize);
            left.append(&keys, &values);
        }
    }

    fn rebalance2_<V: Serializable>(
        parent: &mut WNode<MetadataBlock>,
        l: &mut Child<V>,
        r: &mut Child<V>,
    ) {
        let left = &mut l.node;
        let right = &mut r.node;

        let nr_left = left.nr_entries.get();
        let nr_right = right.nr_entries.get();

        // Ensure the number of entries in each child will be greater
        // than or equal to (max_entries / 3 + 1), so no matter which
        // child is used for removal, the number will still be not
        // less that (max_entries / 3).
        let threshold = 2 * ((Node::<V, ReadProxy>::max_entries() / 3) + 1);

        if ((nr_left + nr_right) as usize) < threshold {
            // merge
            shift_(left, right, -(nr_right as isize));
            parent.remove_at(r.index);
        } else {
            // rebalance
            let target_left = (nr_left + nr_right) / 2;
            shift_(left, right, nr_left as isize - target_left as isize);
            parent.keys.set(r.index, &right.first_key().unwrap());
        }
    }

    fn rebalance2_aux<V: Serializable>(
        spine: &mut Spine,
        left_idx: usize,
        parent: &mut WNode<MetadataBlock>,
    ) -> Result<()> {
        let left_loc = parent.values.get(left_idx);
        let mut left = Child {
            index: left_idx,
            node: w_node::<V>(spine.shadow(left_loc)?),
        };

        let right_loc = parent.values.get(left_idx + 1);
        let mut right = Child {
            index: left_idx + 1,
            node: w_node::<V>(spine.shadow(right_loc)?),
        };

        rebalance2_::<V>(parent, &mut left, &mut right);
        Ok(())
    }

    fn rebalance2<LeafV: Serializable>(spine: &mut Spine, left_idx: usize) -> Result<()> {
        let mut parent = w_node::<MetadataBlock>(spine.child());

        let left_loc = parent.values.get(left_idx);
        if is_leaf(spine, left_loc)? {
            rebalance2_aux::<LeafV>(spine, left_idx, &mut parent)
        } else {
            rebalance2_aux::<MetadataBlock>(spine, left_idx, &mut parent)
        }
    }

    // We dump as many entries from center as possible into left, the the rest
    // in right, then rebalance.  This wastes some cpu, but I want something
    // simple atm.
    fn delete_center_node<V: Serializable>(
        parent: &mut WNode<MetadataBlock>,
        l: &mut Child<V>,
        c: &mut Child<V>,
        r: &mut Child<V>,
    ) {
        let left = &mut l.node;
        let center = &mut c.node;
        let right = &mut r.node;

        let nr_left = left.nr_entries.get();
        let nr_center = center.nr_entries.get();
        // let nr_right = right.nr_entries.get();

        let shift = std::cmp::min::<usize>(
            Node::<V, ReadProxy>::max_entries() - nr_left as usize,
            nr_center as usize,
        );
        shift_(left, center, -(shift as isize));

        if shift != nr_center as usize {
            let shift = nr_center as usize - shift;
            shift_(center, right, shift as isize);
        }

        parent.keys.set(r.index, &right.first_key().unwrap());
        parent.remove_at(c.index);
        r.index -= 1;

        rebalance2_(parent, l, r);
    }

    fn redistribute3<V: Serializable>(
        parent: &mut WNode<MetadataBlock>,
        l: &mut Child<V>,
        c: &mut Child<V>,
        r: &mut Child<V>,
    ) {
        let left = &mut l.node;
        let center = &mut c.node;
        let right = &mut r.node;

        let mut nr_left = left.nr_entries.get() as isize;
        let nr_center = center.nr_entries.get() as isize;
        let mut nr_right = right.nr_entries.get() as isize;

        let total = nr_left + nr_center + nr_right;
        let target_right = total / 3;
        let remainder = if target_right * 3 != total { 1 } else { 0 };
        let target_left = target_right + remainder;

        if nr_left < nr_right {
            let mut shift: isize = nr_left - target_left;

            if shift < 0 && nr_center < -shift {
                // not enough in central node
                shift_(left, center, -nr_center);
                shift += nr_center;
                shift_(left, right, shift);

                nr_right += shift;
            } else {
                shift_(left, center, shift);
            }

            shift_(center, right, target_right - nr_right);
        } else {
            let mut shift: isize = target_right - nr_right;

            if shift > 0 && nr_center < shift {
                // not enough in central node
                shift_(center, right, nr_center);
                shift -= nr_center;
                shift_(left, right, shift);
                nr_left -= shift;
            } else {
                shift_(center, right, shift);
            }

            shift_(left, center, nr_left - target_left);
        }

        parent.keys.set(c.index, &center.first_key().unwrap());
        parent.keys.set(r.index, &right.first_key().unwrap());
    }

    fn rebalance3_<V: Serializable>(
        parent: &mut WNode<MetadataBlock>,
        l: &mut Child<V>,
        c: &mut Child<V>,
        r: &mut Child<V>,
    ) {
        let nr_left = l.node.nr_entries.get();
        let nr_center = c.node.nr_entries.get();
        let nr_right = r.node.nr_entries.get();

        let threshold = ((Node::<V, ReadProxy>::max_entries() / 3) * 4 + 1) as u32;

        if nr_left + nr_center + nr_right < threshold {
            delete_center_node(parent, l, c, r);
        } else {
            redistribute3(parent, l, c, r);
        }
    }

    fn rebalance3_aux<V: Serializable>(
        spine: &mut Spine,
        parent: &mut WNode<MetadataBlock>,
        left_idx: usize,
    ) -> Result<()> {
        let left_loc = parent.values.get(left_idx);
        let mut left = Child {
            index: left_idx,
            node: w_node::<V>(spine.shadow(left_loc)?),
        };

        let center_loc = parent.values.get(left_idx + 1);
        let mut center = Child {
            index: left_idx + 1,
            node: w_node::<V>(spine.shadow(center_loc)?),
        };

        let right_loc = parent.values.get(left_idx + 2);
        let mut right = Child {
            index: left_idx + 2,
            node: w_node::<V>(spine.shadow(right_loc)?),
        };

        rebalance3_::<V>(parent, &mut left, &mut center, &mut right);
        Ok(())
    }

    // Assumes spine.child() is an internal node
    fn rebalance3<LeafV: Serializable>(spine: &mut Spine, left_idx: usize) -> Result<()> {
        let mut parent = w_node::<MetadataBlock>(spine.child());

        let left_loc = parent.values.get(left_idx);
        if is_leaf(spine, left_loc)? {
            rebalance3_aux::<LeafV>(spine, &mut parent, left_idx)
        } else {
            rebalance3_aux::<MetadataBlock>(spine, &mut parent, left_idx)
        }
    }

    // Assumes spine.child() is an internal node
    fn rebalance_children<LeafV: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
        eprintln!("rebalance_children");
        let child = w_node::<MetadataBlock>(spine.child());

        if child.nr_entries.get() == 1 {
            // The only node that's allowed to drop below 1/3 full is the root
            // node.
            ensure!(spine.is_top());

            let gc_loc = child.values.get(0);
            spine.replace_child_loc(gc_loc)?;
        } else {
            eprintln!("2");
            let idx = child.keys.bsearch(&key);
            if idx < 0 {
                // key isn't in the tree
                todo!();
            }

            let has_left_sibling = idx > 0;
            let has_right_sibling = idx < (child.nr_entries.get() - 1) as isize;

            if !has_left_sibling {
                rebalance2::<LeafV>(spine, idx as usize)?;
            } else if !has_right_sibling {
                rebalance2::<LeafV>(spine, (idx - 1) as usize)?;
            } else {
                rebalance3::<LeafV>(spine, (idx - 1) as usize)?;
            }
        }

        Ok(())
    }

    fn patch_parent(spine: &mut Spine, parent_idx: usize, loc: MetadataBlock) {
        if !spine.is_top() {
            let mut parent = w_node::<MetadataBlock>(spine.parent());
            parent.values.set(parent_idx, &loc);
        }
    }

    pub fn remove<LeafV: Serializable>(spine: &mut Spine, key: u32) -> Result<Option<LeafV>> {
        let mut idx = 0isize;

        loop {
            let flags = read_flags(&mut spine.child().r())?;

            if flags == BTreeFlags::Internal {
                let old_loc = spine.child_loc();
                let child = w_node::<MetadataBlock>(spine.child());
                patch_parent(spine, idx as usize, child.loc);

                drop(child);
                // FIXME: we could pass child in to save re-getting it
                rebalance_children::<LeafV>(spine, key)?;

                // The child may have been erased and we don't know what
                // kind of node the new child is.
                if spine.child_loc() != old_loc {
                    continue;
                }

                // Reaquire the child because it may have changed due to the
                // rebalance.
                let child = w_node::<MetadataBlock>(spine.child());
                idx = child.keys.bsearch(&key);

                // We know the key is present or else rebalance_children would have failed.
                // FIXME: check this
                spine.push(child.values.get(idx as usize))?;
            } else {
                let mut child = w_node::<LeafV>(spine.child());
                patch_parent(spine, idx as usize, child.loc);

                let idx = child.keys.bsearch(&key);
                if idx < 0
                    || idx as u32 >= child.nr_entries.get()
                    || child.keys.get(idx as usize) != key
                {
                    return Ok(None);
                }

                let val = child.values.get(idx as usize);
                child.remove_at(idx as usize);
                return Ok(Some(val));
            }
        }
    }
}

//-------------------------------------------------------------------------

pub struct BTree<V: Serializable> {
    tm: Arc<TransactionManager>,
    root: u32,
    phantom: std::marker::PhantomData<V>,
}

struct Frame {
    loc: MetadataBlock,

    // Index into the current node
    index: usize,

    // Nr entries in current node
    nr_entries: usize,
}

pub struct Iterator<'a, V: Serializable> {
    tree: &'a BTree<V>,

    // Holds pairs of (loc, index, nr_entries)
    stack: Option<Vec<Frame>>,
}

fn next_<TreeV: Serializable, NV: Serializable>(
    tree: &BTree<TreeV>,
    stack: &mut Vec<Frame>,
) -> Result<bool> {
    if stack.is_empty() {
        return Ok(false);
    }

    let frame = stack.last_mut().unwrap();

    frame.index += 1;
    if frame.index == frame.nr_entries {
        // We need to move to the next node.
        stack.pop();
        next_::<TreeV, MetadataBlock>(tree, stack)?;

        let frame = stack.last().unwrap();
        let n = tree.read_node::<MetadataBlock>(frame.loc)?;

        if n.nr_entries.get() == 0 {
            return Ok(false);
        }

        let loc = n.values.get(0);
        let n = tree.read_node::<NV>(loc)?;

        stack.push(Frame {
            loc,
            index: 0,
            nr_entries: n.nr_entries.get() as usize,
        });
    }

    Ok(true)
}

impl<'a, V: Serializable> Iterator<'a, V> {
    fn new(tree: &'a BTree<V>) -> Result<Self> {
        let mut stack = Vec::new();
        let mut loc = tree.root();

        loop {
            if tree.is_leaf(loc)? {
                let n = tree.read_node::<V>(loc)?;
                let nr_entries = n.nr_entries.get() as usize;
                if nr_entries == 0 {
                    return Ok(Self { tree, stack: None });
                }

                stack.push(Frame {
                    loc,
                    index: 0,
                    nr_entries,
                });

                return Ok(Self {
                    tree,
                    stack: Some(stack),
                });
            } else {
                let n = tree.read_node::<MetadataBlock>(loc)?;
                let nr_entries = n.nr_entries.get() as usize;

                // we cannot have an internal node without entries
                stack.push(Frame {
                    loc,
                    index: 0,
                    nr_entries,
                });

                loc = n.values.get(0);
            }
        }
    }

    /// Returns (key, value) for the current position.  Returns None
    /// if the cursor has run out of values.
    pub fn get(&self) -> Result<Option<(u32, V)>> {
        match &self.stack {
            None => Ok(None),
            Some(stack) => {
                let frame = stack.last().unwrap();

                // FIXME: cache nodes in frame
                let n = self.tree.read_node::<V>(frame.loc)?;
                let k = n.keys.get(frame.index);
                let v = n.values.get(frame.index);
                Ok(Some((k, v)))
            }
        }
    }

    pub fn next(&mut self) -> Result<bool> {
        match &mut self.stack {
            None => Ok(false),
            Some(stack) => {
                if !next_::<V, V>(self.tree, stack)? {
                    self.stack = None;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
    }
}

impl<V: Serializable> BTree<V> {
    pub fn open_tree(tm: Arc<TransactionManager>, root: u32) -> Self {
        Self {
            tm,
            root,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn empty_tree(tm: Arc<TransactionManager>) -> Result<Self> {
        let root = {
            let root = tm.new_block(&BNODE_KIND)?;
            let root = init_node::<V>(root, true)?;
            root.loc
        };

        Ok(Self {
            tm,
            root,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn root(&self) -> u32 {
        self.root
    }

    pub fn cursor(&self) -> Result<Iterator<V>> {
        Iterator::new(self)
    }

    fn is_leaf(&self, loc: MetadataBlock) -> Result<bool> {
        let b = self.tm.read(loc, &BNODE_KIND)?;
        let flags = read_flags(&mut b.r())?;
        Ok(flags == BTreeFlags::Leaf)
    }

    fn read_node<V2: Serializable>(&self, loc: MetadataBlock) -> Result<Node<V2, ReadProxy>> {
        let b = self.tm.read(loc, &BNODE_KIND)?;
        Ok(Node::<V2, ReadProxy>::new(loc, b))
    }

    //-------------------------------

    pub fn lookup(&self, key: u32) -> Result<Option<V>> {
        let mut block = self.tm.read(self.root, &BNODE_KIND)?;

        loop {
            let flags = read_flags(&mut block.r())?;

            match flags {
                BTreeFlags::Internal => {
                    let node = Node::<u32, ReadProxy>::new(block.loc(), block);

                    let idx = node.keys.bsearch(&key);
                    if idx < 0 || idx >= node.nr_entries.get() as isize {
                        return Ok(None);
                    }

                    let child = node.values.get(idx as usize);
                    block = self.tm.read(child, &BNODE_KIND)?;
                }
                BTreeFlags::Leaf => {
                    let node = Node::<V, ReadProxy>::new(block.loc(), block);

                    let idx = node.keys.bsearch(&key);
                    if idx < 0 || idx >= node.nr_entries.get() as isize {
                        return Ok(None);
                    }

                    return if node.keys.get(idx as usize) == key {
                        Ok(Some(node.values.get(idx as usize)))
                    } else {
                        Ok(None)
                    };
                }
            }
        }
    }

    pub fn insert(&mut self, key: u32, value: &V) -> Result<()> {
        let mut spine = Spine::new(self.tm.clone(), self.root)?;
        insert_utils::insert(&mut spine, key, value)?;
        self.root = spine.get_root();
        Ok(())
    }

    pub fn remove(&mut self, key: u32) -> Result<Option<V>> {
        let mut spine = Spine::new(self.tm.clone(), self.root)?;
        let r = remove_utilities::remove(&mut spine, key)?;
        self.root = spine.get_root();
        Ok(r)
    }

    //-------------------------------

    /// Returns a vec of key, value pairs
    pub fn lookup_range(&self, _key_low: u32, _key_high: u32) -> Result<Vec<(u32, V)>> {
        todo!();
    }

    pub fn insert_range(&mut self, _kvs: &[(u32, V)]) -> Result<()> {
        todo!();
    }

    /// Returns any values removed
    pub fn remove_range(&mut self, _key_low: u32, _key_high: u32) -> Result<Vec<V>> {
        todo!();
    }

    //-------------------------------

    fn check_(
        &self,
        loc: u32,
        key_min: u32,
        key_max: Option<u32>,
        seen: &mut BTreeSet<u32>,
    ) -> Result<u32> {
        let mut total = 0;

        ensure!(!seen.contains(&loc));
        seen.insert(loc);

        let block = self.tm.read(loc, &BNODE_KIND).unwrap();
        let node = r_node(block);

        // check the keys
        let mut last = None;
        for i in 0..node.nr_entries.get() {
            let k = node.keys.get(i as usize);
            ensure!(k >= key_min);

            if let Some(key_max) = key_max {
                ensure!(k < key_max);
            }

            if let Some(last) = last {
                ensure!(k > last);
            }
            last = Some(k);
        }

        if node.flags.get() == BTreeFlags::Internal as u32 {
            for i in 0..node.nr_entries.get() {
                let kmin = node.keys.get(i as usize);
                let kmax = if i == node.nr_entries.get() - 1 {
                    None
                } else {
                    Some(node.keys.get(i as usize + 1))
                };
                let loc = node.values.get(i as usize);
                total += self.check_(loc, kmin, kmax, seen)?;
            }
        } else {
            total += node.keys.len() as u32;
        }

        Ok(total)
    }

    /// Checks the btree is well formed and returns the number of entries
    /// in the tree.
    pub fn check(&self) -> Result<u32> {
        let mut seen = BTreeSet::new();
        self.check_(self.root, 0, None, &mut seen)
    }
}

pub fn btree_refs(data: &ReadProxy, queue: &mut VecDeque<BlockRef>) {
    let node = r_node(data.clone());

    if node.is_leaf() {
        // FIXME: values should be refs, except in the btree unit tests
    } else {
        for i in 0..node.nr_entries.get() {
            queue.push_back(BlockRef::Metadata(node.values.get(i as usize)));
        }
    }
}

//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_allocator::*;
    use crate::core::*;
    use anyhow::{ensure, Result};
    use rand::seq::SliceRandom;
    use std::io::{self, Read, Write};
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

    // We'll test with a value type that is a different size to the internal node values (u32).
    #[derive(Ord, PartialOrd, PartialEq, Eq, Debug)]
    struct Value {
        v: u32,
        extra: u16,
    }

    impl Serializable for Value {
        fn packed_len() -> usize {
            6
        }

        fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
            w.write_u32::<LittleEndian>(self.v)?;
            w.write_u16::<LittleEndian>(self.extra)?;
            Ok(())
        }

        fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
            let v = r.read_u32::<LittleEndian>()?;
            let extra = r.read_u16::<LittleEndian>()?;
            Ok(Self { v, extra })
        }
    }

    #[allow(dead_code)]
    struct Fixture {
        engine: Arc<dyn IoEngine>,
        cache: Arc<MetadataCache>,
        allocator: Arc<Mutex<BlockAllocator>>,
        tm: Arc<TransactionManager>,
        tree: BTree<Value>,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
            let allocator = mk_allocator(cache.clone(), nr_data_blocks)?;
            let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));
            let tree = BTree::empty_tree(tm.clone())?;

            Ok(Self {
                engine,
                cache,
                allocator,
                tm,
                tree,
            })
        }

        fn check(&self) -> Result<u32> {
            self.tree.check()
        }

        fn lookup(&self, key: u32) -> Option<Value> {
            self.tree.lookup(key).unwrap()
        }

        fn insert(&mut self, key: u32, value: Value) -> Result<()> {
            self.tree.insert(key, &value)
        }

        fn remove(&mut self, key: u32) -> Result<Option<Value>> {
            self.tree.remove(key)
        }

        fn commit(&mut self) -> Result<()> {
            let roots = vec![self.tree.root()];
            self.tm.commit(&roots)
        }
    }

    fn mk_value(v: u32) -> Value {
        Value { v, extra: 0 }
    }

    #[test]
    fn empty_btree() -> Result<()> {
        const NR_BLOCKS: u32 = 1024;
        const NR_DATA_BLOCKS: u64 = 102400;

        let mut fix = Fixture::new(NR_BLOCKS, NR_DATA_BLOCKS)?;
        fix.commit()?;

        Ok(())
    }

    #[test]
    fn lookup_fails() -> Result<()> {
        let fix = Fixture::new(1024, 102400)?;
        ensure!(fix.lookup(0).is_none());
        ensure!(fix.lookup(1234).is_none());
        Ok(())
    }

    #[test]
    fn insert_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        fix.insert(0, mk_value(100))?;
        fix.commit()?; // FIXME: shouldn't be needed
        ensure!(fix.lookup(0) == Some(mk_value(100)));

        Ok(())
    }

    fn insert_test(keys: &[u32]) -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;

        fix.commit()?;

        eprintln!("inserting {} keys", keys.len());
        for (i, k) in keys.iter().enumerate() {
            fix.insert(*k, mk_value(*k * 2))?;
            if i % 100 == 0 {
                eprintln!("{}", i);
                let n = fix.check()?;
                ensure!(n == i as u32 + 1);
            }
        }

        fix.commit()?;

        for k in keys {
            let v = fix.lookup(*k).unwrap();
            ensure!(fix.lookup(*k) == Some(mk_value(k * 2)));
        }

        let n = fix.check()?;
        ensure!(n == keys.len() as u32);

        Ok(())
    }

    #[test]
    fn insert_sequence() -> Result<()> {
        let count = 100_000;
        insert_test(&(0..count).collect::<Vec<u32>>())
    }

    #[test]
    fn insert_random() -> Result<()> {
        let count = 100_000;
        let mut keys: Vec<u32> = (0..count).collect();

        // shuffle the keys
        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        insert_test(&keys)
    }

    #[test]
    fn remove_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let key = 100;
        let val = 123;
        fix.insert(key, mk_value(val))?;
        ensure!(fix.lookup(key) == Some(mk_value(val)));
        fix.remove(key)?;
        ensure!(fix.lookup(key) == None);
        Ok(())
    }

    #[test]
    fn remove_random() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        // build a big btree
        // let count = 100_000;
        let count = 10_000;
        for i in 0..count {
            fix.insert(i, mk_value(i * 3))?;
        }
        eprintln!("built tree");

        let mut keys: Vec<u32> = (0..count).collect();
        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        for (i, k) in keys.into_iter().enumerate() {
            ensure!(fix.lookup(k).is_some());
            let mval = fix.remove(k)?;
            ensure!(mval.is_some());
            ensure!(mval.unwrap() == mk_value(k * 3));
            ensure!(fix.lookup(k).is_none());
            if i % 1 == 0 {
                eprintln!("removed {}", i);

                let n = fix.check()?;
                ensure!(n == count - i as u32 - 1);
                eprintln!("checked tree");
            }
        }

        Ok(())
    }

    #[test]
    fn rolling_insert_remove() -> Result<()> {
        // If the GC is not working then we'll run out of metadata space.
        let mut fix = Fixture::new(32, 10240)?;
        fix.commit()?;

        for k in 0..1000_000 {
            fix.insert(k, mk_value(k * 3))?;
            if k > 100 {
                fix.remove(k - 100)?;
            }

            if k % 100 == 0 {
                eprintln!("inserted {} entries", k);
                fix.commit()?;
            }
        }

        Ok(())
    }

    #[test]
    fn empty_cursor() -> Result<()> {
        let mut fix = Fixture::new(16, 1024)?;
        fix.commit()?;

        let c = fix.tree.cursor()?;
        ensure!(c.get()?.is_none());
        Ok(())
    }
}

//-------------------------------------------------------------------------
