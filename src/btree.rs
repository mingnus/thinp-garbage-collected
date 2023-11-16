use anyhow::{ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::byte_types::*;
use crate::spine::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

const NODE_HEADER_SIZE: usize = 8;
const MAX_ENTRIES: usize = (BLOCK_PAYLOAD_SIZE - NODE_HEADER_SIZE) / 8;
const SPACE_THRESHOLD: usize = 8;

enum BTreeFlags {
    Internal = 0,
    Leaf = 1,
}

struct NodeHeader {
    flags: u32,
    nr_entries: u32,
}

fn read_node_header<R: Read>(r: &mut R) -> NodeHeader {
    let flags = r.read_u32::<LittleEndian>().unwrap();
    let nr_entries = r.read_u32::<LittleEndian>().unwrap();

    NodeHeader { flags, nr_entries }
}

fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.flags)?;
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    Ok(())
}

/*
fn key_base(data: &mut [u8]) -> &mut [u8] {
    &mut data[KEYS_OFFSET..VALUES_OFFSET]
}

fn value_base(data: &mut [u8]) -> &mut [u8] {
    &mut data[VALUES_OFFSET..]
}
*/

//-------------------------------------------------------------------------

// FIXME: doesn't need to be pub
pub struct Node<Data> {
    loc: u32,

    flags: U32<Data>,
    nr_entries: U32<Data>,
    keys: U32Array<Data>,
    values: U32Array<Data>,
}

impl<Data: Readable> Node<Data> {
    fn new(loc: u32, data: Data) -> Self {
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        let (flags, data) = data.split_at(4);
        let (nr_entries, data) = data.split_at(4);
        assert!(nr_entries.r().len() == 4);
        let (keys, values) = data.split_at(MAX_ENTRIES * 4);

        let flags = U32::new(flags);
        let nr_entries = U32::new(nr_entries);
        let keys = U32Array::new(keys, nr_entries.get() as usize);
        let values = U32Array::new(values, nr_entries.get() as usize);

        Self {
            loc,
            flags,
            nr_entries,
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

impl<Data: Writeable> Node<Data> {
    fn insert_at(&mut self, idx: usize, key: u32, value: u32) {
        self.keys.insert_at(idx, key);
        self.values.insert_at(idx, value);
        self.nr_entries.inc(1);
    }

    fn remove_at(&mut self, idx: usize) {
        self.keys.remove_at(idx);
        self.values.remove_at(idx);
        self.nr_entries.dec(1);
    }

    /// Returns (keys, values) for the entries that have been lost
    fn shift_left(&mut self, count: usize) -> (Vec<u32>, Vec<u32>) {
        let keys = self.keys.shift_left(count);
        let values = self.values.shift_left(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
    }

    fn prepend(&mut self, keys: &[u32], values: &[u32]) {
        assert!(keys.len() == values.len());
        self.keys.prepend(keys);
        self.values.prepend(values);
        self.nr_entries.inc(keys.len() as u32);
    }

    fn append(&mut self, keys: &[u32], values: &[u32]) {
        self.keys.append(keys);
        self.values.append(values);
        self.nr_entries.inc(keys.len() as u32);
    }

    fn remove_right(&mut self, count: usize) -> (Vec<u32>, Vec<u32>) {
        let keys = self.keys.remove_right(count);
        let values = self.values.remove_right(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
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
}

//-------------------------------------------------------------------------

// Insert and remove both have similarly named utilities, so we keep them
// in separate modules.
mod insert_utils {
    use super::*;

    fn redistribute2(left: &mut WNode, right: &mut WNode) {
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
    fn redistribute3(left: &mut WNode, middle: &mut WNode, right: &mut WNode) {
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

    fn has_space_for_insert<Data: Readable>(node: &Node<Data>) -> bool {
        node.nr_entries.get() < MAX_ENTRIES as u32
    }

    fn split_beneath(spine: &mut Spine, key: u32) -> Result<()> {
        let mut new_parent = w_node(spine.child());
        let nr_left = (new_parent.nr_entries.get() / 2) as usize;
        let (lkeys, lvalues) = new_parent.shift_left(nr_left);
        let (rkeys, rvalues) = new_parent.shift_left(new_parent.nr_entries.get() as usize);

        let mut left = init_node(spine.new_block()?, new_parent.is_leaf())?;
        left.append(&lkeys, &lvalues);

        let mut right = init_node(spine.new_block()?, new_parent.is_leaf())?;
        right.append(&rkeys, &rvalues);

        // setup the parent to point to the two new children
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

    fn rebalance_left(spine: &mut Spine, parent_idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node(spine.parent());
        let left_loc = parent.values.get(parent_idx - 1);
        let left_block = spine.shadow(left_loc)?;
        let mut left = w_node(left_block.clone());
        let mut right = w_node(spine.child());
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let first_key = right.first_key().unwrap();
        parent.keys.set(parent_idx, first_key);

        // Choose the correct child in the spine
        if key < first_key {
            spine.replace_child(left_block);
        }

        Ok(())
    }

    fn rebalance_right(spine: &mut Spine, parent_idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node(spine.parent());
        let right_loc = parent.values.get(parent_idx + 1);
        let right_block = spine.shadow(right_loc)?;
        let mut right = w_node(right_block.clone());
        let mut left = w_node(spine.child());
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let first_key = right.first_key().unwrap();
        parent.keys.set(parent_idx + 1, first_key);

        // Choose the correct child in the spine
        if key >= first_key {
            spine.replace_child(right_block);
        }

        Ok(())
    }

    fn get_node_free_space(node: &RNode) -> usize {
        MAX_ENTRIES - node.nr_entries.get() as usize
    }

    fn get_loc_free_space(tm: &TransactionManager, loc: u32) -> usize {
        let block = tm.read(loc, &BNODE_KIND).unwrap();
        let node = r_node(block);
        get_node_free_space(&node)
    }

    fn split_into_two(spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut left = w_node(spine.child());
        let right_block = spine.new_block()?;
        let mut right = init_node(right_block.clone(), left.is_leaf())?;
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let mut parent = w_node(spine.parent());

        let first_key = right.first_key().unwrap();
        parent.keys.insert_at(idx + 1, first_key);
        parent.values.insert_at(idx + 1, right.loc);
        parent.nr_entries.inc(1);

        // Choose the correct child in the spine
        if key >= first_key {
            spine.replace_child(right_block);
        }

        Ok(())
    }

    fn split_into_three(spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node(spine.parent());

        if idx == 0 {
            // There is no left sibling, so we rebalance 0, 1, 2
            let mut left = w_node(spine.child());
            let right_block = spine.shadow(parent.values.get(1))?;
            let mut right = w_node(right_block.clone());

            let middle_block = spine.new_block()?;
            let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

            redistribute3(&mut left, &mut middle, &mut right);

            // patch up the parent
            let r_first_key = right.first_key().unwrap();
            parent.keys.set(1, r_first_key);

            let m_first_key = middle.first_key().unwrap();
            parent.insert_at(1, m_first_key, middle.loc);

            if key >= r_first_key {
                spine.replace_child(right_block);
            } else if key >= m_first_key {
                spine.replace_child(middle_block);
            }
        } else {
            let left_block = spine.shadow(parent.values.get(idx - 1))?;
            let mut left = w_node(left_block.clone());
            let mut right = w_node(spine.child());

            let middle_block = spine.new_block()?;
            let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

            redistribute3(&mut left, &mut middle, &mut right);

            // patch up the parent
            let r_first_key = right.first_key().unwrap();
            parent.keys.set(idx, r_first_key);

            let m_first_key = middle.first_key().unwrap();
            parent.insert_at(idx, m_first_key, middle.loc);

            if key < m_first_key {
                spine.replace_child(left_block);
            } else if key < r_first_key {
                spine.replace_child(middle_block);
            }
        }

        Ok(())
    }

    fn rebalance_or_split(spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let parent = w_node(spine.parent());
        let nr_parent = parent.nr_entries.get() as usize;

        // Is there a left sibling?
        if idx > 0 {
            let left_loc = parent.values.get(idx - 1);

            // Can we move some entries to it?
            if get_loc_free_space(spine.tm.as_ref(), left_loc) >= SPACE_THRESHOLD {
                return rebalance_left(spine, idx, key);
            }
        }

        // Is there a right sibling? If so, rebalance with it
        if idx < nr_parent - 1 {
            let right_loc = parent.values.get(idx + 1);

            // Can we move some entries to it?
            if get_loc_free_space(spine.tm.as_ref(), right_loc) >= SPACE_THRESHOLD {
                return rebalance_right(spine, idx, key);
            }
        }

        // We didn't manage to rebalance, so we need to split
        if idx == 0 || idx == nr_parent - 1 {
            // When inserting a sequence that is either monotonically
            // increasing or decreasing, it's better to split a single node into two.
            split_into_two(spine, idx, key)
        } else {
            split_into_three(spine, idx, key)
        }
    }

    pub fn insert(spine: &mut Spine, key: u32, value: u32) -> Result<()> {
        let mut idx = 0isize;

        loop {
            let mut child = w_node(spine.child());

            if !has_space_for_insert(&child) {
                if spine.top() {
                    split_beneath(spine, key)?;
                } else {
                    rebalance_or_split(spine, idx as usize, key)?;
                }

                child = w_node(spine.child());
            }

            idx = child.keys.bsearch(key);

            if child.is_leaf() {
                if idx < 0 {
                    child.keys.insert_at(0, key);
                    child.values.insert_at(0, value);
                    child.nr_entries.inc(1);
                } else if idx as usize >= child.keys.len() {
                    // insert
                    child.keys.append(&[key]);
                    child.values.append(&[value]);
                    child.nr_entries.inc(1);
                } else {
                    if child.keys.get(idx as usize) == key {
                        // overwrite
                        child.values.set(idx as usize, value);
                    } else {
                        ensure!(child.keys.get(idx as usize) < key);
                        child.keys.insert_at(idx as usize + 1, key);
                        child.values.insert_at(idx as usize + 1, value);
                        child.nr_entries.inc(1);
                    }
                }

                break;
            } else {
                if idx < 0 {
                    // adjust the keys as we go down the spine.
                    child.keys.set(0, key);
                    idx = 0;
                }

                spine.push(child.values.get(idx as usize))?;

                // Patch up the parent
                let loc = spine.child().loc();
                let mut p = w_node(spine.parent());
                p.values.set(idx as usize, loc);
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
    struct Child {
        index: usize,
        node: WNode,
    }

    fn shift_(left: &mut WNode, right: &mut WNode, count: isize) {
        if count > 0 {
            let (keys, values) = left.remove_right(count as usize);
            right.prepend(&keys, &values);
        } else {
            let (keys, values) = right.shift_left((-count) as usize);
            left.append(&keys, &values);
        }
    }

    fn rebalance2_(parent: &mut WNode, l: &mut Child, r: &mut Child) {
        let left = &mut l.node;
        let right = &mut r.node;

        let nr_left = left.nr_entries.get();
        let nr_right = right.nr_entries.get();

        // Ensure the number of entries in each child will be greater
        // than or equal to (max_entries / 3 + 1), so no matter which
        // child is used for removal, the number will still be not
        // less that (max_entries / 3).
        let threshold = 2 * ((MAX_ENTRIES / 3) + 1);

        if ((nr_left + nr_right) as usize) < threshold {
            // merge
            shift_(left, right, -(nr_right as isize));
            parent.remove_at(r.index);
        } else {
            // rebalance
            let target_left = (nr_left + nr_right) / 2;
            shift_(left, right, nr_left as isize - target_left as isize);
            parent.keys.set(r.index, right.first_key().unwrap());
        }
    }

    fn rebalance2(spine: &mut Spine, left_idx: usize) -> Result<()> {
        let mut parent = w_node(spine.child());

        let left_loc = parent.values.get(left_idx);
        let mut left = Child {
            index: left_idx,
            node: w_node(spine.shadow(left_loc)?),
        };

        let right_loc = parent.values.get(left_idx + 1);
        let mut right = Child {
            index: left_idx + 1,
            node: w_node(spine.shadow(right_loc)?),
        };

        rebalance2_(&mut parent, &mut left, &mut right);
        Ok(())
    }

    // We dump as many entries from center as possible into left, the the rest
    // in right, then rebalance.  This wastes some cpu, but I want something
    // simple atm.
    fn delete_center_node(parent: &mut WNode, l: &mut Child, c: &mut Child, r: &mut Child) {
        let left = &mut l.node;
        let center = &mut c.node;
        let right = &mut r.node;

        let nr_left = left.nr_entries.get();
        let nr_center = center.nr_entries.get();
        // let nr_right = right.nr_entries.get();

        let shift = std::cmp::min::<usize>(MAX_ENTRIES - nr_left as usize, nr_center as usize);
        shift_(left, center, -(shift as isize));

        if shift != nr_center as usize {
            let shift = nr_center as usize - shift;
            shift_(center, right, shift as isize);
        }

        parent.keys.set(r.index, right.first_key().unwrap());
        parent.remove_at(c.index);
        r.index -= 1;

        rebalance2_(parent, l, r);
    }

    fn redistribute3(parent: &mut WNode, l: &mut Child, c: &mut Child, r: &mut Child) {
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

        parent.keys.set(c.index, center.first_key().unwrap());
        parent.keys.set(r.index, right.first_key().unwrap());
    }

    fn rebalance3_(parent: &mut WNode, l: &mut Child, c: &mut Child, r: &mut Child) {
        let nr_left = l.node.nr_entries.get();
        let nr_center = c.node.nr_entries.get();
        let nr_right = r.node.nr_entries.get();

        let threshold = ((MAX_ENTRIES / 3) * 4 + 1) as u32;

        if nr_left + nr_center + nr_right < threshold {
            delete_center_node(parent, l, c, r);
        } else {
            redistribute3(parent, l, c, r);
        }
    }

    fn rebalance3(spine: &mut Spine, left_idx: usize) -> Result<()> {
        let mut parent = w_node(spine.child());

        let left_loc = parent.values.get(left_idx);
        let mut left = Child {
            index: left_idx,
            node: w_node(spine.shadow(left_loc)?),
        };

        let center_loc = parent.values.get(left_idx + 1);
        let mut center = Child {
            index: left_idx + 1,
            node: w_node(spine.shadow(center_loc)?),
        };

        let right_loc = parent.values.get(left_idx + 2);
        let mut right = Child {
            index: left_idx + 2,
            node: w_node(spine.shadow(right_loc)?),
        };

        rebalance3_(&mut parent, &mut left, &mut center, &mut right);
        Ok(())
    }

    fn rebalance_children(spine: &mut Spine, key: u32) -> Result<()> {
        let child = w_node(spine.child());

        // FIXME: this implies a previous op left it in this state
        // If there's only 1 entry in the child then it's doing nothing useful, so
        // we can link the parent directly to the single grandchild.
        if child.nr_entries.get() == 1 {
            // FIXME: it would be more efficient to get the parent to
            // point directly to the grandchild
            let gc_loc = child.values.get(0);

            // Copy the grandchild over the child
            let mut child = spine.child();
            let gc = spine.peek(gc_loc)?;
            child.rw().copy_from_slice(gc.r());
        } else {
            let idx = child.keys.bsearch(key);
            if idx < 0 {
                // key isn't in the tree
                todo!();
            }

            let has_left_sibling = idx > 0;
            let has_right_sibling = idx < (child.nr_entries.get() - 1) as isize;

            if !has_left_sibling {
                rebalance2(spine, idx as usize)?;
            } else if !has_right_sibling {
                rebalance2(spine, (idx - 1) as usize)?;
            } else {
                rebalance3(spine, (idx - 1) as usize)?;
            }
        }

        Ok(())
    }

    // Returns the old value, if present
    fn do_leaf(child: &mut WNode, key: u32) -> Result<Option<u32>> {
        let idx = child.keys.bsearch(key);
        if idx < 0 || idx as u32 >= child.nr_entries.get() || child.keys.get(idx as usize) != key {
            return Ok(None);
        }

        let val = child.values.get(idx as usize);
        child.remove_at(idx as usize);
        Ok(Some(val))
    }

    pub fn remove(spine: &mut Spine, key: u32) -> Result<Option<u32>> {
        let mut idx = 0isize;

        loop {
            let mut child = w_node(spine.child());

            if !spine.top() {
                // patch up the parent node
                let mut parent = w_node(spine.parent());
                parent.values.set(idx as usize, child.loc);
            }

            if child.is_leaf() {
                return do_leaf(&mut child, key);
            }

            drop(child);
            rebalance_children(spine, key)?;

            let mut child = w_node(spine.child());
            if child.flags.get() == BTreeFlags::Leaf as u32 {
                return do_leaf(&mut child, key);
            }

            idx = child.keys.bsearch(key);

            // We know the key is present or else rebalance_children would have failed.
            // FIXME: check this
            spine.push(child.values.get(idx as usize))?;
        }
    }
}

//-------------------------------------------------------------------------

pub struct BTree {
    tm: Arc<TransactionManager>,
    root: u32,
}

impl BTree {
    pub fn new(tm: Arc<TransactionManager>, root: u32) -> Self {
        Self { tm, root }
    }

    pub fn empty_tree(tm: Arc<TransactionManager>) -> Result<Self> {
        let root = {
            let root = tm.new_block(&BNODE_KIND)?;
            let root = init_node(root, true)?;
            root.loc
        };

        Ok(Self { tm, root })
    }

    pub fn root(&self) -> u32 {
        self.root
    }

    pub fn lookup(&self, key: u32) -> Option<u32> {
        let mut block = self.tm.read(self.root, &BNODE_KIND).unwrap();

        loop {
            let node = Node::new(block.loc(), block);

            let idx = node.keys.bsearch(key);
            if idx < 0 || idx >= node.nr_entries.get() as isize {
                return None;
            }

            if node.is_leaf() {
                return if node.keys.get(idx as usize) == key {
                    Some(node.values.get(idx as usize))
                } else {
                    None
                };
            }

            let child = node.values.get(idx as usize);
            block = self.tm.read(child, &BNODE_KIND).unwrap();
        }
    }

    pub fn insert(&mut self, key: u32, value: u32) -> Result<()> {
        let mut spine = Spine::new(self.tm.clone(), self.root)?;
        insert_utils::insert(&mut spine, key, value)?;
        self.root = spine.get_root();
        Ok(())
    }

    pub fn remove(&mut self, key: u32) -> Result<Option<u32>> {
        let mut spine = Spine::new(self.tm.clone(), self.root)?;
        let r = remove_utilities::remove(&mut spine, key)?;
        self.root = spine.get_root();
        Ok(r)
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

//-------------------------------------------------------------------------

#[cfg(test)]
mod test {
    use super::*;
    use crate::block_allocator::*;
    use crate::core::*;
    use anyhow::{ensure, Result};
    use rand::seq::SliceRandom;
    use std::sync::{Arc, Mutex};
    use thinp::io_engine::*;

    fn mk_engine(nr_blocks: u32) -> Arc<dyn IoEngine> {
        Arc::new(CoreIoEngine::new(nr_blocks as u64))
    }

    fn mk_allocator(cache: Arc<MetadataCache>, nr_data_blocks: u64) -> Arc<Mutex<BlockAllocator>> {
        let mut allocator = BlockAllocator::new(cache, nr_data_blocks);
        allocator.allocate_metadata_specific(0); // reserve the superblock
        Arc::new(Mutex::new(allocator))
    }

    #[allow(dead_code)]
    struct Fixture {
        engine: Arc<dyn IoEngine>,
        cache: Arc<MetadataCache>,
        allocator: Arc<Mutex<BlockAllocator>>,
        tm: Arc<TransactionManager>,
        tree: BTree,
    }

    impl Fixture {
        fn new(nr_metadata_blocks: u32, nr_data_blocks: u64) -> Result<Self> {
            let engine = mk_engine(nr_metadata_blocks);
            let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);
            let allocator = mk_allocator(cache.clone(), nr_data_blocks);
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

        fn lookup(&self, key: u32) -> Option<u32> {
            self.tree.lookup(key)
        }

        fn insert(&mut self, key: u32, value: u32) -> Result<()> {
            self.tree.insert(key, value)
        }

        fn remove(&mut self, key: u32) -> Result<Option<u32>> {
            self.tree.remove(key)
        }

        fn commit(&mut self) -> Result<()> {
            self.tm.commit()
        }
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

        fix.insert(0, 100)?;
        fix.commit()?; // FIXME: shouldn't be needed
        ensure!(fix.lookup(0) == Some(100));

        Ok(())
    }

    fn insert_test(keys: &[u32]) -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;

        fix.commit()?;

        for (i, k) in keys.iter().enumerate() {
            fix.insert(*k, *k * 2)?;
            // let n = fix.check()?;
            // ensure!(n == i as u32 + 1);
        }

        fix.commit()?;

        for k in keys {
            ensure!(fix.lookup(*k) == Some(k * 2));
        }

        let n = fix.check()?;
        ensure!(n == keys.len() as u32);

        Ok(())
    }
    #[test]
    fn insert_sequence() -> Result<()> {
        let count = 100000;
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
        fix.insert(key, val)?;
        ensure!(fix.lookup(key) == Some(val));
        fix.remove(key)?;
        ensure!(fix.lookup(key) == None);
        Ok(())
    }

    #[test]
    fn remove_random() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        // build a big btree
        let count = 100_000;
        for i in 0..count {
            fix.insert(i, i * 3)?;
        }
        eprintln!("built tree");

        let mut keys: Vec<u32> = (0..count).collect();
        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        for (i, k) in keys.into_iter().enumerate() {
            ensure!(fix.lookup(k).is_some());
            let mval = fix.remove(k)?;
            ensure!(mval.is_some());
            ensure!(mval.unwrap() == k * 3);
            ensure!(fix.lookup(k).is_none());
            if i % 1000 == 0 {
                eprintln!("removed {}", i);
            }

            /*
            let n = fix.check()?;
            ensure!(n == count - i as u32 - 1);
            */
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------
