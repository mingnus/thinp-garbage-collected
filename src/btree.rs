use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use crate::block_cache::*;
use crate::byte_types::*;
use crate::spine::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

const NODE_KIND: u32 = 0x424e4f44; // 'B' 'N' 'O' 'D'
const NODE_HEADER_SIZE: usize = 8;
const MAX_ENTRIES: usize = (BLOCK_PAYLOAD_SIZE - NODE_HEADER_SIZE) / 8;
const KEYS_OFFSET: usize = NODE_HEADER_SIZE + BLOCK_HEADER_SIZE;
const VALUES_OFFSET: usize = KEYS_OFFSET + MAX_ENTRIES * 4;
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

struct Node<Data> {
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
        self.keys.prepend(keys);
        self.values.prepend(values);
        self.nr_entries.dec(keys.len() as u32);
    }

    fn append(&mut self, keys: &[u32], values: &[u32]) {
        self.keys.append(keys);
        self.values.append(values);
        self.nr_entries.dec(keys.len() as u32);
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
    Node::new(block.loc, block)
}

fn r_node(block: ReadProxy) -> RNode {
    Node::new(block.loc, block)
}

fn init_node(mut block: WriteProxy, is_leaf: bool) -> Result<WNode> {
    let loc = block.loc;

    // initialise the block
    let mut w = std::io::Cursor::new(block.rw());
    write_block_header(
        &mut w,
        BlockHeader {
            loc,
            kind: NODE_KIND,
            sum: 0,
        },
    )?;

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

pub struct BTree {
    tm: Arc<Mutex<TransactionManager>>,
    root: u32,
}

impl BTree {
    pub fn new(tm: Arc<Mutex<TransactionManager>>, root: u32) -> Self {
        Self { tm, root }
    }

    pub fn lookup(&self, key: u32) -> Option<u32> {
        let tm = self.tm.lock().unwrap();
        let mut block = tm.read(self.root).unwrap();

        loop {
            let node = Node::new(block.loc, block);

            let idx = node.keys.bsearch(key);
            if idx < 0 || idx >= node.nr_entries.get() as isize {
                return None;
            }

            if node.flags.get() == BTreeFlags::Leaf as u32 {
                return Some(node.values.get(idx as usize));
            }

            let child = node.values.get(idx as usize);
            block = tm.read(child).unwrap();
        }
    }

    //----------------

    fn redistribute2_(&mut self, left: &mut WNode, right: &mut WNode) {
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
    fn redistribute3_(&mut self, left: &mut WNode, middle: &mut WNode, right: &mut WNode) {
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
            let nr_mode = target_middle - nr_move;
            let (keys, values) = right.shift_left(nr_mode);
            middle.append(&keys, &values);
        } else {
            // Move entries from left to right
            let nr_move = nr_left - target_left - target_middle;
            let (keys, values) = left.remove_right(nr_move);
            right.prepend(&keys, &values);

            // Move entries from middle to right
            let nr_move = nr_middle;
            let (keys, values) = left.remove_right(nr_move);
            middle.prepend(&keys, &values);
        }
    }

    fn has_space_for_insert_<Data: Readable>(&self, node: &Node<Data>) -> bool {
        node.nr_entries.get() < MAX_ENTRIES as u32
    }

    fn split_beneath_(&self, spine: &mut Spine, key: u32) -> Result<()> {
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
        new_parent.append(&vec![lkeys[0], rkeys[0]], &vec![left.loc, right.loc]);

        new_parent.keys.set(0, lkeys[0]);
        new_parent.values.set(0, left.loc);
        new_parent.keys.set(1, rkeys[0]);
        new_parent.values.set(1, right.loc);
        new_parent.nr_entries.set(1);

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

    fn rebalance_left_(&mut self, spine: &mut Spine, parent_idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node(spine.parent());
        let left_loc = parent.values.get(parent_idx - 1);
        let left_block = spine.shadow(left_loc)?;
        let mut left = w_node(left_block.clone());
        let mut right = w_node(spine.child());
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        parent.keys.set(parent_idx, right.keys.get(0));

        // Choose the correct child in the spine
        if key < right.keys.get(0) {
            spine.replace_child(left_block);
        }

        Ok(())
    }

    fn rebalance_right_(&mut self, spine: &mut Spine, parent_idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node(spine.parent());
        let right_loc = parent.values.get(parent_idx + 1);
        let right_block = spine.shadow(right_loc)?;
        let mut right = w_node(right_block.clone());
        let mut left = w_node(spine.child());
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        parent.keys.set(parent_idx + 1, right.keys.get(0));

        // Choose the correct child in the spine
        if key >= right.keys.get(0) {
            spine.replace_child(right_block);
        }

        Ok(())
    }

    fn get_node_free_space_(&self, node: &RNode) -> usize {
        MAX_ENTRIES - node.nr_entries.get() as usize
    }

    fn get_loc_free_space_(&self, loc: u32) -> usize {
        let tm = self.tm.lock().unwrap();
        let block = tm.read(loc).unwrap();
        let node = r_node(block);
        self.get_node_free_space_(&node)
    }

    fn split_into_two_(&mut self, spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut left = w_node(spine.child());
        let mut right = init_node(spine.new_block()?, left.is_leaf())?;
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        let mut parent = w_node(spine.parent());
        parent.keys.insert_at(idx, right.keys.get(0));
        parent.values.insert_at(idx + 1, right.loc);
        let right_loc = right.loc;

        // Choose the correct child in the spine
        if key >= right.keys.get(0) {
            spine.replace_child_loc(right_loc)?;
        }

        Ok(())
    }

    fn split_into_three_(&mut self, spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut parent = w_node(spine.parent());

        if idx == 0 {
            // There is no left sibling, so we rebalance 0, 1, 2
            let mut left = w_node(spine.child());
            let right_block = spine.shadow(parent.values.get(1))?;
            let mut right = w_node(right_block.clone());

            let middle_block = spine.new_block()?;
            let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

            self.redistribute3_(&mut left, &mut middle, &mut right);

            // patch up the parent
            parent.keys.set(1, right.keys.get(0));
            parent.insert_at(1, middle.keys.get(0), middle.loc);

            if key >= right.keys.get(0) {
                spine.replace_child(right_block);
            } else if key >= middle.keys.get(0) {
                spine.replace_child(middle_block);
            }
        } else {
            let left_block = spine.shadow(parent.values.get(idx - 1))?;
            let mut left = w_node(left_block.clone());
            let mut right = w_node(spine.child());

            let middle_block = spine.new_block()?;
            let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

            self.redistribute3_(&mut left, &mut middle, &mut right);

            // patch up the parent
            parent.keys.set(idx, right.keys.get(0));
            parent.insert_at(idx, middle.keys.get(0), middle.loc);

            if key < middle.keys.get(0) {
                spine.replace_child(left_block);
            } else if key < right.keys.get(0) {
                spine.replace_child(middle_block);
            }
        }

        Ok(())
    }

    fn rebalance_or_split_(&mut self, spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let parent = w_node(spine.parent());
        let nr_parent = parent.nr_entries.get() as usize;

        // Is there a left sibling?
        if idx > 0 {
            let left_loc = parent.values.get(idx - 1);

            // Can we move some entries to it?
            if self.get_loc_free_space_(left_loc) >= SPACE_THRESHOLD {
                return self.rebalance_left_(spine, idx, key);
            }
        }

        // Is there a right sibling? If so, rebalance with it
        if idx < nr_parent - 1 {
            let right_loc = parent.values.get(idx + 1);

            // Can we move some entries to it?
            if self.get_loc_free_space_(right_loc) >= SPACE_THRESHOLD {
                return self.rebalance_right_(spine, idx, key);
            }
        }

        // We didn't manage to rebalance, so we need to split
        if idx == 0 || idx == nr_parent - 1 {
            // When inserting a sequence that is either monotonically
            // increasing or decreasing, it's better to split a single node into two.
            self.split_into_two_(spine, idx, key)
        } else {
            self.split_into_three_(spine, idx, key)
        }
    }

    pub fn insert(&mut self, key: u32, value: u32) -> Result<()> {
        let tm = self.tm.clone();
        let mut tm = tm.lock().unwrap();
        let mut spine = Spine::new(&mut tm, self.root)?;
        let mut idx = 0isize;

        loop {
            let mut child = w_node(spine.child());
            let loc = child.loc;

            if !self.has_space_for_insert_(&child) {
                if spine.top() {
                    self.split_beneath_(&mut spine, key)?;
                } else {
                    self.rebalance_or_split_(&mut spine, idx as usize, key)?;
                }

                child = w_node(spine.child());
            }

            idx = child.keys.bsearch(key);
            if idx < 0 {
                child.keys.set(0, key);
                idx = 0;
            }

            if child.flags.get() == BTreeFlags::Leaf as u32 {
                child.values.set(idx as usize, value);
                break;
            }

            spine.push(child.values.get(idx as usize))?;

            // Patch up the parent
            let p = spine.parent();
            child.values.set(idx as usize, loc);
        }

        self.root = spine.get_root();
        Ok(())
    }
}

//-------------------------------------------------------------------------
