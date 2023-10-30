use anyhow::Result;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};

use crate::block_cache::*;
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

fn read_node_header<R: Read>(r: &R) -> NodeHeader {
    let flags = r.read_u32::<LittleEndian>().unwrap();
    let nr_entries = r.read_u32::<LittleEndian>().unwrap();

    NodeHeader { flags, nr_entries }
}

fn write_node_header<W: Write>(w: &mut W, hdr: NodeHeader) -> Result<()> {
    w.write_u32::<LittleEndian>(hdr.flags)?;
    w.write_u32::<LittleEndian>(hdr.nr_entries)?;
    Ok(())
}

fn key_base(data: &mut [u8]) -> &mut [u8] {
    &mut data[KEYS_OFFSET..VALUES_OFFSET]
}

fn value_base(data: &mut [u8]) -> &mut [u8] {
    &mut data[VALUES_OFFSET..]
}

//-------------------------------------------------------------------------

struct U32<'a> {
    data: &'a mut [u8],
}

impl<'a> U32<'a> {
    fn new(data: &mut [u8]) -> Self {
        Self { data }
    }

    fn get(&self) -> u32 {
        let mut data = std::io::Cursor::new(&mut self.data[..4]);
        data.read_u32::<LittleEndian>().unwrap()
    }

    fn set(&self, val: u32) {
        let mut data = std::io::Cursor::new(&mut self.data[..4]);
        data.write_u32::<LittleEndian>(val).unwrap();
    }

    fn inc(&mut self, val: u32) {
        self.set(self.get() + val);
    }

    fn dec(&mut self, val: u32) {
        self.set(self.get() - val);
    }
}

//-------------------------------------------------------------------------

struct U32Array<'a> {
    max_entries: usize,
    nr_entries: usize,
    data: &'a mut [u8],
}

impl<'a> U32Array<'a> {
    fn new(data: &mut [u8], nr_entries: usize) -> Self {
        let max_entries = data.len() / 4;
        Self {
            max_entries,
            nr_entries,
            data,
        }
    }

    fn check_idx(&self, idx: usize) {
        assert!(idx < self.nr_entries);
    }

    fn byte(&self, idx: usize) -> usize {
        idx * 4
    }

    fn get(&self, idx: usize) -> u32 {
        self.check_idx(idx);
        let mut data = std::io::Cursor::new(&mut self.data[self.byte(idx)..self.byte(idx + 1)]);
        data.read_u32::<LittleEndian>().unwrap()
    }

    fn set(&mut self, idx: usize, value: u32) {
        self.check_idx(idx);
        let mut data = std::io::Cursor::new(&mut self.data[self.byte(idx)..self.byte(idx + 1)]);
        data.write_u32::<LittleEndian>(value).unwrap();
    }

    fn bsearch(&self, key: u32) -> isize {
        let mut lo = -1;
        let mut hi = self.nr_entries as isize;

        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let mid_key = self.get(mid as usize);

            if key < mid_key {
                hi = mid;
            } else if key > mid_key {
                lo = mid;
            } else {
                return mid;
            }
        }

        lo
    }

    fn shift_left(&mut self, count: usize) -> Vec<u32> {
        let mut lost = Vec::with_capacity(count);
        for i in 0..count {
            lost.push(self.get(i));
        }
        self.data.copy_within(self.byte(count).., 0);
        self.nr_entries -= count;
        lost
    }

    fn shift_right_(&mut self, count: usize) {
        self.data
            .copy_within(0..self.byte(self.nr_entries), self.byte(count));
        self.nr_entries += count;
    }

    fn remove_right_(&mut self, count: usize) -> Vec<u32> {
        let mut lost = Vec::with_capacity(count);
        for i in 0..count {
            lost.push(self.get(self.nr_entries - count + i));
        }
        self.nr_entries -= count;
        lost
    }

    fn insert_at(&mut self, idx: usize, value: u32) {
        self.data.copy_within(
            self.byte(idx)..self.byte(self.nr_entries),
            self.byte(idx + 1),
        );
        self.nr_entries += 1;
        self.set(idx, value);
    }

    fn remove_at(&mut self, idx: usize) {
        self.data.copy_within(
            self.byte(idx + 1)..self.byte(self.nr_entries),
            self.byte(idx),
        );
        self.nr_entries -= 1;
    }

    fn prepend(&mut self, values: &[u32]) {
        assert!(self.nr_entries + values.len() <= self.max_entries);
        self.shift_right_(values.len());
        for (i, v) in values.iter().enumerate() {
            self.set(i, *v);
        }
        self.nr_entries += values.len();
    }

    fn append(&mut self, values: &[u32]) {
        assert!(self.nr_entries + values.len() <= self.max_entries);
        self.nr_entries += values.len();
        for (i, v) in values.iter().enumerate() {
            self.set(self.nr_entries + i, *v);
        }
    }
}

//-------------------------------------------------------------------------

struct Node<'a> {
    loc: u32,

    flags: U32<'a>,
    nr_entries: U32<'a>,
    keys: U32Array<'a>,
    values: U32Array<'a>,
}

impl<'a> Node<'a> {
    fn new(loc: u32, data: &mut [u8]) -> Self {
        let hdr_begin = BLOCK_HEADER_SIZE;
        let flags = U32::new(&mut data[hdr_begin..hdr_begin + 4]);
        let nr_entries = U32::new(&mut data[hdr_begin + 4..hdr_begin + 8]);
        let keys = U32Array::new(key_base(data), nr_entries.get() as usize);
        let values = U32Array::new(value_base(data), nr_entries.get() as usize);

        Self {
            loc,
            flags,
            nr_entries,
            keys,
            values,
        }
    }

    fn from_block(block: &mut WriteProxy) -> Self {
        Self::new(block.loc, block.as_mut())
    }

    fn is_leaf(&self) -> bool {
        self.flags.get() == BTreeFlags::Leaf as u32
    }

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
        let keys = self.keys.remove_right_(count);
        let values = self.values.remove_right_(count);
        self.nr_entries.dec(count as u32);
        (keys, values)
    }
}

//-------------------------------------------------------------------------

pub struct BTree {
    tm: Arc<Mutex<TransactionManager>>,
    root: u32,
}

struct ShadowSpine<'a> {
    tm: &'a mut TransactionManager,
    new_root: u32,
    parent: Option<WriteProxy>,
    child: Option<WriteProxy>,
}

impl<'a> ShadowSpine<'a> {
    fn new(tm: &'a mut TransactionManager, root: u32) -> Result<Self> {
        let mut child = tm.shadow(root)?;
        let new_root = child.loc;
        Ok(Self {
            tm,
            new_root,
            parent: None,
            child: Some(child),
        })
    }

    fn top(&self) -> bool {
        self.parent.is_none()
    }

    fn push(&mut self, loc: u32) -> Result<()> {
        let block = self.tm.shadow(loc)?;
        self.parent = self.child.take();
        self.child = Some(block);
        Ok(())
    }

    // FIXME: not sure this is needed since we have replace_child()
    fn push_block(&mut self, block: WriteProxy) {
        self.parent = self.child.take();
        self.child = Some(block);
    }

    fn replace_child(&mut self, block: WriteProxy) {
        self.child = Some(block);
    }

    fn replace_child_loc(&mut self, loc: u32) -> Result<()> {
        let block = self.tm.shadow(loc)?;
        self.child = Some(block);
        Ok(())
    }

    fn peek(&self, loc: u32) -> Result<ReadProxy> {
        let block = self.tm.read(loc)?;
        Ok(block)
    }

    // Used for temporary writes, such as siblings for rebalancing.
    // We can always use replace_child() to put them on the spine.
    fn shadow(&mut self, loc: u32) -> Result<WriteProxy> {
        let block = self.tm.shadow(loc)?;
        Ok(block)
    }

    fn child(&self) -> Node {
        let child = self.child;
        Node::new(child.unwrap().loc, &mut child.as_ref().unwrap())
    }

    fn parent(&self) -> Node {
        let parent = self.parent;
        Node::new(parent.unwrap().loc, &mut parent.as_ref().unwrap())
    }

    fn new_node(&mut self, is_leaf: bool) -> Result<WriteProxy> {
        let mut block = self.tm.new_block().unwrap();

        // initialise the block
        let mut w = std::io::Cursor::new(block.as_mut());
        write_block_header(
            &mut w,
            BlockHeader {
                loc: block.loc,
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

        Ok(block)
    }
}

impl BTree {
    pub fn new(tm: Arc<Mutex<TransactionManager>>, root: u32) -> Self {
        Self { tm, root: 0 }
    }

    pub fn lookup(&self, key: u32) -> Option<u32> {
        let mut tm = self.tm.lock().unwrap();
        let mut block = tm.read(self.root).unwrap();

        loop {
            let node = Node::new(block.loc, &mut block);

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

    fn redistribute2_(&mut self, left: &mut Node, right: &mut Node) {
        let nr_left = left.nr_entries.get() as usize;
        let nr_right = right.nr_entries.get() as usize;
        let total = nr_left + nr_right;
        let target_left = total / 2;
        let target_right = total - target_left;

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
    fn redistribute3_(&mut self, left: &mut Node, middle: &mut Node, right: &mut Node) {
        let nr_left = left.nr_entries.get() as usize;
        let nr_middle = middle.nr_entries.get() as usize;
        assert!(nr_middle == 0);
        let nr_right = right.nr_entries.get() as usize;

        let total = nr_left + nr_middle + nr_right;
        let target_left = total / 3;
        let target_middle = (total - target_left) / 2;
        let target_right = total - target_left - target_middle;

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

    fn has_space_for_insert_(&self, node: &Node) -> bool {
        node.nr_entries.get() < MAX_ENTRIES as u32
    }

    fn split_beneath_(&self, spine: &mut ShadowSpine, key: u32) -> Result<()> {
        let mut new_parent = spine.child();
        let nr_left = (new_parent.nr_entries.get() / 2) as usize;
        let (lkeys, lvalues) = new_parent.shift_left(nr_left);
        let (rkeys, rvalues) = new_parent.shift_left(new_parent.nr_entries.get() as usize);

        let mut left_block = spine.new_node(new_parent.is_leaf())?;
        let mut left = Node::from_block(&mut left_block);
        left.append(&lkeys, &lvalues);

        let mut right_block = spine.new_node(new_parent.is_leaf())?;
        let mut right = Node::from_block(&mut right_block);
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
            spine.push(left.loc);
        } else {
            spine.push(right.loc);
        }

        Ok(())
    }

    fn rebalance_left_(&self, spine: &mut ShadowSpine, parent_idx: usize, key: u32) -> Result<()> {
        let mut parent = spine.parent();
        let left_loc = parent.values.get(parent_idx - 1);
        let left_block = spine.shadow(left_loc)?;
        let mut left = Node::new(left_loc, &mut left_block.as_ref());
        let mut right = spine.child();
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        parent.keys.set(parent_idx, right.keys.get(0));

        // Choose the correct child in the spine
        if key < right.keys.get(0) {
            spine.replace_child(left_block);
        }

        Ok(())
    }

    fn rebalance_right_(&self, spine: &mut ShadowSpine, parent_idx: usize, key: u32) -> Result<()> {
        let mut parent = spine.parent();
        let right_loc = parent.values.get(parent_idx + 1);
        let right_block = spine.shadow(right_loc)?;
        let mut right = Node::new(right_loc, &mut right_block.as_ref());
        let mut left = spine.child();
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        parent.keys.set(parent_idx + 1, right.keys.get(0));

        // Choose the correct child in the spine
        if key >= right.keys.get(0) {
            spine.replace_child(right_block);
        }

        Ok(())
    }

    fn get_node_free_space_(&self, node: &Node) -> usize {
        MAX_ENTRIES - node.nr_entries.get() as usize
    }

    fn get_loc_free_space_(&self, loc: u32) -> usize {
        let mut tm = self.tm.lock().unwrap();
        let mut block = tm.read(loc).unwrap();
        let node = Node::new(loc, &mut block);
        self.get_node_free_space_(&node)
    }

    fn split_into_two_(&mut self, spine: &mut ShadowSpine, idx: usize, key: u32) -> Result<()> {
        let mut left = spine.child();
        let mut right_block = spine.new_node(left.is_leaf())?;
        let mut right = Node::from_block(&mut right_block);
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        let mut parent = spine.parent();
        parent.keys.insert_at(idx, right.keys.get(0));
        parent.values.insert_at(idx + 1, right.loc);
        let right_loc = right.loc;
        drop(right);

        // Choose the correct child in the spine
        if key >= right.keys.get(0) {
            spine.replace_child_loc(right_loc);
        }

        Ok(())
    }

    fn split_into_three_(&mut self, spine: &mut ShadowSpine, idx: usize, key: u32) -> Result<()> {
        let mut parent = spine.parent();

        if idx == 0 {
            // There is no left sibling, so we rebalance 0, 1, 2
            let mut left = spine.child();
            let mut right_block = spine.shadow(parent.values.get(1))?;
            let mut right = Node::new(right_block.loc, &mut right_block.as_ref());

            let mut middle_block = spine.new_node(left.is_leaf())?;
            let mut middle = Node::from_block(&mut middle_block);

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
            let mut left_block = spine.shadow(parent.values.get(idx - 1))?;
            let mut left = Node::new(left_block.loc, &mut left_block.as_ref());
            let mut right = spine.child();

            let mut middle_block = spine.new_node(left.is_leaf())?;
            let mut middle = Node::from_block(&mut middle_block);

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

    fn rebalance_or_split_(&mut self, spine: &mut ShadowSpine, idx: usize, key: u32) -> Result<()> {
        let parent = spine.parent();
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
        let mut tm = self.tm.lock().unwrap();
        let mut spine = ShadowSpine::new(&mut tm, self.root)?;
        let mut idx = 0isize;

        loop {
            let mut child = spine.child();
            let loc = child.loc;

            if !self.has_space_for_insert_(&child) {
                if spine.top() {
                    self.split_beneath_(&mut spine, key)?;
                } else {
                    self.rebalance_or_split_(&mut spine, idx as usize, key)?;
                }

                child = spine.child();
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

            spine.push(child.values.get(idx as usize));

            // Patch up the parent
            let mut p = spine.parent();
            child.values.set(idx as usize, loc);
        }

        self.root = spine.new_root;
        Ok(())
    }
}

//-------------------------------------------------------------------------
