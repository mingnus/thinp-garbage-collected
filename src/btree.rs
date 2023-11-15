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

            if node.flags.get() == BTreeFlags::Leaf as u32 {
                return Some(node.values.get(idx as usize));
            }

            let child = node.values.get(idx as usize);
            block = self.tm.read(child, &BNODE_KIND).unwrap();
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
        let block = self.tm.read(loc, &BNODE_KIND).unwrap();
        let node = r_node(block);
        self.get_node_free_space_(&node)
    }

    fn split_into_two_(&mut self, spine: &mut Spine, idx: usize, key: u32) -> Result<()> {
        let mut left = w_node(spine.child());
        let right_block = spine.new_block()?;
        let mut right = init_node(right_block.clone(), left.is_leaf())?;
        self.redistribute2_(&mut left, &mut right);

        // Adjust the parent keys
        let mut parent = w_node(spine.parent());

        parent.keys.insert_at(idx + 1, right.keys.get(0));
        parent.values.insert_at(idx + 1, right.loc);
        parent.nr_entries.inc(1);

        // Choose the correct child in the spine
        if key >= right.keys.get(0) {
            spine.replace_child(right_block);
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
        let mut spine = Spine::new(self.tm.clone(), self.root)?;
        let mut idx = 0isize;

        loop {
            let mut child = w_node(spine.child());

            if !self.has_space_for_insert_(&child) {
                if spine.top() {
                    self.split_beneath_(&mut spine, key)?;
                } else {
                    self.rebalance_or_split_(&mut spine, idx as usize, key)?;
                }

                child = w_node(spine.child());
            }

            idx = child.keys.bsearch(key);

            if child.flags.get() == BTreeFlags::Leaf as u32 {
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

        self.root = spine.get_root();
        Ok(())
    }

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
        use rand::seq::SliceRandom;

        let count = 100_000;
        let mut keys: Vec<u32> = (0..count).collect();

        // shuffle the keys
        let mut rng = rand::thread_rng();
        keys.shuffle(&mut rng);

        insert_test(&keys)
    }
}

//-------------------------------------------------------------------------
