use anyhow::{anyhow, ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{BTreeSet, VecDeque};
use std::io::Write;
use std::sync::Arc;
use tracing::instrument;

use crate::block_allocator::BlockRef;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::insert;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::spine::*;
use crate::transaction_manager::*;

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
            let flags = read_flags(spine.child().r())?;

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

    /*
    /// Removes a range of keys from the tree [kbegin, kend).
    pub fn remove_range<LeafV: Serializable>(
        spine: &mut Spine,
        kbegin: u32,
        kend: u32,
    ) -> Result<()> {
        let mut idx = 0isize;

        loop {
            let flags = read_flags(spine.child().r())?;

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
    */
}

//-------------------------------------------------------------------------

pub struct BTree<V: Serializable> {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
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

pub struct Cursor<'a, V: Serializable> {
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
    if frame.index >= frame.nr_entries {
        // We need to move to the next node.
        stack.pop();
        if !next_::<TreeV, MetadataBlock>(tree, stack)? {
            return Ok(false);
        }

        let frame = stack.last().unwrap();
        let n = tree.read_node::<MetadataBlock>(frame.loc)?;

        let loc = n.values.get(frame.index);
        let n = tree.read_node::<NV>(loc)?;

        stack.push(Frame {
            loc,
            index: 0,
            nr_entries: n.nr_entries.get() as usize,
        });
    }

    Ok(true)
}

fn prev_<TreeV: Serializable, NV: Serializable>(
    tree: &BTree<TreeV>,
    stack: &mut Vec<Frame>,
) -> Result<bool> {
    if stack.is_empty() {
        return Ok(false);
    }
    let frame = stack.last_mut().unwrap();
    if frame.index == 0 {
        // We need to move to the previous node.
        stack.pop();
        if !prev_::<TreeV, MetadataBlock>(tree, stack)? {
            return Ok(false);
        }
        let frame = stack.last().unwrap();
        let n = tree.read_node::<MetadataBlock>(frame.loc)?;
        let loc = n.values.get(frame.index);
        let n = tree.read_node::<NV>(loc)?;
        stack.push(Frame {
            loc,
            index: n.nr_entries.get() as usize - 1,
            nr_entries: n.nr_entries.get() as usize,
        });
    } else {
        frame.index -= 1;
    }

    Ok(true)
}

impl<'a, V: Serializable> Cursor<'a, V> {
    fn new(tree: &'a BTree<V>, key: u32) -> Result<Self> {
        let mut stack = Vec::new();
        let mut loc = tree.root();

        loop {
            if tree.is_leaf(loc)? {
                let n = tree.read_node::<V>(loc)?;
                let nr_entries = n.nr_entries.get() as usize;
                if nr_entries == 0 {
                    eprintln!("empty cursor");
                    return Ok(Self { tree, stack: None });
                }

                let mut idx = n.keys.bsearch(&key);
                if idx < 0 {
                    idx = 0;
                }

                stack.push(Frame {
                    loc,
                    index: idx as usize,
                    nr_entries,
                });

                return Ok(Self {
                    tree,
                    stack: Some(stack),
                });
            }

            let n = tree.read_node::<MetadataBlock>(loc)?;
            let nr_entries = n.nr_entries.get() as usize;

            let mut idx = n.keys.bsearch(&key);
            if idx < 0 {
                idx = 0;
            }

            // we cannot have an internal node without entries
            stack.push(Frame {
                loc,
                index: idx as usize,
                nr_entries,
            });

            loc = n.values.get(idx as usize);
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

    // Move cursor to the next entry.  Returns false if there are no more, and
    // invalidates the cursor.
    pub fn next_entry(&mut self) -> Result<bool> {
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

    // Move cursor to the previous entry.  Returns false if there are no more, and
    // invalidates the cursor.
    pub fn prev_entry(&mut self) -> Result<bool> {
        match &mut self.stack {
            None => Ok(false),
            Some(stack) => {
                if !prev_::<V, V>(self.tree, stack)? {
                    self.stack = None;
                    Ok(false)
                } else {
                    Ok(true)
                }
            }
        }
    }

    /// Returns true if the cursor is at the first entry.
    pub fn is_first(&self) -> bool {
        match &self.stack {
            None => false,
            Some(stack) => {
                for frame in stack.iter() {
                    if frame.index != 0 {
                        return false;
                    }
                }
                true
            }
        }
    }
}

impl<V: Serializable> BTree<V> {
    pub fn open_tree(tm: Arc<TransactionManager>, context: ReferenceContext, root: u32) -> Self {
        Self {
            tm,
            context,
            root,
            phantom: std::marker::PhantomData,
        }
    }

    pub fn empty_tree(tm: Arc<TransactionManager>, context: ReferenceContext) -> Result<Self> {
        let root = {
            let root = tm.new_block(context, &BNODE_KIND)?;
            let root = init_node::<V>(root, true)?;
            root.loc
        };

        Ok(Self {
            tm,
            context,
            root,
            phantom: std::marker::PhantomData,
        })
    }

    pub fn root(&self) -> u32 {
        self.root
    }

    pub fn cursor(&self, key: u32) -> Result<Cursor<V>> {
        Cursor::new(self, key)
    }

    fn is_leaf(&self, loc: MetadataBlock) -> Result<bool> {
        let b = self.tm.read(loc, &BNODE_KIND)?;
        let flags = read_flags(b.r())?;
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
            let flags = read_flags(block.r())?;

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

    fn mk_spine(&self) -> Result<Spine> {
        Spine::new(self.tm.clone(), self.context, self.root)
    }

    /// Optimisation for a paired remove/insert.  Avoids having to ensure space
    /// or rebalance.  Fails if the old_key is not present, or if the new_key doesn't
    /// fit in the old one's location.
    pub fn overwrite(&mut self, old_key: u32, new_key: u32, value: &V) -> Result<()> {
        let mut spine = self.mk_spine()?;
        insert::overwrite(&mut spine, old_key, new_key, value)?;
        self.root = spine.get_root();
        Ok(())
    }

    pub fn insert(&mut self, key: u32, value: &V) -> Result<()> {
        let mut spine = self.mk_spine()?;
        insert::insert(&mut spine, key, value)?;
        self.root = spine.get_root();
        Ok(())
    }

    pub fn remove(&mut self, key: u32) -> Result<Option<V>> {
        let mut spine = self.mk_spine()?;
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
        const SUPERBLOCK_LOCATION: u32 = 0;
        let allocator = BlockAllocator::new(cache, nr_data_blocks, SUPERBLOCK_LOCATION)?;
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
            let tree = BTree::empty_tree(tm.clone(), ReferenceContext::ThinId(0))?;

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

        fn insert(&mut self, key: u32, value: &Value) -> Result<()> {
            self.tree.insert(key, value)
        }

        fn overwrite(&mut self, old_key: u32, new_key: u32, value: &Value) -> Result<()> {
            self.tree.overwrite(old_key, new_key, value)
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

        fix.insert(0, &mk_value(100))?;
        fix.commit()?; // FIXME: shouldn't be needed
        ensure!(fix.lookup(0) == Some(mk_value(100)));

        Ok(())
    }

    fn insert_test_(fix: &mut Fixture, keys: &[u32]) -> Result<()> {
        eprintln!("inserting {} keys", keys.len());
        for (i, k) in keys.iter().enumerate() {
            fix.insert(*k, &mk_value(*k * 2))?;
            if i % 100 == 0 {
                eprintln!("{}", i);
                let n = fix.check()?;
                ensure!(n == i as u32 + 1);
            }
        }

        fix.commit()?;

        for k in keys {
            ensure!(fix.lookup(*k) == Some(mk_value(k * 2)));
        }

        let n = fix.check()?;
        ensure!(n == keys.len() as u32);

        Ok(())
    }

    fn insert_test(keys: &[u32]) -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;
        insert_test_(&mut fix, keys)
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
    fn overwrite_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        fix.insert(0, &mk_value(100))?;
        fix.commit()?; // FIXME: shouldn't be needed
        ensure!(fix.lookup(0) == Some(mk_value(100)));

        fix.overwrite(0, 100, &mk_value(123))?;
        ensure!(fix.lookup(0) == None);
        ensure!(fix.lookup(100) == Some(mk_value(123)));

        Ok(())
    }

    #[test]
    fn overwrite_many() -> Result<()> {
        let count = 100_000;
        let keys: Vec<u32> = (0..count).map(|n| n * 3).collect();

        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;
        insert_test_(&mut fix, &keys)?;

        // Overwrite all keys
        for (i, k) in keys.iter().enumerate() {
            fix.overwrite(*k, *k + 1, &mk_value(*k * 3))?;
            if i % 100 == 0 {
                eprintln!("{}", i);
                let n = fix.check()?;
                ensure!(n == count);
            }
        }

        // Verify
        for k in keys {
            ensure!(fix.lookup(k) == None);
            ensure!(fix.lookup(k + 1) == Some(mk_value(k * 3)));
        }

        Ok(())
    }

    #[test]
    #[cfg(debug_assertions)]
    fn overwrite_bad_key() -> Result<()> {
        let keys: Vec<u32> = (0..10).map(|n| n * 3).collect();

        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;
        insert_test_(&mut fix, &keys)?;

        ensure!(fix.overwrite(0, 10, &mk_value(100)).is_err());
        ensure!(fix.overwrite(9, 0, &mk_value(100)).is_err());

        Ok(())
    }

    #[test]
    fn remove_single() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let key = 100;
        let val = 123;
        fix.insert(key, &mk_value(val))?;
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
            fix.insert(i, &mk_value(i * 3))?;
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
            if i % 100 == 0 {
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

        for k in 0..1_000_000 {
            fix.insert(k, &mk_value(k * 3))?;
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

        let c = fix.tree.cursor(0)?;
        ensure!(c.get()?.is_none());
        Ok(())
    }

    #[test]
    fn populated_cursor() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        // build a big btree
        let count = 1000;
        for i in 0..count {
            fix.insert(i * 3, &mk_value(i * 3))?;
        }
        eprintln!("built tree");

        let first_key = 601;
        let mut c = fix.tree.cursor(first_key)?;

        let mut expected_key = (first_key / 3) * 3;
        loop {
            let (k, _v) = c.get()?.unwrap();
            ensure!(k == expected_key);
            expected_key += 3;

            if !c.next_entry()? {
                ensure!(expected_key == count * 3);
                break;
            }
        }

        Ok(())
    }

    #[test]
    fn cursor_prev() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        // build a big btree
        let count = 1000;
        for i in 0..count {
            fix.insert(i * 3, &mk_value(i * 3))?;
        }
        eprintln!("built tree");

        let first_key = 601;
        let mut c = fix.tree.cursor(first_key)?;

        let mut expected_key = (first_key / 3) * 3;
        loop {
            let (k, _v) = c.get()?.unwrap();
            ensure!(k == expected_key);

            c.prev_entry()?;
            let (k, _v) = c.get()?.unwrap();
            ensure!(k == expected_key - 3);
            c.next_entry()?;

            expected_key += 3;

            if !c.next_entry()? {
                ensure!(expected_key == count * 3);
                break;
            }
        }

        Ok(())
    }
}

//---------------------------------
