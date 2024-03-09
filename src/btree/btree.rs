use anyhow::{ensure, Result};
use std::collections::{BTreeSet, VecDeque};
use std::sync::Arc;

use crate::block_allocator::BlockRef;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::insert;
use crate::btree::node::*;
use crate::btree::remove;
use crate::btree::spine::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

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

    pub fn clone(&self, context: ReferenceContext) -> Self {
        Self {
            tm: self.tm.clone(),
            context,
            root: self.root,
            phantom: self.phantom,
        }
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
        let r = remove::remove(&mut spine, key)?;
        self.root = spine.get_root();
        Ok(r)
    }

    pub fn remove_geq<SplitFn: FnOnce(u32, &V) -> (u32, V)>(
        &mut self,
        key: u32,
        split_fn: SplitFn,
    ) -> Result<()> {
        let mut spine = self.mk_spine()?;
        remove::remove_geq(&mut spine, key, split_fn)?;
        self.root = spine.get_root();
        Ok(())
    }

    pub fn remove_lt<SplitFn: FnOnce(u32, &V) -> (u32, V)>(
        &mut self,
        key: u32,
        split_fn: SplitFn,
    ) -> Result<()> {
        let mut spine = self.mk_spine()?;
        remove::remove_lt(&mut spine, key, split_fn)?;
        self.root = spine.get_root();
        Ok(())
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
    use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
    use rand::seq::SliceRandom;
    use rand::Rng;
    use std::io::{self, Read, Write};
    use std::sync::{Arc, Mutex};
    use test_log::test;
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
    #[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Copy, Clone)]
    struct Value {
        v: u32,
        len: u32,
    }

    impl Serializable for Value {
        fn packed_len() -> usize {
            6
        }

        fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
            w.write_u32::<LittleEndian>(self.v)?;
            w.write_u16::<LittleEndian>(self.len as u16)?;
            Ok(())
        }

        fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
            let v = r.read_u32::<LittleEndian>()?;
            let len = r.read_u16::<LittleEndian>()?;
            Ok(Self { v, len: len as u32 })
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

        fn clone(&self, context: ReferenceContext) -> Self {
            Self {
                engine: self.engine.clone(),
                cache: self.cache.clone(),
                allocator: self.allocator.clone(),
                tm: self.tm.clone(),
                tree: self.tree.clone(context),
            }
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
        Value { v, len: 3 }
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
        ensure!(fix.lookup(0) == Some(mk_value(100)));

        Ok(())
    }

    fn insert_test_(fix: &mut Fixture, keys: &[u32]) -> Result<()> {
        eprintln!("inserting {} keys", keys.len());
        for (i, k) in keys.iter().enumerate() {
            fix.insert(*k, &mk_value(*k * 2))?;
            if i % 1000 == 0 {
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

    #[test]
    fn remove_geq_empty() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let no_split = |k: u32, v: &Value| (k, *v);

        fix.tree.remove_geq(100, no_split)?;
        ensure!(fix.tree.check()? == 0);
        Ok(())
    }

    #[test]
    fn remove_lt_empty() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        fix.commit()?;

        let no_split = |k: u32, v: &Value| (k, *v);

        fix.tree.remove_lt(100, no_split)?;
        ensure!(fix.tree.check()? == 0);
        Ok(())
    }

    fn build_tree(fix: &mut Fixture, count: u32) -> Result<()> {
        fix.commit()?;

        for i in 0..count {
            fix.insert(i, &mk_value(i * 3))?;
        }

        Ok(())
    }

    fn remove_geq_and_verify(fix: &mut Fixture, cut: u32) -> Result<()> {
        let no_split = |k: u32, v: &Value| (k, *v);
        fix.tree.remove_geq(cut, no_split)?;
        ensure!(fix.tree.check()? == cut);

        let mut c = fix.tree.cursor(0)?;

        // Check all entries are below `cut`
        for i in 0..cut {
            let (k, v) = c.get()?.unwrap();
            ensure!(k == i);
            ensure!(v.v == i * 3);
            c.next_entry()?;
        }

        Ok(())
    }

    fn remove_lt_and_verify(fix: &mut Fixture, count: u32, cut: u32) -> Result<()> {
        let no_split = |k: u32, v: &Value| (k, *v);
        fix.tree.remove_lt(cut, no_split)?;
        ensure!(fix.tree.check()? == count - cut);

        let mut c = fix.tree.cursor(0)?;

        // Check all entries are above `cut`
        for i in cut..count {
            let (k, v) = c.get()?.unwrap();
            ensure!(k == i);
            ensure!(v.v == i * 3);
            c.next_entry()?;
        }

        Ok(())
    }

    #[test]
    fn remove_geq_small() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        build_tree(&mut fix, 100)?;
        remove_geq_and_verify(&mut fix, 50)
    }

    #[test]
    fn remove_lt_small() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let count = 100;
        build_tree(&mut fix, count)?;
        remove_lt_and_verify(&mut fix, count, 50)
    }

    #[test]
    fn remove_geq_large() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let nr_entries = 10_000;
        build_tree(&mut fix, nr_entries)?;

        // FIXME: if this is too high we run out of space I think
        let nr_loops = 50;

        for i in 0..nr_loops {
            eprintln!("loop {}", i);
            let scopes = fix.tm.scopes();
            let mut scopes = scopes.lock().unwrap();
            let scope = scopes.new_scope();
            let mut fix = fix.clone(ReferenceContext::Scoped(scope.id));
            let cut = rand::thread_rng().gen_range(0..nr_entries);
            remove_geq_and_verify(&mut fix, cut)?;
        }

        Ok(())
    }

    #[test]
    fn remove_lt_large() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;
        let nr_entries = 10_000;
        build_tree(&mut fix, nr_entries)?;

        let nr_loops = 50;

        for i in 0..nr_loops {
            eprintln!("loop {}", i);
            let scopes = fix.tm.scopes();
            let mut scopes = scopes.lock().unwrap();
            let scope = scopes.new_scope();
            let mut fix = fix.clone(ReferenceContext::Scoped(scope.id));
            let cut = rand::thread_rng().gen_range(0..nr_entries);
            remove_lt_and_verify(&mut fix, nr_entries, cut)?;
        }

        Ok(())
    }
    #[test]
    fn remove_geq_split() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;

        let cut = 150;
        let split = |k: u32, v: &Value| {
            if k + v.len > cut {
                (
                    k,
                    Value {
                        v: v.v,
                        len: cut - k,
                    },
                )
            } else {
                (k, *v)
            }
        };

        fix.insert(100, &Value { v: 200, len: 100 })?;
        fix.tree.remove_geq(150, split)?;

        ensure!(fix.tree.check()? == 1);
        ensure!(fix.tree.lookup(100)?.unwrap() == Value { v: 200, len: 50 });

        Ok(())
    }

    #[test]
    fn remove_lt_split() -> Result<()> {
        let mut fix = Fixture::new(1024, 102400)?;

        let cut = 150;
        let split = |k: u32, v: &Value| {
            if k < cut && k + v.len >= cut {
                (
                    cut,
                    Value {
                        v: v.v,
                        len: (k + v.len) - cut,
                    },
                )
            } else {
                (k, *v)
            }
        };

        fix.insert(100, &Value { v: 200, len: 100 })?;
        fix.tree.remove_lt(150, split)?;

        ensure!(fix.tree.check()? == 1);
        ensure!(fix.tree.lookup(150)?.unwrap() == Value { v: 200, len: 50 });

        Ok(())
    }
}

//---------------------------------
