use anyhow::{ensure, Result};

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::btree::spine::*;
use crate::byte_types::*;
use crate::packed_array::*;

//-------------------------------------------------------------------------

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
    let mut parent = spine.child_node::<MetadataBlock>();

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
    let mut parent = spine.child_node::<MetadataBlock>();

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
    let child = spine.child_node::<MetadataBlock>();

    if child.nr_entries.get() == 1 {
        // The only node that's allowed to drop below 1/3 full is the root
        // node.
        ensure!(spine.is_top());

        let gc_loc = child.values.get(0);
        spine.replace_child_loc(0, gc_loc)?;
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
            let child = spine.child_node::<MetadataBlock>();
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
            let child = spine.child_node::<MetadataBlock>();
            idx = child.keys.bsearch(&key);

            // We know the key is present or else rebalance_children would have failed.
            // FIXME: check this
            spine.push(idx as usize, child.values.get(idx as usize))?;
        } else {
            let mut child = spine.child_node::<LeafV>();
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

//----------------

/// Traverses the spine of a B-tree, applying specific functions to internal and leaf nodes.
///
/// This function walks down the spine of a B-tree, starting from the root node and moving towards
/// the leaves based on the indices returned by `internal_fn`. At each internal node, `internal_fn`
/// is called, which can perform arbitrary operations and decide the next child to descend into by
/// returning an `Option<usize>`. If `internal_fn` returns `None`, the traversal stops early.
/// When a leaf node is encountered, `leaf_fn` is called to perform operations on the leaf.
///
/// # Parameters
///
/// - `spine`: A mutable reference to the `Spine` of the B-tree. The `Spine` should provide access
///   to the current node being visited and allow navigation to child nodes.
/// - `internal_fn`: A function applied to each visited internal node. It receives a mutable
///   reference to the internal node and returns a `Result<Option<usize>>`. The `usize` value
///   represents the index of the child node to visit next. Returning `None` stops the traversal.
/// - `leaf_fn`: A function applied once to the first leaf node encountered during the traversal.
///   It receives a mutable reference to the leaf node. Since it's a `FnOnce`, it can capture and
///   consume variables from its enclosing scope.
///
/// # Returns
///
/// A `Result<()>` indicating the success or failure of the traversal. Errors can originate from
/// any of the operations performed within `internal_fn` or `leaf_fn`, or from the underlying B-tree
/// navigation functions.
///
/// # Examples
///
/// ```ignore
/// let mut spine = // ... obtain spine from your B-tree structure ...
/// walk_spine(&mut spine, |internal_node| {
///     // Perform operations on the internal node...
///     Ok(Some(next_child_index))
/// }, |leaf_node| {
///     // Perform operations on the leaf node...
///     Ok(())
/// }).expect("Failed to walk the spine");
/// ```
///
/// # Note
///
/// This function assumes that the B-tree and the provided functions maintain the invariants
/// necessary for safe traversal and modification. It is the caller's responsibility to ensure
/// that these invariants are upheld.
fn walk_spine<LeafV, InternalFn, LeafFn>(
    spine: &mut Spine,
    internal_fn: InternalFn,
    leaf_fn: LeafFn,
) -> Result<()>
where
    LeafV: Serializable,
    InternalFn: Fn(&mut Node<MetadataBlock, WriteProxy>) -> Result<Option<usize>>, // returns index
    LeafFn: FnOnce(&mut Node<LeafV, WriteProxy>) -> Result<()>,
{
    let mut parent_idx = 0;

    loop {
        match read_flags(spine.child().r())? {
            BTreeFlags::Internal => {
                let mut child = spine.child_node::<MetadataBlock>();
                patch_parent(spine, parent_idx, child.loc);

                if let Some(idx) = internal_fn(&mut child)? {
                    parent_idx = idx;
                    spine.push(parent_idx, child.values.get(parent_idx))?;
                } else {
                    break;
                }
            }
            BTreeFlags::Leaf => {
                let mut child = spine.child_node::<LeafV>();
                patch_parent(spine, parent_idx, child.loc);
                leaf_fn(&mut child)?;
                break;
            }
        }
    }

    Ok(())
}

/// Remove all entries from a node with key >= `key`
fn node_remove_geq<V: Serializable>(child: &mut Node<V, WriteProxy>, key: u32) -> Result<()> {
    let nr_entries = child.nr_entries.get() as usize;

    // If there are no entries, nothing to be done.
    if nr_entries == 0 {
        return Ok(());
    }

    // Search for the key in the child node.  This will return
    // the highest index with key that's <= `key` (possibly -1).
    let search_result = child.keys.bsearch(&key);

    // If all entries are > `key`, remove everything and finish
    if search_result < 0 {
        child.remove_from(0);
        return Ok(());
    }

    // If the all entries are < `key`, nothing to be done.
    if search_result as usize >= nr_entries {
        return Ok(());
    }

    let idx = search_result as usize;

    // If the key exactly matches, include this key in the removal
    if child.keys.get(idx) == key {
        child.remove_from(idx);
    } else {
        // Remove all entries above `idx`
        child.remove_from(idx + 1);
    }

    Ok(())
}

/// Removes all entries in the btree that are >= `key`.  This can leave empty nodes which
/// will need to be corrected when the two halves are zipped up.
/// The split function is used to trim the new last entry in the tree.
pub fn remove_geq<LeafV, SplitFn>(spine: &mut Spine, key: u32, split_fn: SplitFn) -> Result<()>
where
    LeafV: Serializable,
    SplitFn: FnOnce(u32, &LeafV) -> (u32, LeafV),
{
    let internal_fn = |child: &mut Node<MetadataBlock, WriteProxy>| {
        node_remove_geq(child, key)?;

        // Continue with the right most entry.
        let nr_entries = child.keys.len();
        if nr_entries > 0 {
            Ok(Some(nr_entries - 1))
        } else {
            Ok(None)
        }
    };

    let leaf_fn = |child: &mut Node<LeafV, WriteProxy>| {
        node_remove_geq(child, key)?;

        // The last entry in the leaf may need adjusting.
        if let Some((key_old, value_old)) = child.last() {
            let idx = child.keys.len() - 1;
            let (key_new, value_new) = split_fn(key_old, &value_old);
            if key_new != key_old {
                child.keys.set(idx, &key_new);
            }
            if value_new != value_old {
                child.values.set(idx, &value_new);
            }
        }

        Ok(())
    };

    walk_spine(spine, internal_fn, leaf_fn)
}

//----------------

/// Remove all entries from a node with key < `key`
fn node_remove_lt<V: Serializable>(child: &mut Node<V, WriteProxy>, key: u32) -> Result<()> {
    let nr_entries = child.nr_entries.get() as usize;

    // If there are no entries, nothing to be done.
    if nr_entries == 0 {
        return Ok(());
    }

    // Search for the key in the child node.  This will return
    // the highest index with key that's <= `key` (possibly -1).
    let search_result = child.keys.bsearch(&key);

    // If all entries are >= `key`, nothing to be done.
    if search_result < 0 {
        return Ok(());
    }

    // If the all entries are > `key`, remove everything.
    if search_result as usize >= nr_entries {
        child.remove_from(0);
        return Ok(());
    }

    let idx = search_result as usize;

    // Remove all entries starting from the beginning up to `idx`
    child.shift_left(idx);

    Ok(())
}

pub fn remove_lt<LeafV, SplitFn>(spine: &mut Spine, key: u32, split_fn: SplitFn) -> Result<()>
where
    LeafV: Serializable,
    SplitFn: FnOnce(u32, &LeafV) -> (u32, LeafV),
{
    let internal_fn = |child: &mut Node<MetadataBlock, WriteProxy>| {
        node_remove_lt(child, key)?;

        let nr_entries = child.keys.len();
        if nr_entries > 0 {
            Ok(Some(0))
        } else {
            Ok(None)
        }
    };

    let leaf_fn = |child: &mut Node<LeafV, WriteProxy>| {
        node_remove_lt(child, key)?;

        // The first entry in the leaf may need adjusting.
        if let Some((key_old, value_old)) = child.first() {
            let (key_new, value_new) = split_fn(key_old, &value_old);
            if key_new != key_old {
                child.keys.set(0, &key_new);
            }
            if value_new != value_old {
                child.values.set(0, &value_new);
            }
        }

        Ok(())
    };

    walk_spine(spine, internal_fn, leaf_fn)
}

//------------------

/*
// Returns a vec of blocks starting with the root, and always taking the leftmost child.
fn leftmost_spine<LeafV: Serializable>(
    tm: &Arc<TransactionManager>,
    root: MetadataBlock,
) -> Result<Vec<MetadataBlock>> {
    let mut results = Vec::new();
    let mut b = root;

    loop {
        results.push(b);
        let block = tm.read(b, &BNODE_KIND)?;

        match read_flags(block.r())? {
            BTreeFlags::Internal => {
                let child = r_node::<MetadataBlock>(block);
                b = child.first().unwrap().1;
            }
            BTreeFlags::Leaf => {
                break;
            }
        }
    }

    Ok(results)
}

// Walk the spine down the rightmost entries.
fn rightmost_spine<LeafV: Serializable>(spine: &mut Spine) -> Result<()> {
    while read_flags(spine.child().r())? == BTreeFlags::Internal {
        let child = w_node::<MetadataBlock>(spine.child());
        spine.push(child.last().unwrap().1)?;
    }

    Ok(())
}

pub fn merge<LeafV: Serializable>(spine: &mut Spine, right_root: MetadataBlock) -> Result<()> {

    let mut parent_index = 0;

    loop {
        match read_flags(spine.child().r())? {
            BTreeFlags::Internal => {
                let mut child = w_node::<MetadataBlock>(spine.child());
                patch_parent(spine, parent_index, child.loc);
            }
            BTreeFlags::Leaf => {}
        }
    }

    Ok(())
}
*/

//-------------------------------------------------------------------------
