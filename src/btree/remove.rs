use anyhow::{anyhow, ensure, Result};

use crate::block_cache::*;
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
    let b = spine.peek(loc)?;
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
    let left_block = spine.shadow(left_loc)?;
    parent.values.set(left_idx, &left_block.loc());
    let mut left = Child {
        index: left_idx,
        node: w_node::<V>(left_block),
    };

    let right_idx = left_idx + 1;
    let right_loc = parent.values.get(right_idx);
    let right_block = spine.shadow(right_loc)?;
    parent.values.set(right_idx, &right_block.loc());
    let mut right = Child {
        index: right_idx,
        node: w_node::<V>(right_block),
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
    let left_block = spine.shadow(left_loc)?;
    parent.values.set(left_idx, &left_block.loc());
    let mut left = Child {
        index: left_idx,
        node: w_node::<V>(left_block),
    };

    let center_idx = left_idx + 1;
    let center_loc = parent.values.get(center_idx);
    let center_block = spine.shadow(center_loc)?;
    parent.values.set(center_idx, &center_block.loc());
    let mut center = Child {
        index: center_idx,
        node: w_node::<V>(center_block),
    };

    let right_idx = left_idx + 2;
    let right_loc = parent.values.get(right_idx);
    let right_block = spine.shadow(right_loc)?;
    parent.values.set(right_idx, &right_block.loc());
    let mut right = Child {
        index: right_idx,
        node: w_node::<V>(right_block),
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
    let child = spine.child_node::<MetadataBlock>();

    if child.nr_entries.get() == 1 {
        // The only node that's allowed to drop below 1/3 full is the root
        // node.
        ensure!(spine.is_top());

        let gc_loc = child.values.get(0);
        spine.replace_root(gc_loc)?;
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

pub fn remove<LeafV: Serializable>(spine: &mut Spine, key: u32) -> Result<Option<LeafV>> {
    loop {
        if spine.is_internal()? {
            let child = spine.child_node::<MetadataBlock>();
            let old_loc = child.loc;

            drop(child);
            rebalance_children::<LeafV>(spine, key)?;
            let child = spine.child_node::<MetadataBlock>();

            // The child may have been erased and we don't know what
            // kind of node the new child is.
            if child.loc != old_loc {
                continue;
            }

            let idx = child.keys.bsearch(&key);

            // We know the key is present or else rebalance_children would have failed.
            // FIXME: check this
            spine.push(idx as usize)?;
        } else {
            // leaf
            let mut child = spine.child_node::<LeafV>();

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

enum InternalContinuation {
    Continue(usize), // node index
    RemoveNode,      // implicit stop
}

#[derive(Eq, PartialEq)]
enum LeafContinuation {
    Stop,
    RemoveNode,
}

/// Assumes the current child is empty, then goes back up the spine
/// removing empty nodes.
fn remove_empty_nodes(spine: &mut Spine) -> Result<()> {
    // Only the root node is allowed to be empty ...
    if spine.is_top() {
        // ...but only if it's a leaf
        if !spine.is_leaf()? {
            let mut child = spine.child_node::<MetadataBlock>();
            child.flags.set(BTreeFlags::Leaf as u32);
        }
    } else {
        let idx = spine.parent_index();
        spine.pop()?;
        let mut node = spine.child_node::<MetadataBlock>();
        node.remove_at(idx.unwrap());

        if node.is_empty() {
            remove_empty_nodes(spine)?;
        }
    }

    Ok(())
}

/// Traverses a `Spine`, applying provided functions to internal and leaf nodes,
/// facilitating operations like insertion, deletion, or modification.
///
/// This function iteratively applies `internal_fn` to internal nodes and
/// `leaf_fn` to the leaf node. The traversal continues based on the return
/// values of these functions, allowing for dynamic control over the walk
/// through the `Spine`.
///
/// # Arguments
/// * `spine` - A mutable reference to the `Spine` being traversed.
/// * `internal_fn` - Function applied to each internal node. It returns an
///   `InternalContinuation` to indicate whether to continue traversal or
///   remove the current node.
/// * `leaf_fn` - Function applied once to the leaf node, returning a
///   `LeafContinuation` to indicate whether to remove the leaf node.
///
/// # Type Parameters
/// * `LeafV` - Type of values in leaf nodes, must implement `Serializable`.
/// * `InternalFn` - Type of the function for internal nodes. Must return
///   `Result<InternalContinuation>`.
/// * `LeafFn` - Type of the function for the leaf node. Must return
///   `Result<LeafContinuation>`.
///
/// # Returns
/// * `Result<()>` - Ok if the traversal completes successfully, or an error
///   if issues arise during traversal or function execution.
///
/// # `InternalContinuation` and `LeafContinuation`
/// These enums dictate the traversal's next steps. `InternalContinuation`
/// can signal to continue to a child node (`Continue`) or to remove the
/// current node (`RemoveNode`). Similarly, `LeafContinuation` decides if
/// the leaf node should be removed (`RemoveNode`).
fn walk_spine<LeafV, InternalFn, LeafFn>(
    spine: &mut Spine,
    internal_fn: InternalFn,
    leaf_fn: LeafFn,
) -> Result<()>
where
    LeafV: Serializable,
    InternalFn: Fn(&mut Node<MetadataBlock, WriteProxy>) -> Result<InternalContinuation>, // returns index
    LeafFn: FnOnce(&mut Node<LeafV, WriteProxy>) -> Result<LeafContinuation>,
{
    loop {
        if spine.is_internal()? {
            let mut child = spine.child_node::<MetadataBlock>();

            match internal_fn(&mut child)? {
                InternalContinuation::Continue(idx) => {
                    spine.push(idx)?;
                }
                InternalContinuation::RemoveNode => {
                    remove_empty_nodes(spine)?;
                    break;
                }
            }
        } else {
            let mut child = spine.child_node::<LeafV>();
            if leaf_fn(&mut child)? == LeafContinuation::RemoveNode {
                remove_empty_nodes(spine)?;
            }
            break;
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
        use InternalContinuation::*;

        node_remove_geq(child, key)?;

        // Continue with the right most entry.
        let nr_entries = child.keys.len();
        if nr_entries > 0 {
            Ok(Continue(nr_entries - 1))
        } else {
            Ok(RemoveNode)
        }
    };

    let leaf_fn = |child: &mut Node<LeafV, WriteProxy>| {
        use LeafContinuation::*;

        node_remove_geq(child, key)?;

        if child.is_empty() {
            return Ok(RemoveNode);
        }

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

        Ok(Stop)
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
        use InternalContinuation::*;

        node_remove_lt(child, key)?;

        let nr_entries = child.keys.len();
        if nr_entries > 0 {
            Ok(Continue(0))
        } else {
            Ok(RemoveNode)
        }
    };

    let leaf_fn = |child: &mut Node<LeafV, WriteProxy>| {
        use LeafContinuation::*;

        node_remove_lt(child, key)?;

        if child.is_empty() {
            return Ok(RemoveNode);
        }

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

        Ok(Stop)
    };

    walk_spine(spine, internal_fn, leaf_fn)
}

//------------------

pub fn right_most_spine(spine: &mut Spine) -> Result<()> {
    while spine.is_internal()? {
        let node = spine.child_node::<MetadataBlock>();
        let nr_entries = node.nr_entries.get() as usize;

        if nr_entries == 0 {
            return Err(anyhow!(
                "Encountered a node with no entries while tracing the right most spine."
            ));
        }

        let idx = nr_entries - 1;
        spine.push(idx)?;
    }

    Ok(())
}

pub fn left_most_spine(spine: &mut Spine) -> Result<()> {
    while spine.is_internal()? {
        let node = spine.child_node::<MetadataBlock>();
        if node.is_empty() {
            return Err(anyhow!(
                "Encountered a node with no entries while tracing the left most spine."
            ));
        }

        spine.push(0)?;
    }

    Ok(())
}

#[derive(Eq, PartialEq)]
enum MergeOutcome {
    Merged,
    Balanced,
}

fn should_rebalance(nr_left: u32, nr_right: u32, max: u32) -> bool {
    let half = max / 2;

    if nr_left < half || nr_right < half {
        return true;
    }

    let threshold = max / 8;
    nr_left.abs_diff(nr_right) > threshold
}

fn merge_nodes<NV>(left: &mut WNode<NV>, right: &mut WNode<NV>) -> MergeOutcome
where
    NV: Serializable,
{
    let nr_left = left.nr_entries.get();
    let nr_right = right.nr_entries.get();
    let max_entries = WNode::<NV>::max_entries() as u32;

    // Check if both roots can be merged directly without exceeding the max entries limit
    if nr_left + nr_right <= max_entries {
        // We can merge both nodes into one.
        let (keys, values) = right.shift_left(right.nr_entries());
        left.append(&keys, &values);
        MergeOutcome::Merged
    } else {
        // Too many entries to merge.  Should we rebalance?

        if should_rebalance(nr_left, nr_right, max_entries) {
            let target_left = (nr_left + nr_right) / 2;
            shift_(left, right, nr_left as isize - target_left as isize);
        }
        MergeOutcome::Balanced
    }
}

/// Returns true if the right node is empty.  Does not pop the spines if we're at the
/// roots.
fn merge_and_pop<NV>(l_spine: &mut Spine, r_spine: &mut Spine) -> Result<bool>
where
    NV: Serializable,
{
    let mut left = l_spine.child_node::<NV>();
    let mut right = r_spine.child_node::<NV>();

    let outcome = merge_nodes::<NV>(&mut left, &mut right);

    if !l_spine.is_top() {
        l_spine.pop()?;
        r_spine.pop()?;

        if outcome == MergeOutcome::Merged {
            // drop the first entry of right
            let mut right = r_spine.child_node::<MetadataBlock>();
            right.remove_at(0);
        }
    }

    Ok(right.is_empty())
}

fn new_layer<NV>(l_spine: &mut Spine, r_spine: &mut Spine) -> Result<()>
where
    NV: Serializable,
{
    let new = l_spine.new_block()?;
    let mut new_root = w_node::<MetadataBlock>(new);
    let l = l_spine.child_node::<NV>();
    let r = r_spine.child_node::<NV>();
    let keys = vec![l.first_key().unwrap(), r.first_key().unwrap()];
    let values = vec![l.loc, r.loc];
    new_root.append(&keys, &values);
    l_spine.replace_root(new_root.loc)
}

pub fn merge_same_depth<LeafV>(l_spine: &mut Spine, r_spine: &mut Spine) -> Result<()>
where
    LeafV: Serializable,
{
    ensure!(l_spine.len() == r_spine.len());

    let mut r_is_empty;
    loop {
        if l_spine.is_leaf()? {
            r_is_empty = merge_and_pop::<LeafV>(l_spine, r_spine)?;
        } else {
            r_is_empty = merge_and_pop::<MetadataBlock>(l_spine, r_spine)?;
        }

        if l_spine.is_top() {
            break;
        }
    }

    if !r_is_empty {
        // We need a new layer
        if r_spine.is_leaf()? {
            new_layer::<LeafV>(r_spine, l_spine)?;
        } else {
            new_layer::<MetadataBlock>(r_spine, l_spine)?;
        }
    }

    Ok(())
}

pub fn merge<LeafV>(l_spine: &mut Spine, r_spine: &mut Spine) -> Result<()>
where
    LeafV: Serializable,
{
    let len_delta = l_spine.len() - r_spine.len();
    if len_delta > 0 {
        // left spine will become the result

        todo!();
    } else if len_delta < 0 {
        // right spine will become the result
        // FIXME: but the context for the right spine is temporary!?
        todo!();
    } else {
        merge_same_depth::<LeafV>(l_spine, r_spine)?;
    }

    Ok(())
}

//-------------------------------------------------------------------------
