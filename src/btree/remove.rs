use anyhow::{anyhow, ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::{BTreeSet, VecDeque};
use std::io::Write;
use std::sync::Arc;
use tracing::instrument;

use crate::block_allocator::BlockRef;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::spine::*;
use crate::transaction_manager::*;

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

//-------------------------------------------------------------------------
