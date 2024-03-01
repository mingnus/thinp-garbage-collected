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

// Insert and remove both have similarly named utilities, so we keep them
// in separate modules.
fn redistribute2<V: Serializable>(left: &mut WNode<V>, right: &mut WNode<V>) {
    let nr_left = left.nr_entries.get() as usize;
    let nr_right = right.nr_entries.get() as usize;
    let total = nr_left + nr_right;
    let target_left = total / 2;

    match nr_left.cmp(&target_left) {
        std::cmp::Ordering::Less => {
            // Move entries from right to left
            let nr_move = target_left - nr_left;
            let (keys, values) = right.shift_left(nr_move);
            left.append(&keys, &values);
        }
        std::cmp::Ordering::Greater => {
            // Move entries from left to right
            let nr_move = nr_left - target_left;
            let (keys, values) = left.remove_right(nr_move);
            right.prepend(&keys, &values);
        }
        std::cmp::Ordering::Equal => { /* do nothing */ }
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
    new_parent.append(&[lkeys[0], rkeys[0]], &[left.loc, right.loc]);

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

fn rebalance_left<V: Serializable>(spine: &mut Spine, parent_idx: usize, key: u32) -> Result<()> {
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

fn rebalance_right<V: Serializable>(spine: &mut Spine, parent_idx: usize, key: u32) -> Result<()> {
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

fn ensure_space<NV: Serializable>(spine: &mut Spine, key: u32, idx: isize) -> Result<WNode<NV>> {
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
        let flags = read_flags(spine.child().r())?;

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
                child.values.insert_at(0, value);
                child.nr_entries.inc(1);
            } else if idx as usize >= child.keys.len() {
                // insert
                child.keys.append_single(&key);
                child.values.append_single(value);
                child.nr_entries.inc(1);
            } else if child.keys.get(idx as usize) == key {
                // overwrite
                child.values.set(idx as usize, value);
            } else {
                ensure!(child.keys.get(idx as usize) < key);
                child.keys.insert_at(idx as usize + 1, &key);
                child.values.insert_at(idx as usize + 1, value);
                child.nr_entries.inc(1);
            }

            break;
        }
    }

    Ok(())
}

pub fn overwrite<V: Serializable>(
    spine: &mut Spine,
    old_key: u32,
    new_key: u32,
    value: &V,
) -> Result<()> {
    let mut idx;

    loop {
        let flags = read_flags(spine.child().r())?;

        if flags == BTreeFlags::Internal {
            let mut child = w_node::<u32>(spine.child());

            idx = child.keys.bsearch(&new_key);
            if idx < 0 {
                // adjust the keys as we go down the spine.
                child.keys.set(0, &new_key);
                idx = 0;
            }

            spine.push(child.values.get(idx as usize))?;

            // Patch up the parent
            let loc = spine.child().loc();
            let mut p = w_node::<u32>(spine.parent());
            p.values.set(idx as usize, &loc);
        } else {
            let mut child = w_node::<V>(spine.child());

            idx = child.keys.bsearch(&old_key);

            #[cfg(debug_assertions)]
            {
                if idx > 0 {
                    ensure!(child.keys.get(idx as usize - 1) < new_key);
                }

                if (idx as usize) < child.keys.len() - 1 {
                    ensure!(child.keys.get(idx as usize + 1) > new_key);
                }
            }

            if child.keys.get(idx as usize) != old_key {
                return Err(anyhow!("missing key in call to overwrite"));
            }

            child.keys.set(idx as usize, &new_key);
            child.values.set(idx as usize, value);

            break;
        }
    }

    Ok(())
}
//-------------------------------------------------------------------------
