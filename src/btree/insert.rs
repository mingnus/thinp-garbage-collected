use anyhow::{anyhow, ensure, Result};
use tracing::instrument;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::btree::spine::*;
use crate::byte_types::*;
use crate::packed_array::*;
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

#[instrument]
fn split_beneath<NV: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
    let mut new_parent = spine.child_node::<NV>();
    let nr_left = (new_parent.nr_entries.get() / 2) as usize;
    let (lkeys, lvalues) = new_parent.shift_left(nr_left);
    let (rkeys, rvalues) = new_parent.shift_left(new_parent.nr_entries.get() as usize);

    let mut left = init_node(spine.new_block()?, new_parent.is_leaf())?;
    left.append(&lkeys, &lvalues);

    let mut right = init_node(spine.new_block()?, new_parent.is_leaf())?;
    right.append(&rkeys, &rvalues);

    // setup the parent to point to the two new children
    let mut new_parent = spine.child_node::<MetadataBlock>();
    new_parent.flags.set(BTreeFlags::Internal as u32);
    assert!(new_parent.keys.is_empty());
    assert!(new_parent.values.is_empty());
    new_parent.append(&[lkeys[0], rkeys[0]], &[left.loc, right.loc]);

    // Choose the correct child in the spine
    let left_loc = left.loc;
    let right_loc = right.loc;
    drop(left);
    drop(right);

    if key < rkeys[0] {
        spine.push(0, left_loc)?;
    } else {
        spine.push(1, right_loc)?;
    }

    Ok(())
}

#[instrument]
fn rebalance_left<V: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
    let (parent_index, loc) = {
        let mut parent = w_node(spine.parent());
        let parent_idx = spine.parent_index().unwrap();
        let left_loc = parent.values.get(parent_idx - 1);
        let left_block = spine.shadow(left_loc)?;
        let mut left = w_node::<V>(left_block.clone());
        let mut right = spine.child_node::<V>();
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let first_key = right.first_key().unwrap();
        parent.keys.set(parent_idx, &first_key);

        // Choose the correct child in the spine
        if key < first_key {
            (parent_idx - 1, left.loc)
        } else {
            (parent_idx, right.loc)
        }
    };

    spine.pop()?;
    spine.push(parent_index, loc)?;

    Ok(())
}

#[instrument]
fn rebalance_right<V: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
    let (parent_index, loc) = {
        let mut parent = w_node(spine.parent());
        let parent_idx = spine.parent_index().unwrap();
        let right_loc = parent.values.get(parent_idx + 1);
        let right_block = spine.shadow(right_loc)?;
        let mut right = w_node::<V>(right_block.clone());
        let mut left = spine.child_node::<V>();
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let first_key = right.first_key().unwrap();
        parent.keys.set(parent_idx + 1, &first_key);

        // Choose the correct child in the spine
        if key >= first_key {
            (parent_idx + 1, right.loc)
        } else {
            (parent_idx, left.loc)
        }
    };

    spine.pop()?;
    spine.push(parent_index, loc)?;

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

#[instrument]
fn split_into_two<V: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
    let (parent_index, loc) = {
        let mut left = spine.child_node::<V>();
        let right_block = spine.new_block()?;
        let mut right = init_node(right_block.clone(), left.is_leaf())?;
        redistribute2(&mut left, &mut right);

        // Adjust the parent keys
        let mut parent = w_node(spine.parent());
        let parent_index = spine.parent_index().unwrap();

        let first_key = right.first_key().unwrap();
        parent.keys.insert_at(parent_index + 1, &first_key);
        parent.values.insert_at(parent_index + 1, &right.loc);
        parent.nr_entries.inc(1);

        // Choose the correct child in the spine
        if key >= first_key {
            (parent_index + 1, right.loc)
        } else {
            (parent_index, left.loc)
        }
    };

    spine.pop()?;
    spine.push(parent_index, loc)?;

    Ok(())
}

#[instrument]
fn split_into_three<V: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
    let mut parent = w_node::<u32>(spine.parent());
    let parent_index = spine.parent_index().unwrap();

    let (parent_index, loc) = if parent_index == 0 {
        // There is no left sibling, so we rebalance 0, 1, 2
        let mut left = spine.child_node::<V>();
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
            (parent_index + 2, right.loc)
        } else if key >= m_first_key {
            (parent_index + 1, middle.loc)
        } else {
            (parent_index, left.loc)
        }
    } else {
        let left_block = spine.shadow(parent.values.get(parent_index - 1))?;
        let mut left = w_node::<V>(left_block.clone());
        let mut right = spine.child_node::<V>();

        let middle_block = spine.new_block()?;
        let mut middle = init_node(middle_block.clone(), left.is_leaf())?;

        redistribute3(&mut left, &mut middle, &mut right);

        // patch up the parent
        let r_first_key = right.first_key().unwrap();
        parent.keys.set(parent_index, &r_first_key);

        let m_first_key = middle.first_key().unwrap();
        parent.insert_at(parent_index, m_first_key, &middle.loc);

        if key < m_first_key {
            (parent_index - 1, left.loc)
        } else if key < r_first_key {
            (parent_index, middle.loc)
        } else {
            (parent_index + 1, right.loc)
        }
    };

    drop(parent);
    spine.pop()?;
    spine.push(parent_index, loc)?;

    Ok(())
}

#[instrument]
fn rebalance_or_split<V: Serializable>(spine: &mut Spine, key: u32) -> Result<()> {
    let parent = w_node::<u32>(spine.parent());
    let parent_index = spine.parent_index().unwrap();
    let nr_parent = parent.nr_entries.get() as usize;

    // Is there a left sibling?
    if parent_index > 0 {
        let left_loc = parent.values.get(parent_index - 1);

        // Can we move some entries to it?
        if get_loc_free_space::<V>(spine.tm.as_ref(), left_loc) >= SPACE_THRESHOLD {
            return rebalance_left::<V>(spine, key);
        }
    }

    // Is there a right sibling? If so, rebalance with it
    if parent_index < nr_parent - 1 {
        let right_loc = parent.values.get(parent_index + 1);

        // Can we move some entries to it?
        if get_loc_free_space::<V>(spine.tm.as_ref(), right_loc) >= SPACE_THRESHOLD {
            return rebalance_right::<V>(spine, key);
        }
    }

    // We didn't manage to rebalance, so we need to split
    drop(parent);
    if parent_index == 0 || parent_index == nr_parent - 1 {
        // When inserting a sequence that is either monotonically
        // increasing or decreasing, it's better to split a single node into two.
        split_into_two::<V>(spine, key)
    } else {
        split_into_three::<V>(spine, key)
    }
}

fn ensure_space<NV: Serializable>(spine: &mut Spine, key: u32) -> Result<WNode<NV>> {
    let mut child = spine.child_node::<NV>();

    if !has_space_for_insert::<NV, WriteProxy>(&child) {
        drop(child);
        if spine.is_top() {
            split_beneath::<NV>(spine, key)?;
        } else {
            rebalance_or_split::<NV>(spine, key)?;
        }

        child = spine.child_node();
    }

    Ok(child)
}

pub fn insert<V: Serializable>(spine: &mut Spine, key: u32, value: &V) -> Result<()> {
    let mut idx;

    loop {
        if spine.is_internal()? {
            let mut child = ensure_space::<MetadataBlock>(spine, key)?;

            // FIXME: remove, just here whilst hunting a bug
            ensure!(child.nr_entries.get() > 0);
            idx = child.keys.bsearch(&key);

            if idx < 0 {
                // adjust the keys as we go down the spine.
                child.keys.set(0, &key);
                idx = 0;
            }

            spine.push(idx as usize, child.values.get(idx as usize))?;
        } else {
            let mut child = ensure_space::<V>(spine, key)?;

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
        if spine.is_internal()? {
            let mut child = spine.child_node::<u32>();

            idx = child.keys.bsearch(&new_key);
            if idx < 0 {
                // adjust the keys as we go down the spine.
                child.keys.set(0, &new_key);
                idx = 0;
            }

            spine.push(idx as usize, child.values.get(idx as usize))?;
        } else {
            let mut child = spine.child_node::<V>();

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
