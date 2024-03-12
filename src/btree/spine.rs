use anyhow::{anyhow, ensure, Result};
use std::fmt;
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

struct Frame {
    parent_index: Option<usize>,
    block: WriteProxy,
}

/// `Spine` represents a modifiable path within a B-tree structure,
/// encapsulating a stack of nodes from the root to a specific target node.
///
/// This structure is designed to facilitate operations that
/// require modifications along a path in the B-tree, such as insertions,
/// deletions, or rebalancing. It maintains a stack of `Frame` instances,
/// each representing a node in the path, along with its parent's index and
/// a proxy for writing changes to the node.
pub struct Spine {
    tm: Arc<TransactionManager>,
    context: ReferenceContext,
    nodes: Vec<Frame>,
}

impl fmt::Debug for Spine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Spine")
            .field("context", &self.context)
            .field(
                "frames",
                &self
                    .nodes
                    .iter()
                    .map(|frame| (frame.parent_index, frame.block.loc()))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl Spine {
    pub fn new(
        tm: Arc<TransactionManager>,
        context: ReferenceContext,
        root: MetadataBlock,
    ) -> Result<Self> {
        let block = tm.shadow(context, root, &BNODE_KIND)?;
        let nodes = vec![Frame {
            parent_index: None,
            block,
        }];

        Ok(Self { tm, context, nodes })
    }

    pub fn get_root(&self) -> MetadataBlock {
        self.nodes[0].block.loc()
    }

    /// True if there is no parent node
    pub fn is_top(&self) -> bool {
        self.nodes.len() == 1
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    #[allow(dead_code)]
    pub fn is_leaf(&self) -> Result<bool> {
        let child = &self.nodes.last().unwrap().block;
        Ok(read_flags(child.r())? == BTreeFlags::Leaf)
    }

    pub fn is_internal(&self) -> Result<bool> {
        let child = &self.nodes.last().unwrap().block;
        Ok(read_flags(child.r())? == BTreeFlags::Internal)
    }

    fn patch(&mut self, parent_idx: usize, loc: MetadataBlock) {
        let mut child = w_node::<MetadataBlock>(self.child());
        child.values.set(parent_idx, &loc);
    }

    /// Adds a new node to the `Spine`, effectively descending one level
    /// deeper in the B-tree.
    ///
    /// This method takes the index into the current child node.
    /// 1. Retrieves the location (`loc`) of the new node based on the provided `parent_index`.
    /// 2. Creates a shadow copy of the new node for modification, using the transaction manager.
    /// 3. Updates the parent node's child pointer to point to the location of the shadowed new node.
    /// 4. Pushes a new `Frame` onto the `Spine` stack, representing the new node.
    pub fn push(&mut self, parent_index: usize) -> Result<()> {
        let parent = self.child_node();
        let loc = parent.values.get(parent_index);
        let block = self.tm.shadow(self.context, loc, &BNODE_KIND)?;
        self.patch(parent_index, block.loc());
        self.nodes.push(Frame {
            parent_index: Some(parent_index),
            block,
        });
        Ok(())
    }

    /// Removes the top node from the `Spine`, moving one level up in the B-tree.
    ///
    /// This method is used to backtrack after descending into the
    /// tree. It's not allowed to pop the root node.
    ///
    /// # Returns
    /// * `Result<()>` - Ok if the top node was successfully removed, or an error if attempting to pop the root node.
    ///
    /// # Errors
    /// Returns an error if called on a `Spine` with only the root node, as the root cannot be popped.
    pub fn pop(&mut self) -> Result<()> {
        if self.is_top() {
            return Err(anyhow!("can't pop the root of the spine"));
        }

        self.nodes.pop();
        Ok(())
    }

    /// Replaces the root node of the `Spine` with a new block, identified by `loc`.
    ///
    /// This method is typically used during operations that require the
    /// root of the B-tree to be modified, such as when the current root
    /// is split during an insertion that increases the tree's height. It
    /// ensures that the `Spine`'s path reflects the new structure of the
    /// tree by updating the root node to point to the new location.
    pub fn replace_root(&mut self, loc: MetadataBlock) -> Result<()> {
        ensure!(self.is_top());
        let block = self.shadow(loc)?;
        if let Some(last) = self.nodes.last_mut() {
            *last = Frame {
                parent_index: None,
                block,
            };
        }
        Ok(())
    }

    /// Temporarily reads a block at the specified location without
    /// modifying the `Spine`.
    pub fn peek(&self, loc: MetadataBlock) -> Result<ReadProxy> {
        let block = self.tm.read(loc, &BNODE_KIND)?;
        Ok(block)
    }

    /// Used for temporary writes, such as siblings for rebalancing.
    /// We can always use replace_child() to put them on the spine.
    pub fn shadow(&mut self, loc: MetadataBlock) -> Result<WriteProxy> {
        let block = self.tm.shadow(self.context, loc, &BNODE_KIND)?;
        Ok(block)
    }

    fn child(&mut self) -> WriteProxy {
        self.nodes.last_mut().unwrap().block.clone()
    }

    pub fn child_node<LeafV: Serializable>(&mut self) -> Node<LeafV, WriteProxy> {
        w_node(self.child())
    }

    fn parent(&self) -> WriteProxy {
        if self.nodes.len() <= 1 {
            panic!("No parent");
        }

        self.nodes[self.nodes.len() - 2].block.clone()
    }

    pub fn parent_node(&mut self) -> Node<MetadataBlock, WriteProxy> {
        w_node(self.parent())
    }

    pub fn parent_index(&self) -> Option<usize> {
        self.nodes.last().unwrap().parent_index
    }

    pub fn new_block(&self) -> Result<WriteProxy> {
        self.tm.new_block(self.context, &BNODE_KIND)
    }
}

//-------------------------------------------------------------------------
