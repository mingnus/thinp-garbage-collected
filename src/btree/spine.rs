use anyhow::{anyhow, ensure, Result};
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::node::*;
use crate::packed_array::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

struct Frame {
    parent_index: Option<usize>,
    block: WriteProxy,
}

/// A Spine is a stack of nodes that are being modified.  eg, it could
/// represent the path from the root of a btree to an individual leaf.
pub struct Spine {
    pub tm: Arc<TransactionManager>, // FIXME: stop this being public
    context: ReferenceContext,
    nodes: Vec<Frame>,
}

impl Spine {
    pub fn new(
        tm: Arc<TransactionManager>,
        context: ReferenceContext,
        root: MetadataBlock,
    ) -> Result<Self> {
        let block = tm.shadow(context, root, &BNODE_KIND)?;
        let mut nodes = Vec::new();
        nodes.push(Frame {
            parent_index: None,
            block,
        });

        Ok(Self { tm, context, nodes })
    }

    pub fn get_root(&self) -> MetadataBlock {
        self.nodes[0].block.loc()
    }

    /// True if there is no parent node
    pub fn is_top(&self) -> bool {
        self.nodes.len() == 1
    }

    pub fn push(&mut self, parent_index: usize, loc: MetadataBlock) -> Result<()> {
        let block = self.tm.shadow(self.context, loc, &BNODE_KIND)?;
        self.nodes.push(Frame {
            parent_index: Some(parent_index),
            block,
        });
        Ok(())
    }

    pub fn pop(&mut self) -> Result<()> {
        if self.is_top() {
            return Err(anyhow!("can't pop the root of the spine"));
        }

        self.nodes.pop();
        Ok(())
    }

    fn replace_child(&mut self, parent_index: usize, block: WriteProxy) {
        if let Some(last) = self.nodes.last_mut() {
            *last = Frame {
                parent_index: Some(parent_index),
                block,
            };
        }
    }

    pub fn replace_root(&mut self, parent_index: usize, loc: MetadataBlock) -> Result<()> {
        ensure!(self.is_top());
        let block = self.shadow(loc)?;
        self.replace_child(parent_index, block);
        Ok(())
    }

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

    pub fn child(&mut self) -> WriteProxy {
        self.nodes.last_mut().unwrap().block.clone()
    }

    pub fn child_node<LeafV: Serializable>(&mut self) -> Node<LeafV, WriteProxy> {
        w_node(self.child())
    }

    pub fn child_loc(&self) -> MetadataBlock {
        self.nodes.last().unwrap().block.loc()
    }

    pub fn parent(&self) -> WriteProxy {
        if self.nodes.len() <= 1 {
            panic!("No parent");
        }

        self.nodes[self.nodes.len() - 2].block.clone()
    }

    // FIXME: do we need this now that the spine patches the parent entries?
    pub fn parent_index(&self) -> Option<usize> {
        self.nodes.last().unwrap().parent_index
    }

    pub fn new_block(&self) -> Result<WriteProxy> {
        self.tm.new_block(self.context, &BNODE_KIND)
    }
}

// FIXME: this should be done automatically by the spine
pub fn patch_parent(spine: &mut Spine, parent_idx: usize, loc: MetadataBlock) {
    if !spine.is_top() {
        let mut parent = w_node::<MetadataBlock>(spine.parent());
        parent.values.set(parent_idx, &loc);
    }
}

//-------------------------------------------------------------------------
