use anyhow::Result;
use std::sync::Arc;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

/// A Spine is a stack of nodes that are being modified.  They always
/// start with the superblock.  Any operation that modifies a data structure
/// should use this.
// FIXME: Should tm create these?
// How do we handle nested btrees?

/*
fn insert_mapping(spine: &mut Spine, thin_id: u32, mapping: u64) -> Result<()> {
    let dev_scope = spine.scope();

    let dev_root = todo!();
    let dev_tree = BTree::new(dev_root)?;
    let mroot = dev_tree.lookup(thin_id)?;

    let mapping_tree = BTree::new(mroot.get())?;
    mapping_tree.insert(spine, mapping)?;

    mroot.set(m_scope.get());
    // FIXME: set new dev root in superblock

    Ok(())
}
*/

pub struct Spine {
    pub tm: Arc<TransactionManager>, // FIXME: stop this being public
    context: ReferenceContext,
    new_root: MetadataBlock,
    parent: Option<WriteProxy>,
    child: WriteProxy,
}

impl Spine {
    pub fn new(
        tm: Arc<TransactionManager>,
        context: ReferenceContext,
        root: MetadataBlock,
    ) -> Result<Self> {
        let child = tm.shadow(context, root, &BNODE_KIND)?;
        let new_root = child.loc();

        Ok(Self {
            tm,
            context,
            new_root,
            parent: None,
            child,
        })
    }

    pub fn get_root(&self) -> MetadataBlock {
        self.new_root
    }

    /// True if there is no parent node
    pub fn is_top(&self) -> bool {
        self.parent.is_none()
    }

    pub fn push(&mut self, loc: MetadataBlock) -> Result<()> {
        // FIXME: remove
        if let Some(parent) = &self.parent {
            assert!(parent.loc() != loc);
        }

        let mut block = self.tm.shadow(self.context, loc, &BNODE_KIND)?;
        std::mem::swap(&mut block, &mut self.child);
        self.parent = Some(block);
        Ok(())
    }

    pub fn replace_child(&mut self, block: WriteProxy) {
        self.child = block;
    }

    pub fn replace_child_loc(&mut self, loc: MetadataBlock) -> Result<()> {
        assert!(loc != self.child.loc());
        let block = self.shadow(loc)?;
        self.child = block;
        Ok(())
    }

    pub fn peek(&self, loc: MetadataBlock) -> Result<ReadProxy> {
        let block = self.tm.read(loc, &BNODE_KIND)?;
        Ok(block)
    }

    /*
    fn parent_loc(&self) -> ReferenceContext {
        match self.parent {
            None => self.root_context,
            Some(ref p) => ReferenceContext::Block(p.loc()),
        }
    }
    */

    /// Used for temporary writes, such as siblings for rebalancing.
    /// We can always use replace_child() to put them on the spine.
    pub fn shadow(&mut self, loc: MetadataBlock) -> Result<WriteProxy> {
        let block = self.tm.shadow(self.context, loc, &BNODE_KIND)?;
        Ok(block)
    }

    pub fn child(&self) -> WriteProxy {
        self.child.clone()
    }

    pub fn child_loc(&self) -> MetadataBlock {
        self.child.loc()
    }

    pub fn parent(&self) -> WriteProxy {
        match self.parent {
            None => panic!("No parent"),
            Some(ref p) => p.clone(),
        }
    }

    pub fn new_block(&self) -> Result<WriteProxy> {
        self.tm.new_block(self.context, &BNODE_KIND)
    }
}

//-------------------------------------------------------------------------
