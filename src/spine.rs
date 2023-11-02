use anyhow::Result;

use crate::block_cache::*;
use crate::transaction_manager::*;

//-------------------------------------------------------------------------

pub struct Spine<'a> {
    tm: &'a mut TransactionManager,
    new_root: u32,
    parent: Option<WriteProxy>,
    child: WriteProxy,
}

impl<'a> Spine<'a> {
    pub fn new(tm: &'a mut TransactionManager, root: u32) -> Result<Self> {
        let child = tm.shadow(root)?;
        let new_root = child.loc;
        Ok(Self {
            tm,
            new_root,
            parent: None,
            child: child,
        })
    }

    pub fn get_root(&self) -> u32 {
        self.new_root
    }

    pub fn top(&self) -> bool {
        self.parent.is_none()
    }

    pub fn push(&mut self, loc: u32) -> Result<()> {
        let mut block = self.tm.shadow(loc)?;
        std::mem::swap(&mut block, &mut self.child);
        self.parent = Some(block);
        Ok(())
    }

    pub fn replace_child(&mut self, block: WriteProxy) {
        self.child = block;
    }

    pub fn replace_child_loc(&mut self, loc: u32) -> Result<()> {
        let block = self.tm.shadow(loc)?;
        self.child = block;
        Ok(())
    }

    pub fn peek(&self, loc: u32) -> Result<ReadProxy> {
        let block = self.tm.read(loc)?;
        Ok(block)
    }

    // Used for temporary writes, such as siblings for rebalancing.
    // We can always use replace_child() to put them on the spine.
    pub fn shadow(&mut self, loc: u32) -> Result<WriteProxy> {
        let block = self.tm.shadow(loc)?;
        Ok(block)
    }

    pub fn child(&mut self) -> WriteProxy {
        self.child.clone()
    }

    pub fn parent(&self) -> WriteProxy {
        match self.parent {
            None => panic!("No parent"),
            Some(ref p) => p.clone(),
        }
    }

    pub fn new_block(&mut self) -> Result<WriteProxy> {
        self.tm.new_block()
    }
}

//-------------------------------------------------------------------------
