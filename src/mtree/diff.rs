use anyhow::{Error, Result};
use diffs::myers::diff;
use diffs::Diff;

use crate::block_cache::MetadataBlock;

/// Operations that the mtree can perform on the index.
// FIXME: add a overwrite op.
#[derive(Debug, PartialEq, Eq)]
pub enum Edit {
    RemoveRange(u32, Option<u32>), // kbegin, kend
    Insert((u32, MetadataBlock)),  // key, value
}

pub type KeyValue = (u32, MetadataBlock);

struct Editor<'a> {
    old: &'a [KeyValue],
    new: &'a [KeyValue],
    edits: Vec<Edit>,
}

impl<'a> Editor<'a> {
    fn new(old: &'a [KeyValue], new: &'a [KeyValue]) -> Self {
        Self {
            old,
            new,
            edits: Vec::new(),
        }
    }

    fn edits(self) -> Vec<Edit> {
        self.edits
    }
}

impl<'a> Diff for Editor<'a> {
    type Error = Error;

    fn equal(&mut self, _old: usize, _new: usize, _len: usize) -> Result<()> {
        // noop
        Ok(())
    }

    fn delete(&mut self, old: usize, len: usize, _new: usize) -> Result<()> {
        eprintln!("delete old={} len={} new={}", old, len, _new);

        let kbegin = self.old[old].0;
        let mut kend = None;
        if old + len < self.old.len() {
            kend = Some(self.old[old + len].0);
        }

        self.edits.push(Edit::RemoveRange(kbegin, kend));
        Ok(())
    }

    fn insert(&mut self, _old: usize, new: usize, len: usize) -> Result<()> {
        for kv in &self.new[new..new + len] {
            self.edits.push(Edit::Insert(*kv));
        }
        Ok(())
    }

    fn replace(&mut self, old: usize, old_len: usize, new: usize, new_len: usize) -> Result<()> {
        self.delete(old, old_len, new)?;
        self.insert(old, new, new_len)?;
        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Generate a list of operations to transform the old index into the new index.
pub fn calc_edits(old: &[KeyValue], new: &[KeyValue]) -> Result<Vec<Edit>> {
    let mut d = Editor::new(old, new);
    diff(&mut d, old, 0, old.len(), new, 0, new.len())?;
    Ok(d.edits())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mk_edits() {
        let old = vec![(1, 4), (2, 5)];
        let new = vec![(1, 4), (3, 5)];
        let edits = calc_edits(&old, &new).unwrap();
        assert_eq!(
            edits,
            vec![Edit::RemoveRange(2, Some(2)), Edit::Insert((3, 5))]
        );
    }
}
