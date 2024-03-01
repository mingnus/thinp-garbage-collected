use rand::prelude::*;
use rand_chacha::ChaCha20Rng;
use std::collections::BTreeSet;

//-------------------------------------------------------------------------

/// Manages a set of active scope ids.  Used by ReferenceContext::Scoped.
pub struct ScopeRegister {
    rng: ChaCha20Rng,
    active_scopes: BTreeSet<u32>,
}

impl Default for ScopeRegister {
    fn default() -> Self {
        ScopeRegister {
            rng: ChaCha20Rng::from_seed(Default::default()),
            active_scopes: BTreeSet::new(),
        }
    }
}

pub struct ScopeProxy<'a> {
    register: &'a mut ScopeRegister,
    pub id: u32,
}

impl<'a> Drop for ScopeProxy<'a> {
    fn drop(&mut self) {
        self.register.drop_scope(self.id);
    }
}

impl ScopeRegister {
    fn find_unused_id(&mut self) -> u32 {
        for _ in 0..100 {
            let id = self.rng.next_u32();
            if !self.active_scopes.contains(&id) {
                return id;
            }
        }

        panic!("something wrong in scope register");
    }

    pub fn new_scope(&mut self) -> ScopeProxy {
        let id = self.find_unused_id();
        self.active_scopes.insert(id);
        ScopeProxy { register: self, id }
    }

    fn drop_scope(&mut self, id: u32) {
        self.active_scopes.remove(&id);
    }
}

//-------------------------------------------------------------------------
