use crate::block_cache::Kind;

pub const EPHEMERAL_KIND: Kind = Kind(0u32);
pub const SUPERBLOCK_KIND: Kind = Kind(1u32);
pub const BNODE_KIND: Kind = Kind(2u32);
