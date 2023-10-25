use crate::block_cache::*;

use fixed_bitset::FixedBitSet;

//-------------------------------------------------------------------------

// In core implementation for now, but eventually this needs to be on disk.
// No point doing this until we have the final list of operations needed.
#[derive(Debug, Clone)]
pub struct Bitset {
    nr_bits: u64,
    bits: BitSet,
}

impl Bitset {
    pub fn new(nr_bits: u64) -> Bitset {
        Bitset {
            nr_bits,
            bits: FixedBitSet::with_capacity(nr_bits as usize),
        }
    }

    pub fn test(&self, bit: u64) -> bool {
        self.bits.test(bit as usize)
    }

    pub fn set(&mut self, bit: u64) {
        self.bits.set(bit as usize, true);
    }

    pub fn test_and_set(&mut self, bit: u64) -> bool {
        let old = self.bits.test(bit as usize);
        self.bits.set(bit as usize);
        old
    }

    pub fn clear(&mut self, bit: u64) {
        self.bits.set(bit as usize, false);
    }

    pub fn set_first_clear(&mut self) -> Option<u64> {
        let bit = self.bits.first_unset()?;
        self.bits.set(bit, true);
        Some(bit as u64)
    }

    pub fn set_first_clear_in_range(&mut self, range: &Range<u64>) -> Option<u64> {
        let bit = self
            .bits
            .range(range.start as usize, range.end as usize)
            .first_unset()?;
        self.bits.set(bit, true);
        Some(bit as u64)
    }

    pub fn negate(&mut self) {
        self.bits.negate();
    }

    pub fn first_clear(&mut self) -> Option<u64> {
        todo!();
    }
}

//-------------------------------------------------------------------------
