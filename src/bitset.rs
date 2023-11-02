use fixedbitset::FixedBitSet;
use std::ops::Range;

//-------------------------------------------------------------------------

// In core implementation for now, but eventually this needs to be on disk.
// No point doing this until we have the final list of operations needed.
// FIXME: very slow
#[derive(Debug, Clone)]
pub struct Bitset {
    nr_bits: u64,
    bits: FixedBitSet,
    first_clear: u64,
}

impl Bitset {
    pub fn new(nr_bits: u64) -> Bitset {
        Bitset {
            nr_bits,
            bits: FixedBitSet::with_capacity(nr_bits as usize),
            first_clear: 0,
        }
    }

    fn update_first_clear(&mut self, bit: u64, set: bool) {
        if set && bit == self.first_clear {
            self.first_clear += 1;
        } else if !set && bit < self.first_clear {
            self.first_clear = bit;
        }
    }

    pub fn test(&self, bit: u64) -> bool {
        self.bits.contains(bit as usize)
    }

    pub fn set(&mut self, bit: u64) {
        self.update_first_clear(bit, true);
        self.bits.set(bit as usize, true);
    }

    pub fn test_and_set(&mut self, bit: u64) -> bool {
        self.update_first_clear(bit, true);
        let old = self.bits.contains(bit as usize);
        self.bits.set(bit as usize, true);
        old
    }

    pub fn clear(&mut self, bit: u64) {
        self.update_first_clear(bit, false);
        self.bits.set(bit as usize, false);
    }

    fn find_first_unset(&mut self, mut start: usize, end: usize) -> Option<usize> {
        if start < self.first_clear as usize {
            start = std::cmp::min(self.first_clear as usize, end);
        }

        for i in start..end {
            if !self.bits.contains(i) {
                if i == self.first_clear as usize {
                    self.first_clear += 1;
                }
                return Some(i);
            }
        }

        None
    }

    pub fn set_first_clear(&mut self) -> Option<u64> {
        let bit = self.find_first_unset(0, self.nr_bits as usize)?;
        self.bits.set(bit, true);
        Some(bit as u64)
    }

    pub fn set_first_clear_in_range(&mut self, range: &Range<u64>) -> Option<u64> {
        let bit = self.find_first_unset(range.start as usize, range.end as usize)?;
        self.bits.set(bit, true);
        Some(bit as u64)
    }

    pub fn negate(&mut self) {
        let mut new_bits = FixedBitSet::with_capacity(self.nr_bits as usize);
        for b in 0..self.nr_bits {
            if !self.bits.contains(b as usize) {
                new_bits.set(b as usize, true);
            }
        }

        self.bits = new_bits;
    }

    /*
    pub fn first_clear(&mut self) -> Option<u64> {
        todo!();
    }
    */
}

//-------------------------------------------------------------------------
