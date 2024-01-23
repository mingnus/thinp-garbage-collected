use super::*;

use anyhow::{ensure, Result};
use rand::seq::SliceRandom;
use std::sync::Arc;
use thinp::io_engine::*;

use crate::core::*;

//-------------------------------------------------------------------------

fn mk_engine(nr_blocks: u32) -> Arc<dyn IoEngine> {
    Arc::new(CoreIoEngine::new(nr_blocks as u64))
}

#[allow(dead_code)]
struct Fixture {
    engine: Arc<dyn IoEngine>,
    cache: Arc<MetadataCache>,
    alloc_begin: u32,
}

impl Fixture {
    fn new(nr_blocks: u32) -> Result<Self> {
        let engine = mk_engine(nr_blocks);
        let cache = Arc::new(MetadataCache::new(engine.clone(), 16)?);

        Ok(Self {
            engine,
            cache,
            alloc_begin: 0,
        })
    }

    fn alloc(&mut self, kind: &Kind) -> Result<WriteProxy> {
        let b = self.cache.zero_lock(self.alloc_begin, kind)?;
        self.alloc_begin += 1;
        Ok(b)
    }

    fn create_bitset(&mut self, nr_bits: u64) -> Result<Bitset> {
        let blocks_required = Bitset::blocks_required(nr_bits)?;
        let blocks: Vec<u32> = (self.alloc_begin..self.alloc_begin + blocks_required).collect();
        let bitset = Bitset::new(self.cache.clone(), &blocks, nr_bits)?;
        self.alloc_begin += blocks_required;
        Ok(bitset)
    }

    fn extend_bitset(&mut self, bitset: &mut Bitset, extra_bits: u64) -> Result<()> {
        let old_blocks_required = Bitset::blocks_required(bitset.len())?;
        let blocks_required = Bitset::blocks_required(bitset.len() + extra_bits)?;
        let extra_blocks = blocks_required - old_blocks_required;
        let blocks: Vec<u32> = (self.alloc_begin..self.alloc_begin + extra_blocks).collect();
        bitset.extend(&blocks, extra_bits)?;
        self.alloc_begin += extra_blocks;
        Ok(())
    }
}

fn test_create(nr_bits: u64) -> Result<()> {
    let mut fix = Fixture::new(Bitset::blocks_required(nr_bits)?)?;
    let bitset = fix.create_bitset(nr_bits)?;
    ensure!(bitset.len() == nr_bits);
    ensure!(bitset.count() == 0);
    Ok(())
}

#[test]
fn create_small_bitset() -> Result<()> {
    test_create(8192)
}

#[test]
fn create_large_bitset() -> Result<()> {
    let nr_bits = ENTRIES_PER_BITMAP as u64 * ENTRIES_PER_INDEX as u64 * 3;
    test_create(nr_bits)
}

#[test]
fn lookup_fails() -> Result<()> {
    let nr_bits = 32768;
    let mut fix = Fixture::new(16)?;
    let bitset = fix.create_bitset(nr_bits)?;

    ensure!(matches!(bitset.test(0), Ok(false)));
    ensure!(matches!(bitset.test(1234), Ok(false)));

    Ok(())
}

fn test_set_bits(bits: &[u64]) -> Result<()> {
    if bits.is_empty() {
        return Ok(());
    }

    let nr_bits = *bits.iter().max().unwrap() + 1;
    let blocks_required = Bitset::blocks_required(nr_bits)?;

    let mut fix = Fixture::new(blocks_required)?;
    let mut bitset = fix.create_bitset(nr_bits)?;

    bits.iter().try_for_each(|&i| bitset.set(i))?;

    for i in bits {
        ensure!(matches!(bitset.test(*i), Ok(true)));
    }
    ensure!(bitset.count() == bits.len() as u64);

    Ok(())
}

fn test_clear_bits(bits: &[u64]) -> Result<()> {
    if bits.is_empty() {
        return Ok(());
    }

    let nr_bits = *bits.iter().max().unwrap() + 1;
    let blocks_required = Bitset::blocks_required(nr_bits)?;

    let mut fix = Fixture::new(blocks_required)?;
    let mut bitset = fix.create_bitset(nr_bits)?;

    bits.iter().try_for_each(|&i| bitset.set(i))?;

    for i in bits {
        ensure!(matches!(bitset.test(*i), Ok(true)));
    }
    ensure!(bitset.count() == bits.len() as u64);

    bits.iter().try_for_each(|&i| bitset.clear(i))?;

    for i in bits {
        ensure!(matches!(bitset.test(*i), Ok(false)));
    }
    ensure!(bitset.count() == 0);

    Ok(())
}

#[test]
fn set_single() -> Result<()> {
    test_set_bits(&[100])
}

#[test]
fn set_sequence() -> Result<()> {
    let nr_bits = 100_000;
    test_set_bits(&(0..nr_bits).collect::<Vec<u64>>())
}

#[test]
fn set_random() -> Result<()> {
    let nr_bits = 100_000;
    let mut bits: Vec<u64> = (0..nr_bits).collect();

    // shuffle the indexes
    let mut rng = rand::thread_rng();
    bits.shuffle(&mut rng);

    test_set_bits(&bits)
}

#[test]
fn clear_single() -> Result<()> {
    test_clear_bits(&[100])
}

#[test]
fn clear_sequence() -> Result<()> {
    let nr_bits = 100_000;
    test_clear_bits(&(0..nr_bits).collect::<Vec<u64>>())
}

#[test]
fn clear_random() -> Result<()> {
    let nr_bits = 100_000;
    let mut bits: Vec<u64> = (0..nr_bits).collect();

    // shuffle the indexes
    let mut rng = rand::thread_rng();
    bits.shuffle(&mut rng);

    test_clear_bits(&bits)?;
    Ok(())
}

#[test]
fn set_first_sequence() -> Result<()> {
    let nr_bits = 32768;
    let mut fix = Fixture::new(16)?;
    let mut bitset = fix.create_bitset(nr_bits)?;

    for i in 0..nr_bits {
        ensure!(bitset.set_first_clear()? == Some(i));
    }
    ensure!(matches!(bitset.set_first_clear(), Ok(None)));
    ensure!(bitset.count() == nr_bits);

    Ok(())
}

#[test]
fn set_first_with_gaps() -> Result<()> {
    let nr_bits: usize = 32768;
    let mut fix = Fixture::new(16)?;
    let mut bitset = fix.create_bitset(nr_bits as u64)?;

    let mut bits: Vec<u64> = (0..nr_bits as u64).collect();
    let mut rng = rand::thread_rng();
    bits.shuffle(&mut rng);
    bits[nr_bits / 2..].sort_unstable();

    // initialize some bits
    bits[..nr_bits / 2]
        .iter()
        .try_for_each(|&i| bitset.set(i))?;

    // set the rest of the free bits
    for i in &bits[nr_bits / 2..] {
        ensure!(bitset.set_first_clear()? == Some(*i));
    }
    ensure!(bitset.count() == nr_bits as u64);

    Ok(())
}

#[test]
fn clear_all() -> Result<()> {
    let nr_bits = 100_000;
    let mut fix = Fixture::new(16)?;
    let mut bitset = fix.create_bitset(nr_bits)?;

    // set all bits
    for i in 0..nr_bits {
        ensure!(bitset.set_first_clear()? == Some(i));
    }
    ensure!(bitset.count() == nr_bits);

    // clear all bits
    bitset.clear_all()?;
    ensure!(bitset.count() == 0);

    // set all bits again
    for i in 0..nr_bits {
        ensure!(bitset.set_first_clear()? == Some(i));
    }
    ensure!(bitset.count() == nr_bits);

    Ok(())
}

fn test_extend_and_verify(nr_bits: u64) -> Result<()> {
    let origin_nr_bits: u64 = nr_bits / 2;
    let extra_bits = nr_bits - origin_nr_bits;
    let nr_enabled: u64 = nr_bits / 4;

    let mut fix = Fixture::new(Bitset::blocks_required(nr_bits)?)?;
    let mut bitset = fix.create_bitset(origin_nr_bits)?;

    let mut bits: Vec<u64> = (0..nr_bits).collect();
    let mut rng = rand::thread_rng();
    bits[..origin_nr_bits as usize].shuffle(&mut rng);
    bits[origin_nr_bits as usize..].shuffle(&mut rng);
    let first_half = &bits[..origin_nr_bits as usize];
    let second_half = &bits[origin_nr_bits as usize..];

    first_half[0..nr_enabled as usize]
        .iter()
        .try_for_each(|&i| bitset.set(i))?;
    ensure!(bitset.count() == nr_enabled);

    // extend the bitset
    fix.extend_bitset(&mut bitset, extra_bits)?;
    ensure!(bitset.len() == nr_bits);
    ensure!(bitset.count() == nr_enabled);

    // ensure the extended part works
    second_half[..nr_enabled as usize]
        .iter()
        .try_for_each(|&i| bitset.set(i))?;
    for i in &second_half[..nr_enabled as usize] {
        ensure!(matches!(bitset.test(*i), Ok(true)));
    }
    for i in &second_half[nr_enabled as usize..] {
        ensure!(matches!(bitset.test(*i), Ok(false)));
    }
    ensure!(bitset.count() == nr_enabled * 2);

    // ensure the range before expansion is not affected
    for i in &first_half[..nr_enabled as usize] {
        ensure!(matches!(bitset.test(*i), Ok(true)));
    }
    for i in &first_half[nr_enabled as usize..] {
        ensure!(matches!(bitset.test(*i), Ok(false)));
    }

    Ok(())
}

#[test]
fn extend_small_bitset() -> Result<()> {
    test_extend_and_verify(100_000)
}

#[test]
fn extend_large_bitset() -> Result<()> {
    let nr_bits = ENTRIES_PER_BITMAP as u64 * ENTRIES_PER_INDEX as u64 * 5;
    let mut fix = Fixture::new(Bitset::blocks_required(nr_bits)?)?;

    let origin_nr_bits = nr_bits / 2;
    let mut bitset = fix.create_bitset(origin_nr_bits)?;
    ensure!(bitset.len() == origin_nr_bits);

    let extra_bits = nr_bits - origin_nr_bits;
    fix.extend_bitset(&mut bitset, extra_bits)?;
    ensure!(bitset.len() == nr_bits);

    Ok(())
}

#[test]
fn reopen() -> Result<()> {
    let nr_bits = 100_000;
    let nr_enabled = nr_bits / 2;
    let mut fix = Fixture::new(Bitset::blocks_required(nr_bits)? + 1)?;

    let mut bits: Vec<u64> = (0..nr_bits).collect();
    let mut rng = rand::thread_rng();
    bits.shuffle(&mut rng);

    {
        let sb = fix.alloc(&SUPERBLOCK_KIND)?;

        let mut bitset = fix.create_bitset(nr_bits)?;
        bits[..nr_enabled as usize]
            .iter()
            .try_for_each(|&i| bitset.set(i))?;

        let root = bitset.get_root()?;
        let (_, mut sb_data) = sb.split_at(BLOCK_HEADER_SIZE);
        root.pack(&mut sb_data.rw())?;
    }

    fix.cache.flush()?;

    {
        let sb = fix.cache.read_lock(0, &SUPERBLOCK_KIND)?;
        let (_, sb_data) = sb.split_at(BLOCK_HEADER_SIZE);
        let root = BitsetRoot::unpack(&mut sb_data.r())?;

        let bitset = Bitset::open(fix.cache.clone(), root)?;
        ensure!(bitset.len() == nr_bits);
        ensure!(bitset.count() == nr_enabled);

        for i in &bits[..nr_enabled as usize] {
            ensure!(matches!(bitset.test(*i), Ok(true)));
        }
        for i in &bits[nr_enabled as usize..] {
            ensure!(matches!(bitset.test(*i), Ok(false)));
        }
    }

    Ok(())
}

//-------------------------------------------------------------------------
