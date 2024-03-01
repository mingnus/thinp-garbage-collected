use anyhow::{ensure, Result};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::ops::Range;
use std::sync::Arc;
use thinp::math::div_up;

use crate::block_cache::*;
use crate::block_kinds::*;
use crate::byte_types::{Readable, Writeable, U32};
use crate::packed_array::PArray;
use crate::packed_array::Serializable;

#[cfg(test)]
mod tests;

//-------------------------------------------------------------------------

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
struct IndexEntry {
    blocknr: u32,
    nr_free: u32,
    none_free_before: u32,
}

impl IndexEntry {
    fn new(blocknr: u32) -> Self {
        Self {
            blocknr,
            nr_free: ENTRIES_PER_BITMAP,
            none_free_before: 0,
        }
    }

    fn is_empty(&self) -> bool {
        self.nr_free == ENTRIES_PER_BITMAP
    }

    fn copy_from(&mut self, src: &IndexEntry) -> Result<()> {
        self.nr_free = src.nr_free;
        self.none_free_before = src.none_free_before;
        Ok(())
    }

    fn clear(&mut self) -> Result<()> {
        self.nr_free = ENTRIES_PER_BITMAP;
        self.none_free_before = 0;
        Ok(())
    }
}

impl Serializable for IndexEntry {
    fn packed_len() -> usize {
        12
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u32::<LittleEndian>(self.blocknr)?;
        w.write_u32::<LittleEndian>(self.nr_free)?;
        w.write_u32::<LittleEndian>(self.none_free_before)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let blocknr = r.read_u32::<LittleEndian>()?;
        let nr_free = r.read_u32::<LittleEndian>()?;
        let none_free_before = r.read_u32::<LittleEndian>()?;

        Ok(Self {
            blocknr,
            nr_free,
            none_free_before,
        })
    }
}

//-------------------------------------------------------------------------

const ENTRIES_PER_BITMAP: u32 = 32640;

struct Bitmap<Data> {
    ie: IndexEntry,
    bits: Data,
}

impl Bitmap<()> {
    fn index(bit: u64) -> u32 {
        (bit / ENTRIES_PER_BITMAP as u64) as u32
    }

    fn offset(bit: u64) -> usize {
        (bit % ENTRIES_PER_BITMAP as u64) as usize
    }

    fn upper_bound(bit: u64) -> u32 {
        div_up(bit, ENTRIES_PER_BITMAP as u64) as u32
    }

    // TODO: check the value to avoid overflow in div_up
    fn blocks_required(nr_bits: u64) -> Result<u32> {
        Ok(div_up(nr_bits, ENTRIES_PER_BITMAP as u64) as u32)
    }
}

impl<Data: Readable> Bitmap<Data> {
    fn new(ie: &IndexEntry, data: Data) -> Self {
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        Bitmap {
            ie: ie.clone(),
            bits: data,
        }
    }

    fn test(&self, bit: usize) -> Result<bool> {
        let i = bit / 64;
        let v = (&self.bits.r()[i * 8..]).read_u64::<LittleEndian>()?;
        let mask = 1 << (bit % 64);
        Ok((v & mask) != 0)
    }

    // find the first free bit within the range [start, end)
    // TODO: test boundary cases [0, 64), [0, 63)
    fn find_first_unset(&self, start: usize, end: usize) -> Result<Option<usize>> {
        let search_start = std::cmp::max(start, self.ie.none_free_before as usize);
        let i_start = search_start / 64;
        let i_end = end / 64;
        let mut off = search_start % 64;

        for i in i_start..i_end {
            let v = (&self.bits.r()[i * 8..]).read_u64::<LittleEndian>()? >> off;
            let ones = v.trailing_ones() as usize;
            if ones + off < 64 {
                return Ok(Some(i * 64 + ones + off));
            }
            off = 0;
        }

        // handle the last iteration
        // FIXME: maybe merge into the above loop, by setting i_end = div_up(end, 8);
        if end % 64 > 0 {
            let v = (&self.bits.r()[i_end * 8..]).read_u64::<LittleEndian>()? >> off;
            let ones = v.trailing_ones() as usize;
            if ones + off < end % 64 {
                return Ok(Some(i_end * 64 + ones + off));
            }
        }

        Ok(None)
    }
}

impl<Data: Writeable> Bitmap<Data> {
    fn test_and_set(&mut self, bit: usize, set: bool) -> Result<bool> {
        let i = bit / 64;
        let mut v = (&self.bits.r()[i * 8..]).read_u64::<LittleEndian>()?;
        let mask = 1 << (bit % 64);
        let old = v & mask != 0;

        if !old && set {
            v |= mask;
            (&mut self.bits.rw()[i * 8..]).write_u64::<LittleEndian>(v)?;

            self.ie.nr_free -= 1;
            if bit as u32 == self.ie.none_free_before {
                self.ie.none_free_before += 1;
            }
        } else if old && !set {
            v &= !mask;
            (&mut self.bits.rw()[i * 8..]).write_u64::<LittleEndian>(v)?;

            self.ie.nr_free += 1;
            self.ie.none_free_before = std::cmp::min(bit as u32, self.ie.none_free_before);
        }

        Ok(old)
    }

    fn copy_from<T: Readable>(&mut self, src: &Bitmap<T>) -> Result<()> {
        self.bits.rw().copy_from_slice(src.bits.r());
        self.ie.copy_from(&src.ie)
    }
}

fn init_bitmap(mut block: WriteProxy) -> Result<Bitmap<WriteProxy>> {
    let loc = block.loc();

    let mut w = std::io::Cursor::new(block.rw());
    let hdr = BlockHeader {
        loc,
        kind: BITMAP_KIND,
        sum: 0,
    };
    write_block_header(&mut w, &hdr)?;

    let ie = IndexEntry::new(loc);
    Ok(Bitmap::new(&ie, block))
}

//-------------------------------------------------------------------------

const ENTRIES_PER_INDEX: u32 = 338;

struct MetadataIndex<Data> {
    loc: u32,
    next: U32<Data>,
    nr_entries: U32<Data>, // FIXME: is it good to keep the entry counts in each index_block?
    entries: PArray<IndexEntry, Data>,
}

impl MetadataIndex<()> {
    fn index(bitmap_index: u32) -> u32 {
        bitmap_index / ENTRIES_PER_INDEX
    }

    fn offset(bitmap_index: u32) -> usize {
        (bitmap_index % ENTRIES_PER_INDEX) as usize
    }

    fn upper_bound(bitmap_index: u32) -> u32 {
        if bitmap_index == 0 {
            return 1;
        }
        div_up(bitmap_index, ENTRIES_PER_INDEX)
    }

    fn blocks_required(nr_bitmaps: u32) -> Result<u32> {
        if nr_bitmaps == 0 {
            return Ok(1);
        }
        Ok(div_up(nr_bitmaps, ENTRIES_PER_INDEX))
    }
}

impl<Data: Readable> MetadataIndex<Data> {
    fn new(loc: u32, data: Data) -> Self {
        let array_size = ENTRIES_PER_INDEX as usize * IndexEntry::packed_len();
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        let (next, data) = data.split_at(4);
        let (nr_entries, data) = data.split_at(4);
        let (entries, _) = data.split_at(array_size);

        let nr_entries = U32::new(nr_entries);
        let n = nr_entries.get() as usize;
        Self {
            loc,
            next: U32::new(next),
            nr_entries,
            entries: PArray::new(entries, n),
        }
    }

    fn nr_free(&self) -> u32 {
        ENTRIES_PER_INDEX - self.entries.len() as u32
    }
}

impl<Data: Writeable> MetadataIndex<Data> {
    fn append(&mut self, ies: &[IndexEntry]) -> Result<()> {
        self.entries.append(ies);
        self.nr_entries.inc(ies.len() as u32);
        Ok(())
    }
}

fn init_index_block(mut block: WriteProxy) -> Result<MetadataIndex<WriteProxy>> {
    let loc = block.loc();
    let mut w = std::io::Cursor::new(block.rw());

    let hdr = BlockHeader {
        loc,
        kind: INDEXBLOCK_KIND,
        sum: 0,
    };
    write_block_header(&mut w, &hdr)?;

    w.write_u64::<LittleEndian>(0)?;

    Ok(MetadataIndex::new(block.loc(), block))
}

fn load_indexes(cache: Arc<MetadataCache>, root: u32) -> Result<Vec<MetadataIndex<WriteProxy>>> {
    let mut indexes = Vec::new();
    let mut next = root;

    while next != 0 {
        let b = cache.write_lock(root, &INDEXBLOCK_KIND)?;
        let mi = MetadataIndex::new(b.loc(), b);
        next = mi.next.get();
        indexes.push(mi);
    }

    Ok(indexes)
}

//-------------------------------------------------------------------------

#[derive(Eq, Ord, PartialEq, PartialOrd)]
pub struct BitsetRoot {
    nr_bits: u64,
    nr_enabled: u64,
    index_root: u32,
}

impl Serializable for BitsetRoot {
    fn packed_len() -> usize {
        20
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.nr_bits)?;
        w.write_u64::<LittleEndian>(self.nr_enabled)?;
        w.write_u32::<LittleEndian>(self.index_root)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let nr_bits = r.read_u64::<LittleEndian>()?;
        let nr_enabled = r.read_u64::<LittleEndian>()?;
        let index_root = r.read_u32::<LittleEndian>()?;

        Ok(Self {
            nr_bits,
            nr_enabled,
            index_root,
        })
    }
}

//-------------------------------------------------------------------------

pub struct Bitset {
    nr_bits: u64,
    nr_enabled: u64,
    cache: Arc<MetadataCache>,
    indexes: Vec<MetadataIndex<WriteProxy>>,
}

impl Bitset {
    pub fn new(cache: Arc<MetadataCache>, blocks: &[u32], nr_bits: u64) -> Result<Bitset> {
        let mut bitset = Self::empty_(cache, blocks[0])?;
        bitset.extend(&blocks[1..], nr_bits)?;
        Ok(bitset)
    }

    pub fn open(cache: Arc<MetadataCache>, root: BitsetRoot) -> Result<Bitset> {
        let indexes = load_indexes(cache.clone(), root.index_root)?;
        Ok(Self {
            nr_bits: root.nr_bits,
            nr_enabled: root.nr_enabled,
            cache,
            indexes,
        })
    }

    pub fn get_root(&self) -> Result<BitsetRoot> {
        Ok(BitsetRoot {
            nr_bits: self.nr_bits,
            nr_enabled: self.nr_enabled,
            index_root: self.indexes[0].loc,
        })
    }

    fn empty_(cache: Arc<MetadataCache>, root: u32) -> Result<Self> {
        let b = cache.zero_lock(root, &INDEXBLOCK_KIND)?;
        let index_block = init_index_block(b)?;

        Ok(Self {
            nr_bits: 0,
            nr_enabled: 0,
            cache,
            indexes: vec![index_block],
        })
    }

    pub fn len(&self) -> u64 {
        self.nr_bits
    }

    pub fn is_empty(&self) -> bool {
        self.nr_bits == 0
    }

    pub fn count(&self) -> u64 {
        self.nr_enabled
    }

    // FIXME: ensure the expansion is safe from IO errors
    // FIXME: flush the entire cache, or just flush the blocks it uses?
    pub fn extend(&mut self, blocks: &[u32], extra_bits: u64) -> Result<()> {
        let old_nr_bitmaps = Bitmap::blocks_required(self.nr_bits)?;
        let old_nr_indexes = MetadataIndex::blocks_required(old_nr_bitmaps)?;
        let nr_bitmaps = Bitmap::blocks_required(self.nr_bits + extra_bits)?;
        let nr_indexes = MetadataIndex::blocks_required(nr_bitmaps)?;
        let extra_bitmaps = nr_bitmaps - old_nr_bitmaps;
        let extra_indexes = nr_indexes - old_nr_indexes;

        ensure!(extra_bitmaps + extra_indexes == blocks.len() as u32);

        let ies = self.build_ies(&blocks[..extra_bitmaps as usize])?;
        self.extend_indexes(&blocks[extra_bitmaps as usize..])?;
        self.append_ies(&ies)?;

        self.nr_bits += extra_bits;
        Ok(())
    }

    fn blocks_required_(nr_bits: u64) -> Result<(u32, u32)> {
        let nr_bitmaps = Bitmap::blocks_required(nr_bits)?;
        let nr_indexes = MetadataIndex::blocks_required(nr_bitmaps)?;
        Ok((nr_bitmaps, nr_indexes))
    }

    pub fn blocks_required(nr_bits: u64) -> Result<u32> {
        Self::blocks_required_(nr_bits).map(|(nr_bitmaps, nr_indexes)| nr_bitmaps + nr_indexes)
    }

    fn build_ies(&self, blocks: &[u32]) -> Result<Vec<IndexEntry>> {
        let mut ies = Vec::new();

        for loc in blocks {
            let b = self.cache.zero_lock(*loc, &BITMAP_KIND)?;
            let bm = init_bitmap(b)?;
            ies.push(bm.ie);
        }

        Ok(ies)
    }

    fn extend_indexes(&mut self, blocks: &[u32]) -> Result<()> {
        if blocks.is_empty() {
            return Ok(());
        }

        let mut new_indexes = Vec::new();
        let b = self.cache.zero_lock(blocks[0], &INDEXBLOCK_KIND)?;
        new_indexes.push(init_index_block(b)?);
        let mut prev = new_indexes.last_mut().unwrap();

        for loc in &blocks[1..] {
            let b = self.cache.zero_lock(*loc, &INDEXBLOCK_KIND)?;
            let current = init_index_block(b)?;
            prev.next.set(current.loc);
            new_indexes.push(current);
            prev = new_indexes.last_mut().unwrap();
        }

        let last = self.indexes.last_mut().unwrap();
        last.next.set(new_indexes[0].loc);
        self.indexes.extend(new_indexes);

        Ok(())
    }

    fn append_ies(&mut self, entries: &[IndexEntry]) -> Result<()> {
        let nr_bitmaps = Bitmap::blocks_required(self.nr_bits)?;
        let mut index_begin = MetadataIndex::blocks_required(nr_bitmaps)? as usize - 1;
        let mut ies = entries;

        while !ies.is_empty() && index_begin < self.indexes.len() {
            let last = &mut self.indexes[index_begin];
            let nr_free = last.nr_free();
            let len = std::cmp::min(ies.len(), nr_free as usize);

            last.append(&ies[..len])?;
            ies = &ies[len..];

            index_begin += 1;
        }

        ensure!(ies.len() == 0);

        Ok(())
    }

    fn find_ie(&self, ie_index: u32) -> Result<IndexEntry> {
        let mi = &self.indexes[MetadataIndex::index(ie_index) as usize];
        Ok(mi.entries.get(MetadataIndex::offset(ie_index)))
    }

    fn save_ie(&mut self, ie_index: u32, ie: &IndexEntry) -> Result<()> {
        let mi = &mut self.indexes[MetadataIndex::index(ie_index) as usize];
        mi.entries.set(ie_index as usize, ie);
        Ok(())
    }

    pub fn bootstrap(cache: Arc<MetadataCache>, nr_bits: u32, superblock: u32) -> Result<Bitset> {
        let alloc_begin = superblock + 1;
        let blocks_required = Self::blocks_required(nr_bits as u64)?;
        let blocks: Vec<u32> = (alloc_begin..alloc_begin + blocks_required).collect();

        let mut bitset = Self::new(cache, &blocks, nr_bits as u64)?;

        // apply the ref counts itself
        bitset.set(superblock as u64)?;
        blocks.iter().try_for_each(|b| bitset.set(*b as u64))?;

        Ok(bitset)
    }

    pub fn bootstrap_extend(&mut self, extra_bits: u32) -> Result<()> {
        let alloc_begin = self.nr_bits as u32;
        let old_blocks_required = Self::blocks_required(self.nr_bits)?;
        let blocks_required = Self::blocks_required(self.nr_bits + extra_bits as u64)?;
        let extra_blocks = blocks_required - old_blocks_required;
        let blocks: Vec<u32> = (alloc_begin..alloc_begin + extra_blocks).collect();

        self.extend(&blocks, extra_bits as u64)?;

        // apply the ref counts itself
        blocks.iter().try_for_each(|b| self.set(*b as u64))?;

        Ok(())
    }

    pub fn copy_bits(&mut self, src: &Bitset) -> Result<()> {
        ensure!(self.nr_bits == src.nr_bits);

        let nr_bitmaps = Bitmap::blocks_required(self.nr_bits)?;
        let nr_indexes = MetadataIndex::blocks_required(nr_bitmaps)?;
        for ((i, dest_mi), src_mi) in self.indexes.iter_mut().enumerate().zip(src.indexes.iter()) {
            let nr_entries = if i == nr_indexes as usize - 1 {
                nr_bitmaps % ENTRIES_PER_INDEX
            } else {
                ENTRIES_PER_INDEX
            };

            for bi in 0..nr_entries {
                let mut dest_ie = dest_mi.entries.get(bi as usize);
                let src_ie = src_mi.entries.get(bi as usize);

                // copy the bitmap if necessary
                if !src_ie.is_empty() {
                    let dest_blk = self.cache.write_lock(dest_ie.blocknr, &BITMAP_KIND)?;
                    let mut dest_bm = Bitmap::new(&dest_ie, dest_blk);
                    let src_blk = self.cache.read_lock(src_ie.blocknr, &BITMAP_KIND)?;
                    let src_bm = Bitmap::new(&src_ie, src_blk);
                    dest_bm.copy_from(&src_bm)?;
                }

                // update the IndexEntry
                dest_ie.nr_free = src_ie.nr_free;
                dest_ie.none_free_before = src_ie.none_free_before;
                dest_mi.entries.set(bi as usize, &dest_ie);
            }
        }

        self.nr_enabled = src.nr_enabled;
        Ok(())
    }

    // optimized by using the nr_free counter to mark the bitmap cleared
    pub fn clear_all(&mut self) -> Result<()> {
        let nr_bitmaps = Bitmap::blocks_required(self.nr_bits)?;
        let nr_indexes = MetadataIndex::blocks_required(nr_bitmaps)?;
        for (i, mi) in self.indexes.iter_mut().enumerate() {
            let nr_entries = if i == nr_indexes as usize - 1 {
                nr_bitmaps % ENTRIES_PER_INDEX
            } else {
                ENTRIES_PER_INDEX
            };

            for bi in 0..nr_entries {
                let mut ie = mi.entries.get(bi as usize);
                ie.clear()?;
                mi.entries.set(bi as usize, &ie);
            }
        }

        self.nr_enabled = 0;
        Ok(())
    }

    pub fn test(&self, bit: u64) -> Result<bool> {
        let ie = self.find_ie(Bitmap::index(bit))?;

        // optimize for fast clear
        if ie.is_empty() {
            return Ok(false);
        }

        let block = self.cache.read_lock(ie.blocknr, &BITMAP_KIND)?;
        let bm = Bitmap::new(&ie, block);

        bm.test(Bitmap::offset(bit))
    }

    pub fn set(&mut self, bit: u64) -> Result<()> {
        self.test_and_set(bit).map(|_| ())
    }

    pub fn test_and_set(&mut self, bit: u64) -> Result<bool> {
        let bi = Bitmap::index(bit);
        let ie = self.find_ie(bi)?;

        // optimize for fast clear
        let block = if ie.is_empty() {
            self.cache.zero_lock(ie.blocknr, &BITMAP_KIND)?
        } else {
            self.cache.write_lock(ie.blocknr, &BITMAP_KIND)?
        };

        let mut bm = Bitmap::new(&ie, block);
        bm.test_and_set(Bitmap::offset(bit), true).and_then(|old| {
            if !old {
                self.save_ie(bi, &bm.ie)?;
                self.nr_enabled += 1;
            }
            Ok(old)
        })
    }

    pub fn clear(&mut self, bit: u64) -> Result<()> {
        let bi = Bitmap::index(bit);
        let mi = &self.indexes[MetadataIndex::index(bi) as usize];
        let ie = mi.entries.get(MetadataIndex::offset(bi));

        // optimize for fast clear
        if ie.is_empty() {
            return Ok(());
        }

        let block = self.cache.write_lock(ie.blocknr, &BITMAP_KIND)?;
        let mut bm = Bitmap::new(&ie, block);

        bm.test_and_set(Bitmap::offset(bit), false).and_then(|old| {
            if old {
                self.save_ie(bi, &bm.ie)?;
                self.nr_enabled -= 1;
            }
            Ok(())
        })
    }

    // FIXME: ugly
    fn find_first_unset(&mut self, start: u64, end: u64) -> Result<Option<u64>> {
        let bi_start = Bitmap::index(start);
        let bi_end = Bitmap::upper_bound(end);
        let mi_start = MetadataIndex::index(bi_start);
        let mi_end = MetadataIndex::upper_bound(bi_end);

        for mi_idx in mi_start..mi_end {
            let mi = &self.indexes[mi_idx as usize];
            let bi_start = std::cmp::max(bi_start, mi_idx * ENTRIES_PER_INDEX);
            let bi_end = std::cmp::min(bi_end, (mi_idx + 1) * ENTRIES_PER_INDEX);

            for bi in bi_start..bi_end {
                let ie = mi.entries.get(MetadataIndex::offset(bi));

                // optimize the search by testing nr_free
                if ie.is_empty() {
                    return Ok(Some(bi as u64 * ENTRIES_PER_BITMAP as u64));
                }

                if ie.nr_free == 0 {
                    continue;
                }

                let block = self.cache.read_lock(ie.blocknr, &BITMAP_KIND)?;
                let bm = Bitmap::new(&ie, block);

                let bit_start = std::cmp::max(start, bi as u64 * ENTRIES_PER_BITMAP as u64);
                let bit_end = std::cmp::min(end, (bi + 1) as u64 * ENTRIES_PER_BITMAP as u64);
                let mut off_end = Bitmap::offset(bit_end);
                if off_end == 0 {
                    off_end = ENTRIES_PER_BITMAP as usize;
                }

                if let Ok(Some(off)) = bm.find_first_unset(Bitmap::offset(bit_start), off_end) {
                    return Ok(Some(bi as u64 * ENTRIES_PER_BITMAP as u64 + off as u64));
                }
            }
        }

        Ok(None)
    }

    pub fn set_first_clear_in_range(&mut self, range: &Range<u64>) -> Result<Option<u64>> {
        match self.find_first_unset(range.start, range.end) {
            Ok(Some(bit)) => {
                self.set(bit)?;
                Ok(Some(bit))
            }
            other => other,
        }
    }

    pub fn set_first_clear(&mut self) -> Result<Option<u64>> {
        self.set_first_clear_in_range(&(0..self.nr_bits))
    }

    // TODO: implement fast negate from all-0 to all-1?
    /*pub fn negate(&mut self) -> Result<()> {
        let mut new_bits = FixedBitSet::with_capacity(self.nr_bits as usize);
        for b in 0..self.nr_bits {
            if !self.bits.contains(b as usize) {
                new_bits.set(b as usize, true);
            }
        }

        self.bits = new_bits;
        Ok(())
    }*/
}

//-------------------------------------------------------------------------
