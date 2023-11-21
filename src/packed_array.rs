use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};

use crate::byte_types::*;

//-------------------------------------------------------------------------

pub trait Serializable: Ord + Eq + Sized {
    fn packed_len() -> usize;
    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()>;
    fn unpack<R: Read>(r: &mut R) -> io::Result<Self>;
}

//-------------------------------------------------------------------------

impl Serializable for u32 {
    fn packed_len() -> usize {
        4
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u32::<LittleEndian>(*self)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        r.read_u32::<LittleEndian>()
    }
}

impl Serializable for u64 {
    fn packed_len() -> usize {
        8
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(*self)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        r.read_u64::<LittleEndian>()
    }
}

//-------------------------------------------------------------------------

pub struct PArray<S: Serializable, Data> {
    max_entries: usize,
    nr_entries: usize,
    data: Data,

    phantom: std::marker::PhantomData<S>,
}

impl<S: Serializable, Data: Readable> PArray<S, Data> {
    pub fn new(data: Data, nr_entries: usize) -> Self {
        let max_entries = data.r().len() / S::packed_len();
        Self {
            max_entries,
            nr_entries,
            data,
            phantom: std::marker::PhantomData,
        }
    }

    fn byte(idx: usize) -> usize {
        idx * S::packed_len()
    }

    pub fn len(&self) -> usize {
        self.nr_entries
    }

    pub fn check_idx(&self, idx: usize) {
        assert!(idx < self.nr_entries);
    }

    pub fn get(&self, idx: usize) -> S {
        self.check_idx(idx);
        let (_, data) = self.data.split_at(Self::byte(idx));
        S::unpack(&mut data.r()).unwrap()
    }

    pub fn bsearch(&self, key: &S) -> isize {
        if self.nr_entries == 0 {
            return -1;
        }

        let mut lo = -1;
        let mut hi = self.nr_entries as isize;
        while (hi - lo) > 1 {
            let mid = lo + ((hi - lo) / 2);
            let mid_key = self.get(mid as usize);

            if mid_key == *key {
                return mid;
            }

            if mid_key < *key {
                lo = mid;
            } else {
                hi = mid;
            }
        }

        lo
    }
}

impl<S: Serializable, Data: Writeable> PArray<S, Data> {
    pub fn set(&mut self, idx: usize, value: &S) {
        self.check_idx(idx);
        let (_, mut data) = self.data.split_at(Self::byte(idx));
        value.pack(&mut data.rw()).unwrap();
    }

    pub fn shift_left(&mut self, count: usize) -> Vec<S> {
        let mut lost = Vec::with_capacity(count);
        for i in 0..count {
            lost.push(self.get(i));
        }
        self.data.rw().copy_within(Self::byte(count).., 0);
        self.nr_entries -= count;
        lost
    }

    fn shift_right_(&mut self, count: usize) {
        self.data
            .rw()
            .copy_within(0..Self::byte(self.nr_entries), Self::byte(count));
        self.nr_entries += count;
    }

    pub fn remove_right(&mut self, count: usize) -> Vec<S> {
        let mut lost = Vec::with_capacity(count);
        for i in 0..count {
            lost.push(self.get(self.nr_entries - count + i));
        }
        self.nr_entries -= count;
        lost
    }

    pub fn insert_at(&mut self, idx: usize, value: &S) {
        if idx < self.nr_entries {
            self.data.rw().copy_within(
                Self::byte(idx)..Self::byte(self.nr_entries),
                Self::byte(idx + 1),
            );
        }
        self.nr_entries += 1;
        self.set(idx, value);
    }

    pub fn remove_at(&mut self, idx: usize) {
        if idx < self.nr_entries - 1 {
            self.data.rw().copy_within(
                Self::byte(idx + 1)..Self::byte(self.nr_entries),
                Self::byte(idx),
            );
        }
        self.nr_entries -= 1;
    }

    pub fn prepend(&mut self, values: &[S]) {
        assert!(self.nr_entries + values.len() <= self.max_entries);
        self.shift_right_(values.len());
        for (i, v) in values.iter().enumerate() {
            self.set(i, v);
        }
    }

    pub fn append(&mut self, values: &[S]) {
        assert!(self.nr_entries + values.len() <= self.max_entries);
        let nr_entries = self.nr_entries;
        self.nr_entries += values.len();
        for (i, v) in values.iter().enumerate() {
            self.set(nr_entries + i, v);
        }
    }
}

//-------------------------------------------------------------------------
