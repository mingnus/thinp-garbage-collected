use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

//-------------------------------------------------------------------------

pub trait Readable {
    fn r(&self) -> &[u8];
    fn split_at(&self, loc: usize) -> (Self, Self)
    where
        Self: Sized;
}

pub trait Writeable: Readable {
    fn rw(&mut self) -> &mut [u8];
}

pub struct U32<Data> {
    data: Data,
}

impl<Data> U32<Data> {
    pub fn new(data: Data) -> Self {
        Self { data }
    }
}

impl<Data: Readable> U32<Data> {
    pub fn get(&self) -> u32 {
        let mut data = std::io::Cursor::new(self.data.r());
        data.read_u32::<LittleEndian>().unwrap()
    }
}

impl<Data: Writeable> U32<Data> {
    pub fn set(&mut self, val: u32) {
        let mut data = std::io::Cursor::new(self.data.rw());
        data.write_u32::<LittleEndian>(val).unwrap();
    }

    pub fn inc(&mut self, val: u32) {
        self.set(self.get() + val);
    }

    pub fn dec(&mut self, val: u32) {
        self.set(self.get() - val);
    }
}

//-------------------------------------------------------------------------

fn byte(idx: usize) -> usize {
    idx * 4
}

pub struct U32Array<Data> {
    max_entries: usize,
    nr_entries: usize,
    data: Data,
}

impl<Data: Readable> U32Array<Data> {
    pub fn new(data: Data, nr_entries: usize) -> Self {
        let max_entries = data.r().len() / 4;
        Self {
            max_entries,
            nr_entries,
            data,
        }
    }

    pub fn len(&self) -> usize {
        self.nr_entries
    }

    pub fn check_idx(&self, idx: usize) {
        assert!(idx < self.nr_entries);
    }

    pub fn get(&self, idx: usize) -> u32 {
        self.check_idx(idx);
        let (_, data) = self.data.split_at(byte(idx));
        let mut data = std::io::Cursor::new(data.r());
        data.read_u32::<LittleEndian>().unwrap()
    }

    pub fn bsearch(&self, key: u32) -> isize {
        if self.nr_entries == 0 {
            return -1;
        }

        let mut lo = -1;
        let mut hi = self.nr_entries as isize;
        while (hi - lo) > 1 {
            let mid = lo + ((hi - lo) / 2);
            let mid_key = self.get(mid as usize);

            if mid_key == key {
                return mid;
            }

            if mid_key < key {
                lo = mid;
            } else {
                hi = mid;
            }
        }

        lo
    }
}

impl<Data: Writeable> U32Array<Data> {
    pub fn set(&mut self, idx: usize, value: u32) {
        self.check_idx(idx);
        let (_, mut data) = self.data.split_at(byte(idx));
        let mut data = std::io::Cursor::new(data.rw());
        data.write_u32::<LittleEndian>(value).unwrap();
    }

    pub fn shift_left(&mut self, count: usize) -> Vec<u32> {
        let mut lost = Vec::with_capacity(count);
        for i in 0..count {
            lost.push(self.get(i));
        }
        self.data.rw().copy_within(byte(count).., 0);
        self.nr_entries -= count;
        lost
    }

    fn shift_right_(&mut self, count: usize) {
        self.data
            .rw()
            .copy_within(0..byte(self.nr_entries), byte(count));
        self.nr_entries += count;
    }

    pub fn remove_right(&mut self, count: usize) -> Vec<u32> {
        let mut lost = Vec::with_capacity(count);
        for i in 0..count {
            lost.push(self.get(self.nr_entries - count + i));
        }
        self.nr_entries -= count;
        lost
    }

    pub fn insert_at(&mut self, idx: usize, value: u32) {
        if idx < self.nr_entries {
            self.data
                .rw()
                .copy_within(byte(idx)..byte(self.nr_entries), byte(idx + 1));
        }
        self.nr_entries += 1;
        self.set(idx, value);
    }

    pub fn remove_at(&mut self, idx: usize) {
        if idx < self.nr_entries - 1 {
            self.data
                .rw()
                .copy_within(byte(idx + 1)..byte(self.nr_entries), byte(idx));
        }
        self.nr_entries -= 1;
    }

    pub fn prepend(&mut self, values: &[u32]) {
        assert!(self.nr_entries + values.len() <= self.max_entries);
        self.shift_right_(values.len());
        for (i, v) in values.iter().enumerate() {
            self.set(i, *v);
        }
    }

    pub fn append(&mut self, values: &[u32]) {
        assert!(self.nr_entries + values.len() <= self.max_entries);
        let nr_entries = self.nr_entries;
        self.nr_entries += values.len();
        for (i, v) in values.iter().enumerate() {
            self.set(nr_entries + i, *v);
        }
    }
}

//-------------------------------------------------------------------------
