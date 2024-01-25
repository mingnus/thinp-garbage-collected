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

//-------------------------------------------------------------------------

pub struct U16<Data> {
    data: Data,
}

impl<Data> U16<Data> {
    pub fn new(data: Data) -> Self {
        Self { data }
    }
}

impl<Data: Readable> U16<Data> {
    pub fn get(&self) -> u16 {
        self.data.r().read_u16::<LittleEndian>().unwrap()
    }
}

impl<Data: Writeable> U16<Data> {
    pub fn set(&mut self, val: u16) {
        self.data.rw().write_u16::<LittleEndian>(val).unwrap();
    }

    pub fn inc(&mut self, val: u16) {
        self.set(self.get() + val);
    }

    pub fn dec(&mut self, val: u16) {
        self.set(self.get() - val);
    }
}

//-------------------------------------------------------------------------

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
        self.data.r().read_u32::<LittleEndian>().unwrap()
    }
}

impl<Data: Writeable> U32<Data> {
    pub fn set(&mut self, val: u32) {
        self.data.rw().write_u32::<LittleEndian>(val).unwrap();
    }

    pub fn inc(&mut self, val: u32) {
        self.set(self.get() + val);
    }

    pub fn dec(&mut self, val: u32) {
        self.set(self.get() - val);
    }
}

//-------------------------------------------------------------------------

pub struct U64<Data> {
    data: Data,
}

impl<Data> U64<Data> {
    pub fn new(data: Data) -> Self {
        Self { data }
    }
}

impl<Data: Readable> U64<Data> {
    pub fn get(&self) -> u64 {
        self.data.r().read_u64::<LittleEndian>().unwrap()
    }
}

impl<Data: Writeable> U64<Data> {
    pub fn set(&mut self, val: u64) {
        self.data.rw().write_u64::<LittleEndian>(val).unwrap();
    }

    pub fn inc(&mut self, val: u64) {
        self.set(self.get() + val);
    }

    pub fn dec(&mut self, val: u64) {
        self.set(self.get() - val);
    }
}

//-------------------------------------------------------------------------
