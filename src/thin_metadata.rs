use anyhow::{anyhow, Result};
use bsp_block_allocator::allocator::Allocator;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::collections::BTreeMap;
use std::io::{self, Read, Write};
use std::ops::Range;
use std::sync::{Arc, Mutex};

use crate::bitset::*;
use crate::block_allocator::BlockAllocator;
use crate::block_cache::*;
use crate::block_kinds::*;
use crate::btree::BTree;
use crate::byte_types::*;
use crate::packed_array::*;
use crate::transaction_manager::TransactionManager;

//-------------------------------------------------------------------------

const INITIAL_DATA_EXTENTS: u8 = 32;

type ThinId = u32;

struct Superblock<Data: Readable> {
    flags: U32<Data>,
    time: U32<Data>,
    transaction_id: U64<Data>,
    block_allocator_roots: PArray<BitsetRoot, Data>,
    data_mapping_root: U32<Data>,
    device_details_root: U32<Data>,
    data_block_size: U32<Data>, // in 512-byte sectors
}

impl<Data: Readable> Superblock<Data> {
    fn new(data: Data) -> Self {
        let (_, data) = data.split_at(BLOCK_HEADER_SIZE);
        let (flags, data) = data.split_at(4);
        let (time, data) = data.split_at(4);
        let (transaction_id, data) = data.split_at(8);
        let (block_allocator_roots, data) = data.split_at(5 * BitsetRoot::packed_len());
        let (data_mapping_root, data) = data.split_at(4);
        let (device_details_root, data) = data.split_at(4);
        let (data_block_size, _) = data.split_at(4);

        let flags = U32::new(flags);
        let time = U32::new(time);
        let transaction_id = U64::new(transaction_id);
        let block_allocator_roots = PArray::new(block_allocator_roots, 5);
        let data_mapping_root = U32::new(data_mapping_root);
        let device_details_root = U32::new(device_details_root);
        let data_block_size = U32::new(data_block_size);

        Self {
            flags,
            time,
            transaction_id,
            block_allocator_roots,
            data_mapping_root,
            device_details_root,
            data_block_size,
        }
    }
}

#[derive(Ord, Eq, PartialOrd, PartialEq)]
struct BlockTime {
    block: u64,
    time: u32,
}

impl Serializable for BlockTime {
    fn packed_len() -> usize {
        8
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let n = (self.block << 24) | self.time as u64;
        w.write_u64::<LittleEndian>(n)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let n = r.read_u64::<LittleEndian>()?;
        let block = n >> 24;
        let time = n & ((1 << 24) - 1);

        Ok(Self {
            block,
            time: time as u32,
        })
    }
}

#[derive(Clone, Ord, Eq, PartialOrd, PartialEq)]
struct DeviceDetails {
    mapped_blocks: u64,
    transaction_id: u64,
    creation_time: u32,
    snapshotted_time: u32,
}

impl DeviceDetails {
    fn new(transaction_id: u64, time: u32) -> Self {
        Self {
            mapped_blocks: 0,
            transaction_id,
            creation_time: time,
            snapshotted_time: time,
        }
    }
}

impl Serializable for DeviceDetails {
    fn packed_len() -> usize {
        24
    }

    fn pack<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u64::<LittleEndian>(self.mapped_blocks)?;
        w.write_u64::<LittleEndian>(self.transaction_id)?;
        w.write_u32::<LittleEndian>(self.creation_time)?;
        w.write_u32::<LittleEndian>(self.snapshotted_time)
    }

    fn unpack<R: Read>(r: &mut R) -> io::Result<Self> {
        let mapped_blocks = r.read_u64::<LittleEndian>()?;
        let transaction_id = r.read_u64::<LittleEndian>()?;
        let creation_time = r.read_u32::<LittleEndian>()?;
        let snapshotted_time = r.read_u32::<LittleEndian>()?;

        Ok(Self {
            mapped_blocks,
            transaction_id,
            creation_time,
            snapshotted_time,
        })
    }
}

//-------------------------------------------------------------------------

const SUPERBLOCK_LOCATION: u32 = 0;

#[allow(dead_code)]
struct MetadataInner {
    data_extents: Allocator,
    allocator: Arc<Mutex<BlockAllocator>>, // This manages both metadata and data blocks.
    cache: Arc<MetadataCache>,
    tm: Arc<TransactionManager>,
    mapping_tree: BTree<ThinId>,
    details_tree: BTree<DeviceDetails>,
    thin_devices: BTreeMap<ThinId, Arc<ThinDevice>>,
    time: u32,
    transaction_id: u64,
    data_block_size: u32,
}

impl MetadataInner {
    fn format(
        cache: Arc<MetadataCache>,
        data_block_size: u32,
        nr_data_blocks: u64,
    ) -> Result<Self> {
        let allocator = Arc::new(Mutex::new(BlockAllocator::new(
            cache.clone(),
            nr_data_blocks,
            SUPERBLOCK_LOCATION,
        )?));
        let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));
        let data_extents = Allocator::new(nr_data_blocks, INITIAL_DATA_EXTENTS);

        let mapping_tree = BTree::empty_tree(tm.clone())?;
        let details_tree = BTree::empty_tree(tm.clone())?;

        let mut pmd = Self {
            allocator,
            cache,
            tm,
            data_extents,
            details_tree,
            mapping_tree,
            thin_devices: BTreeMap::new(),
            time: 0,
            transaction_id: 0,
            data_block_size,
        };

        pmd.write_initial_superblock()?;

        Ok(pmd)
    }

    fn write_initial_superblock(&mut self) -> Result<()> {
        let mut sblock = self.tm.superblock();

        let mut w = std::io::Cursor::new(sblock.rw());
        let hdr = BlockHeader {
            loc: SUPERBLOCK_LOCATION,
            kind: SUPERBLOCK_KIND,
            sum: 0,
        };
        write_block_header(&mut w, &hdr)?;
        drop(w);

        let roots = self.allocator.lock().unwrap().copy_roots()?;

        let mut sb = Superblock::new(sblock);
        sb.flags.set(0);
        sb.time.set(0);
        sb.transaction_id.set(0);
        sb.block_allocator_roots.prepend(&roots);
        sb.data_mapping_root.set(self.mapping_tree.root());
        sb.device_details_root.set(self.details_tree.root());
        sb.data_block_size.set(self.data_block_size);

        Ok(())
    }

    pub fn open(cache: Arc<MetadataCache>, data_block_size: u32) -> Result<Self> {
        let sblock = cache.read_lock(SUPERBLOCK_LOCATION, &SUPERBLOCK_KIND)?;

        let sb = Superblock::new(sblock);
        if sb.data_block_size.get() != data_block_size {
            return Err(anyhow!("changing the data block size is not supported"));
        }

        let time = sb.time.get();
        let transaction_id = sb.transaction_id.get();
        let roots: Vec<BitsetRoot> = (0..5).map(|i| sb.block_allocator_roots.get(i)).collect();
        let data_mapping_root = sb.data_mapping_root.get();
        let device_details_root = sb.device_details_root.get();
        drop(sb);

        let allocator = Arc::new(Mutex::new(BlockAllocator::open(cache.clone(), &roots)?));
        let tm = Arc::new(TransactionManager::new(allocator.clone(), cache.clone()));
        let nr_data_blocks = allocator.lock().unwrap().nr_data_blocks()?;
        let data_extents = Allocator::new(nr_data_blocks, INITIAL_DATA_EXTENTS);

        let mapping_tree = BTree::open_tree(tm.clone(), data_mapping_root);
        let details_tree = BTree::open_tree(tm.clone(), device_details_root);
        // TODO: allocator.set_roots();

        let pmd = Self {
            allocator,
            cache,
            tm,
            data_extents,
            details_tree,
            mapping_tree,
            thin_devices: BTreeMap::new(),
            time,
            transaction_id,
            data_block_size,
        };

        Ok(pmd)
    }

    fn create_device(&mut self, dev: ThinId, pool: Arc<Pool>) -> Result<Arc<ThinDevice>> {
        if self.thin_devices.contains_key(&dev) || self.details_tree.lookup(dev)?.is_some() {
            return Err(anyhow!("device already exists"));
        }

        let td = Arc::new(ThinDevice::new(dev, self.transaction_id, self.time, pool));
        self.thin_devices.insert(dev, td.clone());
        Ok(td)
    }

    fn open_device(&mut self, dev: ThinId, pool: Arc<Pool>) -> Result<Arc<ThinDevice>> {
        if let Some(td) = self.thin_devices.get(&dev) {
            td.open();
            return Ok(td.clone());
        }

        let details = self
            .details_tree
            .lookup(dev)?
            .ok_or_else(|| anyhow!("device doesn't exist"))?;
        let td = Arc::new(ThinDevice::from_disk(dev, &details, pool));
        self.thin_devices.insert(dev, td.clone());
        Ok(td)
    }

    fn create_thin(&mut self, dev: ThinId, pool: Arc<Pool>) -> Result<()> {
        if self.mapping_tree.lookup(dev)?.is_some() {
            return Err(anyhow!("device already exists"));
        }

        let subtree: BTree<BlockTime> = BTree::empty_tree(self.tm.clone())?;
        self.mapping_tree.insert(dev, &subtree.root())?;
        let td = self.create_device(dev, pool)?;
        td.close();
        Ok(())
    }

    fn create_snap(&mut self, dev: ThinId, origin: ThinId, pool: Arc<Pool>) -> Result<()> {
        if self.mapping_tree.lookup(dev)?.is_some() {
            return Err(anyhow!("device already exists"));
        }

        let origin_root = self
            .mapping_tree
            .lookup(origin)?
            .ok_or_else(|| anyhow!("origin doesn't exist"))?;
        self.tm.inc_ref(origin_root);
        self.mapping_tree.insert(dev, &origin_root)?;

        self.time += 1;

        let snap_dev = self.create_device(dev, pool.clone())?;
        let origin_dev = self.open_device(origin, pool)?;
        snap_dev.set_snapshot_details(&origin_dev, self.time);
        origin_dev.close();
        snap_dev.close();

        Ok(())
    }

    fn delete_thin(&mut self, dev: ThinId, pool: Arc<Pool>) -> Result<()> {
        let td = self.open_device(dev, pool)?;
        if td.is_opened_shared() {
            td.close();
            return Err(anyhow!("device is being used")); // EBUSY
        }
        drop(td);

        if self.thin_devices.remove(&dev).is_none()
            || self.details_tree.remove(dev)?.is_none()
            || self.mapping_tree.remove(dev)?.is_none()
        {
            return Err(anyhow!("device doesn't exist"));
        }

        Ok(())
    }
}

//-------------------------------------------------------------------------

struct Pool {
    inner: Mutex<MetadataInner>,
}

#[allow(dead_code)]
impl Pool {
    pub fn new(
        cache: Arc<MetadataCache>,
        data_block_size: u32,
        nr_data_blocks: u64,
    ) -> Result<Self> {
        let inner = MetadataInner::format(cache, data_block_size, nr_data_blocks)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    pub fn open(cache: Arc<MetadataCache>, data_block_size: u32) -> Result<Self> {
        let inner = MetadataInner::open(cache, data_block_size)?;
        Ok(Self {
            inner: Mutex::new(inner),
        })
    }

    pub fn create_thin(self: &Arc<Self>, dev: ThinId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.create_thin(dev, self.clone())
    }

    pub fn create_snap(self: &Arc<Self>, dev: ThinId, origin: ThinId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.create_snap(dev, origin, self.clone())
    }

    pub fn delete_thin(self: &Arc<Self>, dev: ThinId) -> Result<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.delete_thin(dev, self.clone())
    }

    pub fn commit(&mut self) -> Result<()> {
        todo!();
    }

    pub fn abort(&mut self) -> Result<()> {
        todo!();
    }

    pub fn transation_id(&self) -> u64 {
        todo!();
    }

    pub fn set_transaction_id(&mut self, _old_id: u64, _id: u64) -> Result<()> {
        todo!();
    }

    pub fn reserve_metadata_snap(&mut self) -> Result<()> {
        todo!();
    }

    pub fn release_metadata_snap(&mut self) -> Result<()> {
        todo!();
    }

    pub fn get_metadata_snap(&self) -> Result<u64> {
        todo!();
    }

    pub fn open_thin(&self, _dev: ThinId) -> Result<Arc<ThinDevice>> {
        todo!();
    }

    pub fn changed_this_transaction(&self) -> bool {
        todo!();
    }

    pub fn nr_free_data_blocks(&self) -> Result<u64> {
        todo!();
    }

    pub fn nr_free_metadata_blocks(&self) -> Result<u64> {
        todo!();
    }

    pub fn metadata_dev_size(&self) -> Result<u64> {
        todo!();
    }

    pub fn data_dev_size(&self) -> Result<u64> {
        todo!();
    }

    pub fn resize_data_dev(&mut self, _new_size: u64) -> Result<()> {
        todo!();
    }

    pub fn resize_metadata_dev(&mut self, _new_size: u64) -> Result<()> {
        todo!();
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        todo!();
    }
}

//-------------------------------------------------------------------------

#[allow(dead_code)]
struct LookupResult {
    block: u64,
    shared: bool,
}

#[allow(dead_code)]
struct RangeResult {
    thin_begin: u64,
    thin_end: u64,
    pool_begin: u64,
    maybe_shared: bool,
}

#[allow(dead_code)]
struct ThinDeviceInner {
    pool: Arc<Pool>,
    id: ThinId,
    open_count: u32,
    changed: bool,
    details: DeviceDetails,
}

struct ThinDevice {
    inner: Mutex<ThinDeviceInner>,
}

#[allow(dead_code)]
impl ThinDevice {
    fn new(dev: ThinId, trans_id: u64, time: u32, pool: Arc<Pool>) -> Self {
        let inner = ThinDeviceInner {
            pool,
            id: dev,
            open_count: 1,
            changed: true,
            details: DeviceDetails::new(trans_id, time),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    fn from_disk(dev: ThinId, details: &DeviceDetails, pool: Arc<Pool>) -> Self {
        let inner = ThinDeviceInner {
            pool,
            id: dev,
            open_count: 1,
            changed: false,
            details: details.clone(),
        };
        Self {
            inner: Mutex::new(inner),
        }
    }

    fn open(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.open_count += 1;
    }

    fn close(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.open_count -= 1;
    }

    fn is_opened_shared(&self) -> bool {
        let inner = self.inner.lock().unwrap();
        inner.open_count > 1
    }

    fn set_snapshot_details(&self, origin: &Self, time: u32) {
        let mut inner = self.inner.lock().unwrap();
        let mut origin_inner = origin.inner.lock().unwrap();
        origin_inner.changed = true;
        origin_inner.details.snapshotted_time = time;
        inner.details.snapshotted_time = time;
        inner.details.mapped_blocks = origin_inner.details.mapped_blocks;
    }

    pub fn get_id(&self) -> ThinId {
        todo!();
    }

    pub fn find_block(&self, _block: u64) -> Result<Option<LookupResult>> {
        todo!();
    }

    pub fn find_mapped_range(&self, _search: Range<u64>) -> Result<Option<RangeResult>> {
        todo!();
    }

    pub fn alloc_data_block(&mut self) -> Result<u64> {
        todo!();
    }

    pub fn insert_data_block(&mut self, _block: u64, _data_block: u64) -> Result<()> {
        todo!();
    }

    pub fn remove_range(&mut self, _thin_blocks: Range<u64>) -> Result<()> {
        todo!();
    }

    pub fn changed_this_transaction(&self) -> bool {
        todo!();
    }

    pub fn aborted_with_changes(&self) -> bool {
        todo!();
    }

    pub fn highest_mapped(&self) -> Result<Option<u64>> {
        todo!();
    }

    pub fn mapping_count(&self) -> Result<u64> {
        todo!();
    }
}

//-------------------------------------------------------------------------
