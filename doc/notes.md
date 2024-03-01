# Thin provisioning design review and future direction

## Introduction

This document is a design review for the existing thin provisioning
device mapper target (thinp).  It then describes the direction I want to
take with the next version of thinp.  Finally broadening out to discuss the
future of device mapper in general.

## Current thin provisioning design

### Support thin volumes and snapshots

- Taking snapshots is a fast operation.  No copying of metadata or data involved.

### Metadata cache

- 4k pages.  Cached in a lru cache.

### Fixed chunk size

- Fixed chunk size for a given pool.  This is the smallest unit of allocation.
- This can cause write amplification with small writes if the block is unprovisioned, or a snapshot needs to break sharing.
- Choosing the correct chunk size depends on the use case.  In general large blocks favour provisioning, small blocks favour snapshots.

### Transactionality achived through _persistent_ data structures

- Metadata and data is shared between thin volumes and snapshots.  This is achieved through _reference counting_.  This is expensive.
- To ensure transactionality we could always copy-on-write the metadata.  This would allow us to always rollback to use previous versions of the metadata.  But expensive.  Instead we only cow once within a transaction to create a 'shadow', by keeping transactions long we amortise the cost of the cow.
- Transactions need to be as long as possible.  Forced to commit if we get a REQ_FUA/FLUSH, or if the transaction gets too big.

### Btrees

- Fundamental data structure for thinp.
- Maps 64keys to any serialisable value.
- Does not support ranges.
- full range of 64 bit keys are rarely needed.  Single value type means values are often larger than necessary.
- On average 1/3 of the nodes are empty.
- Recursive btrees supported.  Complicates code.  Implemented this was to allow rolling locks scheme.  Not actually used.
- BTrees are very simple; lots of techniques from 'Modern B-Tree Techniques' could be used to improve them. (But not in kernel)

### Mapping tree
- Btree with 24 bit 'time', and 40 bit 'block'.  64 bit value.
- time used to decide if we need to break sharing.  We don't want any order 'nr snaps' algs.  Redundant final break.
- By far the majority of the metadata is consumed with these nodes.
- Storing single mappings rather than runs of mappings is a waste of space.  But it's simpler.

### Device tree

- Associates a mapping tree with a thin volume.  32 bit thin id.
- Records the 'time' when the most recent snapshot was taken.  Used to decide when to break sharing.

### management of free space through reference counting

- Space maps implement reference counting
- Separate space maps for metadata and data.
- Any count above 2 is expensive because it moves to an overflow btree.  Uses a lot of space and cpu.
- Breaking sharing on leaf nodes can trigger a lot of increments on data blocks.
  (Worse with rtree since more data blocks per leaf)
- space maps are within transaction which causes recursion since they track their own space.
  Getting round this is tricky and requires a lot of care (BOPS etc), makes it harder to reason about the code.
- Data leaks due to power outages require offline repair.

### Summary of design limitations

- Fixed chunk size causing write amplification.
- Reference counting is expensive.  Prohibitively so as we pack more data into nodes.
- Small numbers of data blocks may be leaked due to power outages (but no data lost).
  Requiring offline repair to recover space.
- The metadata is large; eg, the thin_metadata_pack tool can typically compress it to 10 fold.  Smaller metadata would use less memory.  Need run length encoding.  Compressing nodes should be investigated (we can do this if we move to userland).
- Passdown is expensive.
- 16GB limit on metadata.  Which in turn restricts the number of mappings in a pool.

## Future direction

### Moving code to userland

- ublk and io_uring have shown that we can do a lot of the work in userland.
- _vulkan_ graphics api is a good example of how we can move a lot of work to userland.  Queues, command buffers, memory management, etc.
- Kernel code is very expensive to write and maintain and the team is small.
- Userland gives us a choice of implementation language.

- Replace thin kernel driver with a device driver (not dm target) that
  keeps a set of table of linear mappings.  These may be valid for read, but not write.
  Any IO that is to a region that doesn't have a valid mapping is passed
  to the userland thinp.  This way we keep the fast path in kernel,
  and the awkward squad of provisioning, discard and
  breaking sharing can be farmed out to userland.  Driver needs to see whole req queue rather than the single bio that dm targets see.

## Garbage collection

- No reference counting so operations like breaking sharing are cheaper.
- Unable to implement _immediate_ discard passdown (but there is a better way)
- GC will have to run as a background task.
- GC will have to be able to run concurrently with other operations.
- If we lump metadata/data allocators + GC together we can lift it out of the transaction.
- Data leaks due to power outages will automatically be cleaned up by GC.

## Metadata compression

Metadata blocks will no longer be a fixed size, so we'll need to maintain an index.  As blocks get
updated they will inevitably move, leaving holes, possibly negating the benefit of compression.  *But*
if we solve this we can stop using a fixed block size for the _uncompressed_ metadata blocks meaning the
btree nodes can always be full!.

## Block allocator

The block allocator is responsible for allocating and freeing blocks.  Both metadata and data blocks.

### Fields (persisted on disk)

- 3 bitsets for metadata and data
  - allocated blocks
  - GC allocated blocks workspace
  - blocks allocated since GC began
- a lifo queue tracking the depth first walk of the metadata performed by
  the GC.
- potential garbage count.  Used to determine when to run GC.
  Breaking sharing, deleting thins, discarding.

### Fields (not persisted on disk):

- A list of roots that a GC can start from. ie. the root of the mapping trees
  in the live transaction and possibly the metadata snap.

### Notes

- Roots should be updated when a transaction commits.  We know they
  will only be accessed for reads by thinp.  So the GC can safely run in parallel
  with the transaction.

- We need a way of temporarily quiescing the allocator/gc at commit time to ensure
  the allocator on disk data is consistent with the transaction.  eg, bitsets need
  to be updated to say we've allocated a block if it's used in the transaction.

- Any new allocations get recorded in both the allocated blocks bitset and the
  allocated since GC began bitset.  As the final stage of the GC process we OR in
  the 'allocated since' bitsets to take into account changes made during the GC.

- Because the GC walks all the metadata it's important that we don't let it spoil
  the lru cache.
  - Use an extent-allocator for metadata as well as data.  This will give us locality
    when updating the bitset.
  - Do a depth first walk.
  - Introduce a new api to bufio optimised for temporary reads.  If the buffer is in the cache
    use and keep in cache.  If not in cache read and discard once unlocked.

### Bitset representation

There are various ways to represent a bitset depending on the bit density:
  - A sequence of block indexes (sparse populated)
  - A literal bitset (20-80% populated)
  - A sequence of run length encoded blocks (dense populated)

Data structures like 'roaring bitmaps' break up a large bitset into regions and use
whichever one of the above is most appropriate, but at the expense of code complexity.

We should initially use a literal bitset for the 'allocated' bitsets, and
a sequence of block indexes for the 'allocated since' bitsets.

### Resizing metadata

Space for all bitsets will be preallocated.  If we resize the metadata we also must resize
the bitsets.  So the bitsets need to be stored in non-contiguous runs of blocks.


## extent allocator

## Transaction manager

The transaction manager enforces the copy-on-write semantics of the thinp metadata
and thus give us transactionality.  We never modify metadata from the previous transaction;
instead we copy it and modify the copy.  This is done recursively for our metadata trees.
A crucial optimisation is rather than copying a block everytime we modify it we only copy
it the first time it's modified within a transaction.  We try and delay committing the transaction
as long as possible to gain the most benefit from this optimisation.  This 'copy-if-not-already-copied'
operation is known as 'shadowing'.

A per block rw lock is used to protect the block from concurrent access.

A shadow is a copy of a metadata block.  To minimise copying we
try and only copy a block only once within each transaction.
   
There is a corner case we need to be careful of though; if a
shadowed block has the number of times it is referenced increased, since
is was shadowed, but within this transaction, then we need to force another
copy to be made.  But we don't track the reference counts, so we make the
call on whether to copy based on both the parent and the block to be copied.
If None is passed for the old_parent then we always copy.
    
Note: I initially thought we could have a 'inc_ref()' method that just removes
a block from the shadow set.  But this won't work because we need to start
calling inc_ref() for children blocks if we ever shadow that block.

### Interface:

- new metadata block; allocates and zeroes a new block.  Returns read reference.  Allocations
  use an exent allocator to improve locality.
- shadow block; returns a write reference to a block.  If the block has already been
  shadowed then the existing write reference is returned.  This is the only way to acquire
  a write reference to a block (other than the superblock).
- get_superblock; returns a write reference to the superblock.
- commit; quiesces the allocator/gc and writes all dirty blocks to disk except superblock.
  then writes superblock.  Finally it updates the allocator/gc with the new roots.
- inc_ref; tells the tm that there's now more than one reference to a block.  If the block
  is a shadow then tm will forget that it's a shadow to force a copy should a further modification
  be made.  

## btree

Used to hold the mapping between thin ids and their metadata.  32bit values are enough for metadata
so both keys and values should be 32bit.  (Lose the value type, there is not only a single use for btree).
This tree will be relatively small, likely less than a few thousand entries, so we don't need to do any
fancy packing/shuffle to keep the nodes fuller.

## mtree

Range tree used to hold the mapping between thin block and data blocks + modification time.  Leaf
nodes have the potential to reference huge numbers of data blocks, so this really requires a GC
approach.

## Thin metadata

Thin metadata will now constist of a superblock, a btree containing the roots of rtrees.
That's it, no space maps.  Outside the transaction will be the allocator of course.

# Discard passdown

Thinp1 supports discard passdown, which hands down discards to the underlying data device
iff the data block is no longer used.  I regret adding this since it's slow and complicated.
In particular it adjusts the reference counts of the data blocks it's discarding _twice_, to
avoid those blocks being reused whilst the passdown is in progress.

With a GC approach we cannot do discard passdown in the same way.  Because we just don't know
when the data block is free.  However at the end of the GC process we know which blocks have
been newly freed.  So we can do discard passdown at that point.  This is a much better approach
since it allows thinp to reuse the blocks immediately, and avoid the passdown.  Plus the original
discard will not be waiting for the passdown to complete.
