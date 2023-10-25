# Introduction

This document describes a new design for thin provisioning metadata that uses garbage collection
rather than reference counting to manage metadata and data blocks.

## Thinp 1 reference counting

- Space maps implement reference counting
- Any count above 2 is expensive because it moves to an overflow btree.  Uses a lot of space and cpu.
- Breaking sharing on leaf nodes can trigger a lot of increments on data blocks.
  (Worse with rtree since more data blocks per leaf)
- space maps are within transaction which causes recursion since they track their own space.
  Getting round this is tricky and requires a lot of care (BOPS etc)
- Data leaks due to power outages require offline repair.


## Garbage collection

- No reference counting so operations like breaking sharing are cheaper.
- Unable to implement _immediate_ discard passdown (but there is a better way)
- GC will have to run as a background task.
- GC will have to be able to run concurrently with other operations.
- If we lump metadata/data allocators + GC together we can lift it out of the transaction.
- Data leaks due to power outages will automatically be cleaned up by GC.


# Design

    Thin metadata

    btree   rtree

    transaction manager

    block allocator/gc      extent allocator

    bufio

## Block allocator

The block allocator is responsible for allocating and freeing blocks.

### Fields (persisted on disk):

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

### Notes:

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
  the lru cache in bufio.
  - Use an extent-allocator for metadata as well as data.  This will give us locality
    when updating the bitset.
  - Do a depth first walk.
  - Introduce a new api to bufio optimised for temporary reads.  If the buffer is in the cache
    use and keep in cache.  If not in cache read and discard once unlocked.

### Bitset representation

There are various ways to represent a bitset depending on the bit density:
  - A sequence of block indexes (sparse)
  - A literal bitset (20-80%)
  - A sequence of run length encoded blocks (dense)

Data structures like 'roaring bitmaps' break up a large bitset into regions and use
whichever one of the above is most appropriate, but at the expense of code complexity.

I think we should initially use a literal bitset for the 'allocated' bitsets, and
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

## rtree

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
