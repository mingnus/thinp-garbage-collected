Experiment with a thin provisioning metadata format that
doesn't use reference counting space maps, but instead does
garbage collection.  Instead of having a separate GC thread
(stop the world is obviously out of the question), we push
the GC process forward as part of block allocation.