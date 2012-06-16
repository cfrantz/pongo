PongoDB: A memory mapped key-value store
========================================

PongoDB is a concurrent memmory mapped key-value store with a limited
set of simple types that can be easily integrated with Python and other
scripting languages.

PongoDB is meant to be a limited domain replacement for MongoDB.

Supported Data Types
====================

PongoDB supports the following data types:

Primitive Immutable Types
-------------------------
* Null
* Boolean
* Integer
* Datetime
* Uuid -- not supported yet
* Float
* ByteArray
* String

-- Perhaps want to add Locking data types so programs can synchronize
   with each other via Pongo: Event, Lock, rwLock.

Mutable Container Types
-----------------------
* List
* Dictionary

Updates
=======

Any update to a container is done with Read-Copy-Update algorithms.
This allows concurrent readers and writers to make consistent updates
to the datastore without conflicts.

List Operations
===============

* dblist_new()
* dblist_getitem(list, index)
* dblist_setitem(list, index, item)
* dblist_delitem(list, index, item)
* dblist_append(list, item)
* dblist_extend(list, iterable)
* dblist_insert(list, index, item)
* dblist_remove(list, item)

Dict Operations
===============
* dbobject_new()
* dbobject_getitem(dict, key)
* dbobject_setitem(dict, key, value)
* dbobject_delitem(dict, key)
* dbobject_update(dict, dict_compatible_iterable)

Memory Allocator
================
The database is kept in an MMAPed file.  The file is sized in
chunks (currently of 16mb).  At the end of each chunk, there is
a small area (about 1.6% of the total) dedicated to memory
management accounting.

The largest allowed allocation is 1/2 the chunk size (16mb/2 = 8mb now).

Need to lock the allocator during mallocs and frees.  Need to
be able to increase the size of the file when an allocation
fails because of no more memory left. (e.g. add more chunks)

Garbage Collection
==================
  1.  Mark all allocations as freeable.
  2.  Sync with any other processes in the database
  3.  Walk through all allocations referenced from the
      root "data" dictionary.  Unmark the "freeable" item
      as allocated.
  4.  Free all remaining allocations

The garbage collector doesn't know about items referenced on the
stacks various processes.  As such, the GC must synchronize (step 2)
with other processes.

