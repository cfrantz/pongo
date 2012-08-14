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
* Uuid
* Float
* ByteArray
* String

-- Perhaps want to add Locking data types so programs can synchronize
   with each other via Pongo: Event, Lock, rwLock.

Mutable Container Types
-----------------------
* List - A simple linear list structure.
* Dictionary - A simple sorted list of key/value pairs.
* Collection - A balanced binary tree containgin kv pairs.

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

Dict and Collection Operations
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

The garbage collector does not know about items referenced on the
stacks various processes.  As such, the GC must synchronize (step 2)
with other processes.

Objects returned to Python (proxy objects) are referenced by a "pidcache"
so if an update to an object would unreference the object backing the
proxy object, the older object will live as long as the Python proxy object.

The "meta" object
=================
The "meta" object contains parameters that control how PongoDB behaves.
Meta keys that start with a dot (.) are runtime properties and are not
stored in the database meta structure.

* chunksize controls how much space is added to the database file when
  the internal dballoc fails (default is 16 MB).

* id controls the named of the "id" key for objects added to a collection
  and/or automatically given an id (default is "_id").

* .newkey is the function to call to automatically generate an id.  When
  it is None, uuid_generate_time() is used internally (default is None)

* .uuid_class is the class object of the Python UUID class.  It is used
  to recognize UUIDs when serializing primitive types to PongoDB.

* .uuid_constructor is a callable that creates a Python UUID.  Normally,
  it is the same as uuid_class, but the user can change it if needed.
  uuid_constructore will always be called like this:

    uuval = uuid_constructor(None, "16-byte-string")


