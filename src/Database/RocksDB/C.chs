
module Database.RocksDB.C (
    DBFPtr,
    DBOptionsPtr,
    CFFPtr,
    CFPtr,
    IteratorFPtr,
    WriteOptionsPtr,
    ReadOptionsPtr,
    WriteBatchFPtr,
    open,
    openColumnFamilies,
    listColumnFamilies,
    createColumnFamily,
    dropColumnFamily,
    columnFamilyHandleDestroyFunPtr,
    optionsCreate,
    optionsDestroy,
    optionsSetCreateIfMissing,
    optionsSetCreateMissingColumnFamilies,
    optionsSetWriteBufferSize,
    optionsSetDisableAutoCompactions,
    optionsSetLevel0FileNumCompactionTrigger,
    optionsSetLevel0SlowdownWritesTrigger,
    optionsSetLevel0StopWritesTrigger,
    writeoptionsCreate,
    writeoptionsDestroy,
    writeoptionsSetSync,
    writeoptionsDisableWAL,
    readoptionsCreate,
    readoptionsDestroy,
    putCf,
    getCf,
    createIteratorCf,
    iterValid,
    iterSeekToFirst,
    iterSeekToLast,
    iterSeek,
    iterSeekForPrev,
    iterNext,
    iterPrev,
    iterKey,
    iterValue,
    iterGetError,
    put,
    get,
    createIterator,
    write,
    writebatchCreate,
    writebatchClear,
    writebatchCount,
    writebatchPutCf
) where

import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray, peekArray)
import Foreign.Ptr
import Foreign.Storable

allocaNullPtr :: Storable a => (Ptr (Ptr a) -> IO b) -> IO b
allocaNullPtr f = alloca (\ptr -> poke ptr nullPtr >> f ptr)

allocaCSize :: (Ptr CSize -> IO b) -> IO b
allocaCSize f = alloca (\ptr -> poke ptr 0 >> f ptr)

#include "rocksdb/c.h"

{#typedef size_t CSize#}

{#context prefix = "rocksdb" #}

-- pointers

{#pointer *t as DBPtr#}

{#pointer *t as DBFPtr foreign finalizer close as close#}

{#pointer *options_t as DBOptionsPtr#}

{#pointer *writeoptions_t as WriteOptionsPtr#}

{#pointer *readoptions_t as ReadOptionsPtr#}

{#pointer *column_family_handle_t as CFPtr #}

{#pointer *column_family_handle_t as CFFPtr foreign finalizer column_family_handle_destroy as columnFamilyHandleDestroyFunPtr#}

{#pointer *iterator_t as IteratorPtr#}

{#pointer *iterator_t as IteratorFPtr foreign finalizer iter_destroy as iterDestroyFunPtr#}

{#pointer *writebatch_t as WriteBatchFPtr foreign finalizer writebatch_destroy as writeBatchDestroyFunPtr#}

-- functions

-- options

{#fun options_create as ^ { } -> `DBOptionsPtr' #}

{#fun options_destroy as ^ { `DBOptionsPtr' } -> `()' #}

{#fun options_set_create_if_missing as ^ { `DBOptionsPtr', `Bool' } -> `()' #}

{#fun options_set_create_missing_column_families as ^ { `DBOptionsPtr', `Bool' } -> `()' #}

{#fun options_set_write_buffer_size as ^ { `DBOptionsPtr', `CSize' } -> `()' #}

{#fun options_set_disable_auto_compactions as ^ { `DBOptionsPtr', `Bool' } -> `()' #}

{#fun options_set_level0_file_num_compaction_trigger as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun options_set_level0_slowdown_writes_trigger as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun options_set_level0_stop_writes_trigger as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

-- writeOptions

{#fun writeoptions_create as ^ { } -> `WriteOptionsPtr' #}

{#fun writeoptions_destroy as ^ { `WriteOptionsPtr' } -> `()' #}

{#fun writeoptions_set_sync as ^ { `WriteOptionsPtr', `Bool' } -> `()' #}

{#fun writeoptions_disable_WAL as ^ { `WriteOptionsPtr', `Bool' } -> `()' #}

-- readOptions

{#fun readoptions_create as ^ { } -> `ReadOptionsPtr' #}

{#fun readoptions_destroy as ^ { `ReadOptionsPtr' } -> `()' #}

-- db

{#fun open as ^ { `DBOptionsPtr', `CString', allocaNullPtr- `CString' peek*} -> `DBFPtr' #}

{#fun put as ^ { `DBFPtr', `WriteOptionsPtr', `CString', `CSize', `CString', `CSize', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun get as ^ { `DBFPtr', `ReadOptionsPtr', `CString', `CSize', allocaCSize- `CSize' peek*, allocaNullPtr- `CString' peek*} -> `CString' #}

{#fun create_iterator as ^ {`DBFPtr', `ReadOptionsPtr'} -> `IteratorFPtr' #}

{#fun write as ^ { `DBFPtr', `WriteOptionsPtr', `WriteBatchFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

-- column family

{#fun create_column_family as ^ { `DBFPtr', `DBOptionsPtr', `CString', allocaNullPtr- `CString' peek*} -> `CFFPtr' #}

{#fun drop_column_family as ^ { `DBFPtr', `CFFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun list_column_families as ^ { `DBOptionsPtr', `CString', allocaCSize- `CSize' peek*, allocaNullPtr- `CString' peek*} -> `Ptr CString' id #}

{#fun open_column_families as ^ { `DBOptionsPtr', `CString', `CInt',
withArray* `[CString]' ,
withArray* `[DBOptionsPtr]' ,
id `Ptr CFPtr' ,
allocaNullPtr- `CString' peek*} -> `DBFPtr' #}

{#fun put_cf as ^ { `DBFPtr', `WriteOptionsPtr', `CFFPtr', `CString', `CSize', `CString', `CSize', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun get_cf as ^ { `DBFPtr', `ReadOptionsPtr', `CFFPtr', `CString', `CSize', allocaCSize- `CSize' peek*, allocaNullPtr- `CString' peek*} -> `CString' #}

{#fun create_iterator_cf as ^ {`DBFPtr', `ReadOptionsPtr', `CFFPtr'} -> `IteratorFPtr' #}

-- Iterator

{#fun iter_valid as ^ {`IteratorFPtr'} -> `Bool' #}

{#fun iter_seek_to_first as ^ {`IteratorFPtr'} -> `()' #}

{#fun iter_seek_to_last as ^ {`IteratorFPtr'} -> `()' #}

{#fun iter_seek as ^ {`IteratorFPtr', `CString', `CSize'} -> `()' #}

{#fun iter_seek_for_prev as ^ {`IteratorFPtr', `CString', `CSize'} -> `()' #}

{#fun iter_next as ^ {`IteratorFPtr'} -> `()' #}

{#fun iter_prev as ^ {`IteratorFPtr'} -> `()' #}

{#fun iter_key as ^ {`IteratorFPtr', allocaCSize- `CSize' peek*} -> `CString' #}

{#fun iter_value as ^ {`IteratorFPtr', allocaCSize- `CSize' peek*} -> `CString' #}

{#fun iter_get_error as ^ {`IteratorFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

-- Write batch

{#fun writebatch_create as ^ { } -> `WriteBatchFPtr' #}

{#fun writebatch_clear as ^ {`WriteBatchFPtr'} -> `()' #}

{#fun writebatch_count as ^ {`WriteBatchFPtr'} -> `CInt' #}

{#fun writebatch_put_cf as ^ { `WriteBatchFPtr',  `CFFPtr', `CString', `CSize', `CString', `CSize'} -> `()' #}


