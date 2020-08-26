
module Database.RocksDB.C (
    DBFPtr,
    DBOptionsPtr,
    CFFPtr,
    CFPtr,
    IteratorFPtr,
    WriteOptionsPtr,
    ReadOptionsPtr,
    FlushOptionsPtr,
    BlockBasedTableOptionsPtr,
    WriteBatchFPtr,
    open,
    openColumnFamilies,
    openForReadOnlyColumnFamilies,
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
    optionsEnableStatistics,
    optionsSetStatsDumpPeriodSec,
    optionsSetDbWriteBufferSize,
    optionsSetMaxWriteBufferNumber,
    --optionsSetMaxBackgroundJobs,
    optionsSetMaxBackgroundCompactions,
    optionsSetMaxBackgroundFlushes,
    optionsSetSoftPendingCompactionBytesLimit,
    optionsSetHardPendingCompactionBytesLimit,
    optionsSetMaxOpenFiles,
    writeoptionsCreate,
    writeoptionsDestroy,
    writeoptionsSetSync,
    writeoptionsDisableWAL,
    readoptionsCreate,
    readoptionsDestroy,
    flushoptionsCreate,
    flushoptionsDestroy,
    flushoptionsSetWait,
    blockBasedOptionsCreate,
    blockBasedOptionsDestroy,
    blockBasedOptionsSetBlockSize,
    blockBasedOptionsSetCacheIndexAndFilterBlocks,
    blockBasedOptionsSetCacheIndexAndFilterBlocksWithHighPriority,
    blockBasedOptionsSetPinL0FilterAndIndexBlocksInCache,
    optionsSetBlockBasedTableFactory,
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
    writebatchPut,
    writebatchPutCf,
    propertyValue,
    propertyInt,
    propertyIntCf,
    propertyValueCf,
    approximateSizesCf,
    flush,
    flushCf
) where

import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc (alloca)
import Foreign.Marshal.Array (withArray, peekArray)
import Foreign.Ptr
import Foreign.Storable
import Data.Word

allocaNullPtr :: Storable a => (Ptr (Ptr a) -> IO b) -> IO b
allocaNullPtr f = alloca (\ptr -> poke ptr nullPtr >> f ptr)

allocaCSize :: (Ptr CSize -> IO b) -> IO b
allocaCSize f = alloca (\ptr -> poke ptr 0 >> f ptr)

#include "rocksdb/c.h"

{#typedef size_t CSize#}

{#typedef uint64_t CSize#}

{#context prefix = "rocksdb" #}

-- pointers

{#pointer *t as DBPtr#}

{#pointer *t as DBFPtr foreign finalizer close as close#}

{#pointer *options_t as DBOptionsPtr#}

{#pointer *writeoptions_t as WriteOptionsPtr#}

{#pointer *readoptions_t as ReadOptionsPtr#}

{#pointer *flushoptions_t as FlushOptionsPtr#}

{#pointer *block_based_table_options_t as BlockBasedTableOptionsPtr#}

{#pointer *column_family_handle_t as CFPtr #}

{#pointer *column_family_handle_t as CFFPtr foreign finalizer column_family_handle_destroy as columnFamilyHandleDestroyFunPtr#}

{#pointer *iterator_t as IteratorPtr#}

{#pointer *iterator_t as IteratorFPtr foreign finalizer iter_destroy as iterDestroyFunPtr#}

{#pointer *writebatch_t as WriteBatchFPtr foreign finalizer writebatch_destroy as writeBatchDestroyFunPtr#}

-- functions

-- options

{#fun unsafe options_create as ^ { } -> `DBOptionsPtr' #}

{#fun unsafe options_destroy as ^ { `DBOptionsPtr' } -> `()' #}

{#fun unsafe options_set_create_if_missing as ^ { `DBOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe options_set_create_missing_column_families as ^ { `DBOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe options_set_write_buffer_size as ^ { `DBOptionsPtr', `CSize' } -> `()' #}

{#fun unsafe options_set_disable_auto_compactions as ^ { `DBOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe options_set_level0_file_num_compaction_trigger as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun unsafe options_set_level0_slowdown_writes_trigger as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun unsafe options_set_level0_stop_writes_trigger as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun unsafe options_enable_statistics as ^ { `DBOptionsPtr' } -> `()' #}

{#fun unsafe options_set_stats_dump_period_sec as ^ { `DBOptionsPtr', `CUInt' } -> `()' #}

{#fun unsafe options_set_db_write_buffer_size as ^ { `DBOptionsPtr', `CSize' } -> `()' #}

{#fun unsafe options_set_max_write_buffer_number as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

-- {#fun options_set_max_background_jobs as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun unsafe options_set_max_background_compactions as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun unsafe options_set_max_background_flushes as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

{#fun unsafe options_set_soft_pending_compaction_bytes_limit as ^ { `DBOptionsPtr', `CSize' } -> `()' #}

{#fun unsafe options_set_hard_pending_compaction_bytes_limit as ^ { `DBOptionsPtr', `CSize' } -> `()' #}

-- extern ROCKSDB_LIBRARY_API void rocksdb_options_set_max_open_files(
--     rocksdb_options_t*, int);
{#fun unsafe options_set_max_open_files as ^ { `DBOptionsPtr', `CInt' } -> `()' #}

-- writeOptions

{#fun unsafe writeoptions_create as ^ { } -> `WriteOptionsPtr' #}

{#fun unsafe writeoptions_destroy as ^ { `WriteOptionsPtr' } -> `()' #}

{#fun unsafe writeoptions_set_sync as ^ { `WriteOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe writeoptions_disable_WAL as ^ { `WriteOptionsPtr', `Bool' } -> `()' #}

-- readOptions

{#fun unsafe readoptions_create as ^ { } -> `ReadOptionsPtr' #}

{#fun unsafe readoptions_destroy as ^ { `ReadOptionsPtr' } -> `()' #}

-- flush options

{#fun unsafe flushoptions_create as ^ { } -> `FlushOptionsPtr' #}

{#fun unsafe flushoptions_destroy as ^ { `FlushOptionsPtr' } -> `()' #}

{#fun unsafe flushoptions_set_wait as ^ { `FlushOptionsPtr', `Bool' } -> `()' #}

-- block based table options

{#fun unsafe block_based_options_create as ^ { } -> `BlockBasedTableOptionsPtr' #}

{#fun unsafe block_based_options_destroy as ^ { `BlockBasedTableOptionsPtr' } -> `()' #}

{#fun unsafe block_based_options_set_block_size as ^ { `BlockBasedTableOptionsPtr', `CSize' } -> `()' #}

{#fun unsafe block_based_options_set_cache_index_and_filter_blocks as ^ { `BlockBasedTableOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe block_based_options_set_cache_index_and_filter_blocks_with_high_priority as ^ { `BlockBasedTableOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe block_based_options_set_pin_l0_filter_and_index_blocks_in_cache as ^ { `BlockBasedTableOptionsPtr', `Bool' } -> `()' #}

{#fun unsafe options_set_block_based_table_factory as ^ { `DBOptionsPtr', `BlockBasedTableOptionsPtr' } -> `()' #}

-- db

{#fun unsafe open as ^ { `DBOptionsPtr', `CString', allocaNullPtr- `CString' peek*} -> `DBFPtr' #}

{#fun unsafe put as ^ { `DBFPtr', `WriteOptionsPtr', `CString', `CSize', `CString', `CSize', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun unsafe get as ^ { `DBFPtr', `ReadOptionsPtr', `CString', `CSize', allocaCSize- `CSize' peek*, allocaNullPtr- `CString' peek*} -> `CString' #}

{#fun unsafe create_iterator as ^ {`DBFPtr', `ReadOptionsPtr'} -> `IteratorFPtr' #}

{#fun unsafe write as ^ { `DBFPtr', `WriteOptionsPtr', `WriteBatchFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

-- column family

{#fun unsafe create_column_family as ^ { `DBFPtr', `DBOptionsPtr', `CString', allocaNullPtr- `CString' peek*} -> `CFFPtr' #}

{#fun unsafe drop_column_family as ^ { `DBFPtr', `CFFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun unsafe list_column_families as ^ { `DBOptionsPtr', `CString', allocaCSize- `CSize' peek*, allocaNullPtr- `CString' peek*} -> `Ptr CString' id #}

{#fun unsafe open_column_families as ^ {
    `DBOptionsPtr',
    `CString',
    `CInt',
    withArray* `[CString]' ,
    withArray* `[DBOptionsPtr]' ,
    id `Ptr CFPtr' ,
    allocaNullPtr- `CString' peek*
    } -> `DBFPtr' #}

{#fun unsafe open_for_read_only_column_families as ^ {
    `DBOptionsPtr',
    `CString',
    `CInt',
    withArray* `[CString]' ,
    withArray* `[DBOptionsPtr]' ,
    id `Ptr CFPtr' ,
    `Bool',
    allocaNullPtr- `CString' peek*
    } -> `DBFPtr' #}

{#fun unsafe put_cf as ^ { `DBFPtr', `WriteOptionsPtr', `CFFPtr', `CString', `CSize', `CString', `CSize', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun unsafe get_cf as ^ { `DBFPtr', `ReadOptionsPtr', `CFFPtr', `CString', `CSize', allocaCSize- `CSize' peek*, allocaNullPtr- `CString' peek*} -> `CString' #}

{#fun unsafe create_iterator_cf as ^ {`DBFPtr', `ReadOptionsPtr', `CFFPtr'} -> `IteratorFPtr' #}

-- /* Returns NULL if property name is unknown.
--    Else returns a pointer to a malloc()-ed null-terminated value. */
-- extern ROCKSDB_LIBRARY_API char* rocksdb_property_value(rocksdb_t* db,
{#fun unsafe property_value as ^ {`DBFPtr', `CString'} -> `CString' #}

-- /* returns 0 on success, -1 otherwise */
-- int rocksdb_property_int(
--     rocksdb_t* db,
--     const char* propname, uint64_t *out_val);
-- {#fun unsafe property_int as ^ {`DBFPtr', `CString', castPtr `Ptr Word64' peek*} -> `CInt' #}
{#fun unsafe property_int as ^ {`DBFPtr', `CString', allocaCSize- `CSize' peek*} -> `CInt' #}

-- /* returns 0 on success, -1 otherwise */
-- int rocksdb_property_int_cf(
--     rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
--     const char* propname, uint64_t *out_val);
{#fun unsafe property_int_cf as ^ {`DBFPtr', `CFFPtr', `CString', allocaCSize- `CSize' peek*} -> `CInt' #}

-- extern ROCKSDB_LIBRARY_API char* rocksdb_property_value_cf(
--     rocksdb_t* db, rocksdb_column_family_handle_t* column_family,
--     const char* propname);
{#fun unsafe property_value_cf as ^ {`DBFPtr', `CFFPtr', `CString'} -> `CString' #}

{#fun unsafe approximate_sizes_cf as ^ {
    `DBFPtr',
    `CFFPtr',
    `CInt',
    withArray* `[CString]' ,
    withArray* `[CSize]' ,
    withArray* `[CString]' ,
    withArray* `[CSize]' ,
    castPtr `Ptr Word64'
    } -> `()' #}

{#fun unsafe flush as ^ { `DBFPtr', `FlushOptionsPtr', allocaNullPtr- `CString' peek*} -> `()' #}

{#fun unsafe flush_cf as ^ { `DBFPtr', `FlushOptionsPtr', `CFFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

-- Iterator

{#fun unsafe iter_valid as ^ {`IteratorFPtr'} -> `Bool' #}

{#fun unsafe iter_seek_to_first as ^ {`IteratorFPtr'} -> `()' #}

{#fun unsafe iter_seek_to_last as ^ {`IteratorFPtr'} -> `()' #}

{#fun unsafe iter_seek as ^ {`IteratorFPtr', `CString', `CSize'} -> `()' #}

{#fun unsafe iter_seek_for_prev as ^ {`IteratorFPtr', `CString', `CSize'} -> `()' #}

{#fun unsafe iter_next as ^ {`IteratorFPtr'} -> `()' #}

{#fun unsafe iter_prev as ^ {`IteratorFPtr'} -> `()' #}

{#fun unsafe iter_key as ^ {`IteratorFPtr', allocaCSize- `CSize' peek*} -> `CString' #}

{#fun unsafe iter_value as ^ {`IteratorFPtr', allocaCSize- `CSize' peek*} -> `CString' #}

{#fun unsafe iter_get_error as ^ {`IteratorFPtr', allocaNullPtr- `CString' peek*} -> `()' #}

-- Write batch

{#fun unsafe writebatch_create as ^ { } -> `WriteBatchFPtr' #}

{#fun unsafe writebatch_clear as ^ {`WriteBatchFPtr'} -> `()' #}

{#fun unsafe writebatch_count as ^ {`WriteBatchFPtr'} -> `CInt' #}

{#fun unsafe writebatch_put as ^ { `WriteBatchFPtr', `CString', `CSize', `CString', `CSize'} -> `()' #}

{#fun unsafe writebatch_put_cf as ^ { `WriteBatchFPtr',  `CFFPtr', `CString', `CSize', `CString', `CSize'} -> `()' #}
