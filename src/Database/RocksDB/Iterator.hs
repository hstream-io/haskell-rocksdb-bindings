module Database.RocksDB.Iterator where

import Control.Exception (bracket)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Options
import Database.RocksDB.Types
import Foreign.ForeignPtr (finalizeForeignPtr)

newtype Iterator = Iterator C.IteratorFPtr

createIterator :: DB -> ReadOptions -> IO Iterator
createIterator (DB dbPtr) readOpts = withReadOpts readOpts (fmap Iterator . C.createIterator dbPtr)

destroyIterator :: Iterator -> IO ()
destroyIterator (Iterator iter) = finalizeForeignPtr iter

createIteratorCF :: DB -> ReadOptions -> ColumnFamily -> IO Iterator
createIteratorCF (DB dbPtr) readOpts (ColumnFamily cfPtr) = withReadOpts readOpts (\optsPtr -> fmap Iterator (C.createIteratorCf dbPtr optsPtr cfPtr))

withIteratorCF :: DB -> ReadOptions -> ColumnFamily -> (Iterator -> IO a) -> IO a
withIteratorCF db readOpts cf = bracket (createIteratorCF db readOpts cf) destroyIterator
