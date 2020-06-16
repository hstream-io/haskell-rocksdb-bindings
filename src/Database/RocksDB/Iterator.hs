module Database.RocksDB.Iterator where

import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Options
import Database.RocksDB.Types
import Foreign.ForeignPtr (finalizeForeignPtr)

newtype Iterator = Iterator C.IteratorFPtr

createIterator :: MonadIO m => DB -> ReadOptions -> m Iterator
createIterator (DB dbPtr) readOpts = liftIO $ withReadOpts readOpts (fmap Iterator . C.createIterator dbPtr)

destroyIterator :: MonadIO m => Iterator -> m ()
destroyIterator (Iterator iter) = liftIO $ finalizeForeignPtr iter

createIteratorCF :: MonadIO m => DB -> ReadOptions -> ColumnFamily -> m Iterator
createIteratorCF (DB dbPtr) readOpts (ColumnFamily cfPtr) = liftIO $ withReadOpts readOpts (\optsPtr -> fmap Iterator (C.createIteratorCf dbPtr optsPtr cfPtr))
