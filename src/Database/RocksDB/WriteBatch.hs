module Database.RocksDB.WriteBatch where

import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Options
import Database.RocksDB.Types
import Database.RocksDB.Util
import Foreign.C.String (peekCString)
import Foreign.ForeignPtr (finalizeForeignPtr)
import Foreign.Ptr (nullPtr)

newtype WriteBatch = WriteBatch C.WriteBatchFPtr

createWriteBatch :: MonadIO m => m WriteBatch
createWriteBatch = liftIO $ fmap WriteBatch C.writebatchCreate

destroyWriteBatch :: MonadIO m => WriteBatch -> m ()
destroyWriteBatch (WriteBatch batchPtr) = liftIO $ finalizeForeignPtr batchPtr

withWriteBatch :: MonadIO m => (WriteBatch -> IO a) -> m a
withWriteBatch = liftIO . bracket createWriteBatch destroyWriteBatch

clear :: MonadIO m => WriteBatch -> m ()
clear (WriteBatch batchPtr) = liftIO $ C.writebatchClear batchPtr

count :: MonadIO m => WriteBatch -> m Int
count (WriteBatch batchPtr) = liftIO $ fmap cIntToInt (C.writebatchCount batchPtr)

batchPutCF :: MonadIO m => WriteBatch -> ColumnFamily -> ByteString -> ByteString -> m ()
batchPutCF (WriteBatch batchPtr) (ColumnFamily cfPtr) key value = liftIO $
  do
    (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
    (cValue, cValueLen) <- unsafeUseAsCStringLen value return
    C.writebatchPutCf batchPtr cfPtr cKey (intToCSize cKeyLen) cValue (intToCSize cValueLen)
