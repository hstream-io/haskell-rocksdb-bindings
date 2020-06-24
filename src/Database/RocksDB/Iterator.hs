module Database.RocksDB.Iterator where

import Control.Exception (bracket)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString, packCStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Options
import Database.RocksDB.Types
import Database.RocksDB.Util
import Foreign.C.String (peekCString)
import Foreign.ForeignPtr (finalizeForeignPtr)
import Foreign.Ptr (nullPtr)

newtype Iterator = Iterator C.IteratorFPtr

createIterator :: MonadIO m => DB -> ReadOptions -> m Iterator
createIterator (DB dbPtr) readOpts = liftIO $ withReadOpts readOpts (fmap Iterator . C.createIterator dbPtr)

destroyIterator :: MonadIO m => Iterator -> m ()
destroyIterator (Iterator iter) = liftIO $ finalizeForeignPtr iter

createIteratorCF :: MonadIO m => DB -> ReadOptions -> ColumnFamily -> m Iterator
createIteratorCF (DB dbPtr) readOpts (ColumnFamily cfPtr) = liftIO $ withReadOpts readOpts (\optsPtr -> fmap Iterator (C.createIteratorCf dbPtr optsPtr cfPtr))

withIterator :: MonadIO m => DB -> ReadOptions -> (Iterator -> IO a) -> m a
withIterator db readOpts = liftIO . bracket (createIterator db readOpts) destroyIterator

withIteratorCF :: MonadIO m => DB -> ReadOptions -> ColumnFamily -> (Iterator -> IO a) -> m a
withIteratorCF db readOpts cf = liftIO . bracket (createIteratorCF db readOpts cf) destroyIterator

valid :: MonadIO m => Iterator -> m Bool
valid (Iterator iter) = liftIO $ C.iterValid iter

seekToFirst :: MonadIO m => Iterator -> m ()
seekToFirst (Iterator iter) = liftIO $ C.iterSeekToFirst iter

seekToLast :: MonadIO m => Iterator -> m ()
seekToLast (Iterator iter) = liftIO $ C.iterSeekToLast iter

seek :: MonadIO m => Iterator -> ByteString -> m ()
seek (Iterator iter) key = liftIO $ do
  (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
  C.iterSeek iter cKey (intToCSize cKeyLen)

seekForPrev :: MonadIO m => Iterator -> ByteString -> m ()
seekForPrev (Iterator iter) key = liftIO $ do
  (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
  C.iterSeekForPrev iter cKey (intToCSize cKeyLen)

next :: MonadIO m => Iterator -> m ()
next (Iterator iter) = liftIO $ C.iterNext iter

prev :: MonadIO m => Iterator -> m ()
prev (Iterator iter) = liftIO $ C.iterPrev iter

key :: MonadIO m => Iterator -> m ByteString
key (Iterator iter) = liftIO $ do
  (kPtr, kLen) <- C.iterKey iter
  packCStringLen (kPtr, cSizeToInt kLen)

value :: MonadIO m => Iterator -> m ByteString
value (Iterator iter) = liftIO $ do
  (vPtr, vLen) <- C.iterValue iter
  packCStringLen (vPtr, cSizeToInt vLen)

getError :: MonadIO m => Iterator -> m (Maybe String)
getError (Iterator iter) = liftIO $ do
  errPtr <- C.iterGetError iter
  if errPtr == nullPtr
    then return Nothing
    else do
      errStr <- peekCString errPtr
      return $ Just errStr
