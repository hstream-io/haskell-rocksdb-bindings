{-# LANGUAGE LambdaCase #-}

module Database.RocksDB.Base where

import Conduit (Conduit, ConduitT, repeatWhileMC, yield)
import Control.Exception (Exception, bracket, catch, throw, throwIO)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT (..))
import Control.Monad.Trans.Maybe (MaybeT)
import Control.Monad.Trans.Resource (MonadUnliftIO, allocate, runResourceT)
import Data.ByteString (ByteString, packCString, packCStringLen, useAsCStringLen)
import Data.ByteString.Unsafe (unsafePackCStringLen, unsafeUseAsCString, unsafeUseAsCStringLen)
import Data.Default
import Data.Function ((&))
import Data.Maybe (isJust)
import Data.Word (Word64)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Exception
import Database.RocksDB.Iterator
import Database.RocksDB.Options
import Database.RocksDB.Types
import Database.RocksDB.Util
import Database.RocksDB.WriteBatch
import Foreign.C.String (newCString, peekCString, peekCStringLen, withCString)
import Foreign.C.Types (CInt, CSize)
import Foreign.ForeignPtr (finalizeForeignPtr, newForeignPtr)
import Foreign.Marshal.Alloc (free)
import Foreign.Marshal.Array (peekArray, withArray)
import Foreign.Ptr (nullPtr)
import Streamly (Serial)
import qualified Streamly.Prelude as S
import System.Posix.Internals (newFilePath, withFilePath)

open :: MonadIO m => DBOptions -> FilePath -> m DB
open opts path = liftIO $ withDBOpts opts mkDB
  where
    mkDB optsPtr =
      withFilePath
        path
        ( \pathPtr ->
            do
              (dbPtr, errPtr) <- C.open optsPtr pathPtr
              if errPtr == nullPtr
                then return $ DB dbPtr
                else throwDbException "open error: " errPtr
        )

openForReadOnly :: MonadIO m => DBOptions -> FilePath -> Bool -> m DB
openForReadOnly opts path errorIfLogFileExist = liftIO $ withDBOpts opts mkDbReadOnly
  where
    mkDbReadOnly optsPtr =
      withFilePath
        path
        ( \pathPtr ->
            do
              (dbPtr, errPtr) <- C.openForReadOnly optsPtr pathPtr errorIfLogFileExist
              if errPtr == nullPtr
                then return $ DB dbPtr
                else throwDbException "openForReadOnly error: " errPtr
        )

close :: MonadIO m => DB -> m ()
close (DB dbPtr) = liftIO $ finalizeForeignPtr dbPtr

put :: MonadIO m => DB -> WriteOptions -> ByteString -> ByteString -> m ()
put (DB dbPtr) writeOpts key value = liftIO $ withWriteOpts writeOpts put'
  where
    put' opts = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (cValue, cValueLen) <- unsafeUseAsCStringLen value return
      errPtr <- C.put dbPtr opts cKey (intToCSize cKeyLen) cValue (intToCSize cValueLen)
      if errPtr == nullPtr
        then return ()
        else throwDbException "put error: " errPtr

get :: MonadIO m => DB -> ReadOptions -> ByteString -> m (Maybe ByteString)
get (DB dbPtr) readOpts key = liftIO $ withReadOpts readOpts get'
  where
    get' readOptsPtr = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (valuePtr, valueLen, errPtr) <- C.get dbPtr readOptsPtr cKey (intToCSize cKeyLen)
      if errPtr == nullPtr
        then
          if valuePtr == nullPtr
            then return Nothing
            else do
              value <- unsafePackCStringLen (valuePtr, cSizeToInt valueLen)
              return $ Just value
        else throwDbException "get error: " errPtr

range :: DB -> ReadOptions -> Maybe ByteString -> Maybe ByteString -> Serial (ByteString, ByteString)
range db readOpts firstKey lastKey =
  S.bracket
    (createIterator db readOpts)
    destroyIterator
    generateStream
  where
    generateStream :: Iterator -> Serial (ByteString, ByteString)
    generateStream iter = do
      case firstKey of
        Nothing -> seekToFirst iter
        Just k -> seek iter k
      case lastKey of
        Nothing ->
          S.repeatM (getKV iter)
            & S.takeWhile isJust
            & S.map
              ( \case
                  Just kv -> kv
              )
        Just k ->
          S.repeatM (getKV iter)
            & S.takeWhile isJust
            & S.map
              ( \case
                  Just kv -> kv
              )
            & S.takeWhile (\(key, _) -> key <= k)
    getKV :: Iterator -> IO (Maybe (ByteString, ByteString))
    getKV iter = do
      valid <- valid iter
      if valid
        then do
          key <- key iter
          value <- value iter
          next iter
          return $ Just (key, value)
        else do
          errStr <- getError iter
          case errStr of
            Nothing -> return Nothing
            Just str -> liftIO $ throwIO $ RocksDbIOException $ "range error: " ++ str

createColumnFamily :: MonadIO m => DB -> DBOptions -> String -> m ColumnFamily
createColumnFamily (DB dbPtr) opts name = liftIO $ withDBOpts opts mkCF
  where
    mkCF optsPtr =
      withCString
        name
        ( \cname -> do
            (cfPtr, errPtr) <-
              C.createColumnFamily
                dbPtr
                optsPtr
                cname
            if errPtr == nullPtr
              then return $ ColumnFamily cfPtr
              else throwDbException "createColumnFamily error: " errPtr
        )

dropColumnFamily :: MonadIO m => DB -> ColumnFamily -> m ()
dropColumnFamily (DB dbPtr) (ColumnFamily cfPtr) = liftIO $ do
  errPtr <- C.dropColumnFamily dbPtr cfPtr
  if errPtr == nullPtr
    then return ()
    else throwDbException "dropColumnFamily error: " errPtr

listColumnFamilies :: MonadIO m => DBOptions -> FilePath -> m [String]
listColumnFamilies dbOpts dbPath = liftIO $
  runResourceT $
    do
      (_, dbOptsPtr) <- allocate (mkDBOpts dbOpts) C.optionsDestroy
      (_, pathPtr) <- allocate (newFilePath dbPath) free
      (cfCNamesPtr, num, errPtr) <- liftIO $ C.listColumnFamilies dbOptsPtr pathPtr
      if errPtr == nullPtr
        then do
          cfCNames <- liftIO $ peekArray (cSizeToInt num) cfCNamesPtr
          liftIO $ mapM peekCString cfCNames
        else liftIO $ throwDbException "listColumnFamilies error: " errPtr

openColumnFamilies ::
  MonadIO m =>
  DBOptions ->
  FilePath ->
  [ColumnFamilyDescriptor] ->
  m (DB, [ColumnFamily])
openColumnFamilies dbOpts path cfDescriptors = liftIO $
  runResourceT $
    do
      (_, dbOptsPtr) <- allocate (mkDBOpts dbOpts) C.optionsDestroy
      (_, pathPtr) <- allocate (newFilePath path) free
      (_, cfCNames) <- allocate (mapM (newCString . name) cfDescriptors) (mapM_ free)
      (_, cfOptsPtr) <- allocate (mapM (mkDBOpts . options) cfDescriptors) (mapM_ C.optionsDestroy)
      let num = length cfDescriptors
      let emptyCfHandles = replicate num nullPtr
      (dbPtr, cfHandles, errPtr) <-
        liftIO $
          withArray
            emptyCfHandles
            ( \emptyCfHandlesPtr -> do
                (dbPtr, errPtr) <-
                  C.openColumnFamilies
                    dbOptsPtr
                    pathPtr
                    (intToCInt num)
                    cfCNames
                    cfOptsPtr
                    emptyCfHandlesPtr
                cfHandles <- peekArray num emptyCfHandlesPtr
                return (dbPtr, cfHandles, errPtr)
            )
      if errPtr == nullPtr
        then do
          cfFPtrs <- liftIO $ mapM (newForeignPtr C.columnFamilyHandleDestroyFunPtr) cfHandles
          return (DB dbPtr, map ColumnFamily cfFPtrs)
        else liftIO $ throwDbException "openColumnFamilies error: " errPtr

openForReadOnlyColumnFamilies ::
  MonadIO m =>
  DBOptions ->
  FilePath ->
  [ColumnFamilyDescriptor] ->
  Bool ->
  m (DB, [ColumnFamily])
openForReadOnlyColumnFamilies dbOpts path cfDescriptors errorIfLogFileExist = liftIO $
  runResourceT $
    do
      (_, dbOptsPtr) <- allocate (mkDBOpts dbOpts) C.optionsDestroy
      (_, pathPtr) <- allocate (newFilePath path) free
      (_, cfCNames) <- allocate (mapM (newCString . name) cfDescriptors) (mapM_ free)
      (_, cfOptsPtr) <- allocate (mapM (mkDBOpts . options) cfDescriptors) (mapM_ C.optionsDestroy)
      let num = length cfDescriptors
      let emptyCfHandles = replicate num nullPtr
      (dbPtr, cfHandles, errPtr) <-
        liftIO $
          withArray
            emptyCfHandles
            ( \emptyCfHandlesPtr -> do
                (dbPtr, errPtr) <-
                  C.openForReadOnlyColumnFamilies
                    dbOptsPtr
                    pathPtr
                    (intToCInt num)
                    cfCNames
                    cfOptsPtr
                    emptyCfHandlesPtr
                    errorIfLogFileExist
                cfHandles <- peekArray num emptyCfHandlesPtr
                return (dbPtr, cfHandles, errPtr)
            )
      if errPtr == nullPtr
        then do
          cfFPtrs <- liftIO $ mapM (newForeignPtr C.columnFamilyHandleDestroyFunPtr) cfHandles
          return (DB dbPtr, map ColumnFamily cfFPtrs)
        else liftIO $ throwDbException "openForReadOnlyColumnFamilies error: " errPtr

destroyColumnFamily :: MonadIO m => ColumnFamily -> m ()
destroyColumnFamily (ColumnFamily cfPtr) = liftIO $ finalizeForeignPtr cfPtr

putCF :: MonadIO m => DB -> WriteOptions -> ColumnFamily -> ByteString -> ByteString -> m ()
putCF (DB dbPtr) writeOpts (ColumnFamily cfPtr) key value = liftIO $ withWriteOpts writeOpts putCF'
  where
    putCF' opts = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (cValue, cValueLen) <- unsafeUseAsCStringLen value return
      errPtr <- C.putCf dbPtr opts cfPtr cKey (intToCSize cKeyLen) cValue (intToCSize cValueLen)
      if errPtr == nullPtr
        then return ()
        else liftIO $ throwDbException "putCF error: " errPtr

getCF :: MonadIO m => DB -> ReadOptions -> ColumnFamily -> ByteString -> m (Maybe ByteString)
getCF (DB dbPtr) readOpts (ColumnFamily cfPtr) key = liftIO $ withReadOpts readOpts getCF'
  where
    getCF' readOptsPtr = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (valuePtr, valueLen, errPtr) <- C.getCf dbPtr readOptsPtr cfPtr cKey (intToCSize cKeyLen)
      if errPtr == nullPtr
        then
          if valuePtr == nullPtr
            then return Nothing
            else do
              value <- unsafePackCStringLen (valuePtr, cSizeToInt valueLen)
              return $ Just value
        else liftIO $ throwDbException "getCF error: " errPtr

rangeCF :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> Serial (ByteString, ByteString)
rangeCF db readOpts cf firstKey lastKey =
  S.bracket
    (createIteratorCF db readOpts cf)
    destroyIterator
    generateStream
  where
    generateStream :: Iterator -> Serial (ByteString, ByteString)
    generateStream iter = do
      case firstKey of
        Nothing -> seekToFirst iter
        Just k -> seek iter k
      case lastKey of
        Nothing ->
          S.repeatM (getKV iter)
            & S.takeWhile isJust
            & S.map
              ( \case
                  Just kv -> kv
              )
        Just k ->
          S.repeatM (getKV iter)
            & S.takeWhile isJust
            & S.map
              ( \case
                  Just kv -> kv
              )
            & S.takeWhile (\(key, _) -> key <= k)
    getKV :: Iterator -> IO (Maybe (ByteString, ByteString))
    getKV iter = do
      valid <- valid iter
      if valid
        then do
          key <- key iter
          value <- value iter
          next iter
          return $ Just (key, value)
        else do
          errStr <- getError iter
          case errStr of
            Nothing -> return Nothing
            Just str -> liftIO $ throwIO $ RocksDbIOException $ "rangeCF error: " ++ str

write :: MonadIO m => DB -> WriteOptions -> WriteBatch -> m ()
write (DB dbPtr) writeOpts (WriteBatch batchPtr) = liftIO $ withWriteOpts writeOpts write'
  where
    write' opts = do
      errPtr <- C.write dbPtr opts batchPtr
      if errPtr == nullPtr
        then return ()
        else liftIO $ throwDbException "write error: " errPtr

approximateSizesCf :: MonadIO m => DB -> ColumnFamily -> [KeyRange] -> m [Word64]
approximateSizesCf (DB dbPtr) (ColumnFamily cfPtr) keyRanges = liftIO $ do
  let num = length keyRanges
  cStrStartKeys <- mapM (flip unsafeUseAsCStringLen return . startKey) keyRanges
  cStrLimitKeys <- mapM (flip unsafeUseAsCStringLen return . limitKey) keyRanges
  let startKeys = map startKey keyRanges
  let resultSizes = replicate num 0
  withArray
    resultSizes
    ( \resultSizesPtr -> do
        C.approximateSizesCf
          dbPtr
          cfPtr
          (intToCInt num)
          (map fst cStrStartKeys)
          (map (intToCSize . snd) cStrStartKeys)
          (map fst cStrLimitKeys)
          (map (intToCSize . snd) cStrLimitKeys)
          resultSizesPtr
        peekArray num resultSizesPtr
    )

flush :: MonadIO m => DB -> FlushOptions -> m ()
flush (DB dbPtr) flushOpts = liftIO $ withFlushOpts flushOpts flush'
  where
    flush' opts = do
      errPtr <- C.flush dbPtr opts
      if errPtr == nullPtr
        then return ()
        else liftIO $ throwDbException "flush error: " errPtr

flushCF :: MonadIO m => DB -> FlushOptions -> ColumnFamily -> m ()
flushCF (DB dbPtr) flushOpts (ColumnFamily cfPtr) = liftIO $ withFlushOpts flushOpts flushCF'
  where
    flushCF' opts = do
      errPtr <- C.flushCf dbPtr opts cfPtr
      if errPtr == nullPtr
        then return ()
        else liftIO $ throwDbException "flushCF error: " errPtr

getPropertyValue :: MonadIO m => DB -> ByteString -> m (Maybe ByteString)
getPropertyValue (DB dbPtr) propName = liftIO $ do
  propNameCstr <- unsafeUseAsCString propName return
  valuePtr <- C.propertyValue dbPtr propNameCstr
  if valuePtr == nullPtr
    then return Nothing
    else do
      value <- packCString valuePtr
      free valuePtr
      return $ Just value

getPropertyValueCF :: MonadIO m => DB -> ColumnFamily -> ByteString -> m (Maybe ByteString)
getPropertyValueCF (DB dbPtr) (ColumnFamily cfPtr) propName = liftIO $ do
  propNameCstr <- unsafeUseAsCString propName return
  valuePtr <- C.propertyValueCf dbPtr cfPtr propNameCstr
  if valuePtr == nullPtr
    then return Nothing
    else do
      value <- packCString valuePtr
      free valuePtr
      return $ Just value

getPropertyInt :: MonadIO m => DB -> ByteString -> m (Maybe Word64)
getPropertyInt (DB dbPtr) propName = liftIO $ do
  propNameCstr <- unsafeUseAsCString propName return
  (resFlag, res) <- C.propertyInt dbPtr propNameCstr
  if resFlag /= 0
    then return Nothing
    else return $ Just $ cSizeToWord64 res

getPropertyIntCF :: MonadIO m => DB -> ColumnFamily -> ByteString -> m (Maybe Word64)
getPropertyIntCF (DB dbPtr) (ColumnFamily cfPtr) propName = liftIO $ do
  propNameCstr <- unsafeUseAsCString propName return
  (resFlag, res) <- C.propertyIntCf dbPtr cfPtr propNameCstr
  if resFlag /= 0
    then return Nothing
    else return $ Just $ cSizeToWord64 res
