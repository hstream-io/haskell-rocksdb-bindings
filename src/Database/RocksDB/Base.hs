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
import Data.ByteString (ByteString, packCStringLen, useAsCStringLen)
import Data.ByteString.Unsafe (unsafePackCStringLen, unsafeUseAsCStringLen)
import Data.Default
import Data.Function ((&))
import Data.Maybe (isJust)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Iterator
import Database.RocksDB.Options
import Database.RocksDB.Types
import Database.RocksDB.Util
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
                else do
                  errStr <- peekCString errPtr
                  ioError $ userError $ "open error: " ++ errStr
        )

close :: MonadIO m => DB -> m ()
close (DB dbPtr) = liftIO $ finalizeForeignPtr dbPtr

put :: MonadIO m => DB -> WriteOptions -> ByteString -> ByteString -> m ()
put (DB dbPtr) writeOpts key value = liftIO $ withWriteOpts writeOpts put'
  where
    put' opts = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (cValue, cValueLen) <- unsafeUseAsCStringLen value return
      -- (cKey, cKeyLen) <- useAsCStringLen key return
      -- (cValue, cValueLen) <- useAsCStringLen value return
      errPtr <- C.put dbPtr opts cKey (intToCSize cKeyLen) cValue (intToCSize cValueLen)
      if errPtr == nullPtr
        then return ()
        else do
          errStr <- liftIO $ peekCString errPtr
          ioError $ userError $ "put error: " ++ errStr

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
        else do
          errStr <- liftIO $ peekCString errPtr
          ioError $ userError $ "get error: " ++ errStr

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
            Just str -> liftIO $ ioError $ userError $ "range error: " ++ str

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
              else do
                errStr <- peekCString errPtr
                ioError $ userError $ "createColumnFamily error: " ++ errStr
        )

dropColumnFamily :: MonadIO m => DB -> ColumnFamily -> m ()
dropColumnFamily (DB dbPtr) (ColumnFamily cfPtr) = liftIO $ do
  errPtr <- C.dropColumnFamily dbPtr cfPtr
  if errPtr == nullPtr
    then return ()
    else do
      errStr <- peekCString errPtr
      ioError $ userError $ "dropColumnFamily error: " ++ errStr

listColumnFamilies :: MonadIO m => DBOptions -> FilePath -> m [String]
listColumnFamilies dbOpts dbPath = liftIO $ runResourceT $
  do
    (_, dbOptsPtr) <- allocate (mkDBOpts dbOpts) C.optionsDestroy
    (_, pathPtr) <- allocate (newFilePath dbPath) free
    (cfCNamesPtr, num, errPtr) <- liftIO $ C.listColumnFamilies dbOptsPtr pathPtr
    if errPtr == nullPtr
      then do
        cfCNames <- liftIO $ peekArray (cSizeToInt num) cfCNamesPtr
        liftIO $ mapM peekCString cfCNames
      else do
        errStr <- liftIO $ peekCString errPtr
        liftIO $ ioError $ userError $ "listColumnFamilies error: " ++ errStr

openColumnFamilies ::
  MonadIO m =>
  DBOptions ->
  FilePath ->
  [ColumnFamilyDescriptor] ->
  m (DB, [ColumnFamily])
openColumnFamilies dbOpts path cfDescriptors = liftIO $ runResourceT $
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
      else do
        errStr <- liftIO $ peekCString errPtr
        liftIO $ ioError $ userError $ "openColumnFamilies error: " ++ errStr

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
        else do
          errStr <- liftIO $ peekCString errPtr
          liftIO $ ioError $ userError $ "putCF error: " ++ errStr

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
        else do
          errStr <- liftIO $ peekCString errPtr
          liftIO $ ioError $ userError $ "getCF error: " ++ errStr

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
            Just str -> liftIO $ ioError $ userError $ "rangeCF error: " ++ str
