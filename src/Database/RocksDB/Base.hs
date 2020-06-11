{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE RecordWildCards #-}

module Database.RocksDB.Base where

import Conduit (Conduit, ConduitT, repeatWhileMC, yield)
import Control.Exception (Exception, bracket, catch, throw, throwIO)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT (..))
import Control.Monad.Trans.Maybe (MaybeT)
import Control.Monad.Trans.Resource (allocate, runResourceT)
import Data.ByteString (ByteString, packCStringLen, useAsCStringLen)
import Data.ByteString.Unsafe (unsafePackCStringLen, unsafeUseAsCStringLen)
import Data.Function ((&))
import Data.Maybe (isJust)
import qualified Database.RocksDB.C as C
import Foreign.C.String (newCString, peekCString, peekCStringLen, withCString)
import Foreign.C.Types (CInt, CSize)
import Foreign.ForeignPtr (finalizeForeignPtr, newForeignPtr)
import Foreign.Marshal.Alloc (free)
import Foreign.Marshal.Array (peekArray, withArray)
import Foreign.Ptr (nullPtr)
import Streamly (Serial)
import qualified Streamly.Prelude as S
import System.Posix.Internals (newFilePath, withFilePath)

newtype DBException
  = DBException String
  deriving (Show, Eq)

instance Exception DBException

newtype DBOptions = DBOptions {createIfMissing :: Bool}

newtype WriteOptions = WriteOptions {setSync :: Bool}

newtype ReadOptions = ReadOptions {setVerifyChecksums :: Bool}

newtype DB = DB C.DBFPtr

newtype Iterator = Iterator C.IteratorFPtr

newtype ColumnFamily = ColumnFamily C.CFFPtr

open :: FilePath -> DBOptions -> ExceptT DBException IO DB
open path opts = ExceptT $ withDBOpts opts mkDB
  where
    mkDB optsPtr =
      withFilePath
        path
        ( \pathPtr ->
            do
              (dbPtr, errPtr) <- C.open optsPtr pathPtr
              if errPtr == nullPtr
                then return $ Right $ DB dbPtr
                else do
                  errStr <- peekCString errPtr
                  return $ Left $ DBException $ "open error: " ++ errStr
        )

close :: DB -> IO ()
close (DB dbPtr) = finalizeForeignPtr dbPtr

createColumnFamily :: DB -> DBOptions -> String -> ExceptT DBException IO ColumnFamily
createColumnFamily (DB dbPtr) opts name = ExceptT $ withDBOpts opts mkCF
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
              then return $ Right $ ColumnFamily cfPtr
              else do
                errStr <- peekCString errPtr
                return $ Left $ DBException $ "createColumnFamily error: " ++ errStr
        )

dropColumnFamily :: DB -> ColumnFamily -> ExceptT DBException IO ()
dropColumnFamily (DB dbPtr) (ColumnFamily cfPtr) = ExceptT $ do
  errPtr <- C.dropColumnFamily dbPtr cfPtr
  if errPtr == nullPtr
    then return $ Right ()
    else do
      errStr <- peekCString errPtr
      return $ Left $ DBException $ "dropColumnFamily error: " ++ errStr

listColumnFamilies :: DBOptions -> FilePath -> ExceptT DBException IO [String]
listColumnFamilies dbOpts dbPath = ExceptT $ runResourceT $
  do
    (_, dbOptsPtr) <- allocate (mkDBOpts dbOpts) C.optionsDestroy
    (_, pathPtr) <- allocate (newFilePath dbPath) free
    (cfCNamesPtr, num, errPtr) <- liftIO $ C.listColumnFamilies dbOptsPtr pathPtr
    if errPtr == nullPtr
      then do
        cfCNames <- liftIO $ peekArray (cSizeToInt num) cfCNamesPtr
        cfNames <- liftIO $ mapM peekCString cfCNames
        return $ Right cfNames
      else do
        errStr <- liftIO $ peekCString errPtr
        return $ Left $ DBException $ "listColumnFamilies error: " ++ errStr

openColumnFamilies ::
  DBOptions ->
  FilePath ->
  [String] ->
  [DBOptions] ->
  ExceptT DBException IO (DB, [ColumnFamily])
openColumnFamilies dbOpts path cfNames cfOpts = ExceptT $ runResourceT $
  do
    (_, dbOptsPtr) <- allocate (mkDBOpts dbOpts) C.optionsDestroy
    (_, pathPtr) <- allocate (newFilePath path) free
    (_, cfCNames) <- allocate (mapM newCString cfNames) (mapM_ free)
    (_, cfOptsPtr) <- allocate (mapM mkDBOpts cfOpts) (mapM_ C.optionsDestroy)
    let num = length cfNames
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
        return $ Right (DB dbPtr, map ColumnFamily cfFPtrs)
      else do
        errStr <- liftIO $ peekCString errPtr
        return $ Left $ DBException $ "openColumnFamilies error: " ++ errStr

destroyColumnFamily :: ColumnFamily -> IO ()
destroyColumnFamily (ColumnFamily cfPtr) = finalizeForeignPtr cfPtr

putCF :: DB -> WriteOptions -> ColumnFamily -> ByteString -> ByteString -> ExceptT DBException IO ()
putCF (DB dbPtr) writeOpts (ColumnFamily cfPtr) key value = ExceptT $ withWriteOpts writeOpts putCF'
  where
    putCF' opts = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (cValue, cValueLen) <- unsafeUseAsCStringLen value return
      errPtr <- C.putCf dbPtr opts cfPtr cKey (intToCSize cKeyLen) cValue (intToCSize cValueLen)
      if errPtr == nullPtr
        then return $ Right ()
        else do
          errStr <- liftIO $ peekCString errPtr
          return $ Left $ DBException $ "putCF error: " ++ errStr

getCF :: DB -> ReadOptions -> ColumnFamily -> ByteString -> ExceptT DBException IO (Maybe ByteString)
getCF (DB dbPtr) readOpts (ColumnFamily cfPtr) key = ExceptT $ withReadOpts readOpts getCF'
  where
    getCF' readOptsPtr = do
      (cKey, cKeyLen) <- unsafeUseAsCStringLen key return
      (valuePtr, valueLen, errPtr) <- C.getCf dbPtr readOptsPtr cfPtr cKey (intToCSize cKeyLen)
      if errPtr == nullPtr
        then
          if valuePtr == nullPtr
            then return $ Right Nothing
            else do
              value <- unsafePackCStringLen (valuePtr, cSizeToInt valueLen)
              return $ Right $ Just value
        else do
          errStr <- liftIO $ peekCString errPtr
          return $ Left $ DBException $ "putCF error: " ++ errStr

createIteratorCF :: DB -> ReadOptions -> ColumnFamily -> IO Iterator
createIteratorCF (DB dbPtr) readOpts (ColumnFamily cfPtr) = withReadOpts readOpts (\optsPtr -> fmap Iterator (C.createIteratorCf dbPtr optsPtr cfPtr))

destroyIterator :: Iterator -> IO ()
destroyIterator (Iterator iter) = finalizeForeignPtr iter

withIteratorCF :: DB -> ReadOptions -> ColumnFamily -> (Iterator -> IO a) -> IO a
withIteratorCF db readOpts cf = bracket (createIteratorCF db readOpts cf) destroyIterator

rangeCF :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> Serial (ByteString, ByteString)
rangeCF db readOpts cf firstKey lastKey =
  S.bracket
    (createIteratorCF db readOpts cf)
    destroyIterator
    generateStream
  where
    generateStream :: Iterator -> Serial (ByteString, ByteString)
    generateStream (Iterator iter) = do
      case firstKey of
        Nothing -> liftIO $ C.iterSeekToFirst iter
        Just k -> liftIO $ useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
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
    getKV :: C.IteratorFPtr -> IO (Maybe (ByteString, ByteString))
    getKV iter = do
      valid <- C.iterValid iter
      if valid
        then do
          (kPtr, kLen) <- C.iterKey iter
          (vPtr, vLen) <- C.iterValue iter
          key <- packCStringLen (kPtr, cSizeToInt kLen)
          value <- packCStringLen (vPtr, cSizeToInt vLen)
          C.iterNext iter
          return $ Just (key, value)
        else do
          errPtr <- C.iterGetError iter
          if errPtr == nullPtr
            then return Nothing
            else do
              errStr <- peekCString errPtr
              throwIO $ DBException $ "iterator error: " ++ errStr

--rangeCF :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> IO (ConduitT () (Maybe (ByteString, ByteString)) IO ())
--rangeCF db readOpts cf firstKey lastKey = withIteratorCF db readOpts cf $ \(Iterator iter) -> do
--  case firstKey of
--    Nothing -> C.iterSeekToFirst iter
--    Just k -> useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--  return $ loop iter
--  where
--    getKVs :: C.IteratorFPtr -> ConduitT () (Maybe (ByteString, ByteString)) IO ()
--    getKVs iter = do
--      valid <- liftIO $ C.iterValid iter
--      if valid
--        then do
--          c <- liftIO $ pickCurrent iter
--          liftIO $ print c
--          yield $ Just c
--          liftIO $ C.iterNext iter
--          getKVs iter
--        else do
--          errPtr <- liftIO $ C.iterGetError iter
--          if errPtr == nullPtr
--            then yield Nothing
--            else do
--              errStr <- liftIO $ peekCString errPtr
--              liftIO $ print errStr
--              liftIO $ throwIO $ DBException $ "iterator error: " ++ errStr
--    pickCurrent iter = do
--      (valuePtr, valueLen) <- C.iterValue iter
--      print (valuePtr, valueLen)
--      (keyPtr, keyLen) <- C.iterKey iter
--      print (keyPtr, keyLen)
--      key <- packCStringLen (keyPtr, cSizeToInt keyLen)
--      -- key <- packCStringLen (keyPtr, 4)
--      value <- packCStringLen (valuePtr, cSizeToInt valueLen)
--      return (key, value)
--    loop :: C.IteratorFPtr -> ConduitT () (Maybe (ByteString, ByteString)) IO ()
--    loop iter = do
--      valid <- liftIO $ C.iterValid iter
--      if valid
--        then do
--          (key, value) <- liftIO $ pickCurrent iter
--          case lastKey of
--            Nothing -> do
--              yield $ Just (key, value)
--              liftIO $ C.iterNext iter
--              loop iter
--            Just last ->
--              when
--                (key <= last)
--                ( do
--                    yield $ Just (key, value)
--                    liftIO $ C.iterNext iter
--                    loop iter
--                )
--        else do
--          errPtr <- liftIO $ C.iterGetError iter
--          if errPtr == nullPtr
--            then yield Nothing
--            else do
--              errStr <- liftIO $ peekCString errPtr
--              liftIO $ print errStr
--              liftIO $ throwIO $ DBException $ "iterator error: " ++ errStr
--
--iterTest :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> IO ()
--iterTest db readOpts cf firstKey lastKey = withIteratorCF db readOpts cf ttt
--  where
--    loop iter = do
--      valid <- C.iterValid iter
--      when
--        (valid)
--        ( do
--            (kPtr, kLen) <- C.iterKey iter
--            (vPtr, vLen) <- C.iterValue iter
--            key <- packCStringLen (kPtr, cSizeToInt kLen)
--            value <- packCStringLen (vPtr, cSizeToInt vLen)
--            when
--              (maybe True (key <=) lastKey)
--              ( do
--                  print key
--                  print value
--                  C.iterNext iter
--                  loop iter
--              )
--        )
--    ttt (Iterator iter) = do
--      case firstKey of
--        Nothing -> C.iterSeekToFirst iter
--        Just k -> useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--      loop iter
--
--iterTest0 :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> IO [Maybe (ByteString, ByteString)]
--iterTest0 db readOpts cf firstKey lastKey = withIteratorCF db readOpts cf ttt
--  where
--    loop iter = do
--      valid <- C.iterValid iter
--      if (valid)
--        then do
--          (kPtr, kLen) <- C.iterKey iter
--          (vPtr, vLen) <- C.iterValue iter
--          key <- packCStringLen (kPtr, cSizeToInt kLen)
--          value <- packCStringLen (vPtr, cSizeToInt vLen)
--          print (key, value)
--          if maybe True (key <=) lastKey
--            then do
--              C.iterNext iter
--              return $ Just (key, value)
--            else return Nothing
--        else return Nothing
--    www :: Monad m => m a -> (a -> Bool) -> m [a]
--    www ma t = do
--      a <- ma
--      if t a
--        then do
--          r <- www ma t
--          return $ a : r
--        else return [a]
--    ttt (Iterator iter) = do
--      case firstKey of
--        Nothing -> C.iterSeekToFirst iter
--        Just k -> useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--      www (loop iter) isJust
--
--iterTest1 :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> IO (ConduitT () (Maybe (ByteString, ByteString)) IO ())
--iterTest1 db readOpts cf firstKey lastKey = withIteratorCF db readOpts cf ttt
--  where
--    loop :: C.IteratorFPtr -> ConduitT () (Maybe (ByteString, ByteString)) IO ()
--    loop iter = do
--      valid <- liftIO $ C.iterValid iter
--      when
--        (valid)
--        ( do
--            (kPtr, kLen) <- liftIO $ C.iterKey iter
--            (vPtr, vLen) <- liftIO $ C.iterValue iter
--            liftIO $ print (kPtr, kLen)
--            liftIO $ print (vPtr, vLen)
--            key <- liftIO $ packCStringLen (kPtr, cSizeToInt kLen)
--            value <- liftIO $ packCStringLen (vPtr, cSizeToInt vLen)
--            liftIO $ print (key, value)
--            when
--              (maybe True (key <=) lastKey)
--              ( do
--                  yield $ Just (key, value)
--                  liftIO $ C.iterNext iter
--                  loop iter
--              )
--        )
--    ttt :: Iterator -> IO (ConduitT () (Maybe (ByteString, ByteString)) IO ())
--    ttt (Iterator iter) = do
--      case firstKey of
--        Nothing -> C.iterSeekToFirst iter
--        Just k -> useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--      return $ loop iter
--
--iterTest2 :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> IO (ConduitT () (Maybe (ByteString, ByteString)) IO ())
--iterTest2 db readOpts cf firstKey lastKey = withIteratorCF db readOpts cf ttt
--  where
--    loop iter = do
--      valid <- C.iterValid iter
--      if (valid)
--        then do
--          (kPtr, kLen) <- C.iterKey iter
--          (vPtr, vLen) <- C.iterValue iter
--          key <- packCStringLen (kPtr, cSizeToInt kLen)
--          value <- packCStringLen (vPtr, cSizeToInt vLen)
--          print (key, value)
--          if maybe True (key <=) lastKey
--            then do
--              C.iterNext iter
--              return $ Just (key, value)
--            else return Nothing
--        else return Nothing
--    ttt :: Iterator -> IO (ConduitT () (Maybe (ByteString, ByteString)) IO ())
--    ttt (Iterator iter) = do
--      case firstKey of
--        Nothing -> C.iterSeekToFirst iter
--        Just k -> useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--      return $ repeatWhileMC (loop iter) isJust
--
--iterTest3 :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> Serial (Maybe (ByteString, ByteString))
--iterTest3 db readOpts cf firstKey lastKey = do
--  iterator <- liftIO $ createIteratorCF db readOpts cf
--  ttt iterator
--  where
--    loop iter = do
--      valid <- C.iterValid iter
--      if valid
--        then do
--          (kPtr, kLen) <- C.iterKey iter
--          (vPtr, vLen) <- C.iterValue iter
--          key <- packCStringLen (kPtr, cSizeToInt kLen)
--          value <- packCStringLen (vPtr, cSizeToInt vLen)
--          print (key, value)
--          if maybe True (key <=) lastKey
--            then do
--              C.iterNext iter
--              return $ Just (key, value)
--            else return Nothing
--        else return Nothing
--    www :: IO a -> (a -> Bool) -> Serial a
--    www ma t = do
--      a <- liftIO ma
--      if t a
--        then do
--          S.yieldM ma
--          www ma t
--        else S.yieldM ma
--    ttt :: Iterator -> Serial (Maybe (ByteString, ByteString))
--    ttt (Iterator iter) = do
--      case firstKey of
--        Nothing -> liftIO $ C.iterSeekToFirst iter
--        Just k -> liftIO $ useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--      www (loop iter) isJust
--
--iterTest4 :: DB -> ReadOptions -> ColumnFamily -> Maybe ByteString -> Maybe ByteString -> Serial (ByteString, ByteString)
--iterTest4 db readOpts cf firstKey lastKey = do
--  iterator <- liftIO $ createIteratorCF db readOpts cf
--  ttt iterator
--  where
--    loop :: C.IteratorFPtr -> IO (Maybe (ByteString, ByteString))
--    loop iter = do
--      valid <- C.iterValid iter
--      if valid
--        then do
--          (kPtr, kLen) <- C.iterKey iter
--          (vPtr, vLen) <- C.iterValue iter
--          key <- packCStringLen (kPtr, cSizeToInt kLen)
--          value <- packCStringLen (vPtr, cSizeToInt vLen)
--          print (key, value)
--          C.iterNext iter
--          return $ Just (key, value)
--        else do
--          errPtr <- C.iterGetError iter
--          if errPtr == nullPtr
--            then return Nothing
--            else do
--              errStr <- peekCString errPtr
--              throwIO $ DBException $ "iterator error: " ++ errStr
--    ttt :: Iterator -> Serial (ByteString, ByteString)
--    ttt (Iterator iter) = do
--      case firstKey of
--        Nothing -> liftIO $ C.iterSeekToFirst iter
--        Just k -> liftIO $ useAsCStringLen k (\(cStr, len) -> C.iterSeek iter cStr (intToCSize len))
--      case lastKey of
--        Nothing ->
--          S.repeatM (loop iter)
--            & S.takeWhile isJust
--            & S.map
--              ( \case
--                  Just kv -> kv
--              )
--        Just k ->
--          S.repeatM (loop iter)
--            & S.takeWhile isJust
--            & S.map
--              ( \case
--                  Just kv -> kv
--              )
--            & S.takeWhile (\(key, _) -> key <= k)

mkDBOpts :: DBOptions -> IO C.DBOptionsPtr
mkDBOpts DBOptions {..} = do
  opts <- C.optionsCreate
  C.optionsSetCreateIfMissing opts createIfMissing
  return opts

withDBOpts :: DBOptions -> (C.DBOptionsPtr -> IO a) -> IO a
withDBOpts opts = bracket (mkDBOpts opts) C.optionsDestroy

mkWriteOpts :: WriteOptions -> IO C.WriteOptionsPtr
mkWriteOpts WriteOptions {..} = do
  optsPtr <- C.writeoptionsCreate
  C.writeoptionsSetSync optsPtr setSync
  return optsPtr

withWriteOpts :: WriteOptions -> (C.WriteOptionsPtr -> IO a) -> IO a
withWriteOpts opts = bracket (mkWriteOpts opts) C.writeoptionsDestroy

mkReadOpts :: ReadOptions -> IO C.ReadOptionsPtr
mkReadOpts ReadOptions {..} = C.readoptionsCreate

withReadOpts :: ReadOptions -> (C.ReadOptionsPtr -> IO a) -> IO a
withReadOpts opts = bracket (mkReadOpts opts) C.readoptionsDestroy

intToCInt :: Int -> CInt
intToCInt = fromIntegral

cSizeToInt :: CSize -> Int
cSizeToInt = fromIntegral

intToCSize :: Int -> CSize
intToCSize = fromIntegral
