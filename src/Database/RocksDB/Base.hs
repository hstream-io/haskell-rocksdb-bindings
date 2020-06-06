{-# LANGUAGE RecordWildCards #-}

module Database.RocksDB.Base where

import Control.Exception (Exception, bracket)
import Control.Exception (bracket)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Except (ExceptT (..))
import Control.Monad.Trans.Maybe (MaybeT)
import Control.Monad.Trans.Resource (allocate, runResourceT)
import Data.ByteString (ByteString, packCStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import qualified Database.RocksDB.C as C
import Foreign.C.String (newCString, peekCString, withCString)
import Foreign.C.Types (CInt, CSize)
import Foreign.ForeignPtr (finalizeForeignPtr, newForeignPtr)
import Foreign.Marshal.Alloc (free)
import Foreign.Marshal.Array (peekArray, withArray)
import Foreign.Ptr (nullPtr)
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
              value <- packCStringLen (valuePtr, cSizeToInt (valueLen))
              return $ Right $ Just value
        else do
          errStr <- liftIO $ peekCString errPtr
          return $ Left $ DBException $ "putCF error: " ++ errStr

createIteratorCF :: DB -> ReadOptions -> ColumnFamily -> IO Iterator
createIteratorCF (DB dbPtr) readOpts (ColumnFamily cfPtr) = withReadOpts readOpts (\optsPtr -> fmap Iterator (C.createIteratorCf dbPtr optsPtr cfPtr))

destroyIterator :: Iterator -> IO ()
destroyIterator (Iterator iter) = finalizeForeignPtr iter

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
