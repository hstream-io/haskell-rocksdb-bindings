{-# LANGUAGE RecordWildCards #-}

module Database.RocksDB.Options where

import Control.Exception (bracket)
import Data.Default
import qualified Database.RocksDB.C as C
import Database.RocksDB.Util

data DBOptions = DBOptions
  { createIfMissing :: Bool,
    createMissingColumnFamilies :: Bool,
    writeBufferSize :: Int,
    disableAutoCompactions :: Bool,
    level0FileNumCompactionTrigger :: Int,
    level0SlowdownWritesTrigger :: Int,
    level0StopWritesTrigger :: Int
  }

defaultDBOptions :: DBOptions
defaultDBOptions =
  DBOptions
    { createIfMissing = False,
      createMissingColumnFamilies = False,
      writeBufferSize = 67108864,
      disableAutoCompactions = False,
      level0FileNumCompactionTrigger = 4,
      level0SlowdownWritesTrigger = 20,
      level0StopWritesTrigger = 36
    }

instance Default DBOptions where
  def = defaultDBOptions

data WriteOptions = WriteOptions
  { setSync :: Bool,
    disableWAL :: Bool
  }

defaultWriteOptions :: WriteOptions
defaultWriteOptions =
  WriteOptions
    { setSync = False,
      disableWAL = False
    }

instance Default WriteOptions where
  def = defaultWriteOptions

newtype ReadOptions = ReadOptions {setVerifyChecksums :: Bool}

defaultReadOptions :: ReadOptions
defaultReadOptions =
  ReadOptions
    { setVerifyChecksums = False
    }

instance Default ReadOptions where
  def = defaultReadOptions

mkDBOpts :: DBOptions -> IO C.DBOptionsPtr
mkDBOpts DBOptions {..} = do
  opts <- C.optionsCreate
  C.optionsSetCreateIfMissing opts createIfMissing
  C.optionsSetCreateMissingColumnFamilies opts createMissingColumnFamilies
  C.optionsSetWriteBufferSize opts (intToCSize writeBufferSize)
  C.optionsSetDisableAutoCompactions opts disableAutoCompactions
  C.optionsSetLevel0FileNumCompactionTrigger opts (intToCInt level0FileNumCompactionTrigger)
  C.optionsSetLevel0SlowdownWritesTrigger opts (intToCInt level0SlowdownWritesTrigger)
  C.optionsSetLevel0StopWritesTrigger opts (intToCInt level0StopWritesTrigger)
  return opts

withDBOpts :: DBOptions -> (C.DBOptionsPtr -> IO a) -> IO a
withDBOpts opts = bracket (mkDBOpts opts) C.optionsDestroy

mkWriteOpts :: WriteOptions -> IO C.WriteOptionsPtr
mkWriteOpts WriteOptions {..} = do
  optsPtr <- C.writeoptionsCreate
  C.writeoptionsSetSync optsPtr setSync
  C.writeoptionsDisableWAL optsPtr disableWAL
  return optsPtr

withWriteOpts :: WriteOptions -> (C.WriteOptionsPtr -> IO a) -> IO a
withWriteOpts opts = bracket (mkWriteOpts opts) C.writeoptionsDestroy

mkReadOpts :: ReadOptions -> IO C.ReadOptionsPtr
mkReadOpts ReadOptions {..} = C.readoptionsCreate

withReadOpts :: ReadOptions -> (C.ReadOptionsPtr -> IO a) -> IO a
withReadOpts opts = bracket (mkReadOpts opts) C.readoptionsDestroy
