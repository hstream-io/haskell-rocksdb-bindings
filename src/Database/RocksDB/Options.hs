{-# LANGUAGE RecordWildCards #-}

module Database.RocksDB.Options where

import Control.Exception (bracket)
import Data.Default
import Data.Word (Word32, Word64)
import qualified Database.RocksDB.C as C
import Database.RocksDB.Util

data DBOptions = DBOptions
  { createIfMissing :: Bool,
    createMissingColumnFamilies :: Bool,
    writeBufferSize :: Word64,
    disableAutoCompactions :: Bool,
    level0FileNumCompactionTrigger :: Int,
    level0SlowdownWritesTrigger :: Int,
    level0StopWritesTrigger :: Int,
    enableStatistics :: Bool,
    statsDumpPeriodSec :: Word32,
    dbWriteBufferSize :: Word64,
    maxWriteBufferNumber :: Int,
    -- maxBackgroundJobs :: Int,
    maxBackgroundCompactions :: Int,
    maxBackgroundFlushes :: Int,
    softPendingCompactionBytesLimit :: Word64,
    hardPendingCompactionBytesLimit :: Word64
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
      level0StopWritesTrigger = 36,
      enableStatistics = False,
      statsDumpPeriodSec = 600,
      dbWriteBufferSize = 0,
      maxWriteBufferNumber = 2,
      -- maxBackgroundJobs = 2,
      maxBackgroundCompactions = -1,
      maxBackgroundFlushes = -1,
      softPendingCompactionBytesLimit = 68719476736,
      hardPendingCompactionBytesLimit = 274877906944
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

data FlushOptions = FlushOptions
  { wait :: Bool
  }

defaultFlushOptions :: FlushOptions
defaultFlushOptions =
  FlushOptions
    { wait = True
    }

instance Default FlushOptions where
  def = defaultFlushOptions

mkDBOpts :: DBOptions -> IO C.DBOptionsPtr
mkDBOpts DBOptions {..} = do
  opts <- C.optionsCreate
  C.optionsSetCreateIfMissing opts createIfMissing
  C.optionsSetCreateMissingColumnFamilies opts createMissingColumnFamilies
  C.optionsSetWriteBufferSize opts (word64ToCSize writeBufferSize)
  C.optionsSetDisableAutoCompactions opts disableAutoCompactions
  C.optionsSetLevel0FileNumCompactionTrigger opts (intToCInt level0FileNumCompactionTrigger)
  C.optionsSetLevel0SlowdownWritesTrigger opts (intToCInt level0SlowdownWritesTrigger)
  C.optionsSetLevel0StopWritesTrigger opts (intToCInt level0StopWritesTrigger)
  C.optionsEnableStatistics opts
  C.optionsSetStatsDumpPeriodSec opts (word32ToCUInt statsDumpPeriodSec)
  C.optionsSetDbWriteBufferSize opts (word64ToCSize dbWriteBufferSize)
  C.optionsSetMaxWriteBufferNumber opts (intToCInt maxWriteBufferNumber)
  -- C.optionsSetMaxBackgroundJobs opts (intToCInt maxBackgroundJobs)
  C.optionsSetMaxBackgroundCompactions opts (intToCInt maxBackgroundCompactions)
  C.optionsSetMaxBackgroundFlushes opts (intToCInt maxBackgroundFlushes)
  C.optionsSetSoftPendingCompactionBytesLimit opts (word64ToCSize softPendingCompactionBytesLimit)
  C.optionsSetHardPendingCompactionBytesLimit opts (word64ToCSize hardPendingCompactionBytesLimit)
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

mkFlushOpts :: FlushOptions -> IO C.FlushOptionsPtr
mkFlushOpts FlushOptions {..} = do
  optsPtr <- C.flushoptionsCreate
  C.flushoptionsSetWait optsPtr wait
  return optsPtr

withFlushOpts :: FlushOptions -> (C.FlushOptionsPtr -> IO a) -> IO a
withFlushOpts opts = bracket (mkFlushOpts opts) C.flushoptionsDestroy
