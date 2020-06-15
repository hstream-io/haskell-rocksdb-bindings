{-# LANGUAGE RecordWildCards #-}

module Database.RocksDB.Options where

import Control.Exception (bracket)
import Data.Default
import qualified Database.RocksDB.C as C

newtype DBOptions = DBOptions {createIfMissing :: Bool}

newtype WriteOptions = WriteOptions {setSync :: Bool}

newtype ReadOptions = ReadOptions {setVerifyChecksums :: Bool}

defaultDBOptions :: DBOptions
defaultDBOptions =
  DBOptions
    { createIfMissing = False
    }

instance Default DBOptions where
  def = defaultDBOptions

defaultWriteOptions :: WriteOptions
defaultWriteOptions = WriteOptions {setSync = False}

instance Default WriteOptions where
  def = defaultWriteOptions

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
