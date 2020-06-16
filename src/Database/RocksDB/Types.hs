module Database.RocksDB.Types where

import Control.Exception (Exception)
import qualified Database.RocksDB.C as C

newtype DB = DB C.DBFPtr

newtype ColumnFamily = ColumnFamily C.CFFPtr
