module Database.RocksDB.Exception where

import Control.Exception (Exception, throwIO)
import Foreign.C (CString, peekCString)
import Type.Reflection (Typeable)

data RocksDbException
  = RocksDbIOException String
  deriving (Show, Typeable)

instance Exception RocksDbException

throwDbException :: String -> CString -> IO a
throwDbException label errPtr = do
  errStr <- peekCString errPtr
  throwIO $ RocksDbIOException $ label ++ errStr
