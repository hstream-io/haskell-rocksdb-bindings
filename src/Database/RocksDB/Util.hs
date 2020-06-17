module Database.RocksDB.Util where

import Data.Binary (Binary, decode, encode)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Foreign.C.Types (CInt, CSize)

intToCInt :: Int -> CInt
intToCInt = fromIntegral

cSizeToInt :: CSize -> Int
cSizeToInt = fromIntegral

intToCSize :: Int -> CSize
intToCSize = fromIntegral

serialize :: Binary b => b -> ByteString
serialize = BSL.toStrict . encode

deserialize :: Binary b => ByteString -> b
deserialize = decode . BSL.fromStrict
