module Database.RocksDB.Util where

import Data.Binary (Binary, decode, encode)
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Word (Word32, Word64)
import Foreign.C (CULLong, CULong)
import Foreign.C.Types (CInt, CSize, CUInt)

intToCInt :: Int -> CInt
intToCInt = fromIntegral

intToCUInt :: Int -> CUInt
intToCUInt = fromIntegral

cIntToInt :: CInt -> Int
cIntToInt = fromIntegral

cSizeToInt :: CSize -> Int
cSizeToInt = fromIntegral

intToCSize :: Int -> CSize
intToCSize = fromIntegral

word64ToCSize :: Word64 -> CSize
word64ToCSize = fromIntegral

cSizeToWord64 :: CSize -> Word64
cSizeToWord64 = fromIntegral

word32ToCUInt :: Word32 -> CUInt
word32ToCUInt = fromIntegral

serialize :: Binary b => b -> ByteString
serialize = BSL.toStrict . encode

deserialize :: Binary b => ByteString -> b
deserialize = decode . BSL.fromStrict
