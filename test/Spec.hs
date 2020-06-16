{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Conduit ((.|), runConduit, sinkList)
import Control.Exception (throwIO)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (mapExceptT, runExceptT)
import Control.Monad.Trans.Resource (MonadUnliftIO, ResourceT, allocate, register, release, runResourceT)
import Data.ByteString (ByteString)
import Data.Default (def)
import Data.Either.Combinators (isRight)
import Database.RocksDB
import qualified Streamly.Prelude as S
import System.IO.Temp (createTempDirectory)
import Test.Hspec
  ( describe,
    hspec,
    it,
    shouldReturn,
    shouldSatisfy,
  )

main :: IO ()
main = hspec $ describe "Basic DB Functionality" $
  do
    it "open db" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            (dbKey, _) <-
              allocate
                (open DBOptions {createIfMissing = True} path)
                close
            release dbKey
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "put kv to db" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            (dbKey, db) <-
              allocate
                (open DBOptions {createIfMissing = True} path)
                close
            put db def "key" "value"
            release dbKey
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "get value from db" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            (dbKey, db) <-
              allocate
                (open DBOptions {createIfMissing = True} path)
                close
            put db def "key" "value"
            value <- get db def "key"
            release dbKey
            release dirKey
            return value
        )
        `shouldReturn` Just "value"
    it "get nonexistent value from db" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            (dbKey, db) <-
              allocate
                (open DBOptions {createIfMissing = True} path)
                close
            value <- get db def "key"
            release dbKey
            release dirKey
            return value
        )
        `shouldReturn` Nothing
    it "range [firstKey, lastKey]" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            (dbKey, db) <-
              allocate
                (open DBOptions {createIfMissing = True} path)
                close
            put db def "key1" "value1"
            put db def "key2" "value2"
            put db def "key3" "value3"
            r <- liftIO $ S.toList $ range db def (Just "key1") (Just "key3")
            release dbKey
            release dirKey
            return r
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
    it "create column family" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            release cfKey
            release dbKey
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "drop column family" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            dropColumnFamily db cf
            release cfKey
            release dbKey
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "list column families" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cf1Key, cf1) <-
              allocate
                (createColumnFamily db opts "cf1")
                destroyColumnFamily
            (cf2Key, cf2) <-
              allocate
                (createColumnFamily db opts "cf2")
                destroyColumnFamily
            r <- listColumnFamilies opts path
            release cf1Key
            release cf2Key
            release dbKey
            release dirKey
            return r
        )
        `shouldReturn` ["default", "cf1", "cf2"]
    it "open column families" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cf1Key, cf1) <-
              allocate
                (createColumnFamily db opts "cf1")
                destroyColumnFamily
            (cf2Key, cf2) <-
              allocate
                (createColumnFamily db opts "cf2")
                destroyColumnFamily
            release cf1Key
            release cf2Key
            release dbKey
            (key, _) <-
              allocate
                ( openColumnFamilies
                    opts
                    path
                    ["default", "cf1", "cf2"]
                    [opts, opts, opts]
                )
                (\(newDb, cfs) -> mapM_ destroyColumnFamily cfs >> close newDb)
            release key
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "put kv to column family" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            putCF db def cf "key" "value"
            release cfKey
            release dbKey
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "get value from column family" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            putCF db def cf "key" "value"
            v <- getCF db def cf "key"
            release cfKey
            release dbKey
            release dirKey
            return v
        )
        `shouldReturn` Just "value"
    it "get nonexsitent value from column family" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            v <- getCF db def cf "key"
            release cfKey
            release dbKey
            release dirKey
            return v
        )
        `shouldReturn` Nothing
    it "create iterator on column family" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            (iterKey, iter) <-
              allocate
                (createIteratorCF db def cf)
                destroyIterator
            release iterKey
            release cfKey
            release dbKey
            release dirKey
            return "success"
        )
        `shouldReturn` "success"
    it "rangeCF [firstKey, lastKey]" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            putCF db def cf "key1" "value1"
            putCF db def cf "key2" "value2"
            putCF db def cf "key3" "value3"
            r <-
              liftIO
                $ S.toList
                $ rangeCF db def cf (Just "key1") (Just "key3")
            release cfKey
            release dbKey
            release dirKey
            return r
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
    it "rangeCF [firstKey, nothing]" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            putCF db def cf "key1" "value1"
            putCF db def cf "key2" "value2"
            putCF db def cf "key3" "value3"
            r <- liftIO $ S.toList $ rangeCF db def cf (Just "key1") Nothing
            release cfKey
            release dbKey
            release dirKey
            return r
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
    it "rangeCF [nothing, lastKey]" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            putCF db def cf "key1" "value1"
            putCF db def cf "key2" "value2"
            putCF db def cf "key3" "value3"
            r <- liftIO $ S.toList $ rangeCF db def cf Nothing (Just "key3")
            release cfKey
            release dbKey
            release dirKey
            return r
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
    it "rangeCF [nothing, nothing]" $
      runResourceT
        ( do
            (dirKey, path) <- createTempDirectory Nothing "rocksdb"
            let opts = DBOptions {createIfMissing = True}
            (dbKey, db) <- allocate (open opts path) close
            (cfKey, cf) <-
              allocate
                (createColumnFamily db opts "cf")
                destroyColumnFamily
            putCF db def cf "key1" "value1"
            putCF db def cf "key2" "value2"
            putCF db def cf "key3" "value3"
            r <- liftIO $ S.toList $ rangeCF db def cf Nothing Nothing
            release cfKey
            release dbKey
            release dirKey
            return r
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
