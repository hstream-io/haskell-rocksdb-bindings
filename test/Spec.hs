{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Conduit ((.|), runConduit, sinkList)
import Control.Exception (throwIO)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (mapExceptT, runExceptT)
import Control.Monad.Trans.Resource (MonadUnliftIO, ResourceT, allocate, release, runResourceT)
import Data.ByteString (ByteString)
import Data.Default (def)
import Data.Either.Combinators (isRight)
import Database.RocksDB.Base
import Database.RocksDB.Iterator
import Database.RocksDB.Options
import qualified Streamly.Prelude as S
import System.IO.Temp (withSystemTempDirectory)
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
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ open path DBOptions {createIfMissing = True}
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "put kv to db" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  put db def "key" "value"
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "get value from db" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  put db def "key" "value"
                  value <- get db def "key"
                  case value of
                    Nothing -> return ""
                    Just v -> return v
              return $ either (const "error") id e
        )
        `shouldReturn` "value"
    it "get nonexistent value from db" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  value <- get db def "key"
                  case value of
                    Nothing -> return ""
                    Just v -> return v
              return $ either (const "error") id e
        )
        `shouldReturn` ""
    it "create column family" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  cf <- createColumnFamily db opts "cf"
                  lift $ destroyColumnFamily cf
                  lift $ close db
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "drop column family" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  cf <- createColumnFamily db opts "cf"
                  dropColumnFamily db cf
                  lift $ destroyColumnFamily cf
                  lift $ close db
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "list column families" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <- runExceptT $ do
                let opts = DBOptions {createIfMissing = True}
                db <- open path opts
                cf1 <- createColumnFamily db opts "cf1"
                cf2 <- createColumnFamily db opts "cf2"
                lift $ destroyColumnFamily cf1
                lift $ destroyColumnFamily cf2
                lift $ close db
                listColumnFamilies opts path
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "open column families" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <- runExceptT $ do
                let opts = DBOptions {createIfMissing = True}
                db <- open path opts
                cf1 <- createColumnFamily db opts "cf1"
                cf2 <- createColumnFamily db opts "cf2"
                lift $ destroyColumnFamily cf1
                lift $ destroyColumnFamily cf2
                lift $ close db
                (db, cfs) <- openColumnFamilies opts path ["default", "cf1", "cf2"] [opts, opts, opts]
                lift $ mapM destroyColumnFamily cfs
                lift $ close db
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "put kv to column family" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  cf <- createColumnFamily db opts "cf"
                  putCF db def cf "key" "value"
                  lift $ destroyColumnFamily cf
                  lift $ close db
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    it "get value from column family" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  cf <- createColumnFamily db opts "cf"
                  putCF db def cf "key" "value"
                  value <- getCF db def cf "key"
                  lift $ destroyColumnFamily cf
                  lift $ close db
                  case value of
                    Nothing -> return ""
                    Just v -> return v
              return $ either (const "error") id e
        )
        `shouldReturn` "value"
    it "get nonexistent value from column family" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  cf <- createColumnFamily db opts "cf"
                  value <- getCF db def cf "key"
                  lift $ destroyColumnFamily cf
                  lift $ close db
                  case value of
                    Nothing -> return ""
                    Just v -> return v
              return $ either (const "error") id e
        )
        `shouldReturn` ""
    it "range [firstKey, lastKey]" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              er <-
                runExceptT
                  ( do
                      let opts = DBOptions {createIfMissing = True}
                      db <- open path opts
                      put db def "key1" "value1"
                      put db def "key2" "value2"
                      put db def "key3" "value3"
                      return db
                  )
              either
                throwIO
                ( \db ->
                    do
                      r <- S.toList $ range db def (Just "key1") (Just "key3")
                      close db
                      return r
                )
                er
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
    it "create iterator on column family" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              e <-
                runExceptT $ do
                  let opts = DBOptions {createIfMissing = True}
                  db <- open path opts
                  cf <- createColumnFamily db opts "cf"
                  iter <- liftIO $ createIteratorCF db def cf
                  lift $ destroyIterator iter
                  lift $ destroyColumnFamily cf
                  lift $ close db
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
    --    it "range on column family" $
    --      withSystemTempDirectory
    --        "rocksdb"
    --        ( \path ->
    --            do
    --              e <-
    --                runExceptT $ do
    --                  let opts = DBOptions {createIfMissing = True}
    --                  db <- open path opts
    --                  cf <- createColumnFamily db opts "cf"
    --                  putCF db WriteOptions {setSync = False} cf "key1" "value1"
    ----                  putCF db WriteOptions {setSync = False} cf "key2" "value2"
    ----                  putCF db WriteOptions {setSync = False} cf "key3" "value3"
    ----                  putCF db WriteOptions {setSync = False} cf "key4" "value4"
    ----                  putCF db WriteOptions {setSync = False} cf "key5" "value5"
    --                  source <- liftIO $ rangeCF db ReadOptions {setVerifyChecksums = True} cf (Just "key") Nothing
    --                  r <- lift $ runConduit $ source .| sinkList
    --                  lift $ destroyColumnFamily cf
    --                  lift $ close db
    --                  return r
    --              return $ either (const []) id e
    --        )
    --        `shouldReturn` [Just ("key1", "value1"), Just ("key2", "value2"), Just ("key3", "value3")]
    it "rangeCF [Nothing Nothing]" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              er <-
                runExceptT
                  ( do
                      let opts = DBOptions {createIfMissing = True}
                      db <- open path opts
                      cf <- createColumnFamily db opts "cf"
                      putCF db def cf "key1" "value1"
                      putCF db def cf "key2" "value2"
                      putCF db def cf "key3" "value3"
                      return (db, cf)
                  )
              either
                throwIO
                ( \(db, cf) ->
                    do
                      r <- S.toList $ rangeCF db ReadOptions {setVerifyChecksums = True} cf Nothing Nothing
                      destroyColumnFamily cf
                      close db
                      return r
                )
                er
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
    it "rangeCF [Nothing, lastKey]" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              er <-
                runExceptT
                  ( do
                      let opts = DBOptions {createIfMissing = True}
                      db <- open path opts
                      cf <- createColumnFamily db opts "cf"
                      putCF db def cf "key1" "value1"
                      putCF db def cf "key2" "value2"
                      putCF db def cf "key3" "value3"
                      return (db, cf)
                  )
              either
                throwIO
                ( \(db, cf) ->
                    do
                      r <- S.toList $ rangeCF db ReadOptions {setVerifyChecksums = True} cf Nothing (Just "key2")
                      destroyColumnFamily cf
                      close db
                      return r
                )
                er
        )
        `shouldReturn` [("key1", "value1"), ("key2", "value2")]
    it "rangeCF [firstKey, Nothing]" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              er <-
                runExceptT
                  ( do
                      let opts = DBOptions {createIfMissing = True}
                      db <- open path opts
                      cf <- createColumnFamily db opts "cf"
                      putCF db def cf "key1" "value1"
                      putCF db def cf "key2" "value2"
                      putCF db def cf "key3" "value3"
                      return (db, cf)
                  )
              either
                throwIO
                ( \(db, cf) ->
                    do
                      r <- S.toList $ rangeCF db def cf (Just "key2") Nothing
                      destroyColumnFamily cf
                      close db
                      return r
                )
                er
        )
        `shouldReturn` [("key2", "value2"), ("key3", "value3")]
    it "rangeCF [firstKey, lastKey]" $
      withSystemTempDirectory
        "rocksdb"
        ( \path ->
            do
              er <-
                runExceptT
                  ( do
                      let opts = DBOptions {createIfMissing = True}
                      db <- open path opts
                      cf <- createColumnFamily db opts "cf"
                      putCF db def cf "key1" "value1"
                      putCF db def cf "key2" "value2"
                      putCF db def cf "key3" "value3"
                      return (db, cf)
                  )
              either
                throwIO
                ( \(db, cf) ->
                    do
                      r <- S.toList $ rangeCF db def cf (Just "key2") (Just "key2")
                      destroyColumnFamily cf
                      close db
                      return r
                )
                er
        )
        `shouldReturn` [("key2", "value2")]
--    it "iter test" $
--      withSystemTempDirectory
--        "rocksdb"
--        ( \path ->
--            do
--              e <-
--                runExceptT $ do
--                  let opts = DBOptions {createIfMissing = True}
--                  db <- open path opts
--                  cf <- createColumnFamily db opts "cf"
--                  putCF db WriteOptions {setSync = True} cf "key1" "value1"
--                  putCF db WriteOptions {setSync = True} cf "key2" "value2"
--                  putCF db WriteOptions {setSync = True} cf "key3" "value3"
--                  lift $ iterTest db ReadOptions {setVerifyChecksums = False} cf (Just "key2") (Just "key3")
--                  lift $ destroyColumnFamily cf
--                  lift $ close db
--              return $ either show show e
--        )
--        `shouldReturn` ""
--    it "iterTest0" $
--      withSystemTempDirectory
--        "rocksdb"
--        ( \path ->
--            do
--              e <-
--                runExceptT $ do
--                  let opts = DBOptions {createIfMissing = True}
--                  db <- open path opts
--                  cf <- createColumnFamily db opts "cf"
--                  putCF db WriteOptions {setSync = False} cf "key1" "value1"
--                  putCF db WriteOptions {setSync = False} cf "key2" "value2"
--                  putCF db WriteOptions {setSync = False} cf "key3" "value3"
--                  r <- liftIO $ iterTest0 db ReadOptions {setVerifyChecksums = False} cf Nothing Nothing
--                  lift $ destroyColumnFamily cf
--                  lift $ close db
--                  return r
--              return $ either (const []) id e
--        )
--        `shouldReturn` [Just ("key1", "value1"), Just ("key2", "value2"), Just ("key3", "value3")]
--    it "iterTest2" $
--      withSystemTempDirectory
--        "rocksdb"
--        ( \path ->
--            do
--              e <-
--                runExceptT $ do
--                  let opts = DBOptions {createIfMissing = True}
--                  db <- open path opts
--                  cf <- createColumnFamily db opts "cf"
--                  putCF db WriteOptions {setSync = False} cf "key1" "value1"
--                  putCF db WriteOptions {setSync = False} cf "key2" "value2"
--                  putCF db WriteOptions {setSync = False} cf "key3" "value3"
--                  source <- liftIO $ iterTest2 db ReadOptions {setVerifyChecksums = True} cf Nothing Nothing
--                  r <- lift $ runConduit $ source .| sinkList
--                  lift $ destroyColumnFamily cf
--                  lift $ close db
--                  return r
--              return $ either (const []) id e
--        )
--        `shouldReturn` [Just ("key1", "value1"), Just ("key2", "value2"), Just ("key3", "value3")]
--    it "iterTest3" $
--      withSystemTempDirectory
--        "rocksdb"
--        ( \path ->
--            do
--              er <- runExceptT
--                (
--                  do
--                    let opts = DBOptions {createIfMissing = True}
--                    db <- open path opts
--                    cf <- createColumnFamily db opts "cf"
--                    putCF db WriteOptions {setSync = False} cf "key1" "value1"
--                    putCF db WriteOptions {setSync = False} cf "key2" "value2"
--                    putCF db WriteOptions {setSync = False} cf "key3" "value3"
--                    return (db, cf)
--                )
--              either
--                throwIO
--                (
--                  \(db, cf) ->
--                    do
--                      r <- S.toList $ iterTest3 db ReadOptions {setVerifyChecksums = True} cf Nothing Nothing
--                      destroyColumnFamily cf
--                      close db
--                      return r
--                )
--                er
--        )
--        `shouldReturn` [Just ("key1", "value1"), Just ("key2", "value2"), Just ("key3", "value3")]
--    it "iterTest4" $
--      withSystemTempDirectory
--        "rocksdb"
--        ( \path ->
--            do
--              er <- runExceptT
--                (
--                  do
--                    let opts = DBOptions {createIfMissing = True}
--                    db <- open path opts
--                    cf <- createColumnFamily db opts "cf"
--                    putCF db WriteOptions {setSync = False} cf "key1" "value1"
--                    putCF db WriteOptions {setSync = False} cf "key2" "value2"
--                    putCF db WriteOptions {setSync = False} cf "key3" "value3"
--                    return (db, cf)
--                )
--              either
--                throwIO
--                (
--                  \(db, cf) ->
--                    do
--                      r <- S.toList $ iterTest4 db ReadOptions {setVerifyChecksums = True} cf Nothing Nothing
--                      destroyColumnFamily cf
--                      close db
--                      return r
--                )
--                er
--        )
--        `shouldReturn` [("key1", "value1"), ("key2", "value2"), ("key3", "value3")]
--    it "iterTest4-1" $
--      withSystemTempDirectory
--        "rocksdb"
--        ( \path ->
--            do
--              er <- runExceptT
--                (
--                  do
--                    let opts = DBOptions {createIfMissing = True}
--                    db <- open path opts
--                    cf <- createColumnFamily db opts "cf"
--                    putCF db WriteOptions {setSync = False} cf "key1" "value1"
--                    putCF db WriteOptions {setSync = False} cf "key2" "value2"
--                    putCF db WriteOptions {setSync = False} cf "key3" "value3"
--                    return (db, cf)
--                )
--              either
--                throwIO
--                (
--                  \(db, cf) ->
--                    do
--                      r <- S.toList $ iterTest4 db ReadOptions {setVerifyChecksums = True} cf (Just "key2") (Just "key2")
--                      destroyColumnFamily cf
--                      close db
--                      return r
--                )
--                er
--        )
--        `shouldReturn` [("key2", "value2")]
