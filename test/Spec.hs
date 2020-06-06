{-# LANGUAGE BinaryLiterals #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import Control.Exception (throwIO)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (mapExceptT, runExceptT)
import Control.Monad.Trans.Resource (MonadUnliftIO, ResourceT, allocate, release, runResourceT)
import Data.ByteString (ByteString)
import Data.Either.Combinators (isRight)
import Database.RocksDB.Base
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
                  putCF db WriteOptions {setSync = False} cf "key" "value"
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
                  putCF db WriteOptions {setSync = False} cf "key" "value"
                  value <- getCF db ReadOptions {setVerifyChecksums = False} cf "key"
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
                  value <- getCF db ReadOptions {setVerifyChecksums = False} cf "key"
                  lift $ destroyColumnFamily cf
                  lift $ close db
                  case value of
                    Nothing -> return ""
                    Just v -> return v
              return $ either (const "error") id e
        )
        `shouldReturn` ""
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
                  iter <- liftIO $ createIteratorCF db ReadOptions {setVerifyChecksums = False} cf
                  lift $ destroyIterator iter
                  lift $ destroyColumnFamily cf
                  lift $ close db
              return $ either show (const "success") e
        )
        `shouldReturn` "success"
