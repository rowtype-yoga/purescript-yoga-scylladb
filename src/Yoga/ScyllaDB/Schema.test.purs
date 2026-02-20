module Yoga.ScyllaDB.Schema.Test where

import Prelude

import Foreign (unsafeToForeign)
import Unsafe.Coerce (unsafeCoerce)
import Type.Function (type (#))
import Type.Proxy (Proxy(..))
import Data.DateTime (DateTime)
import Data.Maybe (Maybe)
import Test.Spec (Spec, describe, it)
import Test.Spec.Assertions (shouldEqual)
import Yoga.ScyllaDB.ScyllaDB as Scylla
import Yoga.ScyllaDB.Schema as S

type UsersTable = S.Table "users"
  ( id :: Scylla.UUID # S.PartitionKey
  , name :: String
  , email :: String
  )

type PostsTable = S.Table "posts"
  ( user_id :: Scylla.UUID # S.PartitionKey
  , post_id :: Scylla.UUID # S.ClusteringKey
  , title :: String
  , body :: String
  )

type EventsTable = S.Table "events"
  ( sensor_id :: String # S.PartitionKey
  , event_time :: DateTime # S.ClusteringKey
  , data :: String # S.Static
  , value :: Number
  )

spec :: Spec Unit
spec = do
  describe "ScyllaDB Schema" do

    describe "CREATE TABLE DDL" do

      it "generates DDL for simple partition key" do
        let ddl = S.createTableDDL @UsersTable
        ddl `shouldEqual`
          "CREATE TABLE IF NOT EXISTS users (email text, id uuid, name text, PRIMARY KEY ((id)))"

      it "generates DDL with partition + clustering key" do
        let ddl = S.createTableDDL @PostsTable
        ddl `shouldEqual`
          "CREATE TABLE IF NOT EXISTS posts (body text, post_id uuid, title text, user_id uuid, PRIMARY KEY ((user_id), post_id))"

      it "generates DDL with STATIC column" do
        let ddl = S.createTableDDL @EventsTable
        ddl `shouldEqual`
          "CREATE TABLE IF NOT EXISTS events (data text STATIC, event_time timestamp, sensor_id text, value double, PRIMARY KEY ((sensor_id), event_time))"

    describe "INSERT SQL" do

      it "generates INSERT for users" do
        let sql = S.insertSQLFor @UsersTable
        sql `shouldEqual`
          "INSERT INTO users (email, id, name) VALUES (?, ?, ?)"

    describe "SELECT SQL" do

      it "generates SELECT ALL" do
        let sql = S.selectAllSQLFor @UsersTable
        sql `shouldEqual` "SELECT * FROM users"

      it "generates SELECT WHERE" do
        let sql = S.selectWhereSQLFor @UsersTable @(id :: Scylla.UUID)
        sql `shouldEqual`
          "SELECT * FROM users WHERE id = ?"

    describe "UPDATE SQL" do

      it "generates UPDATE with SET and WHERE" do
        let sql = S.updateSQLFor @UsersTable @(name :: String) @(id :: Scylla.UUID)
        sql `shouldEqual`
          "UPDATE users SET name = ? WHERE id = ?"

    describe "DELETE SQL" do

      it "generates DELETE WHERE" do
        let sql = S.deleteSQLFor @UsersTable @(id :: Scylla.UUID)
        sql `shouldEqual`
          "DELETE FROM users WHERE id = ?"

    describe "CQL type names" do

      it "maps Int to int" do
        S.cqlTypeName (Proxy :: Proxy Int) `shouldEqual` "int"

      it "maps String to text" do
        S.cqlTypeName (Proxy :: Proxy String) `shouldEqual` "text"

      it "maps Boolean to boolean" do
        S.cqlTypeName (Proxy :: Proxy Boolean) `shouldEqual` "boolean"

      it "maps Number to double" do
        S.cqlTypeName (Proxy :: Proxy Number) `shouldEqual` "double"

      it "maps DateTime to timestamp" do
        S.cqlTypeName (Proxy :: Proxy DateTime) `shouldEqual` "timestamp"

      it "maps UUID to uuid" do
        S.cqlTypeName (Proxy :: Proxy Scylla.UUID) `shouldEqual` "uuid"

      it "maps Array String to list<text>" do
        S.cqlTypeName (Proxy :: Proxy (Array String)) `shouldEqual` "list<text>"

      it "maps Maybe Int to int" do
        S.cqlTypeName (Proxy :: Proxy (Maybe Int)) `shouldEqual` "int"

    describe "Builder: SELECT" do

      it "generates SELECT * with builder" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.selectAll # S.toSQL
        sql `shouldEqual` "SELECT * FROM users"

      it "generates SELECT * WHERE with builder" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.selectAll # S.where_ @"id = $id" # S.toSQL
        sql `shouldEqual` "SELECT * FROM users WHERE id = $id"

      it "generates SELECT with ORDER BY, LIMIT" do
        let sql = S.from (Proxy :: Proxy PostsTable) # S.selectAll # S.where_ @"user_id = $user_id" # S.orderBy @"post_id DESC" # S.limit @"10" # S.toSQL
        sql `shouldEqual` "SELECT * FROM posts WHERE user_id = $user_id ORDER BY post_id DESC LIMIT 10"

      it "generates SELECT with ALLOW FILTERING" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.selectAll # S.where_ @"name = $name" # S.allowFiltering # S.toSQL
        sql `shouldEqual` "SELECT * FROM users WHERE name = $name ALLOW FILTERING"

      it "generates SELECT specific columns" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.select @"name, email" # S.toSQL
        sql `shouldEqual` "SELECT name, email FROM users"

    describe "Builder: INSERT" do

      it "generates INSERT with builder" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.insert { id: unsafeUUID, name: "Alice", email: "a@b.com" } # S.toSQL
        sql `shouldEqual` "INSERT INTO users (email, id, name) VALUES (?, ?, ?)"

      it "generates INSERT IF NOT EXISTS" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.insert { id: unsafeUUID, name: "Alice", email: "a@b.com" } # S.ifNotExists # S.toSQL
        sql `shouldEqual` "INSERT INTO users (email, id, name) VALUES (?, ?, ?) IF NOT EXISTS"

      it "generates INSERT IF NOT EXISTS USING TTL" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.insert { id: unsafeUUID, name: "Alice", email: "a@b.com" } # S.ifNotExists # S.usingTTL @"3600" # S.toSQL
        sql `shouldEqual` "INSERT INTO users (email, id, name) VALUES (?, ?, ?) IF NOT EXISTS USING TTL 3600"

    describe "Builder: UPDATE" do

      it "generates UPDATE with builder" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.set { name: "Bob" } # S.where_ @"id = $id" # S.toSQL
        sql `shouldEqual` "UPDATE users SET name = ? WHERE id = $id"

      it "generates UPDATE with TTL" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.withTTL @"86400" # S.set { name: "Bob" } # S.where_ @"id = $id" # S.toSQL
        sql `shouldEqual` "UPDATE users USING TTL 86400 SET name = ? WHERE id = $id"

    describe "Builder: DELETE" do

      it "generates DELETE with builder" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.delete # S.where_ @"id = $id" # S.toSQL
        sql `shouldEqual` "DELETE FROM users WHERE id = $id"

      it "generates DELETE IF EXISTS" do
        let sql = S.from (Proxy :: Proxy UsersTable) # S.delete # S.where_ @"id = $id" # S.ifExists # S.toSQL
        sql `shouldEqual` "DELETE FROM users WHERE id = $id IF EXISTS"

    describe "replaceNamedParams" do

      it "replaces $name with ? and extracts values" do
        let entries = [ { name: "id", value: unsafeToForeign "test-uuid" } ]
        let result = S.replaceNamedParams entries "SELECT * FROM users WHERE id = $id"
        result.sql `shouldEqual` "SELECT * FROM users WHERE id = ?"

      it "replaces multiple params" do
        let entries = [ { name: "id", value: unsafeToForeign "test-uuid" }, { name: "name", value: unsafeToForeign "Alice" } ]
        let result = S.replaceNamedParams entries "SELECT * FROM users WHERE id = $id AND name = $name"
        result.sql `shouldEqual` "SELECT * FROM users WHERE id = ? AND name = ?"

unsafeUUID :: Scylla.UUID
unsafeUUID = unsafeCoerce "00000000-0000-0000-0000-000000000000"
