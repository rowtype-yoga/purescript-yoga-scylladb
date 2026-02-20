-- expect: WHERE requires a preceding SELECT, UPDATE (set), or DELETE
module CompileFail.WhereAfterInsert where

import Prelude
import Yoga.ScyllaDB.Schema as S
import Yoga.ScyllaDB.ScyllaDB as Scylla
import Type.Function (type (#))
import Type.Proxy (Proxy(..))
import Unsafe.Coerce (unsafeCoerce)

type UsersTable = S.Table "users"
  ( id :: Scylla.UUID # S.PartitionKey
  , name :: String
  )

bad = S.from (Proxy :: Proxy UsersTable) # S.insert { id: (unsafeCoerce "" :: Scylla.UUID), name: "x" } # S.where_ @"id = $id"
