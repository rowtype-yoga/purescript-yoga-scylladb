-- expect: Lacks "limit"
module CompileFail.WhereAfterLimit where
import Prelude

import Yoga.ScyllaDB.Schema as S
import Yoga.ScyllaDB.ScyllaDB as Scylla
import Type.Function (type (#))
import Type.Proxy (Proxy(..))

type UsersTable = S.Table "users"
  ( id :: Scylla.UUID # S.PartitionKey
  , name :: String
  )

bad = S.from (Proxy :: Proxy UsersTable) # S.selectAll # S.limit @"10" # S.where_ @"id = $id"
