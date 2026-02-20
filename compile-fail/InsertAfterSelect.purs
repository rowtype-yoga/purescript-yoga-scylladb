-- expect: Lacks @Type "select"
module CompileFail.InsertAfterSelect where

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

bad = S.from (Proxy :: Proxy UsersTable) # S.selectAll # S.insert { id: (unsafeCoerce "" :: Scylla.UUID), name: "x" }
