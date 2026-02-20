module Yoga.ScyllaDB.Schema where

import Prelude

import Data.Array (intercalate)
import Data.DateTime (DateTime)
import Data.Maybe (Maybe)
import Data.Symbol (class IsSymbol, reflectSymbol)
import Type.Function (type (#))
import Type.Proxy (Proxy(..))
import Type.RowList (class ListToRow)
import Prim.Boolean (True, False)
import Prim.RowList as RL
import Prim.RowList (class RowToList)
import Yoga.ScyllaDB.ScyllaDB as Scylla

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Core phantom types
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

data Table :: Symbol -> Row Type -> Type
data Table name columns = Table

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Constraint wrappers (used with # operator from Type.Function)
--   Int # PartitionKey = PartitionKey Int
--   Int # ClusteringKey = ClusteringKey Int
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

data PartitionKey :: Type -> Type
data PartitionKey a

data ClusteringKey :: Type -> Type
data ClusteringKey a

data Static :: Type -> Type
data Static a

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Nullability: inferred from Maybe
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class IsNullable a where
  isNullable :: Proxy a -> Boolean

instance IsNullable (Maybe a) where
  isNullable _ = true
else instance IsNullable a where
  isNullable _ = false

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ExtractType: recursively unwrap constraint wrappers to get base type
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ExtractType :: Type -> Type -> Constraint
class ExtractType wrapped typ | wrapped -> typ

instance ExtractType a typ => ExtractType (PartitionKey a) typ
else instance ExtractType a typ => ExtractType (ClusteringKey a) typ
else instance ExtractType a typ => ExtractType (Static a) typ
else instance ExtractType a a

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- PureScript type -> CQL type name
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CQLTypeName a where
  cqlTypeName :: Proxy a -> String

instance CQLTypeName Int where
  cqlTypeName _ = "int"

instance CQLTypeName String where
  cqlTypeName _ = "text"

instance CQLTypeName Boolean where
  cqlTypeName _ = "boolean"

instance CQLTypeName Number where
  cqlTypeName _ = "double"

instance CQLTypeName DateTime where
  cqlTypeName _ = "timestamp"

instance CQLTypeName Scylla.UUID where
  cqlTypeName _ = "uuid"

instance CQLTypeName a => CQLTypeName (Maybe a) where
  cqlTypeName _ = cqlTypeName (Proxy :: Proxy a)

instance CQLTypeName a => CQLTypeName (Array a) where
  cqlTypeName _ = "list<" <> cqlTypeName (Proxy :: Proxy a) <> ">"

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Constraint rendering (STATIC only — PK/CK handled in PRIMARY KEY clause)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RenderConstraint a where
  renderConstraint :: Proxy a -> String

instance RenderConstraint a => RenderConstraint (PartitionKey a) where
  renderConstraint _ = renderConstraint (Proxy :: Proxy a)

else instance RenderConstraint a => RenderConstraint (ClusteringKey a) where
  renderConstraint _ = renderConstraint (Proxy :: Proxy a)

else instance RenderConstraint a => RenderConstraint (Static a) where
  renderConstraint _ = joinConstraints "STATIC" (renderConstraint (Proxy :: Proxy a))

else instance RenderConstraint a where
  renderConstraint _ = ""

joinConstraints :: String -> String -> String
joinConstraints a b = case a, b of
  "", s -> s
  s, "" -> s
  s1, s2 -> s1 <> " " <> s2

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Primary key detection
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class IsPartitionKey a where
  isPartitionKey :: Proxy a -> Boolean

instance IsPartitionKey (PartitionKey a) where
  isPartitionKey _ = true
else instance IsPartitionKey a => IsPartitionKey (Static a) where
  isPartitionKey _ = isPartitionKey (Proxy :: Proxy a)
else instance IsPartitionKey a where
  isPartitionKey _ = false

class IsClusteringKey a where
  isClusteringKey :: Proxy a -> Boolean

instance IsClusteringKey (ClusteringKey a) where
  isClusteringKey _ = true
else instance IsClusteringKey a => IsClusteringKey (Static a) where
  isClusteringKey _ = isClusteringKey (Proxy :: Proxy a)
else instance IsClusteringKey a where
  isClusteringKey _ = false

class IsKeyColumn a where
  isKeyColumn :: Proxy a -> Boolean

instance IsKeyColumn (PartitionKey a) where
  isKeyColumn _ = true
else instance IsKeyColumn (ClusteringKey a) where
  isKeyColumn _ = true
else instance IsKeyColumn a where
  isKeyColumn _ = false

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DDL generation: column definitions
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RenderColumnsRL :: RL.RowList Type -> Constraint
class RenderColumnsRL rl where
  renderColumnsRL :: Proxy rl -> Array String

instance RenderColumnsRL RL.Nil where
  renderColumnsRL _ = []

instance
  ( IsSymbol name
  , ExtractType entry typ
  , CQLTypeName typ
  , RenderConstraint entry
  , RenderColumnsRL tail
  ) =>
  RenderColumnsRL (RL.Cons name entry tail) where
  renderColumnsRL _ = do
    let colName = reflectSymbol (Proxy :: Proxy name)
    let colType = cqlTypeName (Proxy :: Proxy typ)
    let constraints = renderConstraint (Proxy :: Proxy entry)
    let constraintsSuffix = if constraints == "" then "" else " " <> constraints
    [ colName <> " " <> colType <> constraintsSuffix ] <> renderColumnsRL (Proxy :: Proxy tail)

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DDL generation: PRIMARY KEY clause
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class PartitionKeysRL :: RL.RowList Type -> Constraint
class PartitionKeysRL rl where
  partitionKeysRL :: Proxy rl -> Array String

instance PartitionKeysRL RL.Nil where
  partitionKeysRL _ = []

instance
  ( IsSymbol name
  , IsPartitionKey entry
  , PartitionKeysRL tail
  ) =>
  PartitionKeysRL (RL.Cons name entry tail) where
  partitionKeysRL _ =
    let rest = partitionKeysRL (Proxy :: Proxy tail)
    in
      if isPartitionKey (Proxy :: Proxy entry) then [ reflectSymbol (Proxy :: Proxy name) ] <> rest
      else rest

class ClusteringKeysRL :: RL.RowList Type -> Constraint
class ClusteringKeysRL rl where
  clusteringKeysRL :: Proxy rl -> Array String

instance ClusteringKeysRL RL.Nil where
  clusteringKeysRL _ = []

instance
  ( IsSymbol name
  , IsClusteringKey entry
  , ClusteringKeysRL tail
  ) =>
  ClusteringKeysRL (RL.Cons name entry tail) where
  clusteringKeysRL _ =
    let rest = clusteringKeysRL (Proxy :: Proxy tail)
    in
      if isClusteringKey (Proxy :: Proxy entry) then [ reflectSymbol (Proxy :: Proxy name) ] <> rest
      else rest

renderPrimaryKey :: Array String -> Array String -> String
renderPrimaryKey partitionKeys clusteringKeys = case partitionKeys, clusteringKeys of
  [], [] -> ""
  pks, [] -> "PRIMARY KEY ((" <> intercalate ", " pks <> "))"
  pks, cks -> "PRIMARY KEY ((" <> intercalate ", " pks <> "), " <> intercalate ", " cks <> ")"

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CREATE TABLE
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CreateTableDDL a where
  createTableDDL :: String

instance
  ( IsSymbol name
  , RowToList cols rl
  , RenderColumnsRL rl
  , PartitionKeysRL rl
  , ClusteringKeysRL rl
  ) =>
  CreateTableDDL (Table name cols) where
  createTableDDL = do
    let tableName = reflectSymbol (Proxy :: Proxy name)
    let columns = renderColumnsRL (Proxy :: Proxy rl)
    let pks = partitionKeysRL (Proxy :: Proxy rl)
    let cks = clusteringKeysRL (Proxy :: Proxy rl)
    let pkClause = renderPrimaryKey pks cks
    let allParts = columns <> (if pkClause == "" then [] else [pkClause])
    "CREATE TABLE IF NOT EXISTS " <> tableName <> " (" <> intercalate ", " allParts <> ")"

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- INSERT SQL generation
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class InsertColumnsRL :: RL.RowList Type -> Constraint
class InsertColumnsRL rl where
  insertColumnsRL :: Proxy rl -> Array String

instance InsertColumnsRL RL.Nil where
  insertColumnsRL _ = []

instance
  ( IsSymbol name
  , InsertColumnsRL tail
  ) =>
  InsertColumnsRL (RL.Cons name entry tail) where
  insertColumnsRL _ =
    [ reflectSymbol (Proxy :: Proxy name) ] <> insertColumnsRL (Proxy :: Proxy tail)

class InsertSQLFor a where
  insertSQLFor :: String

instance
  ( IsSymbol name
  , RowToList cols rl
  , InsertColumnsRL rl
  ) =>
  InsertSQLFor (Table name cols) where
  insertSQLFor = do
    let tableName = reflectSymbol (Proxy :: Proxy name)
    let cols = insertColumnsRL (Proxy :: Proxy rl)
    let placeholders = map (const "?") cols
    "INSERT INTO " <> tableName
      <> " (" <> intercalate ", " cols <> ")"
      <> " VALUES (" <> intercalate ", " placeholders <> ")"

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- SELECT SQL generation
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SelectAllSQLFor a where
  selectAllSQLFor :: String

instance
  ( IsSymbol name
  ) =>
  SelectAllSQLFor (Table name cols) where
  selectAllSQLFor = "SELECT * FROM " <> reflectSymbol (Proxy :: Proxy name)

class WhereClauseRL :: RL.RowList Type -> Constraint
class WhereClauseRL rl where
  whereClauseRL :: Proxy rl -> Array String

instance WhereClauseRL RL.Nil where
  whereClauseRL _ = []

instance (IsSymbol name, WhereClauseRL tail) => WhereClauseRL (RL.Cons name typ tail) where
  whereClauseRL _ =
    [ reflectSymbol (Proxy :: Proxy name) <> " = ?" ]
      <> whereClauseRL (Proxy :: Proxy tail)

class SelectWhereSQLFor a whereRow where
  selectWhereSQLFor :: String

instance
  ( IsSymbol name
  , RowToList whereRow whereRL
  , WhereClauseRL whereRL
  ) =>
  SelectWhereSQLFor (Table name cols) whereRow where
  selectWhereSQLFor = do
    let tableName = reflectSymbol (Proxy :: Proxy name)
    let conditions = whereClauseRL (Proxy :: Proxy whereRL)
    "SELECT * FROM " <> tableName <> " WHERE " <> intercalate " AND " conditions

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- UPDATE SQL generation
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ColumnCountRL :: RL.RowList Type -> Constraint
class ColumnCountRL rl where
  columnCountRL :: Proxy rl -> Int

instance ColumnCountRL RL.Nil where
  columnCountRL _ = 0

instance ColumnCountRL tail => ColumnCountRL (RL.Cons name typ tail) where
  columnCountRL _ = 1 + columnCountRL (Proxy :: Proxy tail)

class SetClauseRL :: RL.RowList Type -> Constraint
class SetClauseRL rl where
  setClauseRL :: Proxy rl -> Array String

instance SetClauseRL RL.Nil where
  setClauseRL _ = []

instance (IsSymbol name, SetClauseRL tail) => SetClauseRL (RL.Cons name typ tail) where
  setClauseRL _ =
    [ reflectSymbol (Proxy :: Proxy name) <> " = ?" ]
      <> setClauseRL (Proxy :: Proxy tail)

class UpdateSQLFor table setRow whereRow where
  updateSQLFor :: String

instance
  ( IsSymbol name
  , RowToList setRow setRL
  , RowToList whereRow whereRL
  , SetClauseRL setRL
  , WhereClauseRL whereRL
  ) =>
  UpdateSQLFor (Table name cols) setRow whereRow where
  updateSQLFor = do
    let tableName = reflectSymbol (Proxy :: Proxy name)
    let setClauses = setClauseRL (Proxy :: Proxy setRL)
    let whereClauses = whereClauseRL (Proxy :: Proxy whereRL)
    "UPDATE " <> tableName
      <> " SET " <> intercalate ", " setClauses
      <> " WHERE " <> intercalate " AND " whereClauses

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- DELETE SQL generation
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class DeleteSQLFor table whereRow where
  deleteSQLFor :: String

instance
  ( IsSymbol name
  , RowToList whereRow whereRL
  , WhereClauseRL whereRL
  ) =>
  DeleteSQLFor (Table name cols) whereRow where
  deleteSQLFor = do
    let tableName = reflectSymbol (Proxy :: Proxy name)
    let conditions = whereClauseRL (Proxy :: Proxy whereRL)
    "DELETE FROM " <> tableName <> " WHERE " <> intercalate " AND " conditions

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- StripColumns: (name :: String # PartitionKey, ...) -> (name :: String, ...)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class StripColumnsRL :: RL.RowList Type -> RL.RowList Type -> Constraint
class StripColumnsRL rl out | rl -> out

instance StripColumnsRL RL.Nil RL.Nil
instance (ExtractType entry typ, StripColumnsRL tail out') => StripColumnsRL (RL.Cons name entry tail) (RL.Cons name typ out')

class StripColumns :: Row Type -> Row Type -> Constraint
class StripColumns cols result | cols -> result

instance (RowToList cols rl, StripColumnsRL rl outRL, ListToRow outRL result) => StripColumns cols result

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- InsertNonKeyColumnsRL: columns that are NOT part of the primary key
-- (useful for building insert params excluding auto-generated key columns)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class NonKeyColumnsRL :: RL.RowList Type -> Constraint
class NonKeyColumnsRL rl where
  nonKeyColumnsRL :: Proxy rl -> Array String

instance NonKeyColumnsRL RL.Nil where
  nonKeyColumnsRL _ = []

instance
  ( IsSymbol name
  , IsKeyColumn entry
  , NonKeyColumnsRL tail
  ) =>
  NonKeyColumnsRL (RL.Cons name entry tail) where
  nonKeyColumnsRL _ =
    let rest = nonKeyColumnsRL (Proxy :: Proxy tail)
    in
      if isKeyColumn (Proxy :: Proxy entry) then rest
      else [ reflectSymbol (Proxy :: Proxy name) ] <> rest
