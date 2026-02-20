module Yoga.ScyllaDB.Schema where

import Prelude

import Control.Monad.Except (runExcept)
import Data.Array (intercalate)
import Data.DateTime (DateTime)
import Data.Either (Either(..))
import Data.Foldable (foldl)
import Data.Map as Map
import Data.Maybe (Maybe(..))
import Data.Nullable (toNullable)
import Data.String.Regex as Regex
import Data.String.Regex.Flags as RegexFlags
import Data.Symbol (class IsSymbol, reflectSymbol)
import Data.Traversable (traverse)
import Effect.Aff (Aff, throwError)
import Effect.Exception as Exception
import Foreign (Foreign, unsafeToForeign)
import Prim.Row as Row
import Prim.RowList as RL
import Prim.RowList (class RowToList)
import Prim.Symbol as Symbol
import Prim.TypeError (class Fail, Text)
import Record as Record
import Type.Proxy (Proxy(..))
import Type.RowList (class ListToRow)
import Unsafe.Coerce (unsafeCoerce)
import Yoga.JSON (class ReadForeign, readImpl)
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

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Q type: Builder phantom type for CQL queries
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

newtype Q :: Row (Row Type) -> Row Type -> Row Type -> Row Type -> Type
newtype Q tables result params stage = Q { sql :: String, values :: Array Foreign }

toSQL :: forall tables result params stage. Q tables result params stage -> String
toSQL (Q q) = q.sql

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- SingleTable: extract name and cols from a one-entry tables row
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SingleTable :: Row (Row Type) -> Symbol -> Row Type -> Constraint
class SingleTable tables name cols | tables -> name cols

instance
  ( RowToList tables (RL.Cons name cols RL.Nil)
  , IsSymbol name
  ) =>
  SingleTable tables name cols

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- HasClause / HasAnyDML: stage constraints
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class HasClause :: Symbol -> Row Type -> Constraint
class HasClause label row

instance Row.Cons label Unit rest row => HasClause label row

class HasAnyDML :: Row Type -> Constraint
class HasAnyDML stage

instance (RL.RowToList stage rl, HasAnyDMLRL rl) => HasAnyDML stage

class HasAnyDMLRL :: RL.RowList Type -> Constraint
class HasAnyDMLRL rl

instance HasAnyDMLRL (RL.Cons "select" Unit rest)
else instance HasAnyDMLRL (RL.Cons "set" Unit rest)
else instance HasAnyDMLRL (RL.Cons "delete" Unit rest)
else instance HasAnyDMLRL rest => HasAnyDMLRL (RL.Cons label typ rest)
else instance Fail (Text "WHERE requires a preceding SELECT, UPDATE (set), or DELETE") => HasAnyDMLRL RL.Nil

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- FieldToCQLValue: convert a PureScript value to Foreign for CQL
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class FieldToCQLValue :: Type -> Constraint
class FieldToCQLValue a where
  fieldToCQLValue :: a -> Foreign

instance FieldToCQLValue a => FieldToCQLValue (Maybe a) where
  fieldToCQLValue = toNullable >>> unsafeCoerce
else instance FieldToCQLValue DateTime where
  fieldToCQLValue = unsafeCoerce
else instance FieldToCQLValue Scylla.UUID where
  fieldToCQLValue = unsafeCoerce
else instance FieldToCQLValue a where
  fieldToCQLValue = unsafeCoerce

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- RecordValuesRL: walk a RowList and extract values from a record
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class RecordValuesRL :: RL.RowList Type -> Row Type -> Constraint
class RecordValuesRL rl row where
  recordValuesRL :: Proxy rl -> { | row } -> Array Foreign

instance RecordValuesRL RL.Nil row where
  recordValuesRL _ _ = []

instance
  ( IsSymbol name
  , Row.Cons name typ rest row
  , FieldToCQLValue typ
  , RecordValuesRL tail row
  ) =>
  RecordValuesRL (RL.Cons name typ tail) row where
  recordValuesRL _ rec =
    [ fieldToCQLValue (Record.get (Proxy :: Proxy name) rec) ]
      <> recordValuesRL (Proxy :: Proxy tail) rec

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ColumnNamesRL: extract column names from a RowList
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ColumnNamesRL :: RL.RowList Type -> Constraint
class ColumnNamesRL rl where
  columnNamesRL :: Proxy rl -> Array String

instance ColumnNamesRL RL.Nil where
  columnNamesRL _ = []

instance (IsSymbol name, ColumnNamesRL tail) => ColumnNamesRL (RL.Cons name typ tail) where
  columnNamesRL _ =
    [ reflectSymbol (Proxy :: Proxy name) ] <> columnNamesRL (Proxy :: Proxy tail)

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ValidateSetColumnsRL: ensure SET columns exist in the table
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ValidateSetColumnsRL :: RL.RowList Type -> Row Type -> Constraint
class ValidateSetColumnsRL rl cols

instance ValidateSetColumnsRL RL.Nil cols

instance
  ( IsSymbol name
  , Row.Cons name colType rest cols
  , ValidateSetColumnsRL tail cols
  ) =>
  ValidateSetColumnsRL (RL.Cons name typ tail) cols

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: from
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

from
  :: forall name cols tables
   . IsSymbol name
  => Row.Cons name cols () tables
  => Proxy (Table name cols)
  -> Q tables () () ()
from _ = Q { sql: reflectSymbol (Proxy :: Proxy name), values: [] }

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: selectAll
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

selectAll
  :: forall tables name cols result r p stage stage'
   . SingleTable tables name cols
  => StripColumns cols result
  => Row.Lacks "select" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "set" stage
  => Row.Lacks "delete" stage
  => Row.Lacks "where" stage
  => Row.Lacks "orderBy" stage
  => Row.Lacks "limit" stage
  => Row.Cons "select" Unit stage stage'
  => Q tables r p stage
  -> Q tables result p stage'
selectAll (Q q) = Q (q { sql = "SELECT * FROM " <> q.sql })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: delete
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

delete
  :: forall tables name cols r p stage stage'
   . SingleTable tables name cols
  => IsSymbol name
  => Row.Lacks "select" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "set" stage
  => Row.Lacks "delete" stage
  => Row.Cons "delete" Unit stage stage'
  => Q tables r p stage
  -> Q tables () () stage'
delete (Q _) = Q { sql: "DELETE FROM " <> reflectSymbol (Proxy :: Proxy name), values: [] }

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder SetClauseRL (for builder pattern — uses bare ?)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class BuilderSetClauseRL :: RL.RowList Type -> Constraint
class BuilderSetClauseRL rl where
  builderSetClauseRL :: Proxy rl -> Array String

instance BuilderSetClauseRL RL.Nil where
  builderSetClauseRL _ = []

instance (IsSymbol name, BuilderSetClauseRL tail) => BuilderSetClauseRL (RL.Cons name typ tail) where
  builderSetClauseRL _ =
    [ reflectSymbol (Proxy :: Proxy name) <> " = ?" ]
      <> builderSetClauseRL (Proxy :: Proxy tail)

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: insert
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

insert
  :: forall tables name cols userRow userRowRL stage stage'
   . SingleTable tables name cols
  => RowToList userRow userRowRL
  => ColumnNamesRL userRowRL
  => RecordValuesRL userRowRL userRow
  => IsSymbol name
  => Row.Lacks "select" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "set" stage
  => Row.Lacks "delete" stage
  => Row.Cons "insert" Unit stage stage'
  => { | userRow }
  -> Q tables () () stage
  -> Q tables () () stage'
insert rec (Q _) = Q { sql, values }
  where
  tableName = reflectSymbol (Proxy :: Proxy name)
  colNames = columnNamesRL (Proxy :: Proxy userRowRL)
  placeholders = map (const "?") colNames
  sql = "INSERT INTO " <> tableName
    <> " (" <> intercalate ", " colNames <> ")"
    <> " VALUES (" <> intercalate ", " placeholders <> ")"
  values = recordValuesRL (Proxy :: Proxy userRowRL) rec

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: set (UPDATE)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

set
  :: forall tables name cols setRow setRL stage stage'
   . SingleTable tables name cols
  => RowToList setRow setRL
  => IsSymbol name
  => ValidateSetColumnsRL setRL cols
  => BuilderSetClauseRL setRL
  => RecordValuesRL setRL setRow
  => Row.Lacks "select" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "set" stage
  => Row.Lacks "delete" stage
  => Row.Cons "set" Unit stage stage'
  => { | setRow }
  -> Q tables () () stage
  -> Q tables () () stage'
set rec (Q q) = Q { sql, values }
  where
  setClauses = builderSetClauseRL (Proxy :: Proxy setRL)
  sql = "UPDATE " <> q.sql <> " SET " <> intercalate ", " setClauses
  values = recordValuesRL (Proxy :: Proxy setRL) rec

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ParseWhere: extract $paramName tokens from WHERE clause
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ParseWhere :: Symbol -> Row (Row Type) -> Row Type -> Constraint
class ParseWhere sym tables params | sym tables -> params

instance
  ( Symbol.Cons h t sym
  , ParseWhereGo h t tables RL.Nil outRL
  , ListToRow outRL params
  ) =>
  ParseWhere sym tables params

class ParseWhereGo :: Symbol -> Symbol -> Row (Row Type) -> RL.RowList Type -> RL.RowList Type -> Constraint
class ParseWhereGo head tail tables paramsIn paramsOut | head tail tables paramsIn -> paramsOut

-- When we see $, collect the param name and resolve its type
instance
  ( CollectParamName rest "" paramName restAfter
  , SingleTable tables tableName cols
  , RowToList cols colsRL
  , ResolveParamType paramName colsRL typ
  , EndOrContinue restAfter tables (RL.Cons paramName typ paramsIn) paramsOut
  ) =>
  ParseWhereGo "$" rest tables paramsIn paramsOut

-- End of string: done
else instance ParseWhereGo h "" tables paramsIn paramsIn

-- Default: skip regular characters and continue
else instance
  ( Symbol.Cons h2 t2 rest
  , ParseWhereGo h2 t2 tables paramsIn paramsOut
  ) =>
  ParseWhereGo h rest tables paramsIn paramsOut

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CollectParamName: accumulate identifier chars after $
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CollectParamName :: Symbol -> Symbol -> Symbol -> Symbol -> Constraint
class CollectParamName input acc name rest | input acc -> name rest

instance CollectParamName "" acc acc ""
else instance
  ( Symbol.Cons h t input
  , CollectParamNameChar h t acc name rest
  ) =>
  CollectParamName input acc name rest

class CollectParamNameChar :: Symbol -> Symbol -> Symbol -> Symbol -> Symbol -> Constraint
class CollectParamNameChar char tail acc name rest | char tail acc -> name rest

instance
  ( Symbol.Cons " " tail rest
  ) =>
  CollectParamNameChar " " tail acc acc rest

else instance
  ( Symbol.Cons ")" tail rest
  ) =>
  CollectParamNameChar ")" tail acc acc rest

else instance
  ( Symbol.Cons "," tail rest
  ) =>
  CollectParamNameChar "," tail acc acc rest

else instance
  ( Symbol.Append acc char acc'
  , CollectParamName tail acc' name rest
  ) =>
  CollectParamNameChar char tail acc name rest

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- EndOrContinue: handle end-of-string or continue parsing
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class EndOrContinue :: Symbol -> Row (Row Type) -> RL.RowList Type -> RL.RowList Type -> Constraint
class EndOrContinue rest tables paramsIn paramsOut | rest tables paramsIn -> paramsOut

instance EndOrContinue "" tables paramsIn paramsIn
else instance
  ( Symbol.Cons h t rest
  , ParseWhereGo h t tables paramsIn paramsOut
  ) =>
  EndOrContinue rest tables paramsIn paramsOut

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ResolveParamType: look up a param name in table columns
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ResolveParamType :: Symbol -> RL.RowList Type -> Type -> Constraint
class ResolveParamType name rl typ | name rl -> typ

instance (ExtractType entry typ) => ResolveParamType name (RL.Cons name entry tail) typ
else instance ResolveParamType name tail typ => ResolveParamType name (RL.Cons other entry tail) typ

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: where_
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

where_
  :: forall @whr tables result params p stage stage'
   . IsSymbol whr
  => ParseWhere whr tables params
  => HasAnyDML stage
  => Row.Lacks "where" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "orderBy" stage
  => Row.Lacks "limit" stage
  => Row.Cons "where" Unit stage stage'
  => Q tables result p stage
  -> Q tables result params stage'
where_ (Q q) = Q (q { sql = q.sql <> " WHERE " <> reflectSymbol (Proxy :: Proxy whr) })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: orderBy
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

orderBy
  :: forall @cols tables result params stage stage'
   . IsSymbol cols
  => HasClause "select" stage
  => Row.Lacks "orderBy" stage
  => Row.Lacks "limit" stage
  => Row.Cons "orderBy" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params stage'
orderBy (Q q) = Q (q { sql = q.sql <> " ORDER BY " <> reflectSymbol (Proxy :: Proxy cols) })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ParseLimitOffset: accept numeric literal or $paramName
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ParseLimitOffset :: Symbol -> Row Type -> Row Type -> Constraint
class ParseLimitOffset sym paramsIn paramsOut | sym paramsIn -> paramsOut

instance
  ( Symbol.Cons h t sym
  , ParseLimitOffsetChar h t paramsIn paramsOut
  ) =>
  ParseLimitOffset sym paramsIn paramsOut

class ParseLimitOffsetChar :: Symbol -> Symbol -> Row Type -> Row Type -> Constraint
class ParseLimitOffsetChar head tail paramsIn paramsOut | head tail paramsIn -> paramsOut

instance
  ( Symbol.Append "" tail paramName
  , Row.Cons paramName Int paramsIn paramsOut
  ) =>
  ParseLimitOffsetChar "$" tail paramsIn paramsOut

else instance ParseLimitOffsetChar h tail paramsIn paramsIn

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: limit
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

limit
  :: forall @sym tables result params params' stage stage'
   . ParseLimitOffset sym params params'
  => IsSymbol sym
  => HasClause "select" stage
  => Row.Lacks "limit" stage
  => Row.Cons "limit" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params' stage'
limit (Q q) = Q (q { sql = q.sql <> " LIMIT " <> reflectSymbol (Proxy :: Proxy sym) })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CQL-specific: ifNotExists (after INSERT)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ifNotExists
  :: forall tables result params stage stage'
   . HasClause "insert" stage
  => Row.Lacks "ifNotExists" stage
  => Row.Cons "ifNotExists" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params stage'
ifNotExists (Q q) = Q (q { sql = q.sql <> " IF NOT EXISTS" })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CQL-specific: ifExists (after UPDATE/DELETE WHERE)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

ifExists
  :: forall tables result params stage stage'
   . HasClause "where" stage
  => Row.Lacks "ifExists" stage
  => Row.Cons "ifExists" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params stage'
ifExists (Q q) = Q (q { sql = q.sql <> " IF EXISTS" })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CQL-specific: usingTTL (after INSERT)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

usingTTL
  :: forall @ttl tables result params stage stage'
   . IsSymbol ttl
  => HasClause "insert" stage
  => Row.Lacks "usingTTL" stage
  => Row.Cons "usingTTL" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params stage'
usingTTL (Q q) = Q (q { sql = q.sql <> " USING TTL " <> reflectSymbol (Proxy :: Proxy ttl) })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CQL-specific: withTTL (before set, produces UPDATE t USING TTL n)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

withTTL
  :: forall @ttl tables result params stage stage'
   . IsSymbol ttl
  => Row.Lacks "set" stage
  => Row.Lacks "select" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "delete" stage
  => Row.Lacks "withTTL" stage
  => Row.Cons "withTTL" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params stage'
withTTL (Q q) = Q (q { sql = q.sql <> " USING TTL " <> reflectSymbol (Proxy :: Proxy ttl) })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- CQL-specific: allowFiltering (after SELECT WHERE)
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

allowFiltering
  :: forall tables result params stage stage'
   . HasClause "select" stage
  => Row.Lacks "allowFiltering" stage
  => Row.Cons "allowFiltering" Unit stage stage'
  => Q tables result params stage
  -> Q tables result params stage'
allowFiltering (Q q) = Q (q { sql = q.sql <> " ALLOW FILTERING" })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ParseSelect: parse "col1, col2" into a result row
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ParseSelect :: Symbol -> Row (Row Type) -> Row Type -> Constraint
class ParseSelect sym tables result | sym tables -> result

instance ParseSelect "" tables ()
else instance
  ( Symbol.Cons h t sym
  , ParseSelectGo h t "" tables RL.Nil outRL
  , ListToRow outRL result
  ) =>
  ParseSelect sym tables result

class ParseSelectGo :: Symbol -> Symbol -> Symbol -> Row (Row Type) -> RL.RowList Type -> RL.RowList Type -> Constraint
class ParseSelectGo head tail acc tables resultIn resultOut | head tail acc tables resultIn -> resultOut

-- Comma with accumulated column name: flush column and continue
instance
  ( SingleTable tables tableName cols
  , RowToList cols colsRL
  , ResolveParamType acc colsRL typ
  , Symbol.Cons h2 t2 rest
  , SkipSpaces h2 t2 h3 t3
  , ParseSelectGo h3 t3 "" tables (RL.Cons acc typ resultIn) resultOut
  ) =>
  ParseSelectGo "," rest acc tables resultIn resultOut

-- Space: skip and continue accumulating
else instance
  ( Symbol.Cons h2 t2 rest
  , ParseSelectGo h2 t2 acc tables resultIn resultOut
  ) =>
  ParseSelectGo " " rest acc tables resultIn resultOut

-- End of string: flush final column
else instance
  ( Symbol.Append acc h acc'
  , SingleTable tables tableName cols
  , RowToList cols colsRL
  , ResolveParamType acc' colsRL typ
  ) =>
  ParseSelectGo h "" acc tables resultIn (RL.Cons acc' typ resultIn)

-- Regular character with more input: accumulate
else instance
  ( Symbol.Append acc h acc'
  , Symbol.Cons h2 t2 rest
  , ParseSelectGo h2 t2 acc' tables resultIn resultOut
  ) =>
  ParseSelectGo h rest acc tables resultIn resultOut

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- SkipSpaces: skip leading spaces
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class SkipSpaces :: Symbol -> Symbol -> Symbol -> Symbol -> Constraint
class SkipSpaces head tail outHead outTail | head tail -> outHead outTail

instance (Symbol.Cons h2 t2 rest, SkipSpaces h2 t2 outHead outTail) => SkipSpaces " " rest outHead outTail
else instance SkipSpaces h t h t

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Builder: select @"col1, col2"
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

select
  :: forall @sel tables result r p stage stage'
   . IsSymbol sel
  => ParseSelect sel tables result
  => Row.Lacks "select" stage
  => Row.Lacks "insert" stage
  => Row.Lacks "set" stage
  => Row.Lacks "delete" stage
  => Row.Lacks "where" stage
  => Row.Lacks "orderBy" stage
  => Row.Lacks "limit" stage
  => Row.Cons "select" Unit stage stage'
  => Q tables r p stage
  -> Q tables result p stage'
select (Q q) = Q (q { sql = "SELECT " <> reflectSymbol (Proxy :: Proxy sel) <> " FROM " <> q.sql })

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- ParamsToArray: convert params record to Array { name, value }
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class ParamsToArray :: RL.RowList Type -> Row Type -> Constraint
class ParamsToArray rl row where
  paramsToArray :: Proxy rl -> { | row } -> Array { name :: String, value :: Foreign }

instance ParamsToArray RL.Nil row where
  paramsToArray _ _ = []

instance
  ( IsSymbol name
  , Row.Cons name typ rest row
  , Row.Lacks name rest
  , ParamsToArray tail row
  ) =>
  ParamsToArray (RL.Cons name typ tail) row where
  paramsToArray _ rec =
    [ { name: reflectSymbol (Proxy :: Proxy name)
      , value: unsafeToForeign (Record.get (Proxy :: Proxy name) rec)
      }
    ]
      <> paramsToArray (Proxy :: Proxy tail) rec

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- replaceNamedParams: replace $name with ? and build values array
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

namedParamRegex :: Regex.Regex
namedParamRegex = case Regex.regex "\\$[a-zA-Z_][a-zA-Z0-9_]*" RegexFlags.global of
  Right r -> r
  Left _ -> unsafeCoerce unit

replaceNamedParams :: Array { name :: String, value :: Foreign } -> String -> { sql :: String, values :: Array Foreign }
replaceNamedParams entries sql = do
  let replacements = entries # foldl (\m e -> Map.insert ("$" <> e.name) "?" m) Map.empty
  let
    sql' = Regex.replace' namedParamRegex
      ( \match_ _ -> case Map.lookup match_ replacements of
          Nothing -> match_
          Just v -> v
      )
      sql
  { sql: sql', values: map (\e -> e.value) entries }

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Query execution
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

decodeRows :: forall a. ReadForeign a => Array Foreign -> Either String (Array a)
decodeRows rows = case traverse (\r -> readImpl r # runExcept) rows of
  Left errs -> Left (show errs)
  Right a -> Right a

runQuery
  :: forall tables result params paramsRL stage
   . RowToList params paramsRL
  => ParamsToArray paramsRL params
  => ReadForeign { | result }
  => Scylla.Client
  -> { | params }
  -> Q tables result params stage
  -> Aff (Array { | result })
runQuery client params (Q q) = do
  let entries = paramsToArray (Proxy :: Proxy paramsRL) params
  let { sql, values } = replaceNamedParams entries q.sql
  let allValues = q.values <> values
  result <- Scylla.execute (Scylla.CQL sql) allValues client
  case decodeRows result.rows of
    Left errs -> throwError (Exception.error ("Row decode failed: " <> errs))
    Right rows -> pure rows

runQueryOne
  :: forall tables result params paramsRL stage
   . RowToList params paramsRL
  => ParamsToArray paramsRL params
  => ReadForeign { | result }
  => Scylla.Client
  -> { | params }
  -> Q tables result params stage
  -> Aff (Maybe { | result })
runQueryOne client params q = do
  rows <- runQuery client params q
  pure case rows of
    [ x ] -> Just x
    _ -> Nothing

runExecute
  :: forall tables params paramsRL stage
   . RowToList params paramsRL
  => ParamsToArray paramsRL params
  => Scylla.Client
  -> { | params }
  -> Q tables () params stage
  -> Aff Unit
runExecute client params (Q q) = do
  let entries = paramsToArray (Proxy :: Proxy paramsRL) params
  let { sql, values } = replaceNamedParams entries q.sql
  let allValues = q.values <> values
  void $ Scylla.execute (Scylla.CQL sql) allValues client
