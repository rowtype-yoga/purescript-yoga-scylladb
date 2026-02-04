module Yoga.ScyllaDB.TypedQuery where

import Prelude

import Data.Either (Either(..))
import Data.Map (Map)
import Data.Maybe (Maybe(..))
import Data.Traversable (traverse)
import Effect.Aff (Aff)
import Foreign (Foreign)
import Heterogeneous.Folding (class HFoldlWithIndex)
import Yoga.ScyllaDB.ScyllaDB as Scylla
import Yoga.SQL.Types (SQLParameter, SQLQuery, TurnIntoSQLParam, argsFor, sqlQueryToString)
import Unsafe.Coerce (unsafeCoerce)
import Yoga.JSON (class ReadForeign)
import Yoga.JSON as JSON

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Convert SQL Types to CQL Types
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- | Convert a SQLParameter to Foreign for ScyllaDB
sqlParamToForeign :: SQLParameter -> Foreign
sqlParamToForeign = unsafeCoerce

-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
-- Type-Safe Query Execution
-- ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

-- | Execute a typed CQL query and parse results with yoga-json
executeSql
  :: forall @params @result
   . HFoldlWithIndex TurnIntoSQLParam (Map String SQLParameter) { | params } (Map String SQLParameter)
  => ReadForeign result
  => SQLQuery params
  -> { | params }
  -> Scylla.Client
  -> Aff (Either String (Array result))
executeSql sqlQuery params client = do
  let
    cql = Scylla.CQL (sqlQueryToString sqlQuery)
    sqlParams = argsFor sqlQuery params
    foreignParams = map sqlParamToForeign sqlParams
  result <- Scylla.execute cql foreignParams client
  pure $ traverse parseRow result.rows
  where
  parseRow :: Foreign -> Either String result
  parseRow row = case (JSON.read row :: Either _ result) of
    Left errors -> Left (show errors)
    Right parsed -> Right parsed

-- | Execute a typed CQL query and return a single row
executeSqlOne
  :: forall @params @result
   . HFoldlWithIndex TurnIntoSQLParam (Map String SQLParameter) { | params } (Map String SQLParameter)
  => ReadForeign result
  => SQLQuery params
  -> { | params }
  -> Scylla.Client
  -> Aff (Either String (Maybe result))
executeSqlOne sqlQuery params client = do
  result <- executeSql @params @result sqlQuery params client
  pure $ result <#> case _ of
    [ x ] -> Just x
    [] -> Nothing
    _ -> Nothing -- More than one result, return Nothing

-- | Execute a mutation (INSERT/UPDATE/DELETE)
executeMutation
  :: forall @params
   . HFoldlWithIndex TurnIntoSQLParam (Map String SQLParameter) { | params } (Map String SQLParameter)
  => SQLQuery params
  -> { | params }
  -> Scylla.Client
  -> Aff Unit
executeMutation sqlQuery params client = do
  let
    cql = Scylla.CQL (sqlQueryToString sqlQuery)
    sqlParams = argsFor sqlQuery params
    foreignParams = map sqlParamToForeign sqlParams
  _ <- Scylla.execute cql foreignParams client
  pure unit
