module Yoga.ScyllaDB.ScyllaDB where

import Prelude

import Data.Array as Array
import Data.Maybe (Maybe)
import Data.Newtype (class Newtype)
import Data.Nullable (Nullable)
import Data.Nullable as Nullable
import Data.Time.Duration (Milliseconds)
import Effect (Effect)
import Effect.Aff (Aff)
import Effect.Uncurried (EffectFn1, EffectFn2, EffectFn3, EffectFn4, runEffectFn1, runEffectFn2, runEffectFn3, runEffectFn4)
import Foreign (Foreign)
import Foreign.Object (Object)
import Prim.Row (class Union)
import Promise (Promise)
import Promise.Aff (toAffE) as Promise
import Unsafe.Coerce (unsafeCoerce)

-- Opaque ScyllaDB types
foreign import data Client :: Type
foreign import data ResultSet :: Type

-- Newtypes for type safety

-- Connection configuration
newtype ContactPoint = ContactPoint String

derive instance Newtype ContactPoint _
derive newtype instance Eq ContactPoint
derive newtype instance Show ContactPoint

newtype Keyspace = Keyspace String

derive instance Newtype Keyspace _
derive newtype instance Eq Keyspace
derive newtype instance Show Keyspace

newtype Datacenter = Datacenter String

derive instance Newtype Datacenter _
derive newtype instance Eq Datacenter
derive newtype instance Show Datacenter

newtype Username = Username String

derive instance Newtype Username _
derive newtype instance Eq Username
derive newtype instance Show Username

newtype Password = Password String

derive instance Newtype Password _
derive newtype instance Eq Password
derive newtype instance Show Password

newtype PoolSize = PoolSize Int

derive instance Newtype PoolSize _
derive newtype instance Eq PoolSize
derive newtype instance Ord PoolSize
derive newtype instance Show PoolSize

newtype ReplicationFactor = ReplicationFactor Int

derive instance Newtype ReplicationFactor _
derive newtype instance Eq ReplicationFactor
derive newtype instance Ord ReplicationFactor
derive newtype instance Show ReplicationFactor

-- Query types
newtype CQL = CQL String

derive instance Newtype CQL _
derive newtype instance Eq CQL
derive newtype instance Show CQL

-- Consistency levels
data ConsistencyLevel
  = Any
  | One
  | Two
  | Three
  | Quorum
  | All
  | LocalQuorum
  | EachQuorum
  | LocalOne

derive instance Eq ConsistencyLevel
derive instance Ord ConsistencyLevel

instance Show ConsistencyLevel where
  show Any = "ANY"
  show One = "ONE"
  show Two = "TWO"
  show Three = "THREE"
  show Quorum = "QUORUM"
  show All = "ALL"
  show LocalQuorum = "LOCAL_QUORUM"
  show EachQuorum = "EACH_QUORUM"
  show LocalOne = "LOCAL_ONE"

-- Result types
type Row = Foreign

type QueryResultImpl =
  { rows :: Array Row
  , pageState :: Nullable String
  , rowLength :: Int
  }

type QueryResult =
  { rows :: Array Row
  , pageState :: Maybe String
  , rowLength :: Int
  }

toQueryResult :: QueryResultImpl -> QueryResult
toQueryResult r =
  { rows: r.rows
  , pageState: Nullable.toMaybe r.pageState
  , rowLength: r.rowLength
  }

-- Client configuration
type ClientOptionsImpl =
  ( contactPoints :: Array ContactPoint
  , localDataCenter :: Datacenter
  , keyspace :: Keyspace
  , credentials ::
      { username :: Username
      , password :: Password
      }
  , pooling ::
      { coreConnectionsPerHost :: PoolSize
      , maxConnectionsPerHost :: PoolSize
      }
  , socketOptions ::
      { connectTimeout :: Milliseconds
      , readTimeout :: Milliseconds
      }
  , queryOptions ::
      { consistency :: ConsistencyLevel
      , prepare :: Boolean
      , fetchSize :: Int
      }
  , protocolOptions ::
      { port :: Int
      , maxSchemaAgreementWaitSeconds :: Int
      }
  , policies ::
      { loadBalancing :: Foreign
      , retry :: Foreign
      , reconnection :: Foreign
      }
  )

-- Create ScyllaDB client
foreign import createClientImpl :: forall opts. EffectFn1 { | opts } Client

createClient :: forall opts opts_. Union opts opts_ ClientOptionsImpl => { | opts } -> Effect Client
createClient opts = runEffectFn1 createClientImpl opts

-- Connect to cluster
foreign import connectImpl :: EffectFn1 Client (Promise Unit)

connect :: Client -> Aff Unit
connect = runEffectFn1 connectImpl >>> Promise.toAffE

-- Execute query
foreign import executeImpl :: EffectFn3 Client CQL (Array Foreign) (Promise QueryResultImpl)

execute :: CQL -> Array Foreign -> Client -> Aff QueryResult
execute cql params client =
  runEffectFn3 executeImpl client cql params
    # Promise.toAffE
    <#> toQueryResult

-- Execute query with options
type ExecuteOptionsImpl =
  ( consistency :: ConsistencyLevel
  , prepare :: Boolean
  , fetchSize :: Int
  , pageState :: String
  , traceQuery :: Boolean
  , timestamp :: Number
  )

foreign import executeWithOptionsImpl :: forall opts. EffectFn4 Client CQL (Array Foreign) { | opts } (Promise QueryResultImpl)

executeWithOptions :: forall opts opts_. Union opts opts_ ExecuteOptionsImpl => CQL -> Array Foreign -> { | opts } -> Client -> Aff QueryResult
executeWithOptions cql params opts client =
  runEffectFn4 executeWithOptionsImpl client cql params opts
    # Promise.toAffE
    <#> toQueryResult

-- Execute batch queries
type BatchQuery =
  { query :: CQL
  , params :: Array Foreign
  }

foreign import batchImpl :: EffectFn2 Client (Array BatchQuery) (Promise QueryResultImpl)

batch :: Array BatchQuery -> Client -> Aff QueryResult
batch queries client =
  runEffectFn2 batchImpl client queries
    # Promise.toAffE
    <#> toQueryResult

-- Execute batch with options
type BatchOptionsImpl =
  ( consistency :: ConsistencyLevel
  , logged :: Boolean
  , counter :: Boolean
  , timestamp :: Number
  )

foreign import batchWithOptionsImpl :: forall opts. EffectFn3 Client (Array BatchQuery) { | opts } (Promise QueryResultImpl)

batchWithOptions :: forall opts opts_. Union opts opts_ BatchOptionsImpl => Array BatchQuery -> { | opts } -> Client -> Aff QueryResult
batchWithOptions queries opts client =
  runEffectFn3 batchWithOptionsImpl client queries opts
    # Promise.toAffE
    <#> toQueryResult

-- Stream query results (returns array for simplicity - could be enhanced with streaming)
foreign import streamImpl :: EffectFn3 Client CQL (Array Foreign) (Promise (Array Row))

stream :: CQL -> Array Foreign -> Client -> Aff (Array Row)
stream cql params client =
  runEffectFn3 streamImpl client cql params
    # Promise.toAffE

-- Prepared statements
foreign import data PreparedStatement :: Type

foreign import prepareImpl :: EffectFn2 Client CQL (Promise PreparedStatement)

prepare :: CQL -> Client -> Aff PreparedStatement
prepare cql client =
  runEffectFn2 prepareImpl client cql
    # Promise.toAffE

foreign import executePreparedImpl :: EffectFn3 Client PreparedStatement (Array Foreign) (Promise QueryResultImpl)

executePrepared :: PreparedStatement -> Array Foreign -> Client -> Aff QueryResult
executePrepared stmt params client =
  runEffectFn3 executePreparedImpl client stmt params
    # Promise.toAffE
    <#> toQueryResult

-- Metadata operations
foreign import getKeyspaceImpl :: EffectFn2 Client Keyspace (Nullable Foreign)

getKeyspace :: Keyspace -> Client -> Effect (Maybe Foreign)
getKeyspace keyspace client =
  runEffectFn2 getKeyspaceImpl client keyspace
    <#> Nullable.toMaybe

foreign import getTableImpl :: EffectFn3 Client Keyspace String (Nullable Foreign)

getTable :: Keyspace -> String -> Client -> Effect (Maybe Foreign)
getTable keyspace table client =
  runEffectFn3 getTableImpl client keyspace table
    <#> Nullable.toMaybe

-- Get hosts
foreign import getHostsImpl :: EffectFn1 Client (Array Foreign)

getHosts :: Client -> Effect (Array Foreign)
getHosts = runEffectFn1 getHostsImpl

-- Shutdown client
foreign import shutdownImpl :: EffectFn1 Client (Promise Unit)

shutdown :: Client -> Aff Unit
shutdown = runEffectFn1 shutdownImpl >>> Promise.toAffE

-- Health check
foreign import pingImpl :: EffectFn1 Client (Promise Boolean)

ping :: Client -> Aff Boolean
ping = runEffectFn1 pingImpl >>> Promise.toAffE

-- Type-safe CQL parameters (like Postgres ToPGValue)
class ToCQLValue a where
  toCQLValue :: a -> Foreign

instance ToCQLValue String where
  toCQLValue = unsafeCoerce

instance ToCQLValue Int where
  toCQLValue = unsafeCoerce

instance ToCQLValue Number where
  toCQLValue = unsafeCoerce

instance ToCQLValue Boolean where
  toCQLValue = unsafeCoerce

instance ToCQLValue Foreign where
  toCQLValue = identity

instance ToCQLValue UUID where
  toCQLValue = unsafeCoerce

instance ToCQLValue a => ToCQLValue (Array a) where
  toCQLValue arr = unsafeCoerce (map toCQLValue arr)

-- Parameter helpers
param :: forall a. ToCQLValue a => a -> Array Foreign
param x = [ toCQLValue x ]

params :: forall a. ToCQLValue a => Array a -> Array Foreign
params xs = map toCQLValue xs

-- UUID helpers
foreign import data UUID :: Type

foreign import uuidImpl :: Effect UUID

uuid :: Effect UUID
uuid = uuidImpl

foreign import timeUuidImpl :: Effect UUID

timeUuid :: Effect UUID
timeUuid = timeUuidImpl

foreign import uuidFromStringImpl :: EffectFn1 String (Nullable UUID)

uuidFromString :: String -> Effect (Maybe UUID)
uuidFromString str =
  runEffectFn1 uuidFromStringImpl str
    <#> Nullable.toMaybe

-- Common query patterns

-- Create keyspace
createKeyspace :: Keyspace -> ReplicationFactor -> Client -> Aff QueryResult
createKeyspace (Keyspace ks) (ReplicationFactor rf) client =
  execute
    (CQL $ "CREATE KEYSPACE IF NOT EXISTS " <> ks <> " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': " <> show rf <> "}")
    []
    client

-- Drop keyspace
dropKeyspace :: Keyspace -> Client -> Aff QueryResult
dropKeyspace (Keyspace ks) client =
  execute
    (CQL $ "DROP KEYSPACE IF EXISTS " <> ks)
    []
    client

-- Use keyspace
useKeyspace :: Keyspace -> Client -> Aff QueryResult
useKeyspace (Keyspace ks) client =
  execute
    (CQL $ "USE " <> ks)
    []
    client
