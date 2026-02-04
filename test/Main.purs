module Test.Scylladb.Main where

import Prelude

import Data.Array as Array
import Data.Either (Either(..))
import Data.Maybe (Maybe(..))
import Effect (Effect)
import Effect.Aff (Aff, bracket, delay, launchAff_, throwError, try)
import Effect.Class (liftEffect)
import Effect.Console (log)
import Effect.Exception (error)
import Data.Time.Duration (Milliseconds(..))
import Test.Spec (Spec, around, describe, it)
import Test.Spec.Assertions (shouldEqual, shouldSatisfy)
import Test.Spec.Reporter.Console (consoleReporter)
import Test.Spec.Runner (runSpec)
import Yoga.Test.Docker as Docker
import Yoga.ScyllaDB.ScyllaDB as Scylla

-- Test configuration
testContactPoint :: Scylla.ContactPoint
testContactPoint = Scylla.ContactPoint "127.0.0.1"

testDatacenter :: Scylla.Datacenter
testDatacenter = Scylla.Datacenter "datacenter1"

testKeyspace :: Scylla.Keyspace
testKeyspace = Scylla.Keyspace "test_ks"

testPort :: Int
testPort = 9043 -- Test port from docker-compose.test.yml

-- Helper to create and manage ScyllaDB connection
withScylla :: (Scylla.Client -> Aff Unit) -> Aff Unit
withScylla test = do
  client <- liftEffect $ Scylla.createClient
    { contactPoints: [ testContactPoint ]
    , localDataCenter: testDatacenter
    , protocolOptions: { port: testPort, maxSchemaAgreementWaitSeconds: 10 }
    }
  _ <- Scylla.connect client

  -- Wait for cluster to be ready
  delay (Milliseconds 1000.0)

  -- Clean up test keyspace before each test
  _ <- try $ Scylla.dropKeyspace testKeyspace client

  test client

  _ <- Scylla.shutdown client
  pure unit

-- Helper to check if value is Left
isLeft :: forall a b. Either a b -> Boolean
isLeft (Left _) = true
isLeft _ = false

-- Helper to set up test keyspace
setupKeyspace :: Scylla.Client -> Aff Unit
setupKeyspace client = do
  _ <- Scylla.createKeyspace testKeyspace (Scylla.ReplicationFactor 1) client
  _ <- Scylla.useKeyspace testKeyspace client
  pure unit

-- Helper to set up users table
setupUsersTable :: Scylla.Client -> Aff Unit
setupUsersTable client = do
  _ <- Scylla.execute
    ( Scylla.CQL
        """
    CREATE TABLE test_ks.users (
      id uuid PRIMARY KEY,
      name text,
      email text,
      age int,
      active boolean
    )
  """
    )
    []
    client
  pure unit

spec :: Spec Unit
spec = do
  describe "Yoga.ScyllaDB Integration Tests" do

    -- Connection Management Tests
    around withScylla do
      describe "Connection Management" do
        it "connects to ScyllaDB successfully" \client -> do
          healthy <- Scylla.ping client
          healthy `shouldEqual` true

        it "retrieves cluster hosts" \client -> do
          hosts <- liftEffect $ Scylla.getHosts client
          (Array.length hosts) `shouldSatisfy` (\l -> l > 0)

    describe "Connection Errors" do
      it "handles connection failures" do
        result <- try do
          client <- liftEffect $ Scylla.createClient
            { contactPoints: [ Scylla.ContactPoint "invalid-host-9999" ]
            , localDataCenter: testDatacenter
            , protocolOptions: { port: 9999, maxSchemaAgreementWaitSeconds: 10 }
            }
          Scylla.connect client
        result `shouldSatisfy` isLeft

    -- Keyspace Operations Tests
    around withScylla do
      describe "Keyspace Operations" do
        it "creates and drops keyspaces" \client -> do
          _ <- Scylla.createKeyspace testKeyspace (Scylla.ReplicationFactor 1) client
          _ <- Scylla.dropKeyspace testKeyspace client
          pure unit

        it "uses keyspace" \client -> do
          setupKeyspace client
          result <- Scylla.useKeyspace testKeyspace client
          result.rowLength `shouldEqual` 0

        it "retrieves keyspace metadata" \client -> do
          setupKeyspace client
          meta <- liftEffect $ Scylla.getKeyspace testKeyspace client
          case meta of
            Just _ -> pure unit
            Nothing -> throwError (error "Expected to find keyspace metadata")

    -- Table Operations Tests
    around withScylla do
      describe "Table Operations" do
        it "creates tables with various column types" \client -> do
          setupKeyspace client
          result <- Scylla.execute
            ( Scylla.CQL
                """
            CREATE TABLE test_ks.users (
              id uuid PRIMARY KEY,
              name text,
              age int,
              email text,
              active boolean,
              created_at timestamp
            )
          """
            )
            []
            client
          result.rowLength `shouldEqual` 0

        it "inserts and queries data" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId <- liftEffect Scylla.uuid
          _ <- Scylla.execute (Scylla.CQL "INSERT INTO test_ks.users (id, name, age) VALUES (?, ?, ?)")
            [ Scylla.toCQLValue userId, Scylla.toCQLValue "Alice", Scylla.toCQLValue 30 ]
            client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users WHERE id = ?")
            [ Scylla.toCQLValue userId ]
            client
          result.rowLength `shouldEqual` 1

        it "updates data" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId <- liftEffect Scylla.uuid
          _ <- Scylla.execute (Scylla.CQL "INSERT INTO test_ks.users (id, name, email) VALUES (?, ?, ?)")
            [ Scylla.toCQLValue userId, Scylla.toCQLValue "Bob", Scylla.toCQLValue "bob@example.com" ]
            client

          _ <- Scylla.execute (Scylla.CQL "UPDATE test_ks.users SET email = ? WHERE id = ?")
            [ Scylla.toCQLValue "bob.new@example.com", Scylla.toCQLValue userId ]
            client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users WHERE id = ?")
            [ Scylla.toCQLValue userId ]
            client
          result.rowLength `shouldEqual` 1

        it "deletes data" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId <- liftEffect Scylla.uuid
          _ <- Scylla.execute (Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)")
            [ Scylla.toCQLValue userId, Scylla.toCQLValue "Charlie" ]
            client

          _ <- Scylla.execute (Scylla.CQL "DELETE FROM test_ks.users WHERE id = ?")
            [ Scylla.toCQLValue userId ]
            client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users WHERE id = ?")
            [ Scylla.toCQLValue userId ]
            client
          result.rowLength `shouldEqual` 0

    -- Consistency Level Tests
    around withScylla do
      describe "Consistency Levels" do
        it "executes with ONE consistency level" \client -> do
          setupKeyspace client
          result <- Scylla.executeWithOptions
            (Scylla.CQL "SELECT * FROM system.local")
            []
            { consistency: Scylla.One }
            client
          result.rowLength `shouldSatisfy` (\l -> l > 0)

        it "executes with QUORUM consistency level" \client -> do
          setupKeyspace client
          result <- Scylla.executeWithOptions
            (Scylla.CQL "SELECT * FROM system.local")
            []
            { consistency: Scylla.Quorum }
            client
          result.rowLength `shouldSatisfy` (\l -> l > 0)

        it "executes with LOCAL_ONE consistency level" \client -> do
          setupKeyspace client
          result <- Scylla.executeWithOptions
            (Scylla.CQL "SELECT * FROM system.local")
            []
            { consistency: Scylla.LocalOne }
            client
          result.rowLength `shouldSatisfy` (\l -> l > 0)

    -- Prepared Statement Tests
    around withScylla do
      describe "Prepared Statements" do
        it "prepares and executes statements" \client -> do
          setupKeyspace client
          setupUsersTable client

          prepared <- Scylla.prepare
            (Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)")
            client

          userId <- liftEffect Scylla.uuid
          result <- Scylla.executePrepared prepared
            [ Scylla.toCQLValue userId, Scylla.toCQLValue "Diana" ]
            client
          result.rowLength `shouldEqual` 0

        it "reuses prepared statements for multiple inserts" \client -> do
          setupKeyspace client
          setupUsersTable client

          prepared <- Scylla.prepare
            (Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)")
            client

          userId1 <- liftEffect Scylla.uuid
          userId2 <- liftEffect Scylla.uuid
          userId3 <- liftEffect Scylla.uuid

          _ <- Scylla.executePrepared prepared [ Scylla.toCQLValue userId1, Scylla.toCQLValue "Eve" ] client
          _ <- Scylla.executePrepared prepared [ Scylla.toCQLValue userId2, Scylla.toCQLValue "Frank" ] client
          _ <- Scylla.executePrepared prepared [ Scylla.toCQLValue userId3, Scylla.toCQLValue "Grace" ] client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users") [] client
          result.rowLength `shouldEqual` 3

    -- Batch Operations Tests
    around withScylla do
      describe "Batch Operations" do
        it "executes batch inserts" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId1 <- liftEffect Scylla.uuid
          userId2 <- liftEffect Scylla.uuid

          let
            batchQueries =
              [ { query: Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)"
                , params: [ Scylla.toCQLValue userId1, Scylla.toCQLValue "Henry" ]
                }
              , { query: Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)"
                , params: [ Scylla.toCQLValue userId2, Scylla.toCQLValue "Iris" ]
                }
              ]

          _ <- Scylla.batch batchQueries client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users") [] client
          result.rowLength `shouldEqual` 2

        it "executes batch with consistency options" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId <- liftEffect Scylla.uuid

          let
            batchQueries =
              [ { query: Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)"
                , params: [ Scylla.toCQLValue userId, Scylla.toCQLValue "Jack" ]
                }
              ]

          result <- Scylla.batchWithOptions batchQueries
            { consistency: Scylla.One, logged: true }
            client
          result.rowLength `shouldEqual` 0

    -- UUID Tests
    around withScylla do
      describe "UUID Operations" do
        it "generates UUIDs" \_ -> do
          _ <- liftEffect Scylla.uuid
          _ <- liftEffect Scylla.uuid
          pure unit -- Just check it doesn't error

        it "generates time-based UUIDs" \_ -> do
          _ <- liftEffect Scylla.timeUuid
          _ <- liftEffect Scylla.timeUuid
          pure unit -- Just check it doesn't error

        it "parses UUIDs from strings" \_ -> do
          -- Valid UUID v4 format
          maybeUuid <- liftEffect $ Scylla.uuidFromString "550e8400-e29b-41d4-a716-446655440000"
          case maybeUuid of
            Just _ -> pure unit
            Nothing -> throwError (error "Expected to parse valid UUID")

        it "returns Nothing for invalid UUID strings" \_ -> do
          maybeUuid <- liftEffect $ Scylla.uuidFromString "not-a-valid-uuid"
          case maybeUuid of
            Nothing -> pure unit
            Just _ -> throwError (error "Expected Nothing for invalid UUID")

    -- Data Type Tests
    around withScylla do
      describe "Data Types" do
        it "handles various CQL data types" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId <- liftEffect Scylla.uuid
          _ <- Scylla.execute (Scylla.CQL "INSERT INTO test_ks.users (id, name, age, active) VALUES (?, ?, ?, ?)")
            [ Scylla.toCQLValue userId, Scylla.toCQLValue "Kate", Scylla.toCQLValue 25, Scylla.toCQLValue true ]
            client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users WHERE id = ?")
            [ Scylla.toCQLValue userId ]
            client
          result.rowLength `shouldEqual` 1

        it "handles null values" \client -> do
          setupKeyspace client
          setupUsersTable client

          userId <- liftEffect Scylla.uuid
          _ <- Scylla.execute (Scylla.CQL "INSERT INTO test_ks.users (id, name) VALUES (?, ?)")
            [ Scylla.toCQLValue userId, Scylla.toCQLValue "Leo" ]
            client

          result <- Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.users WHERE id = ?")
            [ Scylla.toCQLValue userId ]
            client
          result.rowLength `shouldEqual` 1

    -- Error Handling Tests
    around withScylla do
      describe "Error Handling" do
        it "handles invalid CQL syntax" \client -> do
          result <- try $ Scylla.execute (Scylla.CQL "INVALID CQL SYNTAX") [] client
          case result of
            Left _ -> pure unit
            Right _ -> throwError (error "Expected error for invalid CQL syntax")

        it "handles missing table errors" \client -> do
          setupKeyspace client
          result <- try $ Scylla.execute (Scylla.CQL "SELECT * FROM test_ks.non_existent_table") [] client
          case result of
            Left _ -> pure unit
            Right _ -> throwError (error "Expected error for missing table")

        it "handles missing keyspace errors" \client -> do
          result <- try $ Scylla.execute (Scylla.CQL "SELECT * FROM nonexistent_ks.users") [] client
          case result of
            Left _ -> pure unit
            Right _ -> throwError (error "Expected error for missing keyspace")

main :: Effect Unit
main = launchAff_ do
  liftEffect $ log "\n🧪 Starting ScyllaDB Integration Tests (with Docker)\n"

  bracket
    -- Start Docker before tests
    ( do
        liftEffect $ log "⏳ Starting ScyllaDB and waiting for it to be ready (may take 30-60s)..."
        Docker.startService "packages/yoga-scylladb/docker-compose.test.yml" 60
        liftEffect $ log "✅ ScyllaDB is ready!\n"
    )
    -- Stop Docker after tests (always runs!)
    ( \_ -> do
        Docker.stopService "packages/yoga-scylladb/docker-compose.test.yml"
        liftEffect $ log "✅ Cleanup complete\n"
    )
    -- Run tests
    (\_ -> runSpec [ consoleReporter ] spec)
