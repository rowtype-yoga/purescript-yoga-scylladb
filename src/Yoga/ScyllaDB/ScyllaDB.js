import cassandra from 'cassandra-driver';

// Map PureScript consistency levels to Cassandra driver
const mapConsistency = (level) => {
  const levelStr = String(level);
  switch (levelStr) {
    case 'ANY': return cassandra.types.consistencies.any;
    case 'ONE': return cassandra.types.consistencies.one;
    case 'TWO': return cassandra.types.consistencies.two;
    case 'THREE': return cassandra.types.consistencies.three;
    case 'QUORUM': return cassandra.types.consistencies.quorum;
    case 'ALL': return cassandra.types.consistencies.all;
    case 'LOCAL_QUORUM': return cassandra.types.consistencies.localQuorum;
    case 'EACH_QUORUM': return cassandra.types.consistencies.eachQuorum;
    case 'LOCAL_ONE': return cassandra.types.consistencies.localOne;
    default: return cassandra.types.consistencies.localQuorum;
  }
};

// Create ScyllaDB client
export const createClientImpl = (config) => {
  const clientConfig = {
    contactPoints: config.contactPoints || [],
    localDataCenter: config.localDataCenter,
    keyspace: config.keyspace,
    protocolOptions: {},
    socketOptions: {},
    queryOptions: {},
    pooling: {}
  };

  // Credentials
  if (config.credentials) {
    clientConfig.credentials = {
      username: config.credentials.username,
      password: config.credentials.password
    };
  }

  // Pooling options
  if (config.pooling) {
    if (config.pooling.coreConnectionsPerHost) {
      clientConfig.pooling.coreConnectionsPerHost = config.pooling.coreConnectionsPerHost;
    }
    if (config.pooling.maxConnectionsPerHost) {
      clientConfig.pooling.maxConnectionsPerHost = config.pooling.maxConnectionsPerHost;
    }
  }

  // Socket options
  if (config.socketOptions) {
    if (config.socketOptions.connectTimeout) {
      clientConfig.socketOptions.connectTimeout = config.socketOptions.connectTimeout;
    }
    if (config.socketOptions.readTimeout) {
      clientConfig.socketOptions.readTimeout = config.socketOptions.readTimeout;
    }
  }

  // Query options
  if (config.queryOptions) {
    if (config.queryOptions.consistency) {
      clientConfig.queryOptions.consistency = mapConsistency(config.queryOptions.consistency);
    }
    if (config.queryOptions.prepare !== undefined) {
      clientConfig.queryOptions.prepare = config.queryOptions.prepare;
    }
    if (config.queryOptions.fetchSize) {
      clientConfig.queryOptions.fetchSize = config.queryOptions.fetchSize;
    }
  }

  // Protocol options
  if (config.protocolOptions) {
    if (config.protocolOptions.port) {
      clientConfig.protocolOptions.port = config.protocolOptions.port;
    }
    if (config.protocolOptions.maxSchemaAgreementWaitSeconds) {
      clientConfig.protocolOptions.maxSchemaAgreementWaitSeconds = 
        config.protocolOptions.maxSchemaAgreementWaitSeconds;
    }
  }

  // Policies (pass through as-is if provided)
  if (config.policies) {
    clientConfig.policies = config.policies;
  }

  // Pass through any other options
  const client = new cassandra.Client({
    ...clientConfig,
    ...config
  });

  return client;
};

// Connect to cluster
export const connectImpl = (client) => client.connect();

// Execute query
export const executeImpl = async (client, query, params) => {
  const result = await client.execute(query, params, { prepare: true });
  return {
    rows: result.rows || [],
    pageState: result.pageState || null,
    rowLength: result.rowLength || (result.rows ? result.rows.length : 0)
  };
};

// Execute query with options
export const executeWithOptionsImpl = async (client, query, params, options) => {
  const execOptions = { ...options };
  
  // Map consistency if provided
  if (options.consistency) {
    execOptions.consistency = mapConsistency(options.consistency);
  }

  const result = await client.execute(query, params, execOptions);
  return {
    rows: result.rows || [],
    pageState: result.pageState || null,
    rowLength: result.rowLength || (result.rows ? result.rows.length : 0)
  };
};

// Execute batch queries
export const batchImpl = async (client, queries) => {
  const result = await client.batch(queries, { prepare: true });
  return {
    rows: result.rows || [],
    pageState: result.pageState || null,
    rowLength: result.rowLength || 0
  };
};

// Execute batch with options
export const batchWithOptionsImpl = async (client, queries, options) => {
  const execOptions = { ...options };
  
  // Map consistency if provided
  if (options.consistency) {
    execOptions.consistency = mapConsistency(options.consistency);
  }

  // Map logged batch type
  if (options.logged !== undefined) {
    execOptions.logged = options.logged;
  }

  // Map counter batch type
  if (options.counter !== undefined) {
    execOptions.counter = options.counter;
  }

  const result = await client.batch(queries, execOptions);
  return {
    rows: result.rows || [],
    pageState: result.pageState || null,
    rowLength: result.rowLength || 0
  };
};

// Stream query results
export const streamImpl = async (client, query, params) => {
  return new Promise((resolve, reject) => {
    const rows = [];
    
    client.stream(query, params, { prepare: true })
      .on('readable', function() {
        let row;
        while (row = this.read()) {
          rows.push(row);
        }
      })
      .on('end', () => {
        resolve(rows);
      })
      .on('error', (err) => {
        reject(err);
      });
  });
};

// Prepared statements
export const prepareImpl = async (client, query) => {
  return client.connect().then(() => {
    // The driver automatically prepares queries when prepare: true is used
    // We return a prepared query object
    return { query, client };
  });
};

export const executePreparedImpl = async (client, preparedStmt, params) => {
  const result = await client.execute(preparedStmt.query, params, { prepare: true });
  return {
    rows: result.rows || [],
    pageState: result.pageState || null,
    rowLength: result.rowLength || (result.rows ? result.rows.length : 0)
  };
};

// Metadata operations
export const getKeyspaceImpl = (client, keyspace) => {
  const metadata = client.metadata;
  const ks = metadata.keyspaces[keyspace];
  return ks || null;
};

export const getTableImpl = (client, keyspace, table) => {
  const metadata = client.metadata;
  const ks = metadata.keyspaces[keyspace];
  if (!ks) return null;
  
  const tbl = ks.tables[table];
  return tbl || null;
};

// Get hosts
export const getHostsImpl = (client) => {
  const hosts = client.getState().getConnectedHosts();
  return Array.from(hosts);
};

// Shutdown client
export const shutdownImpl = (client) => client.shutdown();

// Health check
export const pingImpl = async (client) => {
  try {
    await client.execute('SELECT now() FROM system.local', [], { prepare: false });
    return true;
  } catch (err) {
    return false;
  }
};

// UUID helpers
export const uuidImpl = () => cassandra.types.Uuid.random();

export const timeUuidImpl = () => cassandra.types.TimeUuid.now();

export const uuidFromStringImpl = (str) => {
  try {
    return cassandra.types.Uuid.fromString(str);
  } catch (err) {
    return null;
  }
};
