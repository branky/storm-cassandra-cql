package com.hmsonline.trident.cql;

import backtype.storm.Config;
import backtype.storm.metric.api.CountMetric;
import backtype.storm.task.IMetricsContext;
import backtype.storm.topology.ReportedFailedException;
import com.datastax.driver.core.*;
import com.datastax.driver.core.BatchStatement.Type;
import com.datastax.driver.core.exceptions.*;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.state.OpaqueValue;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.TransactionalValue;
import storm.trident.state.map.IBackingMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @param <T> The generic state to back
 * @author robertlee
 */
public class CassandraCqlMapState<T> implements IBackingMap<T> {
    private static final Logger LOG = LoggerFactory.getLogger(CassandraCqlMapState.class);

    @SuppressWarnings("serial")
    public static class Options<T> implements Serializable {
        public int localCacheSize = 5000;
        public String globalKey = "globalkey";
        public String keyspace = "mykeyspace";
        public String tableName = "mytable";
        public Integer ttl = 86400; // 1 day
        public boolean async = false;
    }

    /////////////////////////////////////////////
    // Static Methods For Specific State Type StateFactory
    /////////////////////////////////////////////

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(CqlRowMapper mapper) {
        Options<OpaqueValue> options = new Options<OpaqueValue>();
        return opaque(mapper, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory opaque(CqlRowMapper mapper, Options<OpaqueValue> opts) {
        return new CassandraCqlMapStateFactory(mapper, StateType.OPAQUE, opts, ConsistencyLevel.QUORUM);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(CqlRowMapper mapper) {
        Options<TransactionalValue> options = new Options<TransactionalValue>();
        return transactional(mapper, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory transactional(CqlRowMapper mapper, Options<TransactionalValue> opts) {
        return new CassandraCqlMapStateFactory(mapper, StateType.TRANSACTIONAL, opts, ConsistencyLevel.QUORUM);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory nonTransactional(CqlRowMapper mapper) {
        Options<Object> options = new Options<Object>();
        return nonTransactional(mapper, options);
    }

    @SuppressWarnings("rawtypes")
    public static StateFactory nonTransactional(CqlRowMapper mapper, Options<Object> opts) {
        return new CassandraCqlMapStateFactory(mapper, StateType.NON_TRANSACTIONAL, opts, ConsistencyLevel.QUORUM);
    }

    //////////////////////////////
    // Instance Variables
    //////////////////////////////
    private Options<T> options;
    private final Session session;

    @SuppressWarnings("rawtypes")
    private CqlRowMapper mapper;
    
    private PreparedStatement preparedStatement;

    // Metrics for storm metrics registering
    CountMetric _mreads;
    CountMetric _mwrites;
    CountMetric _mexceptions;

    @SuppressWarnings({"rawtypes"})
    public CassandraCqlMapState(Session session, CqlRowMapper mapper, Options<T> options, Map conf) {
        this.options = options;
        this.session = session;
        this.mapper = mapper;
    }

    ////////////////////////////////////
    // Overridden Methods for IBackingMap
    ////////////////////////////////////
    @SuppressWarnings("unchecked")
    @Override
    public List<T> multiGet(List<List<Object>> keys) {
        try {
            List<T> values = new ArrayList<T>();

            for (List<Object> rowKey : keys) {
                Statement statement = mapper.retrieve(rowKey);
                ResultSet results = session.execute(statement);
                // TODO: Better way to check for empty results besides accessing entire results list
                Iterator<Row> rowIter = results.iterator();
                Row row;
                if (results != null && rowIter.hasNext() && (row = rowIter.next()) != null) {
                    if (rowIter.hasNext()) {
                        LOG.error("Found non-unique value for key [{}]", rowKey);
                    } else {
                        values.add((T) mapper.getValue(row));
                    }
                } else {
                    values.add(null);
                }
            }

            _mreads.incrBy(values.size());
            LOG.debug("Retrieving the following keys: {} with values: {}", keys, values);
            return values;
        } catch (Exception e) {
            checkCassandraException(e);
            throw new IllegalStateException("Impossible to reach this code");
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void multiPut(List<List<Object>> keys, List<T> values) {
        LOG.debug("Putting the following keys: {} with values: {}", keys, values);
        if (options.async) {
            try {
                List<ResultSetFuture> futures = new ArrayList<ResultSetFuture>(keys.size());
                for (int i = 0; i < keys.size(); i++) {
                    List<Object> key = keys.get(i);
                    T val = values.get(i);
                    RegularStatement retrievedStatment = (RegularStatement)mapper.map(key, val);
                    if (preparedStatement == null) {
                        preparedStatement = session.prepare(retrievedStatment);
                    }
                    Object[] bindValues = new Object[key.size() + 1];
                    bindValues = key.toArray(bindValues);
                    bindValues[bindValues.length - 1] = val;
                    BoundStatement bind = preparedStatement.bind(bindValues);
                    ResultSetFuture future = session.executeAsync(bind);
                    futures.add(future);
                }
                for(ResultSetFuture future: futures){
                    future.getUninterruptibly();
                    _mwrites.incrBy(1);
                }
            } catch (Exception e) {
                checkCassandraException(e);
                LOG.error("Exception {} caught.", e);
            }

        } else {
            try {
                List<Statement> statements = new ArrayList<Statement>();
                // Retrieve the mapping statement for the key,val pair
                for (int i = 0; i < keys.size(); i++) {
                    List<Object> key = keys.get(i);
                    T val = values.get(i);
                    Statement retrievedStatment = mapper.map(key, val);
                    if (retrievedStatment instanceof BatchStatement) { // Allows for BatchStatements to be returned by the mapper.
                        BatchStatement batchedStatment = (BatchStatement) retrievedStatment;
                        statements.addAll(batchedStatment.getStatements());
                    } else {
                        statements.add(retrievedStatment);
                    }
                }

                // Execute all the statements as a batch.
                BatchStatement batch = new BatchStatement(Type.LOGGED);
                batch.addAll(statements);
                session.execute(batch);

                _mwrites.incrBy(statements.size());
            } catch (Exception e) {
                checkCassandraException(e);
                LOG.error("Exception {} caught.", e);
            }
        }

    }

    private void checkCassandraException(Exception e) {
        _mexceptions.incr();
        if (e instanceof AlreadyExistsException ||
                e instanceof AuthenticationException ||
                e instanceof DriverException ||
                e instanceof DriverInternalError ||
                e instanceof InvalidConfigurationInQueryException ||
                e instanceof InvalidQueryException ||
                e instanceof InvalidTypeException ||
                e instanceof QueryExecutionException ||
                e instanceof QueryTimeoutException ||
                e instanceof QueryValidationException ||
                e instanceof ReadTimeoutException ||
                e instanceof SyntaxError ||
                e instanceof TraceRetrievalException ||
                e instanceof TruncateException ||
                e instanceof UnauthorizedException ||
                e instanceof UnavailableException ||
                e instanceof ReadTimeoutException ||
                e instanceof WriteTimeoutException) {
            throw new ReportedFailedException(e);
        } else {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("rawtypes")
    public void registerMetrics(Map conf, IMetricsContext context) {
        int bucketSize = (Integer) (conf.get(Config.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS));
        _mreads = context.registerMetric("cassandra/readCount", new CountMetric(), bucketSize);
        _mwrites = context.registerMetric("cassandra/writeCount", new CountMetric(), bucketSize);
        _mexceptions = context.registerMetric("cassandra/exceptionCount", new CountMetric(), bucketSize);
    }
}
