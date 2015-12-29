package com.blazegraph.gremlin.structure;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailQuery;

import com.bigdata.bop.engine.IRunningQuery;
import com.bigdata.bop.engine.QueryEngine;
import com.bigdata.bop.fed.QueryEngineFactory;
import com.bigdata.journal.IIndexManager;
import com.bigdata.rdf.changesets.ChangeAction;
import com.bigdata.rdf.changesets.ChangeRecord;
import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.BigdataSailBooleanQuery;
import com.bigdata.rdf.sail.BigdataSailGraphQuery;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.BigdataSailTupleQuery;
import com.bigdata.rdf.sail.BigdataSailUpdate;
import com.bigdata.rdf.sail.QueryCancellationHelper;
import com.bigdata.rdf.sail.QueryCancelledException;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sail.webapp.BigdataRDFContext.AbstractQueryTask;
import com.bigdata.rdf.sail.webapp.StatusServlet;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.sparql.ast.QueryType;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUpdate;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.util.MillisecondTimestampFactory;
import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.listener.BlazeGraphEdit.Action;
import com.blazegraph.gremlin.listener.BlazeGraphListener;
import com.blazegraph.gremlin.util.Code;
import com.blazegraph.gremlin.util.LambdaLogger;

public class BlazeGraphEmbedded extends BlazeGraph {

    private final transient static LambdaLogger log = LambdaLogger.getLogger(BlazeGraphEmbedded.class);

    public static interface Options {
        
        /**
         * LocalBlazeGraph assumes the user has a well-configured and fully
         * initialized BigdataSailRepository already built and set in the 
         * Configuration.
         */
        String REPOSITORY = BlazeGraphEmbedded.class.getName() + ".repository";
        
    }
    
    static {
        /*
         * We do not want auto-commit for SPARQL Update.
         * 
         * TODO FIXME Make this a configurable property.
         */
        AST2BOpUpdate.AUTO_COMMIT = false;
    }
    
    public static BlazeGraphEmbedded open(final Configuration config) {
        final BigdataSailRepository repo = (BigdataSailRepository)
                config.getProperty(Options.REPOSITORY);
        Objects.requireNonNull(repo);
        if (!repo.getDatabase().isStatementIdentifiers()) {
            throw new IllegalArgumentException("BlazeGraph/TP3 requires statement identifiers.");
        }
        
        /*
         * Grab the last commit time and also check for clock skew.
         */
        final long lastCommitTime = lastCommitTime(repo);
        config.setProperty(BlazeGraph.Options.LAST_COMMIT_TIME, lastCommitTime);
        
        return new BlazeGraphEmbedded(config);
    }
    
    /*
     * Take a quick peek at the last commit time on the supplied journal file.
     * If it's ahead of our system time, then use that last commit time as
     * a lower bound on blaze's transaction timestamps.  Otherwise just use
     * the system time as usual.
     */
    private static long lastCommitTime(final BigdataSailRepository repo) {

        final long systemTime = System.currentTimeMillis();

        log.debug(() -> "temporarily setting lower bound to Long.MAX_VALUE: " + (Long.MAX_VALUE-100));

        /*
         * Temporarily set the lower bound to something way way in the future
         * in case the journal really is ahead of our system clock.
         */
        MillisecondTimestampFactory.setLowerBound(Long.MAX_VALUE-100);

        /*
         * Pull the last commit time from the journal.
         */
//        final long lastCommitTime =
//                journal.getRootBlockView().getLastCommitTime();
        final long lastCommitTime =
                repo.getDatabase().getIndexManager().getLastCommitTime();

        final long lowerBound;
        
        if (lastCommitTime > 0l && systemTime < lastCommitTime) {

            log.info(() -> "found clock skew, using last commit time: " + lastCommitTime);

            /*
             * If the journal has a last commit time and if that last commit
             * time is in the future relative to our system clock, use the
             * last commit time as a lower bound on blaze transaction
             * timestamps.
             */
            lowerBound = lastCommitTime;

        } else {

            log.debug(() -> "setting lower bound to system time as normal: " + systemTime);

            /*
             * Otherwise just use the system time as we normally would.
             */
            lowerBound = systemTime;

        }
        
        MillisecondTimestampFactory.setLowerBound(lowerBound);
        
        return lowerBound;

    }
    


    private final BigdataSailRepository repo;
    
    private final List<BlazeGraphListener> listeners = new CopyOnWriteArrayList<>();
    
    private final BlazeTransaction tx = new BlazeTransaction();

    private BlazeGraphEmbedded(final Configuration config) {
        super(config);
        this.repo = (BigdataSailRepository) config.getProperty(Options.REPOSITORY);
    }
    
    public BigdataSailRepository getRepository() {
        return repo;
    }
    
    public BigdataValueFactory rdfValueFactory() {
        return (BigdataValueFactory) repo.getValueFactory();
    }
    
    public QueryEngine getQueryEngine() {
        final IIndexManager ndxManager = getIndexManager();
        final QueryEngine queryEngine = (QueryEngine) 
                QueryEngineFactory.getInstance().getQueryController(ndxManager);
        return queryEngine;
    }

    public IIndexManager getIndexManager() {
        return repo.getDatabase().getIndexManager();
    }

    @Override
    protected Stream<BindingSet> select(final RepositoryConnection cxn, 
            final String queryStr, final String externalQueryId) {

        logQuery(queryStr);
        try {
            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) 
                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            final UUID queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
                        query, query.getASTContainer(), QueryType.CONSTRUCT,
                        externalQueryId);
            
            sparqlLog.trace(() -> "optimized AST:\n"+query.optimize());
            
            final TupleQueryResult result = query.evaluate();
            final Optional<Code> onClose = 
                    Optional.of(() -> finalizeQuery(queryId));
            return new GraphStreamer<>(cxn, result, onClose).stream();
        } catch (Exception ex) {
            /*
             * If there is an exception while preparing/evaluating the
             * query (before we get a TupleQueryResult) or during construction
             * of the closeable iterator / stream then we must close the read 
             * connection ourselves.
             */
            closeRead(cxn);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
        
    }
    
    protected Stream<Statement> project(final RepositoryConnection cxn, 
            final String queryStr, final String externalQueryId) {
        
        logQuery(queryStr);
        try {
            final BigdataSailGraphQuery query = (BigdataSailGraphQuery) 
                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
            final UUID queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
                        query, query.getASTContainer(), QueryType.CONSTRUCT,
                        externalQueryId);
        
            sparqlLog.trace(() -> "optimized AST:\n"+query.optimize());
        
            final GraphQueryResult result = query.evaluate();
            final Optional<Code> onClose = 
                    Optional.of(() -> finalizeQuery(queryId));
            return new GraphStreamer<>(cxn, result, onClose).stream();
        } catch (Exception ex) {
            /*
             * If there is an exception while preparing/evaluating the
             * query (before we get a GraogQueryResult) or during construction
             * of the closeable iterator / stream then we must close the read 
             * connection ourselves.
             */
            closeRead(cxn);
            if (ex instanceof RuntimeException) {
                throw (RuntimeException) ex;
            } else {
                throw new RuntimeException(ex);
            }
        }
        
    }
    
    protected boolean ask(final RepositoryConnection cxn, 
            final String queryStr, final String externalQueryId) {
        
        logQuery(queryStr);
        return Code.wrapThrow(() -> { /* try */ 
            final BigdataSailBooleanQuery query = (BigdataSailBooleanQuery) 
                    cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
            final UUID queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
                        query, query.getASTContainer(), QueryType.CONSTRUCT,
                        externalQueryId);
        
//            sparqlLog.trace(() -> "optimized AST:\n"+query.optimize());
        
            final boolean result = query.evaluate();
            finalizeQuery(queryId);
            return result;
        }, () -> { /* finally */
            /*
             * With ask we close the read connection no matter what.
             */
            closeRead(cxn);
        });
        
    }
    
    protected void update(final RepositoryConnection cxn, 
            final String queryStr, final String externalQueryId) {
        
        logQuery(queryStr);
        Code.wrapThrow(() -> {
            final BigdataSailUpdate update = (BigdataSailUpdate) 
                    cxn.prepareUpdate(QueryLanguage.SPARQL, queryStr);
            update.execute();
        });
    }
    
    private void logQuery(final String queryStr) {
        sparqlLog.trace(() -> "query:\n"+ 
                (queryStr.length() <= SPARQL_LOG_MAX ? 
                        queryStr : queryStr.substring(0, SPARQL_LOG_MAX)+" ..."));
    }


    
    
    @Override
    public BlazeTransaction tx() {
        return tx;
    }

    private boolean open = true;
    
    @Override
    public synchronized void close() throws Exception {
//        // if there is a connection open, commit and close
//        tx.doCommit();
        if (open) {
            tx.close();
            repo.shutDown();
            open = false;
        }
    }
    
    @Override
    protected BigdataSailRepositoryConnection writeCxn() {
        tx.readWrite();
        return tx.cxn();
    }

    @Override
    protected BigdataSailRepositoryConnection readCxn() {
        return Code.wrapThrow(() -> repo.getReadOnlyConnection());
    }
    
    public String dumpStore() throws Exception {
        final BigdataSailRepositoryConnection cxn = readCxn();
        try {
            return cxn.getTripleStore().dumpStore().toString();
        } finally {
            cxn.close();
        }
    }

    public class BlazeTransaction extends AbstractThreadLocalTransaction {

        protected final 
        ThreadLocal<BigdataSailRepositoryConnection> tlTx = 
                ThreadLocal.withInitial(() -> null);
        
        public BlazeTransaction() {
            super(BlazeGraphEmbedded.this);
        }

        @Override
        public boolean isOpen() {
            return (tlTx.get() != null);
        }
        
        public void bulkLoad() {
            readWrite();
            setBulkLoad(true);
        }
        
        @Override
        protected void doOpen() {
            Code.wrapThrow(() -> tlTx.set(repo.getUnisolatedConnection()));
        }

        @Override
        protected void doCommit() throws TransactionException {
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                try {
                    cxn.commit();
                } catch (Exception ex) {
                    throw new TransactionException(ex);
                } finally {
                    tlTx.remove();
                    Code.wrapThrow(() -> cxn.close());
                    setBulkLoad(false);
                }
            }
        }

        @Override
        protected void doRollback() throws TransactionException {
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                try {
                    cxn.rollback();
                } catch (Exception ex) {
                    throw new TransactionException(ex);
                } finally {
                    tlTx.remove();
                    Code.wrapThrow(() -> cxn.close());
                    setBulkLoad(false);
                }
            }
        }

        @Override
        protected void doClose() {
            super.doClose();
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                tlTx.remove();
                Code.wrapThrow(() -> cxn.close());
                setBulkLoad(false);
            }
        }
        
        public BigdataSailRepositoryConnection cxn() {
            return tlTx.get();
        }
        
    }

    public void addListener(final BlazeGraphListener listener) {
        this.listeners.add(listener);
    }

    public void removeListener(final BlazeGraphListener listener) {
        this.listeners.remove(listener);
    }

    private class ChangeLogTransformer implements IChangeLog {
    
        /**
         * We need to batch and materialize these.
         */
        private final List<IChangeRecord> removes = new LinkedList<>();
        
        /**
         * Changed events coming from bigdata.
         */
        @Override
        public void changeEvent(final IChangeRecord record) {
            /*
             * Watch out for history change events.
             */
            
            if (record.getStatement().getStatementType() == StatementEnum.History) {
                return;
            }
            
            /*
             * Adds come in already materialized. Removes do not. Batch and
             * materialize at commit or abort notification.
             */
            if (record.getChangeAction() == ChangeAction.REMOVED) {
                removes.add(record);
            } else {
                notify(record);
            }
        }
        
        /**
         * Turn a change record into a graph edit and notify the graph listeners.
         * 
         * @param record
         *          Bigdata change record.
         */
        protected void notify(final IChangeRecord record) {
            // some kinds of edits will be ignored
            toGraphEdit(record).ifPresent(edit ->
                listeners.forEach(listener -> 
                    listener.graphEdited(edit, record.toString()))
            );
        }
        
        /**
         * Turn a bigdata change record into a graph edit.
         * 
         * @param record
         *          Bigdata change record
         * @return
         *          graph edit
         */
        protected Optional<BlazeGraphEdit> toGraphEdit(final IChangeRecord record) {
            
            final Action action;
            if (record.getChangeAction() == ChangeAction.INSERTED) {
                action = Action.Add;
            } else if (record.getChangeAction() == ChangeAction.REMOVED) {
                action = Action.Remove;
            } else {
                /*
                 * Truth maintenance.
                 */
                return Optional.empty();
            }
            
            return transforms.graphAtom.apply(record.getStatement())
                        .map(atom -> new BlazeGraphEdit(action, atom));
            
        }
        
        /**
         * Materialize a batch of change records.
         * 
         * @param records
         *          Bigdata change records
         * @return
         *          Same records with materialized values
         */
        protected List<IChangeRecord> materialize(final List<IChangeRecord> records) {
            
            try {
                final AbstractTripleStore db = tx.cxn().getTripleStore();
    
                final List<IChangeRecord> materialized = new LinkedList<IChangeRecord>();
    
                // collect up the ISPOs out of the unresolved change records
                final ISPO[] spos = new ISPO[records.size()];
                int i = 0;
                for (IChangeRecord rec : records) {
                    spos[i++] = rec.getStatement();
                }
    
                // use the database to resolve them into BigdataStatements
                final BigdataStatementIterator it = db
                        .asStatementIterator(new ChunkedArrayIterator<ISPO>(i,
                                spos, null/* keyOrder */));
    
                /*
                 * the BigdataStatementIterator will produce BigdataStatement
                 * objects in the same order as the original ISPO array
                 */
                for (IChangeRecord rec : records) {
                    final BigdataStatement stmt = it.next();
                    materialized.add(new ChangeRecord(stmt, rec.getChangeAction()));
                }
    
                return materialized;
            } catch (RuntimeException ex) {
                throw ex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            
        }
    
        /**
         * Notification of transaction beginning.
         */
        @Override
        public void transactionBegin() {
            listeners.forEach(BlazeGraphListener::transactionBegin);
        }
    
        /**
         * Notification of transaction preparing for commit.
         */
        @Override
        public void transactionPrepare() {
            listeners.forEach(BlazeGraphListener::transactionPrepare);
        }
    
        /**
         * Notification of transaction committed.
         */
        @Override
        public void transactionCommited(final long commitTime) {
            notifyRemoves();
            listeners.forEach(l -> l.transactionCommited(commitTime));
        }
    
        /**
         * Notification of transaction aborted.
         */
        @Override
        public void transactionAborted() {
            notifyRemoves();
            listeners.forEach(BlazeGraphListener::transactionAborted);
        }
        
        @Override
        public void close() { }
        
        /**
         * Materialize and notify listeners of the remove events.
         */
        protected void notifyRemoves() {
            if (listeners.size() > 0) {
                final List<IChangeRecord> removes = materialize(this.removes);
                this.removes.clear();
                removes.forEach(this::notify);
            } else {
                this.removes.clear();
            }
        }
    
    }
    
    ////////////////////////////////////////////////////////////////////////
    // Query Cancellation Section
    ////////////////////////////////////////////////////////////////////////
    
    /**
     * 
     * <p>
     * Note: This is also responsible for noticing the time at which the
     * query begins to execute and storing the {@link RunningQuery} in the
     * {@link #queries} map.
     * 
     * @param The connection.
     */
    protected UUID setupQuery(final BigdataSailRepositoryConnection cxn,
            final SailQuery query, final ASTContainer astContainer,
            final QueryType queryType, final String extId) {

        setMaxQueryTime(query);
        
        // Note the begin time for the query.
        final long begin = System.nanoTime();

        // Figure out the UUID under which the query will execute.
        final UUID queryUuid = setQueryId(astContainer, UUID.randomUUID());
        
        //Set to UUID of internal ID if it is null.
        final String extQueryId = extId == null?queryUuid.toString():extId;
        
        if (log.isDebugEnabled() && extId == null) {
            log.debug("Received null external query ID.  Using "
                    + queryUuid.toString());
        }
        
        final boolean isUpdateQuery = queryType != QueryType.ASK
                && queryType != QueryType.CONSTRUCT
                && queryType != QueryType.DESCRIBE
                && queryType != QueryType.SELECT;
        
        final RunningQuery r = new RunningQuery(extQueryId, queryUuid, begin, isUpdateQuery);

        // Stuff it in the maps of running queries.
        queries.put(extQueryId, r);
        queries2.put(queryUuid, r);
        
        if (log.isDebugEnabled()) {
            log.debug("Setup Query (External ID, UUID):  ( " + extQueryId
                    + " , " + queryUuid + " )");
            log.debug("External query for " + queryUuid + " is :\n"
                    + getQuery(queryUuid).getExtQueryId());
            log.debug(runningQueriesToString());
        }

        return queryUuid;
        
    }
     
    /**
     * Determines the {@link UUID} which will be associated with the
     * {@link IRunningQuery}. If {@link QueryHints#QUERYID} has already been
     * used by the application to specify the {@link UUID} then that
     * {@link UUID} is noted. Otherwise, a random {@link UUID} is generated and
     * assigned to the query by binding it on the query hints.
     * <p>
     * Note: The ability to provide metadata from the {@link ASTContainer} in
     * the {@link StatusServlet} or the "EXPLAIN" page depends on the ability to
     * cross walk the queryIds as established by this method.
     * 
     * @param query
     *            The query.
     * 
     * @param queryUuid
     * 
     * @return The {@link UUID} which will be associated with the
     *         {@link IRunningQuery} and never <code>null</code>.
     */
    protected UUID setQueryId(final ASTContainer astContainer, UUID queryUuid) {

        // Figure out the effective UUID under which the query will run.
        final String queryIdStr = astContainer.getQueryHint(QueryHints.QUERYID);
        if (queryIdStr == null) {
            // Not specified, so generate and set on query hint.
            queryUuid = UUID.randomUUID();
        } 

        astContainer.setQueryHint(QueryHints.QUERYID, queryUuid.toString());

        return queryUuid;
    }
    
    /**
     * Wrapper method to clean up query and throw exception is interrupted.
     * 
     * @param queryId
     * @throws QueryCancelledException
     */
    protected void finalizeQuery(final UUID queryId) throws QueryCancelledException {

        if (queryId == null)
            return;

        // Need to call before tearDown
        final boolean isQueryCancelled = isQueryCancelled(queryId);

        tearDownQuery(queryId);

        if (isQueryCancelled) {

            if (log.isDebugEnabled()) {
                log.debug(queryId + " execution canceled.");
            }

            throw new QueryCancelledException(queryId + " execution canceled.", queryId);
        }

    }

    /**
     * The currently executing queries (does not include queries where a client
     * has established a connection but the query is not running because the
     * {@link #queryService} is blocking).
     * <p>
     * Note: This includes both SPARQL QUERY and SPARQL UPDATE requests.
     * However, the {@link AbstractQueryTask#queryUuid} might not yet be bound
     * since it is not set until the request begins to execute. See
     * {@link AbstractQueryTask#setQueryId(ASTContainer)}.
     */
    private static final 
    ConcurrentHashMap<String/* extQueryId */, RunningQuery> queries = new ConcurrentHashMap<>();

    /**
     * The currently executing QUERY and UPDATE requests.
     * <p>
     * Note: This does not include requests where a client has established a
     * connection to the SPARQL end point but the request is not executing
     * because the {@link #queryService} is blocking).
     * <p>
     * Note: This collection was introduced because the SPARQL UPDATE requests
     * are not executed on the {@link QueryEngine} and hence we can not use
     * {@link QueryEngine#getRunningQuery(UUID)} to resolve the {@link Future}
     */
    private static final 
    ConcurrentHashMap<UUID/* queryUuid */, RunningQuery> queries2 = new ConcurrentHashMap<>();

    
    public RunningQuery getQuery(final UUID queryUuid) {
        return queries2.get(queryUuid);
    }

    public RunningQuery getQuery(String extQueryId) {
        return queries.get(extQueryId);
    }
    
    /**
     * 
     * Remove the query from the internal queues.
     * 
     */
    protected void tearDownQuery(UUID queryId) {
        
        if (queryId != null) {
            
            if(log.isDebugEnabled()) {
                log.debug("Tearing down query: " + queryId );
                log.debug("queries2 has " + queries2.size());
            }

            final RunningQuery r = queries2.get(queryId);

            if (r != null) {
                queries.remove(r.getExtQueryId(), r);
                queries2.remove(queryId);

                if (log.isDebugEnabled()) {
                    log.debug("Tearing down query: " + queryId);
                    log.debug("queries2 has " + queries2.size());
                }
            }

        }

    }
    
    /**
     * Helper method to determine if a query was cancelled.
     * 
     * @param queryId
     * @return
     */
    protected boolean isQueryCancelled(final UUID queryId) {

        if (log.isDebugEnabled()) {
            log.debug(queryId);
        }
        
        RunningQuery q = getQuery(queryId);

        if (log.isDebugEnabled() && q != null) {
            log.debug(queryId + " isCancelled: " + q.isCancelled());
        }

        if (q != null) {
            return q.isCancelled();
        }

        return false;
    }
    
    public String runningQueriesToString()
    {
        final Collection<RunningQuery> queries = queries2.values();
        
        final Iterator<RunningQuery> iter = queries.iterator();
        
        final StringBuffer sb = new StringBuffer();
    
        while(iter.hasNext()){
            final RunningQuery r = iter.next();
            sb.append(r.getQueryUuid() + " : \n" + r.getExtQueryId());
        }
        
        return sb.toString();
        
    }
    
    public Collection<RunningQuery> getRunningQueries() {
        final Collection<RunningQuery> queries = queries2.values();

        return queries;
    }

    public void cancel(final UUID queryId) {
        Objects.requireNonNull(queryId);
        
        QueryCancellationHelper.cancelQuery(queryId, this.getQueryEngine());

        RunningQuery q = getQuery(queryId);

        if(q != null) {
            //Set the status to cancelled in the internal queue.
            q.setCancelled(true);
        }
    }

    public void cancel(final String uuid) {
        cancel(UUID.fromString(uuid));
    }

    public void cancel(final RunningQuery rQuery) {
        if (rQuery != null) {
            final UUID queryId = rQuery.getQueryUuid();
            cancel(queryId);
        }
    }

    /**
     * <strong>DO NOT INVOKE FROM APPLICATION CODE</strong> - this method
     * deletes the KB instance and destroys the backing database instance. It is
     * used to help tear down unit tests.
     */
    public void __tearDownUnitTest() {
        repo.getSail().__tearDownUnitTest();
    }

}
