/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

Contact:
     SYSTAP, LLC
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@systap.com

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.blazegraph.gremlin.embedded;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.openrdf.model.Statement;
import org.openrdf.query.BindingSet;
import org.openrdf.query.GraphQueryResult;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResult;

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
import com.bigdata.relation.accesspath.AbstractArrayBuffer;
import com.bigdata.striterator.ChunkedArrayIterator;
import com.bigdata.util.MillisecondTimestampFactory;
import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.listener.BlazeGraphEdit.Action;
import com.blazegraph.gremlin.listener.BlazeGraphListener;
import com.blazegraph.gremlin.structure.BlazeGraph;
import com.blazegraph.gremlin.util.Code;
import com.blazegraph.gremlin.util.LambdaLogger;

/**
 * An implementation of the tinkerpop3 API that uses an embedded SAIL repository
 * instance (same JVM).
 * <p/>
 * Currently BlazeGraphEmbedded is the only concrete implementation of the
 * Blazegraph Tinkerpop3 API. BlazeGraphEmbedded is backed by an embedded (same
 * JVM) instance of Blazegraph. This puts the enterprise features of Blazegraph
 * (high-availability, scale-out, etc.) out of reach for the 1.0 version of the
 * TP3 integration, since those features are accessed via Blazegraph's
 * client/server API. A TP3 integration with the client/server version of
 * Blazegraph is reserved for a future blazegraph-tinkerpop release.
 * <p/>
 * Blazegraph's concurrency model is MVCC, which more or less lines up with
 * Tinkerpop's Transaction model. When you open a BlazeGraphEmbedded instance,
 * you are working with the unisolated (writer) view of the database. This view
 * supports Tinkerpop Transactions, and reads are done against the unisolated
 * connection, so uncommitted changes will be visible. A BlazeGraphEmbedded can
 * be shared across multiple threads, but only one thread can have a Tinkerpop
 * Transaction open at a time (other threads will be blocked until the
 * transaction is closed). A TP3 Transaction is automatically opened on any read
 * or write operation, and automatically closed on any commit or rollback
 * operation. The Transaction can also be closed manually, which you will need
 * to do after read operations to unblock other waiting threads.
 * <p/>
 * BlazegraphGraphEmbedded's database operations are thus single-threaded, but
 * Blazegraph/MVCC allows for many concurrent readers in parallel with both the
 * single writer and other readers. This is possible by opening a read-only view
 * that will read against the last commit point on the database. The read-only
 * view can be be accessed in parallel to the writer without any of the
 * restrictions described above. To get a read-only snapshot, use the following
 * pattern:
 * <p/>
 * <pre>
 * final BlazeGraphEmbedded unisolated = ...; 
 * final BlazeGraphReadOnly readOnly = unisolated.readOnlyConnection(); 
 * try { 
 *     // read operations against readOnly
 * } finally { 
 *     readOnly.close(); 
 * }
 * </pre>
 * <p/>
 * BlazeGraphReadOnly extends BlazeGraphEmbedded and thus offers all the same
 * operations, except write operations will not be permitted
 * (BlazeGraphReadOnly.tx() will throw an exception). You can open as many
 * read-only views as you like, but we recommend you use a connection pool so as
 * not to overtax system resources. Applications should be written with the
 * one-writer many-readers paradigm front of mind.
 * <p/>
 * Important: Make sure to close the read-only view as soon as you are done with
 * it.
 * 
 * @author mikepersonick
 */
public class BlazeGraphEmbedded extends BlazeGraph {

    private final transient static LambdaLogger log = LambdaLogger.getLogger(BlazeGraphEmbedded.class);

    static {
        /**
         * We do not want auto-commit for SPARQL Update.
         * 
         * TODO FIXME Make this a configurable property.
         */
        AST2BOpUpdate.AUTO_COMMIT = false;
    }

    /**
     * Open a BlazeGraphEmbedded (unisolated) instance wrapping the provided
     * SAIL repository with no additional configuration options.
     * 
     * @param repo
     *          an open and initialized repository
     * @return
     *          BlazeGraphEmbedded instance
     */
    public static BlazeGraphEmbedded open(final BigdataSailRepository repo) {
        return open(repo, new BaseConfiguration());
    }
    
    /**
     * Open a BlazeGraphEmbedded (unisolated) instance wrapping the provided
     * SAIL repository and using the supplied configuration.
     * 
     * @return
     *          an open and initialized repository
     * @param config
     *          additional configuration
     * @return
     *          instance
     */
    public static BlazeGraphEmbedded open(final BigdataSailRepository repo,
            final Configuration config) {
        Objects.requireNonNull(repo);
        if (!repo.getDatabase().isStatementIdentifiers()) {
            throw new IllegalArgumentException("BlazeGraph/TP3 requires statement identifiers.");
        }
        
        /*
         * Grab the last commit time and also check for clock skew.
         */
        final long lastCommitTime = lastCommitTime(repo);
        config.setProperty(BlazeGraph.Options.LIST_INDEX_FLOOR, lastCommitTime);

        return new BlazeGraphEmbedded(repo, config);
    }
    
    /**
     * Take a quick peek at the last commit time on the supplied journal file.
     * If it's ahead of our system time, then use that last commit time as a
     * lower bound on blaze's transaction timestamps. Otherwise just use the
     * system time as usual.  Guards against clock skew (sharing journals 
     * between machines with different clock times).
     */
    protected static long lastCommitTime(final BigdataSailRepository repo) {

        final long systemTime = System.currentTimeMillis();

        log.debug(() -> "temporarily setting lower bound to Long.MAX_VALUE: " + (Long.MAX_VALUE - 100));

        /*
         * Temporarily set the lower bound to something way way in the future in
         * case the journal really is ahead of our system clock.
         */
        MillisecondTimestampFactory.setLowerBound(Long.MAX_VALUE - 100);

        /*
         * Pull the last commit time from the journal.
         */
        // final long lastCommitTime =
        // journal.getRootBlockView().getLastCommitTime();
        final long lastCommitTime = repo.getDatabase().getIndexManager().getLastCommitTime();

        final long lowerBound;

        if (lastCommitTime > 0l && systemTime < lastCommitTime) {

            log.info(() -> "found clock skew, using last commit time: " + lastCommitTime);

            /*
             * If the journal has a last commit time and if that last commit
             * time is in the future relative to our system clock, use the last
             * commit time as a lower bound on blaze transaction timestamps.
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
    
    /**
     * Embedded SAIL repository.
     */
    protected final BigdataSailRepository repo;
    
    /**
     * Graph listeners.
     * 
     * @see {@link BlazeGraphListener}
     */
    protected final List<BlazeGraphListener> listeners = new CopyOnWriteArrayList<>();
    
    /**
     * Tinkerpop3 transaction objects - manages use of the unisolated
     * BigdataSailRepositoryConnection.
     */
    private final BlazeTransaction tx = new BlazeTransaction();

    /**
     * Listen for change events from the SAIL (RDF), convert to PG data 
     * ({@link BlazeGraphEdit}s), forward notifications to 
     * {@link BlazeGraphListener}s.
     */
    protected final ChangeLogTransformer listener = new ChangeLogTransformer();
    
    /**
     * Hidden constructor - use {@link #open(BigdataSailRepository, Configuration)}.
     * 
     * @return
     *          an open and initialized repository
     * @param config
     *          additional configuration
     */
    protected BlazeGraphEmbedded(final BigdataSailRepository repo,
            final Configuration config) {
        super(config);
        
        this.repo = repo;
    }
    
    /**
     * Open a read-only view of the data at the last commit point.  The read view
     * can be used for read operations in parallel with the write view (this
     * view) and other read views.  Read views do not supports Tinkerpop
     * Transactions or write operations.
     * <p/>
     * These should be bounded in number by your application and always
     * closed when done using them.
     *  
     * @return
     *      a read-only view on the last commit point
     */
    public BlazeGraphReadOnly readOnlyConnection() {
        if (closed) throw Exceptions.alreadyClosed();
        
        final BigdataSailRepositoryConnection cxn =
                Code.wrapThrow(() -> repo.getReadOnlyConnection());
        return new BlazeGraphReadOnly(repo, cxn, config);
    }
    
    /**
     * Return the {@link BlazeTransaction} instance.
     */
    @Override
    public BlazeTransaction tx() {
        if (closed) throw Exceptions.alreadyClosed();
        
        return tx;
    }
    
    /**
     * Tinkerpop3 Transaction object.  Wraps access to the unsiolated SAIL
     * connection in a thread local.  Access will be re-entrant, but the 
     * unisolated connection cannot be shared across multiple threads.  Threads
     * attempting to access the unisolated SAIL connection (via tx().open(),
     * which happens automatically on any write or read operation) will be
     * blocked by the connection owner thread until that thread closes it (via
     * tx().close(), which happens automatically on any commit or rollback
     * operation). 
     * 
     * @author mikepersonick
     */
    public class BlazeTransaction extends AbstractThreadLocalTransaction {

        private final 
        ThreadLocal<BigdataSailRepositoryConnection> tlTx = 
                ThreadLocal.withInitial(() -> null);
        
        private BlazeTransaction() {
            super(BlazeGraphEmbedded.this);
        }

        /**
         * True if this thread has the unisolated connection open.
         */
        @Override
        public boolean isOpen() {
            return (tlTx.get() != null);
        }
        
        /**
         * Grab the unisolated SAIL connection and attach the {@link ChangeLogTransformer}
         * to it.  This operation will block if the unisolated connection is
         * being held open by another thread.
         */
        @Override
        protected void doOpen() {
            Code.wrapThrow(() -> {
                final BigdataSailRepositoryConnection cxn =
                        repo.getUnisolatedConnection();
                cxn.addChangeLog(listener);
                tlTx.set(cxn);   
            });
        }

        /**
         * Commit and close the unisolated connection (if open).
         */
        @Override
        protected void doCommit() throws TransactionException {
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                try {
                    cxn.commit();
                } catch (Exception ex) {
                    throw new TransactionException(ex);
                } finally {
                    /*
                     * Close the connection on commit per the required
                     * semantics of Tinkerpop3.
                     */
                    close(cxn);
                }
            }
        }

        /**
         * Rollback and close the unisolated connection (if open).
         */
        @Override
        protected void doRollback() throws TransactionException {
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                try {
                    cxn.rollback();
                } catch (Exception ex) {
                    throw new TransactionException(ex);
                } finally {
                    /*
                     * Close the connection on rollback.  Not safe to re-use.
                     */
                    close(cxn);
                }
            }
        }

        /**
         * Close the unisolated connection (if open), releasing it for use
         * by other threads.
         */
        @Override
        protected void doClose() {
            super.doClose();
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                close(cxn);
            }
        }
        
        /**
         * Internal close operation.  Remove the change listener, remove
         * the connection from the {@link ThreadLocal}, close the connection.
         */
        private void close(final BigdataSailRepositoryConnection cxn) {
            cxn.removeChangeLog(listener);
            tlTx.remove();
            Code.wrapThrow(() -> cxn.close());
        }
        
        /**
         * Direct access to the unisolated connection.  May return null if the
         * connection has not been opened yet by this thread.
         * 
         * @return unisolated {@link BigdataSailRepositoryConnection}
         */
        public BigdataSailRepositoryConnection cxn() {
            return tlTx.get();
        }
        
        /**
         * Flush the statement buffers to the indices without committing.
         */
        public void flush() {
            final BigdataSailRepositoryConnection cxn = tlTx.get();
            if (cxn != null) {
                Code.wrapThrow(() -> cxn.flush());
            }
        }
        
    }
    
    /**
     * Listen for change events from the SAIL (RDF), convert to PG data 
     * ({@link BlazeGraphEdit}s), forward notifications to 
     * {@link BlazeGraphListener}s.
     */
    private class ChangeLogTransformer implements IChangeLog {
        
        /**
         * We need to buffer and materialize these.  Remove events come in
         * unmaterialized.  Default buffer size is 1000.
         */
        private final AbstractArrayBuffer<IChangeRecord> records = 
                new AbstractArrayBuffer<IChangeRecord>(1000, IChangeRecord.class, null) {

            @Override
            protected long flush(final int n, final IChangeRecord[] a) {
                /*
                 * Materialize, notify, close.
                 */
                try (Stream<IChangeRecord> s = materialize(n, a)) {
                    s.forEach(ChangeLogTransformer.this::notify);
                }
                return n;
            }

        };
        
        /**
         * Changed events coming from bigdata.
         */
        @Override
        public void changeEvent(final IChangeRecord record) {
            if (listeners.isEmpty())
                return;
            
            /*
             * Watch out for history change events.
             */
            if (record.getStatement().getStatementType() == StatementEnum.History) {
                return;
            }
            
            /*
             * Adds come in already materialized. Removes do not. We batch and
             * materialize in bulk.
             */
            records.add(record);
            
        }
        
        /**
         * Turn a change record into a graph edit and notify the graph listeners.
         * 
         * @param record
         *          Bigdata change record.
         */
        protected void notify(final IChangeRecord record) {
            if (listeners.isEmpty())
                return;

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
            
            return graphAtomTransform().apply(record.getStatement())
                        .map(atom -> new BlazeGraphEdit(action, atom));
            
        }
        
        /**
         * Materialize a batch of change records.  You MUST close this stream
         * when done.
         * 
         * @param n
         *          number of valid elements in the array
         * @param a
         *          array of {@link IChangeRecord}s
         * @return
         *          Same records with materialized values
         */
        protected Stream<IChangeRecord> materialize(final int n, final IChangeRecord[] a) {
            
            final AbstractTripleStore db = cxn().getTripleStore();

            /*
             * Collect up unmaterialized ISPOs out of the change records
             * (only the removes are unmaterialized).
             */
            int i = 0;
            final ISPO[] spos = new ISPO[n];
            for (IChangeRecord rec : a) {
                if (rec.getChangeAction() == ChangeAction.REMOVED)
                    spos[i++] = rec.getStatement();
            }

            /*
             * Use the database to resolve them into BigdataStatements
             */
            final BigdataStatementIterator it = db
                    .asStatementIterator(new ChunkedArrayIterator<ISPO>(i,
                            spos, null/* keyOrder */));

            /*
             * Stream the records, replacing removes with materialized versions
             * of same.
             */
            return Arrays.stream(a, 0, n).onClose(() -> it.close())
                     .map(r -> {
                         if (r.getChangeAction() == ChangeAction.REMOVED) {
                             final BigdataStatement stmt = it.next();
                             return new ChangeRecord(stmt, ChangeAction.REMOVED);
                         } else {
                             return r;
                         }
                     });
            
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
            if (listeners.isEmpty()) {
                records.reset();
            } else {
                records.flush();
                listeners.forEach(l -> l.transactionCommited(commitTime));
            }
        }
    
        /**
         * Notification of transaction aborted.
         */
        @Override
        public void transactionAborted() {
            if (listeners.isEmpty()) {
                records.reset();
            } else {
                records.flush();
                listeners.forEach(BlazeGraphListener::transactionAborted);
            }
        }
        
        @Override
        public void close() { 
            records.reset();
        }
        
    }
    
    /**
     * Return the unisolated SAIL connection.  Automatically opens the 
     * Tinkerpop3 Transaction if not already open.
     */
    @Override
    public BigdataSailRepositoryConnection cxn() {
        if (closed) throw Exceptions.alreadyClosed();

        tx.readWrite();
        return tx.cxn();
    }

    /**
     * Pass through to tx().commit().
     * 
     * @see {@link BlazeTransaction#commit}.
     */
    public void commit() {
        if (closed) throw Exceptions.alreadyClosed();
        
        tx().commit();
    }

    /**
     * Pass through to tx().rollbakc().
     * 
     * @see {@link BlazeTransaction#rollback}.
     */
    public void rollback() {
        if (closed) throw Exceptions.alreadyClosed();
        
        tx().rollback();
    }

    /**
     * Pass through to tx().flush().
     * 
     * @see {@link BlazeTransaction#flush}.
     */
    public void flush() {
        if (closed) throw Exceptions.alreadyClosed();
        
        tx().flush();
    }

    /**
     * Close the unisolated connection if open and close the repository.  Default
     * close behavior is to roll back any uncommitted changes.
     * 
     * @see {@link Transaction.CLOSE_BEHAVIOR}
     */
    protected volatile boolean closed = false;
    @Override
    public synchronized void close() {
        if (closed)
            return;
        
        tx.close();
        Code.wrapThrow(() -> repo.shutDown());
        closed = true;
    }
    
    /**
     * Add a {@link BlazeGraphListener}.
     * 
     * @param listener the listener
     */
    public void addListener(final BlazeGraphListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Remove a {@link BlazeGraphListener}.
     * 
     * @param listener the listener
     */
    public void removeListener(final BlazeGraphListener listener) {
        this.listeners.remove(listener);
    }

    /**
     * Return the RDF value factory.
     * 
     * @see {@link BigdataValueFactory}
     */
    public BigdataValueFactory rdfValueFactory() {
        return (BigdataValueFactory) repo.getValueFactory();
    }
    
    /**
     * Return the Sparql query engine.
     */
    private QueryEngine getQueryEngine() {
        final IIndexManager ndxManager = getIndexManager();
        final QueryEngine queryEngine = (QueryEngine) 
                QueryEngineFactory.getInstance().getQueryController(ndxManager);
        return queryEngine;
    }

    /**
     * Return the database index manager.
     */
    private IIndexManager getIndexManager() {
        return repo.getDatabase().getIndexManager();
    }

    /**
     * {@inheritDoc}
     * 
     * Logs the query at INFO and logs the optimized AST at TRACE.
     */
    @Override
    protected Stream<BindingSet> _select( 
            final String queryStr, final String extQueryId) {

        logQuery(queryStr);
        return Code.wrapThrow(() -> {
            final BigdataSailTupleQuery query = (BigdataSailTupleQuery) 
                    cxn().prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
            setMaxQueryTime(query);
            final UUID queryId = setupQuery(query.getASTContainer(), 
                                            QueryType.SELECT, extQueryId);
            
            sparqlLog.trace(() -> "optimized AST:\n"+query.optimize());
            
            /*
             * Result is closed automatically by GraphStreamer.
             */
            final TupleQueryResult result = query.evaluate();
            final Optional<Runnable> onClose = 
                    Optional.of(() -> finalizeQuery(queryId));
            return new GraphStreamer<>(result, onClose).stream();
        });
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * Logs the query at INFO and logs the optimized AST at TRACE.
     */
    @Override
    protected Stream<Statement> _project( 
            final String queryStr, final String extQueryId) {
        
        logQuery(queryStr);
        return Code.wrapThrow(() -> {
            final BigdataSailGraphQuery query = (BigdataSailGraphQuery) 
                    cxn().prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
            setMaxQueryTime(query);
            final UUID queryId = setupQuery(query.getASTContainer(), 
                                            QueryType.CONSTRUCT, extQueryId);
        
            sparqlLog.trace(() -> "optimized AST:\n"+query.optimize());
        
            /*
             * Result is closed automatically by GraphStreamer.
             */
            final GraphQueryResult result = query.evaluate();
            final Optional<Runnable> onClose = 
                    Optional.of(() -> finalizeQuery(queryId));
            return new GraphStreamer<>(result, onClose).stream();
        });
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * Logs the query at INFO.
     */
    @Override
    protected boolean _ask( 
            final String queryStr, final String extQueryId) {
        
        logQuery(queryStr);
        return Code.wrapThrow(() -> { /* try */ 
            final BigdataSailBooleanQuery query = (BigdataSailBooleanQuery) 
                    cxn().prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
            setMaxQueryTime(query);
            final UUID queryId = setupQuery(query.getASTContainer(), 
                                            QueryType.ASK, extQueryId);
        
//            sparqlLog.trace(() -> "optimized AST:\n"+query.optimize());
        
            try {
                return query.evaluate();
            } finally {
                finalizeQuery(queryId);
            }
        });
        
    }
    
    /**
     * {@inheritDoc}
     * 
     * Logs the query at INFO.
     */
    @Override
    protected void _update( 
            final String queryStr, final String extQueryId) {
        
        logQuery(queryStr);
        Code.wrapThrow(() -> {
            final BigdataSailUpdate query = (BigdataSailUpdate) 
                    cxn().prepareUpdate(QueryLanguage.SPARQL, queryStr);
            final UUID queryId = 
                    setupQuery(query.getASTContainer(), 
                               null /* QueryType.UPDATE */, extQueryId);
            
            try {
                query.execute();
            } finally {
                finalizeQuery(queryId);
            }
        });
        
    }
    
    /**
     * Chop queries at a max length for logging.
     */
    private void logQuery(final String queryStr) {
        sparqlLog.info(() -> "query:\n"+ 
                (queryStr.length() <= sparqlLogMax ? 
                        queryStr : queryStr.substring(0, sparqlLogMax)+" ..."));
    }

    /**
     * Dump all the statements in the store to a String.  Bypasses the SAIL,
     * so you must call {@link #flush()} to get an accurate picture.
     */
    public String dumpStore() throws Exception {
        return repo.getDatabase().dumpStore().toString();
    }
    
    /**
     * Count all the statements in the store.  Bypasses the SAIL,
     * so you must call {@link #flush()} to get an accurate picture.
     */
    public long statementCount() throws Exception {
        return repo.getDatabase().getStatementCount(true);
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
    private UUID setupQuery(final ASTContainer astContainer,
            final QueryType queryType, final String extId) {

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
    private UUID setQueryId(final ASTContainer astContainer, UUID queryUuid) {

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
    private void finalizeQuery(final UUID queryId) throws QueryCancelledException {

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

    /**
     * Get a running query by internal query id.
     */
    private RunningQuery getQuery(final UUID queryUuid) {
        return queries2.get(queryUuid);
    }

    /**
     * Remove the query from the internal queues.
     */
    private void tearDownQuery(UUID queryId) {
        
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
     */
    private boolean isQueryCancelled(final UUID queryId) {

        if (log.isDebugEnabled()) {
            log.debug(queryId);
        }
        
        final RunningQuery q = getQuery(queryId);

        if (log.isDebugEnabled() && q != null) {
            log.debug(queryId + " isCancelled: " + q.isCancelled());
        }

        if (q != null) {
            return q.isCancelled();
        }

        return false;
    }

    /**
     * Return a list of all running queries in string form.
     */
    public String runningQueriesToString() {
        return queries2.values().stream()
                .map(r -> r.getQueryUuid() + " : \n" + r.getExtQueryId())
                .collect(Collectors.joining("\n"));
    }
    
    /**
     * Return a list of all running queries.
     */
    @Override
    public Collection<RunningQuery> getRunningQueries() {
        return queries2.values();
    }

    /**
     * Cancel a running query by internal id.
     */
    @Override
    public void cancel(final UUID queryId) {
        Objects.requireNonNull(queryId);
        
        QueryCancellationHelper.cancelQuery(queryId, this.getQueryEngine());

        final RunningQuery q = getQuery(queryId);

        if(q != null) {
            //Set the status to cancelled in the internal queue.
            q.setCancelled(true);
        }
    }

    /**
     * Cancel a running query.
     */
    @Override
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
