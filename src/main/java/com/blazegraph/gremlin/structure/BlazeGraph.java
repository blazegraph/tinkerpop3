package com.blazegraph.gremlin.structure;

import static com.blazegraph.gremlin.util.Lambdas.toMap;
import static java.util.stream.Collectors.toList;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Query;
import org.openrdf.repository.RepositoryConnection;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.RDRHistory;
import com.bigdata.rdf.sail.model.RunningQuery;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.blazegraph.gremlin.listener.BlazeGraphAtom;
import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.listener.BlazeGraphEdit.Action;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.Code;
import com.blazegraph.gremlin.util.LambdaLogger;
import com.blazegraph.gremlin.util.Lambdas;
import com.blazegraph.gremlin.util.Streams;

import info.aduna.iteration.CloseableIteration;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn("com.blazegraph.gremlin.structure.StructureModifiedSuite")
public abstract class BlazeGraph implements Graph {
    
//    static {
//        Thread.setDefaultUncaughtExceptionHandler((thread,throwable) -> {
//            throwable.printStackTrace();
//            System.exit(0);
//        });
//    }

    protected final transient static LambdaLogger log = LambdaLogger.getLogger(BlazeGraph.class);
    
    protected final transient static LambdaLogger sparqlLog = LambdaLogger.getLogger(BlazeGraph.class.getName() + ".SparqlLog");
    
    /**
     * Maximum number of chars to print through the SparqlLogger.
     */
    public static final int SPARQL_LOG_MAX = 10000;
    

    
    public static interface Options {
        
        String VALUE_FACTORY = BlazeGraph.class.getName() + ".valueFactory";
        
        String READ_FROM_WRITE_CXN = BlazeGraph.class.getName() + ".readFromWriteCxn";
        
        String LAST_COMMIT_TIME = BlazeGraph.class.getName() + ".lastCommitTime";
        
        String MAX_QUERY_TIME = BlazeGraph.class.getName() + ".maxQueryTime";
        
    }
    
    private final BlazeValueFactory vf;
    
    private final Configuration config;
    
    private final SparqlGenerator sparql;
    
    /**
     * Counter for Cardinality.list vertex property ids, which also serve as 
     * their list index.  By starting at System.currentTimeMillis() we are 
     * guaranteed to have monotonically increasing ids/indices
     * for a given vertex/key, assuming no system clock skew.  
     * 
     * We could use the last commit time on the underlying journal instead,
     * which would then make us impervious to bad system clock times.
     */
    private final AtomicLong vpIdFactory;
    
    /**
     * Max Query Time used to globally set the query timeout.
     * 
     * Default is 0 (unlimited)
     */
    private final int maxQueryTime;
    
    /**
     * URI used for labeling elements.
     */
    private final URI LABEL;
    
    /**
     * URI used for list item values.
     */
    private final URI VALUE;
    
    protected final Transforms transforms;
    
    private transient volatile boolean readFromWriteCxn = false;
    
    private transient volatile boolean bulkLoad = false;
    
    protected BlazeGraph(final Configuration config) {
        this.config = config;
        
        this.vf = Optional.ofNullable((BlazeValueFactory) 
                                config.getProperty(Options.VALUE_FACTORY))
                          .orElse(DefaultBlazeValueFactory.INSTANCE);
        this.sparql = new SparqlGenerator(this.vf);
        
        this.readFromWriteCxn = config.getBoolean(
                Options.READ_FROM_WRITE_CXN, false);
        
        final long lastCommitTime = config.getLong(
                Options.LAST_COMMIT_TIME, System.currentTimeMillis());
        this.vpIdFactory = new AtomicLong(lastCommitTime);
        
        this.maxQueryTime = config.getInt(Options.MAX_QUERY_TIME, 0);
        
        this.LABEL = vf.labelURI();
        this.VALUE = vf.valueURI();
        
        this.transforms = new Transforms();
    }
    
    /**
     * Return the factory used to round-trip between Tinkerpop values and
     * RDF values.
     */
    public BlazeValueFactory valueFactory() {
        return vf;
    }
    
    /**
     * RDF value factory for Sesame model objects.
     */
    public abstract BigdataValueFactory rdfValueFactory();
    
    /**
     * Different implementations will return different types of connections
     * depending on the mode (client/server, embedded, read-only, etc.)
     */
    protected abstract RepositoryConnection writeCxn();
    
    /**
     * A read-only connection can be used for read operations without blocking
     * or being blocked by writers.
     */
    protected abstract RepositoryConnection readCxn();
    
    public boolean isReadFromWriteCxn() {
        return readFromWriteCxn;
    }
    
    public void setReadFromWriteCxn(final boolean readFromWriteCxn) {
        this.readFromWriteCxn = readFromWriteCxn;
    }
    
    /**
     * Execute the supplied code fragement and read from the write connection 
     * during its execution.
     */
    public void readFromWriteCxn(final Code code) {
        if (isReadFromWriteCxn()) {
            Code.wrapThrow(code);
        } else {
            setReadFromWriteCxn(true);
            Code.wrapThrow(code, () -> setReadFromWriteCxn(false));
        }
    }
    
    public boolean isBulkLoad() {
        return bulkLoad;
    }
    
    public void setBulkLoad(final boolean bulkLoad) {
        this.bulkLoad = bulkLoad;
    }
    
    /**
     * Execute the supplied code fragment in bulk load mode.
     */
    public void bulkLoad(final Code code) {
        if (isBulkLoad()) {
            Code.wrapThrow(code);
        } else {
            setBulkLoad(true);
            Code.wrapThrow(code, () -> setBulkLoad(false));
        }
    }
    
    public RepositoryConnection openRead() {
        return readFromWriteCxn ? writeCxn() : readCxn();
    }
    
    public void closeRead(final RepositoryConnection cxn) {
        Code.wrapThrow(() -> { 
            if (!readFromWriteCxn) cxn.close(); 
        });
    }
    
    @Override
    public BlazeVertex addVertex(final Object... kvs) {
        ElementHelper.legalPropertyKeyValueArray(kvs);
        final Optional<Object> suppliedId = validateSuppliedId(kvs);
        
        final String id = suppliedId.map(String.class::cast)
                                    .orElse(UUID.randomUUID().toString());
        final String label = ElementHelper.getLabelValue(kvs)
                                          .orElse(Vertex.DEFAULT_LABEL);
        ElementHelper.validateLabel(label);
        
        if (!bulkLoad) {
            final Optional<BlazeVertex> existing = vertex(id);
            if (existing.isPresent()) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
            }
        }

        log.info(() -> "v("+id+", "+label+")");
        
        final BigdataValueFactory rdfvf = rdfValueFactory();
        final BigdataURI uri = rdfvf.asValue(vf.elementURI(id));
        final Literal rdfLabel = rdfvf.asValue(vf.toLiteral(label));
        
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            cxn.add(uri, vf.labelURI(), rdfLabel);
        });
        
        final BlazeVertex vertex = new BlazeVertex(this, uri, rdfLabel);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.set, kvs);
        return vertex;
    }

    BlazeEdge addEdge(final BlazeVertex from, final BlazeVertex to,
            final String label, final Object... kvs) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(kvs);
        final Optional<Object> suppliedId = validateSuppliedId(kvs);
        
        final String id = suppliedId.map(String.class::cast)
                                    .orElse(UUID.randomUUID().toString());

        if (!bulkLoad) {
            final Optional<BlazeEdge> existing = edge(id);
            if (existing.isPresent()) {
                throw Graph.Exceptions.vertexWithIdAlreadyExists(id);
            }
        }

        log.info(() -> "v("+from+")-e("+id+", "+label+")->v("+to+")");
        
        final BigdataValueFactory rdfvf = rdfValueFactory();
        
        final URI uri = rdfvf.asValue(vf.elementURI(id));
        final Literal rdfLabel = rdfvf.asValue(vf.toLiteral(label));
        
        final BigdataStatement edgeStmt = 
                rdfvf.createStatement(from.rdfId(), uri, to.rdfId());
        
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            // blaze:person:1 blaze:knows:7 blaze:person:2 .
            cxn.add(edgeStmt);
            // <<blaze:person:1 blaze:knows:7 blaze:person:2>> rdfs:label "knows" .
            cxn.add(rdfvf.createBNode(edgeStmt), vf.labelURI(), rdfLabel);
        });
        
        final BlazeEdge edge = new BlazeEdge(this, edgeStmt, rdfLabel, from, to);
        ElementHelper.attachProperties(edge, kvs);
        return edge;
    }
    
    <V> BlazeProperty<V> property(final BlazeReifiedElement element, 
            final String key, final V val) {
        final BigdataValueFactory rdfvf = rdfValueFactory();
        final BigdataBNode s = element.rdfId();
        final URI p = rdfvf.asValue(vf.propertyURI(key));
        final Literal lit = rdfvf.asValue(vf.toLiteral(val));
        
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            final BigdataStatement stmt = rdfvf.createStatement(s, p, lit);
            if (!bulkLoad) {
                // remove (<<stmt>> <key> ?)
                cxn.remove(s, p, null);
            }
            // add (<<stmt>> <key> "val")
            cxn.add(stmt);
        });
        
        final BlazeProperty<V> prop = new BlazeProperty<V>(this, element, p, lit);
        return prop;
    }
    
    <V> BlazeVertexProperty<V> vertexProperty(final BlazeVertex vertex, 
            final String key, final V val, final Cardinality cardinality) {
        final BigdataValueFactory rdfvf = rdfValueFactory();
        final URI s = vertex.rdfId();
        final URI p = rdfvf.asValue(vf.propertyURI(key));
        final Literal lit = rdfvf.asValue(vf.toLiteral(val));
        
        final BigdataStatement stmt;
        if (cardinality == Cardinality.list) {
            final Literal timestamp = rdfvf.createLiteral(
                    String.valueOf(vpIdFactory.getAndIncrement()), 
                    PackedLongIV.PACKED_LONG);
            stmt = rdfvf.createStatement(s, p, timestamp);
        } else {
            stmt = rdfvf.createStatement(s, p, lit);
        }
        final BigdataBNode sid = rdfvf.createBNode(stmt);
        final String vpId = vertexPropertyId(stmt);
        
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            if (cardinality == Cardinality.list) {
                // <s> <key> timestamp .
                cxn.add(stmt);
                // <<<s> <key> timestamp>> rdf:value "val" .
                cxn.add(sid, vf.valueURI(), lit);
            } else {
                if (cardinality == Cardinality.single && !bulkLoad) {
                    // need to clean 
                    // <s> <p> ?o . filter (?o != <o>) .
                    // <<<s> <p> ?o>> ?pp ?oo .
                    final String queryStr = sparql.cleanVertexProps(s, p, lit);
                    update(cxn, queryStr, nextQueryId());
                }
                // << stmt >> <key> "val" .
                cxn.add(stmt);
            }
        });
        
        final BlazeProperty<V> prop = 
                new BlazeProperty<>(this, vertex, p, lit);
        final BlazeVertexProperty<V> bvp =
                new BlazeVertexProperty<>(prop, vpId, sid);
        return bvp;
    }
    
    private final String VERTEX_PROPERTY_ID = "<<%s, %s, %s>>";
    String vertexPropertyId(final URI s, final URI p, final Literal o) {
        return String.format(VERTEX_PROPERTY_ID, s.toString(), p.toString(), o.toString());
    }
    
    String vertexPropertyId(final Statement stmt) {
        return vertexPropertyId((URI) stmt.getSubject(), 
                                (URI) stmt.getPredicate(), 
                                (Literal) stmt.getObject());
    }
    
    protected final Optional<Object> validateSuppliedId(final Object... kvs) {
        final Optional<Object> suppliedId = ElementHelper.getIdValue(kvs);
        if (suppliedId.isPresent() && !(suppliedId.get() instanceof String)) {
            throw Vertex.Exceptions.userSuppliedIdsOfThisTypeNotSupported();
        }
        return suppliedId;
    }
    
    void remove(final BlazeVertex vertex) {
        try (CloseableIterator<Edge> it = vertex.edges(Direction.BOTH)) {
            it.forEachRemaining(e -> {
                remove((BlazeReifiedElement) e);
            });
        }
        try (CloseableIterator<VertexProperty<Object>> it = vertex.properties()) {
            it.forEachRemaining(vp -> {
                remove((BlazeReifiedElement) vp);
            });
        }
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            final URI uri = vertex.rdfId();
            cxn.remove(uri, null, null);
        });
    }

    void remove(final BlazeReifiedElement element) {
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            final BigdataBNode sid = element.rdfId();
            final BigdataStatement stmt = sid.getStatement();
            cxn.remove(stmt);
            cxn.remove(sid, null, null);
        });
    }
    
    <V> void remove(final BlazeProperty<V> prop) {
        final RepositoryConnection cxn = writeCxn();
        Code.wrapThrow(() -> {
            cxn.remove(prop.s(), prop.p(), prop.o());
        });
    }
    
    public int vertexCount() {
        final String queryStr = sparql.vertexCount();
        return count(queryStr);
    }
    
    public Optional<BlazeVertex> vertex(final Object vertexId) {
        try (final CloseableIterator<Vertex> it = vertices(vertexId)) {
            final Optional<BlazeVertex> v = it.hasNext() ? 
                    Optional.of((BlazeVertex) it.next()) : Optional.empty();
            if (it.hasNext()) {
                throw new IllegalStateException("Multiple vertices found with id: " + vertexId);
            }
            return v;
        }
    }
    
    @Override
    public CloseableIterator<Vertex> vertices(final Object... vertexIds) {
        final List<URI> uris = validateIds(vertexIds);
        final String queryStr = sparql.vertices(uris);

        final RepositoryConnection cxn = openRead();
        return Code.wrapThrow(() -> { 
            final Stream<Vertex> stream = select(cxn, queryStr, nextQueryId())
                    .map(transforms.vertex);
            return CloseableIterator.of(stream);
        });
    }
    
    protected String nextQueryId() {
        return UUID.randomUUID().toString();
    }
    
    public int edgeCount() {
        final String queryStr = sparql.edgeCount();
        return count(queryStr);
    }
    
    public Optional<BlazeEdge> edge(final Object edgeId) {
        try (final CloseableIterator<Edge> it = edges(edgeId)) {
            final Optional<BlazeEdge> e = it.hasNext() ? 
                    Optional.of((BlazeEdge) it.next()) : Optional.empty();
            if (it.hasNext()) {
                throw new IllegalStateException("Multiple edges found with id: " + edgeId);
            }
            return e;
        }
    }
    
    @Override
    public CloseableIterator<Edge> edges(final Object... edgeIds) {
        final List<URI> uris = validateIds(edgeIds);
        final String queryStr = sparql.edges(uris);
        
        final RepositoryConnection cxn = openRead();
        return Code.wrapThrow(() -> {
            final Stream<Edge> stream = select(cxn, queryStr, nextQueryId())
                    .map(transforms.edge);
            return CloseableIterator.of(stream);
        });
//        return Code.wrapThrow(() -> 
//            selectResolve(queryStr, (cxn, result) -> new EdgeIterator(cxn, result))
//        );
    }
    
//    private <E extends Element> Iterator<E> elements(final String queryStr,
//            final BiFunction<RepositoryConnection, TupleQueryResult, Iterator<E>> iterator) {
//        final RepositoryConnection cxn = openRead();
//        final Code _finally = () -> closeRead(cxn);
//        
//        return Code.wrapThrow(() -> {
//            final TupleQueryResult result = select(cxn, queryStr);
//            return iterator.apply(cxn, result);
//        }, _finally);            
//    }
//    
//    private <R> CloseableIterator<R> selectResolve(final String queryStr,
//            final BiFunction<RepositoryConnection, TupleQueryResult, CloseableIterator<R>> resolver) 
//                    throws Exception {
//        final RepositoryConnection cxn = openRead();
//        try {
//            final TupleQueryResult result = select(cxn, queryStr);
//            return resolver.apply(cxn, result);
//        } catch (Exception ex) {
//            /*
//             * If there is an exception while preparing/evaluating the
//             * query (before we get a TupleQueryResult) or during construction
//             * of the closeable iterator then we must close the read connection 
//             * ourselves.
//             */
//            closeRead(cxn);
//            throw ex;
//        }
//    }
    
    CloseableIterator<Edge> edgesFromVertex(final BlazeVertex src, 
            final Direction dir, final String... edgeLabels) {
        final List<Literal> lits = 
                Stream.of(edgeLabels).map(vf::toLiteral).collect(toList());
        final String queryStr = sparql.edgesFromVertex(src, dir, lits);
        
        final RepositoryConnection cxn = openRead();
        return Code.wrapThrow(() -> {
            final Stream<Edge> stream = select(cxn, queryStr, nextQueryId())
                    .map(transforms.edge);
            return CloseableIterator.of(stream);
        });
//        return Code.wrapThrow(() -> 
//            selectResolve(queryStr, (cxn, result) -> new EdgeIterator(cxn, result))
//        );
//      }, () -> closeRead(cxn));
//        return elements(queryStr, (cxn, result) -> new EdgeIterable(cxn, result));
    }
    
    <V> CloseableIterator<Property<V>> properties(
            final BlazeReifiedElement element, final String... keys) {
        // read s ?p ?o where ?p != rdfs:label && isLiteral(?o)
        // supply values (or not) for ?p
        
        final List<URI> uris = 
                Stream.of(keys).map(vf::propertyURI).collect(toList());
        final String queryStr = sparql.properties(element, uris);
        
        final RepositoryConnection cxn = openRead();
        return Code.wrapThrow(() -> { 
            final Stream<Property<V>> stream = select(cxn, queryStr, nextQueryId())
                    .map(transforms.<V>property(element)); 
            return CloseableIterator.of(stream);
        });
//        return Code.wrapThrow(() -> 
//            selectResolve(queryStr, (cxn, result) -> new PropertyIterator<>(cxn, result, element))
//        );
    }
    
    <V> CloseableIterator<VertexProperty<V>> properties(
            final BlazeVertex vertex, final String... keys) {
        // read s ?p ?o where ?p != rdfs:label && isLiteral(?o)
        // supply values (or not) for ?p
        
        final List<URI> uris = 
                Stream.of(keys).map(vf::propertyURI).collect(toList());
        final String queryStr = sparql.properties(vertex, uris);
        
        final RepositoryConnection cxn = openRead();
        return Code.wrapThrow(() -> {
            final Stream<VertexProperty<V>> stream = select(cxn, queryStr, nextQueryId())
                    .map(transforms.<V>vertexProperty(vertex)); 
            return CloseableIterator.of(stream);
        });
//        return Code.wrapThrow(() -> 
//            selectResolve(queryStr, (cxn, result) -> new VertexPropertyIterator<>(cxn, result, vertex))
//        );
    }
    
//    protected TupleQueryResult select(final RepositoryConnection cxn, 
//            final String queryStr) throws Exception {
//        log.debug(() -> "\n"+queryStr);
//        final TupleQuery tq = 
//                cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
//        return tq.evaluate();
//    }
    
    protected int count(final String queryStr) {
//        final TupleQueryResult tqr = select(cxn, queryStr);
//        try {
//            final BindingSet bs = tqr.next();
//            return ((Literal) bs.getValue("count")).intValue();
//        } finally {
//            tqr.close();
//        }
        final RepositoryConnection cxn = openRead();
        return Code.wrapThrow(
            () -> { /* try */ 
                try (Stream<BindingSet> stream = select(cxn, queryStr, nextQueryId())) {
                    return stream.map(bs -> (Literal) bs.getValue("count"))
                                 .map(Literal::intValue)
                                 .findFirst().get();
                }
            },  
            () -> closeRead(cxn) /* finally */
        );
    }
    
//    protected void update(final RepositoryConnection cxn, 
//            final String queryStr) throws Exception {
//        log.debug(() -> "\n"+queryStr);
//        cxn.prepareUpdate(QueryLanguage.SPARQL, queryStr).execute();
//    }
    
    protected List<URI> validateIds(final Object... elementIds) {
        ElementHelper.validateMixedElementIds(Element.class, elementIds);
        return Stream.of(elementIds)
                .map(elementId -> {
                    if (elementId instanceof String) {
                        return (String) elementId;
                    } else if (elementId instanceof Element) {
                        final Object id = ((Element) elementId).id();
                        if (id instanceof String) {
                            return (String) id;
                        }
                    }
                    throw new IllegalArgumentException("Unknown element id type: " + elementId + " ("+ elementId.getClass()+")");
                })
                .map(vf::elementURI)
                .collect(Collectors.toList());
    }
    
//    public abstract class GraphIterator<T,E> implements CloseableIterator<T> {
//
//        private final RepositoryConnection cxn;
//        
//        private final CloseableIteration<E,?> result;
//        
//        private final UUID queryId;
//        
//        public GraphIterator(final RepositoryConnection cxn,
//                final CloseableIteration<E,?> result) {
//            this(cxn, result, null);
//        }
//        
//        public GraphIterator(final RepositoryConnection cxn,
//                final CloseableIteration<E,?> result,
//                final UUID queryId) {
//            this.cxn = cxn;
//            this.result = result;
//            this.queryId = queryId;
//        }
//        
//        @Override
//        public boolean hasNext() {
//            return Code.wrapThrow(() -> { 
//                return result.hasNext(); 
//            });
//        }
//
//        @Override
//        public T next() {
//            return Code.wrapThrow(
//                () -> { /* try */ 
//                    return convert(result.next());
//                }, 
//                () -> { /* finally */
//                    if (!hasNext()) {
//                        close();
//                    }
//                });
//        }
//        
//        @Override
//        public void close() {
//            Code.wrapThrow(
//                () -> { /* try */
//                    finalizeQuery(queryId);
//                    result.close();
//                }, 
//                () -> { /* finally */
//                    closeRead(cxn);
//                });
//        }
//        
//        protected abstract T convert(final E e);
//        
//    }
    
    protected class Transforms {
    
        protected final 
        Function<BindingSet, Vertex> vertex = bs -> {
            
            final BigdataURI uri = (BigdataURI) bs.getValue("id");
            final Literal label = (Literal) bs.getValue("label");
            final BlazeVertex vertex = new BlazeVertex(BlazeGraph.this, uri, label);
            return vertex;
            
        };
        
        protected final 
        Function<BindingSet, Edge> edge = bs -> {
            
            final BigdataBNode s = (BigdataBNode) bs.getValue("sid");
            final BigdataStatement stmt = s.getStatement();
            final Literal label = (Literal) bs.getValue("label");
//            if (bs.hasBinding("fromLabel") && bs.hasBinding("toLabel")) {
                final BigdataURI fromURI = (BigdataURI) stmt.getSubject();
                final Literal fromLabel = (Literal) bs.getValue("fromLabel");
                final BlazeVertex from = 
                        new BlazeVertex(BlazeGraph.this, fromURI, fromLabel);
                
                final BigdataURI toURI = (BigdataURI) stmt.getObject();
                final Literal toLabel = (Literal) bs.getValue("toLabel");
                final BlazeVertex to = 
                        new BlazeVertex(BlazeGraph.this, toURI, toLabel);
                return new BlazeEdge(BlazeGraph.this, stmt, label, from, to);
//            } else {
//                return new BlazeEdge(BlazeGraph.this, stmt, label);
//            }
                
        };
        
        protected final <V> Function<BindingSet, Property<V>> 
        property(final BlazeReifiedElement e) {
            
            return bs -> {
                log.debug(() -> bs);
                final URI p = (URI) bs.getValue("p");
                final Literal o = (Literal) bs.getValue("o");
                final BlazeProperty<V> prop = 
                        new BlazeProperty<>(BlazeGraph.this, e, p, o);
                return prop;
            };
            
        }
        
        protected final <V> Function<BindingSet, VertexProperty<V>> 
        vertexProperty(final BlazeVertex v) {
            
            return bs -> {
                final URI p = (URI) bs.getValue("p");
                final Literal o = (Literal) bs.getValue("o");
                final BigdataBNode sid = (BigdataBNode) bs.getValue("vp");
                final String vpId = vertexPropertyId(sid.getStatement());
                final BlazeProperty<V> prop = 
                        new BlazeProperty<>(BlazeGraph.this, v, p, o);
                final BlazeVertexProperty<V> bvp = 
                        new BlazeVertexProperty<>(prop, vpId, sid);
                return bvp;
            };
            
        }
        
        /**
         * Convert SPARQL/RDF results into PG form.
         */
        protected final 
        Function<BindingSet,BlazeBindingSet> bindingSet = bs -> {
            
            final Map<String, Object> map = bs.getBindingNames().stream()
                .map(key -> new SimpleEntry<String,Object>(key, bs.getBinding(key).getValue()))
                .map(e -> {
                    final String key = e.getKey();
                    final Object val = e.getValue();
                    final SimpleEntry<String,Object> _e;
                    if (val instanceof Literal) {
                        _e = new SimpleEntry<>(key, vf.fromLiteral((Literal) val));
                    } else if (val instanceof URI) {
                        _e = new SimpleEntry<>(key, vf.fromURI((URI) val));
                    } else {
                        final BigdataBNode sid = (BigdataBNode) val;
                        final BigdataStatement stmt = sid.getStatement();
                        if (stmt.getObject() instanceof URI) {
                            // return edge id
                            _e = new SimpleEntry<>(key, vf.fromURI(stmt.getPredicate()));
                        } else {
                            // return vertex property id
                            _e = new SimpleEntry<>(key, vertexPropertyId(stmt)); 
                        }
                    }
                    return _e;
                })
                .collect(toMap(LinkedHashMap::new));
                        
            return new BlazeBindingSet(map);
            
        };
        
        /**
         * Convert a unit of RDF data to an atomic unit of PG data.
         */
        protected final 
        Function<Statement, Optional<BlazeGraphAtom>> graphAtom = stmt -> {

            final Resource s = stmt.getSubject();
            final URI p = stmt.getPredicate();
            final Value o = stmt.getObject();
            
            if (s instanceof URI) {
                
                if (o instanceof URI) {
                    // blaze:a blaze:x blaze:b
                    
                    /*
                     * We actually want to ignore these and wait for
                     * <<blaze:a blaze:x blaze:b>> rdfs:label "label" .
                     * to emit a VertexAtom.
                     */
                    return Optional.empty();
                }
                
                final BigdataURI uri = (BigdataURI) s;
                final String vertexId = vf.fromURI(uri);
                final Literal lit = (Literal) o;
                
                if (LABEL.equals(p)) {
                    // blaze:a rdfs:label "label" .
                    
                    final String label = (String) vf.fromLiteral(lit);
                    
                    return Optional.of(
                            new BlazeGraphAtom.VertexAtom(vertexId, label));
                    
                } else {

                    final URI dt = lit.getDatatype();
                    
                    if (dt != null && PackedLongIV.PACKED_LONG.equals(dt)) {
                        // blaze:a blaze:key "0"^^"blaze:packedLong"
                        
                        /*
                         * We actually want to ignore these and wait for
                         * <<blaze:a blaze:key "0"^^"blaze:packedLong">> rdfs:value "val" .
                         * to emit a VertexPropertyAtom.
                         */
                        return Optional.empty();
                        
                    } else {
                        // blaze:a blaze:key "val" .
                        
                        final String key = vf.fromURI(p);
                        final Object val = vf.fromLiteral(lit);
                        final String vpId = vertexPropertyId(uri, p, lit);
                        
                        return Optional.of(
                            new BlazeGraphAtom.VertexPropertyAtom(vertexId, key, val, vpId));
                        
                    }
                    
                }
                
            } else { // s instanceof BigdataBNode
                
                final BigdataBNode sid = (BigdataBNode) s;
                final BigdataStatement reified = sid.getStatement();
                final Literal lit = (Literal) o;
                
                if (LABEL.equals(p)) {
                    // <<blaze:a blaze:x blaze:b>> rdfs:label "label" .
                    
                    final String edgeId = vf.fromURI(reified.getPredicate());
                    final String fromId = vf.fromURI((URI) reified.getSubject());
                    final String toId = vf.fromURI((URI) reified.getObject());
                    final String label = (String) vf.fromLiteral(lit);
                    
                    return Optional.of(
                        new BlazeGraphAtom.EdgeAtom(edgeId, label, fromId, toId));
                    
                } else if (VALUE.equals(p)) {
                    // <<blaze:a blaze:key "0"^^"blaze:packedLong">> rdfs:value "val" .
                    
                    final String vertexId = vf.fromURI((URI) reified.getSubject());
                    final String key = vf.fromURI(reified.getPredicate());
                    final Object val = vf.fromLiteral(lit);
                    final String vpId = vertexPropertyId(reified);
                    
                    return Optional.of(
                        new BlazeGraphAtom.VertexPropertyAtom(vertexId, key, val, vpId));

                } else {
                    
                    final String key = vf.fromURI(p);
                    final Object val = vf.fromLiteral(lit);
                    
                    if (reified.getObject() instanceof URI) {
                        // <<blaze:a blaze:x blaze:b>> blaze:key "val" .

                        final String edgeId = vf.fromURI(reified.getPredicate());
                        
                        return Optional.of(
                                new BlazeGraphAtom.PropertyAtom(edgeId, key, val));
                                    
                    } else {
                        // <<blaze:a blaze:key "val">> blaze:key "val" .
                        // <<blaze:a blaze:key "0"^^"blaze:packedLong">> blaze:key "val" .
                        
                        final String vpId = vertexPropertyId(reified);
                        
                        return Optional.of(
                            new BlazeGraphAtom.PropertyAtom(vpId, key, val));
                                
                    }
                    
                }
                
            }
            
        };
        
        protected final 
        Function<BindingSet,Optional<BlazeGraphEdit>> history = bs -> {
            
          final BigdataBNode sid = (BigdataBNode) bs.getValue("sid");
          final BigdataStatement stmt = sid.getStatement();
          final URI a = (URI) bs.getValue("action");
          final Literal t = (Literal) bs.getValue("time");
          
          if (!t.getDatatype().equals(XSD.DATETIME)) {
              throw new RuntimeException("Unexpected timestamp in result: " + bs);
          }
          
          final BlazeGraphEdit.Action action;
          if (a.equals(RDRHistory.Vocab.ADDED)) {
              action = Action.Add;
          } else if (a.equals(RDRHistory.Vocab.REMOVED)) {
              action = Action.Remove;
          } else {
              throw new RuntimeException("Unexpected action in result: " + bs);
          }
          
          final long timestamp = DateTimeExtension.getTimestamp(t.getLabel());
          return graphAtom.apply(stmt)
                          .map(atom -> new BlazeGraphEdit(action, atom, timestamp));
          
      };
      
    }
        
//    public class VertexIterator extends GraphIterator<Vertex, BindingSet> {
//        
//        public VertexIterator(final RepositoryConnection cxn,
//                final TupleQueryResult result) {
//            super(cxn, result);
//        }
//        
//        protected final Vertex convert(final BindingSet bs) {
//            return vertexTransform.apply(bs);
//        };
//        
//    }

//    public class EdgeIterator extends GraphIterator<Edge, BindingSet> {
//        
//        public EdgeIterator(final RepositoryConnection cxn,
//                final TupleQueryResult result) {
//            super(cxn, result);
//        }
//        
//        protected final Edge convert(final BindingSet bs) {
//            return edgeTransform.apply(bs);
//        };
//        
//    }
    
//    public class PropertyTransform<V> 
//            implements Function<BindingSet, Property<V>> {
//        
//        private final BlazeReifiedElement e;
//        
//        public PropertyTransform(final BlazeReifiedElement e) {
//            this.e = e;
//        }
//        
//        public Property<V> apply(final BindingSet bs) {
//            log.debug(() -> bs);
//            final URI p = (URI) bs.getValue("p");
//            final Literal o = (Literal) bs.getValue("o");
//            final BlazeProperty<V> prop = 
//                    new BlazeProperty<>(BlazeGraph.this, e, p, o);
//            return prop;
//        }
//    };
    
//    public class PropertyIterator<V> 
//            extends GraphIterator<Property<V>, BindingSet> {
//        
//        private final BlazeReifiedElement e;
//        
//        public PropertyIterator(final RepositoryConnection cxn,
//                final TupleQueryResult result, final BlazeReifiedElement e) {
//            super(cxn, result);
//            this.e = e;
//        }
//        
//        protected final Property<V> convert(final BindingSet bs) {
//            return new PropertyTransform<V>(e).apply(bs);
//        };
//        
//    }
    
//    public class VertexPropertyTransform<V> 
//            implements Function<BindingSet, VertexProperty<V>> {
//        
//        private final BlazeVertex v;
//        
//        public VertexPropertyTransform(final BlazeVertex v) {
//            this.v = v;
//        }
//        
//        public VertexProperty<V> apply(final BindingSet bs) {
//            final URI p = (URI) bs.getValue("p");
//            final Literal o = (Literal) bs.getValue("o");
//            final BigdataBNode sid = (BigdataBNode) bs.getValue("vp");
//            final String vpId = vertexPropertyId(sid.getStatement());
//            final BlazeProperty<V> prop = 
//                    new BlazeProperty<>(BlazeGraph.this, v, p, o);
//            final BlazeVertexProperty<V> bvp = 
//                    new BlazeVertexProperty<>(prop, vpId, sid);
//            return bvp;
//        };
//        
//    }

//    public class VertexPropertyIterator<V> 
//            extends GraphIterator<VertexProperty<V>, BindingSet> {
//        
//        private final BlazeVertex v;
//        
//        public VertexPropertyIterator(final RepositoryConnection cxn,
//                final TupleQueryResult result, final BlazeVertex v) {
//            super(cxn, result);
//            this.v = v;
//        }
//        
//        protected final VertexProperty<V> convert(final BindingSet bs) {
//            return new VertexPropertyTransform<V>(v).apply(bs);
//        };
//        
//    }
    
    @Override
    public abstract Transaction tx();

    @Override
    public abstract void close() throws Exception;

    @Override
    public Configuration configuration() {
        return config;
    }

    @Override
    public Features features() {
        return BlazeGraphFeatures.INSTANCE;
    }
    
    @Override
    public String toString() {
        return StringFactory.graphString(this, "vertices:" + vertexCount() + " edges:" + edgeCount());
    }
    
    /**
     * TODO FIXME
     */
    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    /**
     * TODO FIXME Implement GraphComputer over DASL API
     */
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * TODO FIXME Implement GraphComputer over DASL API
     */
    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    
    /**
     * Project a subgraph using a SPARQL query.
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public CloseableIterator<BlazeGraphAtom> project(final String queryStr) 
            throws Exception {
        return this.project(queryStr, nextQueryId());
    }
    
    /**
     * Project a subgraph using a SPARQL query.
     *
     * This version allows passing an external system ID to allow association
     * between queries in the query engine when using an Embedded Client.
     * 
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public CloseableIterator<BlazeGraphAtom> project(final String queryStr,
            String externalQueryId) throws Exception {
        
        final RepositoryConnection cxn = openRead();
        
//        if (sparqlLog.isTraceEnabled()) {
//            sparqlLog.trace("query:\n"+ (queryStr.length() <= SPARQL_LOG_MAX 
//                    ? queryStr : queryStr.substring(0, SPARQL_LOG_MAX)+" ..."));
//        }
//                
//        final GraphQueryResult result;
//        UUID queryId = null;
//
//        try {
//            
//            final org.openrdf.query.GraphQuery query = 
//                    cxn.prepareGraphQuery(QueryLanguage.SPARQL, queryStr);
//            
//            setMaxQueryTime(query);
//            
//            if (query instanceof BigdataSailGraphQuery
//                    && cxn instanceof BigdataSailRepositoryConnection) {
//
//                final BigdataSailGraphQuery bdtq = (BigdataSailGraphQuery) query;
//                queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
//                        bdtq.getASTContainer(), QueryType.CONSTRUCT,
//                        externalQueryId);
//            }
//        
//            if (sparqlLog.isTraceEnabled()) {
//                if (query instanceof BigdataSailGraphQuery) {
//                    final BigdataSailGraphQuery bdgq = (BigdataSailGraphQuery) query;
//                    sparqlLog.trace("optimized AST:\n"+bdgq.optimize());
//                }
//            }
//        
//            result = query.evaluate();
//
//        } catch (Exception ex) {
//            if (!readFromWriteCxn) {
//                cxn.close();
//            }
//            throw ex;
//        }
        
//        final IStriterator sitr = new Striterator(new WrappedResult<Statement>(
//                result, readFromWriteCxn ? null : cxn, queryId));
//        
//        sitr.addFilter(new Filter() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            public boolean isValid(final Object e) {
//                final Statement stmt = (Statement) e;
//                // do not project history
//                return stmt.getSubject() instanceof URI;
//            }
//        });
//        
//        sitr.addFilter(new Resolver() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            protected Object resolve(final Object e) {
//                final Statement stmt = (Statement) e;
//                return toGraphAtom(stmt);
//            }
//        });
//        
//        return (ICloseableIterator<BlazeGraphAtom>) sitr;

        final Stream<BlazeGraphAtom> stream =
                project(cxn, queryStr, externalQueryId)
                        .map(transforms.graphAtom)
                        .filter(Optional::isPresent)
                        .map(Optional::get);
        
        return CloseableIterator.of(stream);
        
    }
    
//    /**
//     * Convert a unit of RDF data to an atomic unit of PG data.
//     */
//    protected Optional<BlazeGraphAtom> toGraphAtom(final Statement stmt) {
//
//        final Resource s = stmt.getSubject();
//        final URI p = stmt.getPredicate();
//        final Value o = stmt.getObject();
//        
//        if (s instanceof URI) {
//            
//            if (o instanceof URI) {
//                // blaze:a blaze:x blaze:b
//                
//                /*
//                 * We actually want to ignore these and wait for
//                 * <<blaze:a blaze:x blaze:b>> rdfs:label "label" .
//                 * to emit a VertexAtom.
//                 */
//                return Optional.empty();
//            }
//            
//            final BigdataURI uri = (BigdataURI) s;
//            final String vertexId = vf.fromURI(uri);
//            final Literal lit = (Literal) o;
//            
//            if (LABEL.equals(p)) {
//                // blaze:a rdfs:label "label" .
//                
//                final String label = (String) vf.fromLiteral(lit);
//                
//                return Optional.of(
//                        new BlazeGraphAtom.VertexAtom(vertexId, label));
//                
//            } else {
//
//                final URI dt = lit.getDatatype();
//                
//                if (dt != null && PackedLongIV.PACKED_LONG.equals(dt)) {
//                    // blaze:a blaze:key "0"^^"blaze:packedLong"
//                    
//                    /*
//                     * We actually want to ignore these and wait for
//                     * <<blaze:a blaze:key "0"^^"blaze:packedLong">> rdfs:value "val" .
//                     * to emit a VertexPropertyAtom.
//                     */
//                    return Optional.empty();
//                    
//                } else {
//                    // blaze:a blaze:key "val" .
//                    
//                    final String key = vf.fromURI(p);
//                    final Object val = vf.fromLiteral(lit);
//                    final String vpId = vertexPropertyId(uri, p, lit);
//                    
//                    return Optional.of(
//                        new BlazeGraphAtom.VertexPropertyAtom(vertexId, key, val, vpId));
//                    
//                }
//                
//            }
//            
//        } else { // s instanceof BigdataBNode
//            
//            final BigdataBNode sid = (BigdataBNode) s;
//            final BigdataStatement reified = sid.getStatement();
//            final Literal lit = (Literal) o;
//            
//            if (LABEL.equals(p)) {
//                // <<blaze:a blaze:x blaze:b>> rdfs:label "label" .
//                
//                final String edgeId = vf.fromURI(reified.getPredicate());
//                final String fromId = vf.fromURI((URI) reified.getSubject());
//                final String toId = vf.fromURI((URI) reified.getObject());
//                final String label = (String) vf.fromLiteral(lit);
//                
//                return Optional.of(
//                    new BlazeGraphAtom.EdgeAtom(edgeId, label, fromId, toId));
//                
//            } else if (VALUE.equals(p)) {
//                // <<blaze:a blaze:key "0"^^"blaze:packedLong">> rdfs:value "val" .
//                
//                final String vertexId = vf.fromURI((URI) reified.getSubject());
//                final String key = vf.fromURI(reified.getPredicate());
//                final Object val = vf.fromLiteral(lit);
//                final String vpId = vertexPropertyId(reified);
//                
//                return Optional.of(
//                    new BlazeGraphAtom.VertexPropertyAtom(vertexId, key, val, vpId));
//
//            } else {
//                
//                final String key = vf.fromURI(p);
//                final Object val = vf.fromLiteral(lit);
//                
//                if (reified.getObject() instanceof URI) {
//                    // <<blaze:a blaze:x blaze:b>> blaze:key "val" .
//
//                    final String edgeId = vf.fromURI(reified.getPredicate());
//                    
//                    return Optional.of(
//                            new BlazeGraphAtom.PropertyAtom(edgeId, key, val));
//                                
//                } else {
//                    // <<blaze:a blaze:key "val">> blaze:key "val" .
//                    // <<blaze:a blaze:key "0"^^"blaze:packedLong">> blaze:key "val" .
//                    
//                    final String vpId = vertexPropertyId(reified);
//                    
//                    return Optional.of(
//                        new BlazeGraphAtom.PropertyAtom(vpId, key, val));
//                            
//                }
//                
//            }
//            
//        }
//        
//    }
    
    /**
     * Select results using a SPARQL query.
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public CloseableIterator<BlazeBindingSet> select(final String queryStr)
            throws Exception {
        return this.select(queryStr, nextQueryId());
    }

    /**
     * Select results using a SPARQL query.
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public CloseableIterator<BlazeBindingSet> select(final String queryStr,
            String extQueryId) throws Exception {

        final RepositoryConnection cxn = openRead();
        
//        if (sparqlLog.isTraceEnabled()) {
//            sparqlLog.trace("query:\n"+ (queryStr.length() <= SPARQL_LOG_MAX 
//                    ? queryStr : queryStr.substring(0, SPARQL_LOG_MAX)+" ..."));
//        }
//
//        final TupleQueryResult result;
//        UUID queryId = null;
//
//        try {
//            
//            final TupleQuery query = (TupleQuery) 
//                    cxn.prepareTupleQuery(QueryLanguage.SPARQL, queryStr);
//
//            setMaxQueryTime(query);
//
//            if (query instanceof BigdataSailTupleQuery
//                    && cxn instanceof BigdataSailRepositoryConnection) {
//
//                final BigdataSailTupleQuery bdtq = (BigdataSailTupleQuery) query;
//                queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
//                        bdtq.getASTContainer(), QueryType.SELECT,
//                        externalQueryId);
//            }
//            
//            if (sparqlLog.isTraceEnabled()) {
//                if (query instanceof BigdataSailTupleQuery) {
//                    final BigdataSailTupleQuery bdtq = (BigdataSailTupleQuery) query;
//                    sparqlLog.trace("optimized AST:\n"+bdtq.optimize());
//                }
//            }
//            
//            result = query.evaluate();
//        
//        } catch (Exception ex) {
//            if (!readFromWriteCxn) {
//                cxn.close();
//            }
//            throw ex;
//        }
        
//        final IStriterator sitr = new Striterator(
//                new WrappedResult<BindingSet>(result,
//                        readFromWriteCxn ? null : cxn, queryId));
//        
//        sitr.addFilter(new Resolver() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            protected Object resolve(final Object e) {
//                final BindingSet bs = (BindingSet) e;
//                return convert(bs);
//            }
//        });
//        
//        return (ICloseableIterator<BlazeBindingSet>) sitr;
        
        final Stream<BlazeBindingSet> stream = 
                select(cxn, queryStr, extQueryId)
                        .map(transforms.bindingSet);
                        
        return CloseableIterator.of(stream);
            
    }
    
//    /**
//     * Convert SPARQL/RDF results into PG form.
//     */
//    private final BigdataBindingSet convert(final BindingSet bs) {
//        
//        final BigdataBindingSet bbs = new BigdataBindingSet();
//        
//        for (String key : bs.getBindingNames()) {
//            
//            final Value val= bs.getBinding(key).getValue();
//            
//            final Object o;
//            if (val instanceof Literal) {
//                o = vf.fromLiteral((Literal) val);
//            } else if (val instanceof URI) {
//                o = vf.fromURI((URI) val);
//            } else {
//                continue;
//            }
//            
//            bbs.put(key, o);
//            
//        }
//        
//        return bbs;
//        
//    };
    
    /**
     * Select results using a SPARQL query.
     */
    public boolean ask(final String queryStr) throws Exception {
        return ask(queryStr, nextQueryId());
    }

    /**
     * Select results using a SPARQL query.
     */
    public boolean ask(final String queryStr, String externalQueryId)
            throws Exception {
        
        final RepositoryConnection cxn = readFromWriteCxn ? 
                writeCxn() : readCxn();
                
        return ask(cxn, queryStr, externalQueryId);
                
//        UUID queryId = null;
//        
//        try {
//            
//            final BooleanQuery query = (BooleanQuery) 
//                    cxn.prepareBooleanQuery(QueryLanguage.SPARQL, queryStr);
//
//            setMaxQueryTime(query);
//            
//            if (query instanceof BigdataSailQuery
//                    && cxn instanceof BigdataSailRepositoryConnection) {
//
//                final BigdataSailQuery bdtq = (BigdataSailQuery) query;
//                queryId = setupQuery((BigdataSailRepositoryConnection) cxn,
//                        bdtq.getASTContainer(), QueryType.ASK,
//                        externalQueryId);
//            }
//            
//            final boolean result = query.evaluate();
//            
//            finalizeQuery(queryId);
//            
//            return result;
//            
//        } finally {
//        
//            if (!readFromWriteCxn) {
//                cxn.close();
//            }
//            
//        }
            
    }
    
    /**
     * Update graph using SPARQL Update.
     */
    public void update(final String queryStr) {
        update(queryStr, nextQueryId());
    }
        
    
    /**
     * Update graph using SPARQL Update.
     */
    public void update(final String queryStr, final String extQueryId) {
        update(writeCxn(), queryStr, extQueryId);
    }
    
    /**
     * If history is enabled, return an iterator of historical graph edits 
     * related to any of the supplied ids.  To enable history, make sure
     * the database is in statement identifiers mode and that the RDR History
     * class is enabled.
     * <p>
     * Warning: You MUST close this iterator when finished.
     * 
     * @see {@link AbstractTripleStore.Options#STATEMENT_IDENTIFIERS}
     * @see {@link AbstractTripleStore.Options#RDR_HISTORY_CLASS}
     * @see {@link RDRHistory}
     */
    public CloseableIterator<BlazeGraphEdit> history(final List<String> ids) {
   
        final List<URI> uris = 
                ids.stream().map(vf::elementURI).collect(toList());
        final String queryStr = sparql.history(uris);
                    
        final RepositoryConnection cxn = openRead();
        final Stream<BlazeGraphEdit> stream = 
                select(cxn, queryStr, nextQueryId())
                        .map(transforms.history)
                        .filter(Optional::isPresent)
                        .map(Optional::get);
        return CloseableIterator.of(stream);

//        final IStriterator sitr = new Striterator(new WrappedResult<BindingSet>(
//                result, readFromWriteCxn ? null : cxn, queryId
//                ));
//        
//        sitr.addFilter(new Resolver() {
//            private static final long serialVersionUID = 1L;
//            @Override
//            protected Object resolve(final Object e) {
//                final BindingSet bs = (BindingSet) e;
//                final URI s = (URI) bs.getValue("s");
//                final URI p = (URI) bs.getValue("p");
//                final Value o = bs.getValue("o");
//                final URI a = (URI) bs.getValue("action");
//                final Literal t = (Literal) bs.getValue("time");
//                
//                if (!t.getDatatype().equals(XSD.DATETIME)) {
//                    throw new RuntimeException("Unexpected timestamp in result: " + bs);
//                }
//                
//                final BlazeGraphEdit.Action action;
//                if (a.equals(RDRHistory.Vocab.ADDED)) {
//                    action = Action.Add;
//                } else if (a.equals(RDRHistory.Vocab.REMOVED)) {
//                    action = Action.Remove;
//                } else {
//                    throw new RuntimeException("Unexpected action in result: " + bs);
//                }
//                
//                final BlazeGraphAtom atom = toGraphAtom(s, p, o).get();
//                
//                final long timestamp = DateTimeExtension.getTimestamp(t.getLabel());
//                
//                return new BlazeGraphEdit(action, atom, timestamp);
//            }
//        });
//        
//        return (ICloseableIterator<BlazeGraphEdit>) sitr;
            
    }
    
//    public class WrappedResult<E> implements CloseableIterator<E> {
//        
//        private final CloseableIteration<E,?> it;
//        
//        private final RepositoryConnection cxn;
//        
//        private final UUID queryId;
//        
//        public WrappedResult(final CloseableIteration<E,?> it, 
//                final RepositoryConnection cxn) {
//            this.it = it;
//            this.cxn = cxn;
//            this.queryId = null;
//        }
//
//        /**
//         * Allows you to pass a query UUID to perform a tear down
//         * when it exits.
//         * 
//         * @param it
//         * @param cxn
//         * @param queryId
//         */
//        public WrappedResult(final CloseableIteration<E,?> it, 
//                final RepositoryConnection cxn, UUID queryId) {
//            this.it = it;
//            this.cxn = cxn;
//            this.queryId = queryId;
//        }
//
//        @Override
//        public boolean hasNext() {
//            return Code.wrapThrow(() -> {
//                return it.hasNext();
//            });
//        }
//
//        @Override
//        public E next() {
//            try {
//                return (E) it.next();
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            }
//        }
//
//        @Override
//        public void remove() {
//            throw new UnsupportedOperationException();
//        }
//        
//        @Override
//        public void close() {
//            try {
//                finalizeQuery(queryId);
//                it.close();
//            } catch (RuntimeException ex) {
//                throw ex;
//            } catch (Exception ex) {
//                throw new RuntimeException(ex);
//            } finally {
//                if (cxn != null) {
//                    try {
//                        cxn.close();
//                    } catch (RepositoryException e) {
//                        log.warn("Could not close connection");
//                    }
//                }
//            }   
//        }
//        
////        @Override
////        protected void finalize() throws Throwable {
////            super.finalize();
////            System.err.println("closed: " + closed);
////        }
//        
//    }
    
    protected class GraphStreamer<E> {

        private final RepositoryConnection cxn;
        
        private final CloseableIteration<E,?> result;
        
//        private final UUID queryId;
        
        private final Code onClose;
        
        private final Iterator<E> it;
        
        public GraphStreamer(final RepositoryConnection cxn,
                final CloseableIteration<E,?> result,
                final Optional<Code> onClose) {
            this.cxn = cxn;
            this.result = result;
//            this.queryId = queryId;
            this.onClose = onClose.isPresent() ? onClose.get() : null;
            this.it = new Iterator<E>() {
                
                @Override
                public boolean hasNext() {
                    return Code.wrapThrow(() -> { 
                        return result.hasNext(); 
                    });
                }

                @Override
                public E next() {
                    return Code.wrapThrow(
                        () -> { /* try */ 
                            return result.next();
                        }, 
                        () -> { /* finally */
                            if (!hasNext()) {
                                close();
                            }
                        }
                    );
                }
                
            };
        }
        
        public Stream<E> stream() {
            return Streams.of(it).onClose(() -> close());
        }
        
        private volatile boolean closed = false;
        public void close() {
            if (closed) 
                return;
            
            Code.wrapThrow(
                () -> { /* try */
//                    finalizeQuery(queryId);
                    result.close();
                    onClose.run();
                }, 
                () -> { /* finally */
                    closeRead(cxn);
                    closed = true;
                }
            );
        }
        
    }
    


    protected abstract Stream<BindingSet> select(final RepositoryConnection cxn, final String queryStr, final String externalQueryId);
    
    protected abstract Stream<Statement> project(final RepositoryConnection cxn, final String queryStr, final String externalQueryId);
    
    protected abstract boolean ask(final RepositoryConnection cxn, final String queryStr, final String externalQueryId);
    
    protected abstract void update(final RepositoryConnection cxn, final String queryStr, final String externalQueryId);
    
    /**
     * Utility function to set the Query timeout to the global
     * setting if it is configured.
     */
    protected void setMaxQueryTime(final Query query) {
        if (maxQueryTime > 0) {
            query.setMaxQueryTime(maxQueryTime);
        }
    }
    
    /**
     * Return a Collection of running queries
     * 
     * @return
     */
    public abstract Collection<RunningQuery> getRunningQueries();

    /**
     * Kill a running query specified by the UUID. Do nothing if the query has
     * completed.
     * 
     * @param queryId
     */
    public abstract void cancel(UUID queryId);

    /**
     * Kill a running query specified by the UUID String.
     * Do nothing if the query has completed.
     * 
     * @param String uuid
     */
    public abstract void cancel(String uuid);

    /**
     * Kill a running query specified by the RunningQuery object. Do nothing if
     * the query has completed.
     * 
     * @param r
     */
    public abstract void cancel(RunningQuery r);
    
//    /**
//     * Return the {@link RunningQuery} for a currently executing SPARQL QUERY or
//     * UPDATE request.
//     * 
//     * @param queryId2
//     *            The {@link UUID} for the request.
//     * 
//     * @return The {@link RunningQuery} iff it was found.
//     */
//    public abstract RunningQuery getQuery(final UUID queryId2);
//    
//    /**
//     * Return the {@link RunningQuery} for a currently executing SPARQL QUERY or
//     * UPDATE request.
//     * 
//     * @param queryId2
//     *            The {@link UUID} for the request.
//     * 
//     * @return The {@link RunningQuery} iff it was found.
//     */
//    public abstract RunningQuery getQuery(final String extQueryId);
//
//    /**
//     * Embedded clients can override this to access query management
//     * capabilities.
//     * 
//     * @param cxn
//     * @param astContainer
//     * 
//     * @return
//     */
//    protected abstract UUID setupQuery(
//            final BigdataSailRepositoryConnection cxn,
//            ASTContainer astContainer, QueryType queryType, String extQueryId);
//    
//    /**
//     * Wrapper method to clean up query and throw exception is interrupted. 
//     * 
//     * @param queryId
//     * @throws QueryCancelledException 
//     */
//    protected void finalizeQuery(final UUID queryId)
//            throws QueryCancelledException {
//
//        if (queryId == null)
//            return;
//        
//        //Need to call before tearDown
//        final boolean isQueryCancelled = isQueryCancelled(queryId);
//        
//        tearDownQuery(queryId);
//        
//        if(isQueryCancelled){
//        
//            if(log.isDebugEnabled()) {
//                log.debug(queryId + " execution canceled.");
//            }
//            
//            throw new QueryCancelledException(queryId + " execution canceled.",
//                    queryId);
//        }
//        
//    }
//
//    /**
//     * Embedded clients can override this to access query management
//     * capabilities.
//     * 
//     * @param absQuery
//     */
//    protected abstract void tearDownQuery(UUID queryId);
//    
//    /**
//     * Helper method to determine if a query was cancelled.
//     * 
//     * @param queryId
//     * @return
//     */
//    protected abstract boolean isQueryCancelled(final UUID queryId);
    
}
