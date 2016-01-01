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
package com.blazegraph.gremlin.structure;

import static com.blazegraph.gremlin.util.Lambdas.toMap;
import static java.util.stream.Collectors.toList;

import java.util.Arrays;
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
import org.openrdf.query.impl.MapBindingSet;
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
import com.bigdata.rdf.store.AbstractTripleStore;
import com.blazegraph.gremlin.listener.BlazeGraphAtom;
import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.listener.BlazeGraphEdit.Action;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.Code;
import com.blazegraph.gremlin.util.LambdaLogger;
import com.blazegraph.gremlin.util.Streams;

import info.aduna.iteration.CloseableIteration;

//@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn("com.blazegraph.gremlin.structure.StructureStandardSuite")
public abstract class BlazeGraph implements Graph {
    
    protected final transient static LambdaLogger log = LambdaLogger.getLogger(BlazeGraph.class);
    
    protected final transient static LambdaLogger sparqlLog = LambdaLogger.getLogger(BlazeGraph.class.getName() + ".SparqlLog");
    
    /**
     * Maximum number of chars to print through the SparqlLogger.
     */
    public static final int SPARQL_LOG_MAX = 10000;
    

    
    public static interface Options {
        
        String VALUE_FACTORY = BlazeGraph.class.getName() + ".valueFactory";
        
//        String READ_FROM_WRITE_CXN = BlazeGraph.class.getName() + ".readFromWriteCxn";
        
        String LAST_COMMIT_TIME = BlazeGraph.class.getName() + ".lastCommitTime";
        
        String MAX_QUERY_TIME = BlazeGraph.class.getName() + ".maxQueryTime";
        
    }
    
    public static enum Match {
        
        ANY,
        
        ALL,
        
        EXACT;
        
    }
    
    private final BlazeValueFactory vf;
    
    protected final Configuration config;
    
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
    
//    private transient volatile boolean readFromWriteCxn = false;
    
    private transient volatile boolean bulkLoad = false;
    
    protected BlazeGraph(final Configuration config) {
        this.config = config;
        
        this.vf = Optional.ofNullable((BlazeValueFactory) 
                                config.getProperty(Options.VALUE_FACTORY))
                          .orElse(DefaultBlazeValueFactory.INSTANCE);
        this.sparql = new SparqlGenerator(this.vf);
        
//        this.readFromWriteCxn = config.getBoolean(
//                Options.READ_FROM_WRITE_CXN, false);
        
        final long lastCommitTime = config.getLong(
                Options.LAST_COMMIT_TIME, System.currentTimeMillis());
        this.vpIdFactory = new AtomicLong(lastCommitTime);
        
        this.maxQueryTime = config.getInt(Options.MAX_QUERY_TIME, 0);
        
        this.LABEL = vf.label();
        this.VALUE = vf.value();
        
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
    
//    /**
//     * Different implementations will return different types of connections
//     * depending on the mode (client/server, embedded, read-only, etc.)
//     */
//    protected abstract RepositoryConnection writeCxn();
//    
//    /**
//     * A read-only connection can be used for read operations without blocking
//     * or being blocked by writers.
//     */
//    protected abstract RepositoryConnection readCxn();
//    
//    public boolean isReadFromWriteCxn() {
//        return readFromWriteCxn;
//    }
//    
//    public void setReadFromWriteCxn(final boolean readFromWriteCxn) {
//        this.readFromWriteCxn = readFromWriteCxn;
//    }
//    
//    /**
//     * Execute the supplied code fragement and read from the write connection 
//     * during its execution.
//     */
//    public void readFromWriteCxn(final Code code) {
//        if (isReadFromWriteCxn()) {
//            Code.wrapThrow(code);
//        } else {
//            setReadFromWriteCxn(true);
//            Code.wrapThrow(code, () -> setReadFromWriteCxn(false));
//        }
//    }
    
    protected abstract RepositoryConnection cxn();
    
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
    
//    public RepositoryConnection openRead() {
//        return readFromWriteCxn ? writeCxn() : readCxn();
//    }
//    
//    public void closeRead(final RepositoryConnection cxn) {
//        Code.wrapThrow(() -> { 
//            if (!readFromWriteCxn) cxn.close(); 
//        });
//    }
    
    @Override
    public BlazeVertex addVertex(final Object... kvs) {
        ElementHelper.legalPropertyKeyValueArray(kvs);
        final Optional<Object> suppliedId = validateSuppliedId(kvs);
        
        final String id = suppliedId.map(String.class::cast)
                                    .orElse(nextId());
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
        
        final RepositoryConnection cxn = cxn();
        Code.wrapThrow(() -> {
            cxn.add(uri, LABEL, rdfLabel);
        });
        
        final BlazeVertex vertex = new BlazeVertex(this, uri, rdfLabel);
        ElementHelper.attachProperties(vertex, VertexProperty.Cardinality.set, kvs);
        return vertex;
    }

    public BlazeEdge addEdge(final BlazeVertex from, final BlazeVertex to,
            final String label, final Object... kvs) {
        ElementHelper.validateLabel(label);
        ElementHelper.legalPropertyKeyValueArray(kvs);
        final Optional<Object> suppliedId = validateSuppliedId(kvs);
        
        final String id = suppliedId.map(String.class::cast)
                                    .orElse(nextId());

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
        
        final RepositoryConnection cxn = cxn();
        Code.wrapThrow(() -> {
            // blaze:person:1 blaze:knows:7 blaze:person:2 .
            cxn.add(edgeStmt);
            // <<blaze:person:1 blaze:knows:7 blaze:person:2>> rdfs:label "knows" .
            cxn.add(rdfvf.createBNode(edgeStmt), LABEL, rdfLabel);
        });
        
        final BlazeEdge edge = new BlazeEdge(this, edgeStmt, rdfLabel, from, to);
        ElementHelper.attachProperties(edge, kvs);
        return edge;
    }
    
    private final String nextId() {
        final String id = UUID.randomUUID().toString();
        return id;//.substring(id.length()-5);
    }
    
    <V> BlazeProperty<V> property(final BlazeReifiedElement element, 
            final String key, final V val) {
        final BigdataValueFactory rdfvf = rdfValueFactory();
        final BigdataBNode s = element.rdfId();
        final URI p = rdfvf.asValue(vf.propertyURI(key));
        final Literal lit = rdfvf.asValue(vf.toLiteral(val));
        
        final RepositoryConnection cxn = cxn();
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
        
        final RepositoryConnection cxn = cxn();
        Code.wrapThrow(() -> {
            if (cardinality == Cardinality.list) {
                // <s> <key> timestamp .
                cxn.add(stmt);
                // <<<s> <key> timestamp>> rdf:value "val" .
                cxn.add(sid, VALUE, lit);
            } else {
                if (cardinality == Cardinality.single && !bulkLoad) {
                    final String queryStr = sparql.cleanVertexProps(s, p, lit);
                    update(queryStr);
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
//        try (CloseableIterator<Edge> it = vertex.edges(Direction.BOTH)) {
//            it.forEachRemaining(e -> {
//                remove((BlazeReifiedElement) e);
//            });
//        }
//        try (CloseableIterator<VertexProperty<Object>> it = vertex.properties()) {
//            it.forEachRemaining(vp -> {
//                remove((BlazeReifiedElement) vp);
//            });
//        }
//        final RepositoryConnection cxn = cxn();
//        Code.wrapThrow(() -> {
//            final URI uri = vertex.rdfId();
//            cxn.remove(uri, null, null);
//        });
        
        final String queryStr = sparql.removeVertex(vertex);
        update(queryStr);
    }

    void remove(final BlazeReifiedElement element) {
//        final BigdataBNode sid = element.rdfId();
//        final BigdataStatement stmt = sid.getStatement();
//        final RepositoryConnection cxn = cxn();
//        Code.wrapThrow(() -> {
//            cxn.remove(stmt);
//            cxn.remove(sid, null, null);
//        });
        
        final String queryStr = sparql.removeReifiedElement(element);
        update(queryStr);
    }
    
    <V> void remove(final BlazeProperty<V> prop) {
        final RepositoryConnection cxn = cxn();
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

        final Stream<Vertex> stream = _select(queryStr, nextQueryId())
                .map(transforms.vertex);
        return CloseableIterator.of(stream);
    }
    
    protected String nextQueryId() {
        return UUID.randomUUID().toString();
    }
    
    public int edgeCount() {
        final String queryStr = sparql.edgeCount();
        return count(queryStr);
    }
    
    public Optional<BlazeEdge> edge(final Object edgeId) {
        try (CloseableIterator<Edge> it = edges(edgeId)) {
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
        
        final Stream<Edge> stream = _select(queryStr, nextQueryId())
                .map(transforms.edge);
        return CloseableIterator.of(stream);
    }
    
    CloseableIterator<Edge> edgesFromVertex(final BlazeVertex src, 
            final Direction dir, final String... edgeLabels) {
        final List<Literal> lits = 
                Stream.of(edgeLabels).map(vf::toLiteral).collect(toList());
        final String queryStr = sparql.edgesFromVertex(src, dir, lits);
        
        final Stream<Edge> stream = _select(queryStr, nextQueryId())
                .map(transforms.edge);
        return CloseableIterator.of(stream);
    }
    
    <V> CloseableIterator<Property<V>> properties(
            final BlazeReifiedElement element, final String... keys) {
        final List<URI> uris = 
                Stream.of(keys).map(vf::propertyURI).collect(toList());
        final String queryStr = sparql.properties(element, uris);
        
        final Stream<Property<V>> stream = _select(queryStr, nextQueryId())
                .map(transforms.<V>property(element)); 
        return CloseableIterator.of(stream);
    }
    
    <V> CloseableIterator<VertexProperty<V>> properties(
            final BlazeVertex vertex, final String... keys) {
        final List<URI> uris = 
                Stream.of(keys).map(vf::propertyURI).collect(toList());
        final String queryStr = sparql.vertexProperties(vertex, uris);
        
        final Stream<VertexProperty<V>> stream = _select(queryStr, nextQueryId())
                .map(transforms.<V>vertexProperty(vertex)); 
        return CloseableIterator.of(stream);
    }
    
    protected int count(final String queryStr) {
        try (Stream<BindingSet> stream = _select(queryStr, nextQueryId())) {
            return stream.map(bs -> (Literal) bs.getValue("count"))
                         .map(Literal::intValue)
                         .findFirst().get();
        }
    }
    
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
                    throw new IllegalArgumentException(
                            "Unknown element id type: " + elementId + 
                            " ("+ elementId.getClass()+")");
                })
                .map(vf::elementURI)
                .collect(Collectors.toList());
    }
    
    protected class Transforms {
    
        public final 
        Function<BindingSet, Vertex> vertex = bs -> {
            
            final BigdataURI uri = (BigdataURI) bs.getValue("vertex");
            final Literal label = (Literal) bs.getValue("label");
            final BlazeVertex vertex = new BlazeVertex(BlazeGraph.this, uri, label);
            return vertex;
            
        };
        
        public final 
        Function<BindingSet, Edge> edge = bs -> {
            
            final BigdataBNode s = (BigdataBNode) bs.getValue("edge");
            final BigdataStatement stmt = s.getStatement();
            final Literal label = (Literal) bs.getValue("label");
            final BigdataURI fromURI = (BigdataURI) stmt.getSubject();
            final Literal fromLabel = (Literal) bs.getValue("fromLabel");
            final BlazeVertex from = 
                    new BlazeVertex(BlazeGraph.this, fromURI, fromLabel);
            
            final BigdataURI toURI = (BigdataURI) stmt.getObject();
            final Literal toLabel = (Literal) bs.getValue("toLabel");
            final BlazeVertex to = 
                    new BlazeVertex(BlazeGraph.this, toURI, toLabel);
            return new BlazeEdge(BlazeGraph.this, stmt, label, from, to);
                
        };
        
        public final <V> Function<BindingSet, Property<V>> 
        property(final BlazeReifiedElement e) {
            return bs -> {
                
                log.debug(() -> bs);
                final URI key = (URI) bs.getValue("key");
                final Literal val = (Literal) bs.getValue("val");
                final BlazeProperty<V> prop = 
                        new BlazeProperty<>(BlazeGraph.this, e, key, val);
                return prop;
                
            };
        }
        
        public final <V> Function<BindingSet, VertexProperty<V>> 
        vertexProperty(final BlazeVertex v) {
            return bs -> {

                log.debug(() -> bs);
//                final URI key = (URI) bs.getValue("key");
                final Literal val = (Literal) bs.getValue("val");
                final BigdataBNode sid = (BigdataBNode) bs.getValue("vp");
                final BigdataStatement stmt = sid.getStatement();
                final URI key = stmt.getPredicate();
                final String vpId = vertexPropertyId(stmt);
                final BlazeProperty<V> prop = 
                        new BlazeProperty<>(BlazeGraph.this, v, key, val);
                final BlazeVertexProperty<V> bvp = 
                        new BlazeVertexProperty<>(prop, vpId, sid);
                return bvp;

            };
        }
        
        /**
         * Convert SPARQL/RDF results into PG form.
         */
        public final 
        Function<BindingSet,BlazeBindingSet> bindingSet = bs -> {
            
            log.debug(() -> bs);
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
        public final 
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
        
        public final 
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
            return graphAtom.apply(stmt).map(atom -> new BlazeGraphEdit(action, atom, timestamp));

        };
      
        public final <V> Function<BindingSet, Property<V>> 
        search() {
            return bs -> {
                
                log.debug(() -> bs);
                final Property<V> prop;
                if (bs.hasBinding("edge")) {
                    /*
                     * Edge property
                     */
                    final BlazeEdge edge = (BlazeEdge) this.edge.apply(bs);
                    prop = this.<V>property(edge).apply(bs);
                } else {
                    final BlazeVertex vertex = (BlazeVertex) this.vertex.apply(bs);
                    if (bs.hasBinding("vpVal")) {
                        /*
                         * VertexProperty property
                         */
                        final MapBindingSet remap = new MapBindingSet() {{
                            addBinding("vp", bs.getValue("vp"));
                            addBinding("val", bs.getValue("vpVal"));
                        }};
                        final BlazeVertexProperty<V> vp = (BlazeVertexProperty<V>) 
                                this.<V>vertexProperty(vertex).apply(remap);
                        prop = this.<V>property(vp).apply(bs);
                    } else {
                        /*
                         * Vertex property
                         */
                        prop = this.<V>vertexProperty(vertex).apply(bs);
                    }
                }
                return prop;
                
            };
        }
        
    }
        
    @Override
    public abstract Transaction tx();

    @Override
    public abstract void close();

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
        
        final Stream<BlazeGraphAtom> stream =
                _project(queryStr, externalQueryId)
                        .map(transforms.graphAtom)
                        .filter(Optional::isPresent)
                        .map(Optional::get);
        
        return CloseableIterator.of(stream);
        
    }
    
    /**
     * Select results using a SPARQL query.
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public CloseableIterator<BlazeBindingSet> select(final String queryStr) {
        return this.select(queryStr, nextQueryId());
    }

    /**
     * Select results using a SPARQL query.
     * <p>
     * Warning: You MUST close this iterator when finished.
     */
    public CloseableIterator<BlazeBindingSet> select(
            final String queryStr, final String extQueryId) {

        final Stream<BlazeBindingSet> stream = 
                _select(queryStr, extQueryId)
                        .map(transforms.bindingSet);
                        
        return CloseableIterator.of(stream);
            
    }
    
    /**
     * Select results using a SPARQL query.
     */
    public boolean ask(final String queryStr) {
        return ask(queryStr, nextQueryId());
    }

    /**
     * Select results using a SPARQL query.
     */
    public boolean ask(final String queryStr, final String extQueryId) {
        return _ask(queryStr, extQueryId);
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
        _update(queryStr, extQueryId);
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
    public CloseableIterator<BlazeGraphEdit> history(final String... ids) {
        return history(Arrays.asList(ids));
    }
        
    public CloseableIterator<BlazeGraphEdit> history(final List<String> ids) {
        validateHistoryIds(ids);
        
        final List<URI> uris = 
                ids.stream().map(vf::elementURI).collect(toList());
        final String queryStr = sparql.history(uris);
                    
        final Stream<BlazeGraphEdit> stream = 
                _select(queryStr, nextQueryId())
                        .map(transforms.history)
                        .filter(Optional::isPresent)
                        .map(Optional::get);
        
        return CloseableIterator.of(stream);

    }
    
    private void validateHistoryIds(final List<String> ids) {
        ids.forEach(id -> {
            if (id.startsWith("<<") && id.endsWith(">>")) {
                throw new IllegalArgumentException(
                        "History not (yet) supported for VertexProperty elements: " + id);
            }
        });
    }
    
    public <V> CloseableIterator<Property<V>> search(final String search, 
            final Match match) {
        
        final String queryStr = sparql.search(search, match);
        
        final Stream<Property<V>> stream = _select(queryStr, nextQueryId())
                .map(transforms.<V>search()); 
        
        return CloseableIterator.of(stream);
        
    }
    
    protected class GraphStreamer<E> {

//        private final RepositoryConnection cxn;
        
        private final CloseableIteration<E,?> result;
        
        private final Code onClose;
        
        private final Iterator<E> it;
        
        public GraphStreamer(//final RepositoryConnection cxn,
                final CloseableIteration<E,?> result,
                final Optional<Code> onClose) {
//            this.cxn = cxn;
            this.result = result;
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
                    result.close();
                    onClose.run();
                }, 
                () -> { /* finally */
//                    closeRead(cxn);
                    closed = true;
                }
            );
        }
        
    }

    protected abstract Stream<BindingSet> _select( 
            final String queryStr, final String extQueryId);
    
    protected abstract Stream<Statement> _project( 
            final String queryStr, final String extQueryId);
    
    protected abstract boolean _ask( 
            final String queryStr, final String extQueryId);
    
    protected abstract void _update( 
            final String queryStr, final String extQueryId);
    
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
     * Kill a running query specified by the RunningQuery object. Do nothing if
     * the query has completed.
     * 
     * @param r
     */
    public abstract void cancel(RunningQuery r);
    
}
