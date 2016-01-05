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

import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BasicRepositoryProvider;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;
import com.blazegraph.gremlin.embedded.BlazeGraphReadOnly;
import com.blazegraph.gremlin.listener.BlazeGraphAtom;
import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.listener.BlazeGraphListener;
import com.blazegraph.gremlin.structure.BlazeGraph.Match;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.LambdaLogger;

/**
 * Some sample code demonstrating additional BlazeGraph functionality beyond
 * the standard Tinkerpop3 API.
 *  
 * @author mikepersonick
 */
public class SampleCode {

    private final static transient LambdaLogger log = LambdaLogger.getLogger(SampleCode.class);
    
    protected BlazeGraphEmbedded graph = null;
    
    /**
     * Open a BlazeGraph instance backed by the supplied journal file.
     */
    public static BlazeGraphEmbedded open(final File file) {
        /*
         * A journal file is the persistence mechanism for an embedded 
         * Blazegraph instance.
         */
        final String journal = file.getAbsolutePath();
        
        /*
         * BasicRepositoryProvider will create a Blazegraph repository using the
         * specified journal file with a reasonable default configuration set
         * for the Tinkerpop3 API. This will also open a previously created
         * repository if the specified journal already exists.
         * 
         * ("Bigdata" is the legacy product name for Blazegraph).
         * 
         * See BasicRepositoryProvider for more details on the default SAIL 
         * configuration.
         */
        final BigdataSailRepository repo = BasicRepositoryProvider.open(journal);
        
        /*
         * Open a BlazeGraphEmbedded instance with no additional configuration.
         * See BlazeGraphEmbedded.Options for additional configuration options.
         */
        final BlazeGraphEmbedded graph = BlazeGraphEmbedded.open(repo);
        return graph;
    }
    
    @Before
    public void setUp() throws Exception {
        /*
         * TestRepositoryProvider extends BasicRepositoryProvider to create a 
         * temp journal file for the lifespan of the individual test method.  
         * You'll want to create your own BasicRepositoryProvider for your
         * application (or just use BasicRepositoryProvider out of the box).
         */
        final BigdataSailRepository repo = TestRepositoryProvider.open();
        /*
         * Open a BlazeGraphEmbedded instance with no additional configuration.
         * See BlazeGraphEmbedded.Options for additional configuration options.
         */
        this.graph = BlazeGraphEmbedded.open(repo);
    }
    
    @After
    public void tearDown() throws Exception {
        /*
         * Always close the graph when finished.
         */
        this.graph.close();
        /*
         * But don't do this - this will destroy the journal.  This is just
         * for the purposes of unit testing.
         */
        this.graph.__tearDownUnitTest();
    }

    
    /**
     * Demonstration of the bulk load API.
     * <p/>
     * The bulk load API provides a means of fast unchecked loading of data into
     * Blazegraph. By default, Blazegraph/TP3 is in "incremental update" mode.
     * Incremental update does strict checking on vertex and edge id re-use and
     * enforcement of property key cardinality. Both of these validations
     * require a read against the database indices. Blazegraph benefits greatly
     * from buffering and batch inserting statements into the database indices.
     * Buffering and batch insert are defeated by interleaving reads and removes
     * into the loading process, which is what happens with the normal
     * validation steps in incremental update mode. The bulk load API is a means
     * of bypassing the strict validation that normally occurs in incremental
     * update mode. The bulk load API is suitable for use in loading
     * "known-good" datasets - datasets that have already been validated for
     * consistency errors.
     */
    @Test
    public void demonstrateBulkLoadAPI() throws Exception {
        
        /*
         * Bulk load another graph.
         */
        final TinkerGraph theCrew = TinkerFactory.createTheCrew();
        graph.bulkLoad(theCrew);
        graph.tx().commit();
        
        assertEquals(6, graph.vertexCount());
        
        /*
         * Execute a code block in bulk load mode.
         */
        graph.bulkLoad(() -> {
            graph.addVertex(T.id, "a");
            graph.addVertex(T.id, "b");
        });
        graph.tx().commit();
        
        assertEquals(8, graph.vertexCount());
        
        /*
         * Manually set and reset bulk load mode. 
         */
        graph.setBulkLoad(true);
        graph.addVertex(T.id, "c");
        graph.addVertex(T.id, "d");
        graph.setBulkLoad(false);
        graph.tx().commit();

        assertEquals(10, graph.vertexCount());

        /*
         * Be careful not to introduce consistency errors while in bulk load
         * mode. 
         */
        graph.bulkLoad(() -> {
            final BlazeVertex e = graph.addVertex(T.id, "e", T.label, "foo");
            e.property(Cardinality.single, "key", "v1");
            e.property(Cardinality.single, "key", "v2");
            /*
             * Consistency error - cardinality enforcement has been bypassed,
             * resulting in two values for Cardinality.single.
             */
            assertEquals(2, e.properties("key").count());
            
            graph.addVertex(T.id, "e", T.label, "bar");
            /*
             * Consistency error - we've created a new vertex with the same id
             * as an existing one.
             */
            assertEquals(2, graph.vertices("e").count());
        });
        graph.tx().rollback();
        
        assertEquals(10, graph.vertexCount());

        try (CloseableIterator<Vertex> it = graph.vertices()) {
            log.info(() -> it.stream());
        }
        
    }
    
    
    /**
     * Demonstration of the search API.
     * <p/>
     * The search API lets you use Blazegraph's built-in full text index to
     * perform Lucene-style searches against your graph. Searches are done
     * against property values - the search results are an iterator of Property
     * objects (connected to the graph elements to which they belong). The
     * search API will find both properties (on vertices and edges) and
     * meta-properties (on vertex properties). To use the API, specify a search
     * string and a match behavior. The search string can consist of one or more
     * tokens to find. The match behavior tells the search engine how to look
     * for the tokens - find any of them (or), find all of them (and), or find
     * only exact matches for the search string. "Exact" is somewhat expensive
     * relative to "All" - it's usually better to use "All" and live with
     * slightly less precision in the search results. If a "*" appears at the
     * end of a token, the search engine will use prefix matching instead of
     * token matching. Prefix match is either on or off for the entire search,
     * it is not done on a token by token basis.
     */
    @Test
    public void demonstrateSearchAPI() throws Exception {
        
        /*
         * Load a toy graph.
         */
        graph.bulkLoad(() -> {
            final BlazeVertex v = graph.addVertex();
            v.property(Cardinality.set, "key", "hello foo");
            v.property(Cardinality.set, "key", "hello bar");
            v.property(Cardinality.set, "key", "hello foo bar");
            v.property(Cardinality.set, "key", "hello bar foo");
        });
        graph.tx().commit();
        
        // all four vertex properties contain "foo" or "bar"
        assertEquals(4, graph.search("foo bar", Match.ANY).count());
        // three contain "foo"
        assertEquals(3, graph.search("foo", Match.ANY).count());
        // and three contain "bar"
        assertEquals(3, graph.search("bar", Match.ANY).count());
        // only two contain both
        assertEquals(2, graph.search("foo bar", Match.ALL).count());
        // and only one contains exactly "foo bar"
        assertEquals(1, graph.search("foo bar", Match.EXACT).count());
        
        // prefix match
        assertEquals(4, graph.search("hell*", Match.ANY).count());
        
    }
    
    /**
     * Demonstration of the listener API.
     * <p/>
     * This API lets you attach listeners to the graph to track graph elements
     * and properties as they are added or removed and to receive notifications
     * of commits and rollbacks. The listener API leverages an internal
     * Blazegraph API that notifies listeners of change events in the RDF
     * statement indices. RDF breaks down nicely into atomic graph units -
     * statements - and the Blazegraph change API notifies listeners of add or
     * remove events for individual statements. Blazegraph provides a
     * corresponding atomic graph unit for property graphs called the
     * BlazeGraphAtom. There are four types of atoms - VertexAtom, EdgeAtom,
     * PropertyAtom, and VertexPropertyAtom. Each atom references the element id
     * to which it belongs - VertexPropertyAtom references both the vertex id
     * and the internal vertex property id. VertexAtom and EdgeAtom contain the
     * element label, PropertyAtom and VertexPropertyAtom contain a key/value
     * pair. These atoms represent the smallest unit of information about the
     * property graph and actually correspond one-to-one with RDF statements in
     * the Blazegraph/TP3 data model. BlazeGraphAtoms are wrapped inside
     * BlazeGraphEdits, which describe the action taken on the atom (add or
     * remove).
     */
    @Test
    public void demonstrateListenerAPI() throws Exception {
        
        /*
         * Set up a simple listener to collect edits and log events.
         */
        final List<BlazeGraphEdit> edits = new LinkedList<>();
        final BlazeGraphListener listener = new BlazeGraphListener() {

            @Override
            public void transactionCommited(long commitTime) {
                log.info(() -> "transactionCommitted: "+commitTime);
            }

            @Override
            public void transactionAborted() {
                log.info(() -> "transactionAborted");
            }

            @Override
            public void graphEdited(final BlazeGraphEdit edit, final String rdfEdit) {
                log.info(() -> "graphEdited: " + edit);
                edits.add(edit);
            }
            
        };
        graph.addListener(listener);

        /*
         * Add a couple vertices.
         */
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        graph.commit();

        assertEquals(2, edits.size());
        edits.stream().forEach(edit -> {
            assertEquals(BlazeGraphEdit.Action.Add, edit.getAction());
            assertEquals(BlazeGraphAtom.VertexAtom.class, edit.getAtom().getClass());
        });
        edits.clear();
        
        /*
         * Add vertex properties.
         */
        a.property("key", "val");
        b.property("key", "val");
        graph.tx().commit();
        
        assertEquals(2, edits.size());
        edits.stream().forEach(edit -> {
            assertEquals(BlazeGraphEdit.Action.Add, edit.getAction());
            final BlazeGraphAtom.VertexPropertyAtom atom = 
                    (BlazeGraphAtom.VertexPropertyAtom) edit.getAtom();
            assertEquals("key", atom.getKey());
            assertEquals("val", atom.getVal());
        });
        edits.clear();
        
        /*
         * Add an edge.
         */
        final BlazeEdge edge = a.addEdge(Edge.DEFAULT_LABEL, b);
        graph.commit();

        assertEquals(1, edits.size());
        edits.stream().forEach(edit -> {
            assertEquals(BlazeGraphEdit.Action.Add, edit.getAction());
            assertEquals(BlazeGraphAtom.EdgeAtom.class, edit.getAtom().getClass());
        });
        edits.clear();
        
        /*
         * Add an edge property.
         */
        edge.property("key", "val");
        graph.commit();

        assertEquals(1, edits.size());
        edits.stream().forEach(edit -> {
            assertEquals(BlazeGraphEdit.Action.Add, edit.getAction());
            assertEquals(BlazeGraphAtom.PropertyAtom.class, edit.getAtom().getClass());
        });
        edits.clear();

        /*
         * Remove the edge will result in two edits - the edge and its property.
         */
        edge.remove();
        graph.commit();

        assertEquals(2, edits.size());
        edits.stream().forEach(edit -> {
            assertEquals(BlazeGraphEdit.Action.Remove, edit.getAction());
        });
        edits.clear();

    }
    
    /**
     * Demonstration of the history API.
     * <p/>
     * The history API is closely related to the listener API in that the unit
     * of information is the same - BlazeGraphEdits. With the history API, you
     * can look back in time on the history of the graph - when vertices and
     * edges were added or removed or what the property history looks like for
     * individual elements. With the history API you specify the element ids of
     * interest and get back an iterator of edits for those ids. The history
     * information is also modeled using RDF*, meaning the history is part of
     * the raw RDF graph itself. Thus the history information can also be
     * accessed via Sparql.
     */
    @Test
    public void demonstrateHistoryAPI() throws Exception {
        
        /*
         * Add a vertex.
         */
        final BlazeVertex a = graph.addVertex(T.id, "a");
        graph.tx().commit();
        
        /*
         * Add a property.
         */
        a.property(Cardinality.single, "key", "foo");
        graph.tx().commit();
        
        /*
         * Change the value.
         */
        a.property(Cardinality.single, "key", "bar");
        graph.tx().commit();
        
        /*
         * Remove the vertex.
         */
        a.remove();
        graph.tx().commit();

        /*
         * Get the history, which should be the following:
         * 
         * 1. VertexAtom("a"), action=add, time=t0
         * 2. VertexPropertyAtom("key"="foo"), action=add, time=t1
         * 3. VertexPropertyAtom("key"="foo"), action=remove, time=t2
         * 4. VertexPropertyAtom("key"="bar"), action=add, time=t2
         * 5. VertexPropertyAtom("key"="bar"), action=remove, time=t3
         * 6. VertexAtom("a"), action=add, time=t3
         */
        List<BlazeGraphEdit> history = graph.history("a").collect();
        assertEquals(6, history.size());
        
        log.info(() -> history.stream());
        
    }
    
    /**
     * Demonstration of the concurrency API.
     * <p/>
     * Blazegraph's concurrency model is MVCC, which more or less lines up with
     * Tinkerpop's Transaction model. When you open a BlazeGraphEmbedded
     * instance, you are working with the unisolated (writer) view of the
     * database. This view supports Tinkerpop Transactions, and reads are done
     * against the unisolated connection, so uncommitted changes will be
     * visible. A BlazeGraphEmbedded can be shared across multiple threads, but
     * only one thread can have a Tinkerpop Transaction open at a time (other
     * threads will be blocked until the transaction is closed). A TP3
     * Transaction is automatically opened on any read or write operation, and
     * automatically closed on any commit or rollback operation. The Transaction
     * can also be closed manually, which you will need to do after read
     * operations to unblock other waiting threads.
     * <p/>
     * BlazegraphGraphEmbedded's database operations are thus single-threaded,
     * but Blazegraph/MVCC allows for many concurrent readers in parallel with
     * both the single writer and other readers. This is possible by opening a
     * read-only view that will read against the last commit point on the
     * database. The read-only view can be be accessed in parallel to the writer
     * without any of the restrictions described above.
     * <p/>
     * BlazeGraphReadOnly extends BlazeGraphEmbedded and thus offers all the
     * same operations, except write operations will not be permitted
     * (BlazeGraphReadOnly.tx() will throw an exception). You can open as many
     * read-only views as you like, but we recommend you use a connection pool
     * so as not to overtax system resources. Applications should be written
     * with the one-writer many-readers paradigm front of mind.
     */
    @Test 
    public void demonstrateConcurrencyAPI() throws Exception {

        /*
         * Add and commit a vertex.
         */
        final BlazeVertex a = graph.addVertex(T.id, "a");
        graph.commit();

        /*
         * Open a read-only view on the last commit point.
         */
        final BlazeGraphReadOnly readView = graph.readOnlyConnection();
        try {
            
            /*
             * Open the unisolated connection to the database.
             */
            graph.tx().open();
            
            /*
             * Reads against the read-only view are still possible despite the
             * write connection being open, even if this operation were to occur
             * in a different thread. This operation would not be possible
             * against the unisolated view from another thread. (Unisolated view
             * is single-threaded.)
             */
            assertEquals(1, readView.vertexCount());
            
            final BlazeVertex b = graph.addVertex(T.id, "b");
            graph.commit();

            /*
             * The read view still sees only one vertex, since it is still
             * reading against the last commit point at the time the view was
             * created. 
             */
            assertEquals(1, readView.vertexCount());
            assertEquals(2, graph.vertexCount());
            
        } finally {
            readView.close();
        }
        
    }
    
    /**
     * Demonstration of the Sparql/PG API.
     * <p/>
     * 
     */
    @Test 
    public void demonstrateSparqlAPI() throws Exception {
        
        /*
         * Bulk load the classic graph.
         */
        final TinkerGraph classic = TinkerFactory.createClassic();
        graph.bulkLoad(classic);
        graph.tx().commit();

        /*
         * "Who created a project named 'lop' that was also created by someone 
         * who is 29 years old? Return the two creators."
         * 
         * gremlin> g.V().match(
         *        __.as('a').out('created').as('b'),
         *        __.as('b').has('name', 'lop'),
         *        __.as('b').in('created').as('c'),
         *        __.as('c').has('age', 29)).
         *      select('a','c').by('name')
         * ==>[a:marko, c:marko]
         * ==>[a:josh, c:marko]
         * ==>[a:peter, c:marko]
         */
        final String sparql = 
                "select ?a ?c { " +
                     // vertex named "lop"
                "    ?lop <blaze:name> \"lop\" . " +
                     // created by ?c
                "    <<?c_id ?x ?lop>> rdfs:label \"created\" . " +
                     // whose age is 29
                "    ?c_id <blaze:age> \"29\"^^xsd:int . " +
                     // created by ?a
                "    <<?a_id ?y ?lop>> rdfs:label \"created\" . " +
                     // gather names
                "    ?a_id <blaze:name> ?a . " +
                "    ?c_id <blaze:name> ?c . " +
                "}";

        /*
         * Run the query, auto-translate RDF values to PG values.
         */
        final List<BlazeBindingSet> results = graph.select(sparql).collect();
        
        log.info(() -> results.stream());
        assertEquals(3, results.size());
        assertEquals(1, results.stream().map(bs -> bs.get("c")).distinct().count());
        assertEquals(3, results.stream().map(bs -> bs.get("a")).distinct().count());
        
    }
    
}
