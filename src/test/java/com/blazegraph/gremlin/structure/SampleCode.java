package com.blazegraph.gremlin.structure;

import static org.junit.Assert.*;

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;
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
    
    @Before
    public void setUp() throws Exception {
        /*
         * TestRepositoryProvider extends BasicRepositoryProvider and creates a 
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
        
        /*
         * Execute a code block in bulk load mode.
         */
        graph.bulkLoad(() -> {
            graph.addVertex(T.id, "a");
            graph.addVertex(T.id, "b");
        });
        graph.tx().commit();
        
        /*
         * Manually set and reset bulk load mode. 
         */
        graph.setBulkLoad(true);
        graph.addVertex(T.id, "c");
        graph.addVertex(T.id, "d");
        graph.setBulkLoad(false);
        graph.tx().commit();
        
        try (CloseableIterator<Vertex> it = graph.vertices()) {
            log.info(it.stream());
        }
        
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
        assertEquals(3, graph.search("foo", Match.ANY).count());
        // only two contain both
        assertEquals(2, graph.search("foo bar", Match.ALL).count());
        // and only one contains exactly "foo bar"
        assertEquals(1, graph.search("foo bar", Match.EXACT).count());
        
        // prefix match
        assertEquals(4, graph.search("hell*", Match.ANY).count());
        
    }
    
    /**
     * Demonstration of the listener API.
     */
    @Test
    public void demonstrateListenerAPI() throws Exception {
        
    }
    
    /**
     * Demonstration of the history API.
     */
    @Test
    public void demonstrateHistoryAPI() throws Exception {
        
    }
    
    /**
     * Demonstration of the concurrency API.
     */
    @Test 
    public void demonstrateConcurrencyAPI() throws Exception {
        
    }
    
    /**
     * Demonstration of the Sparql/PG API.
     */
    @Test 
    public void demonstrateSparqlAPI() throws Exception {
        
    }
    
}
