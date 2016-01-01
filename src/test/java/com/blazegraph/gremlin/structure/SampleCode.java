package com.blazegraph.gremlin.structure;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;
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
     * Demonstration of the search API.
     */
    @Test
    public void demonstrateSearchAPI() throws Exception {
        
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
