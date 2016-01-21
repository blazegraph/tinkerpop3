# blazegraph-gremlin

Welcome to the Blazegraph/Tinkerpop3 project.  The TP3 implementation has some significant differences from the TP2 version.  The data model has been changed to use RDF*, an RDF reification framework described [here](https://wiki.blazegraph.com/wiki/index.php/Reification_Done_Right).

The concept behind blazegraph-gremlin is that property graph (PG) data can be loaded and accessed via the Tinkerpop3 API, but underneath the hood the data will be stored as RDF using the PG data model described in this document.  Once PG data has been loaded you can interact with it just like you would interact with ordinary RDF - you can run SPARQL queries or interact with the data via the SAIL API.  It just works.  The PG data model is also customizable via a round-tripping interface called the BlazeValueFactory, also described in detail in this document.

Some interesting features of the Blazegraph/TP3 implementation include:

* Listener API - subscribe to notifications about updates to the graph (adds and removes of vertices/edges/properties, commits, rollbacks, etc.)
* History API - capture full or partial history of edits to the graph.
* Built-in full text index and search API to find graph elements.
* Automatic SPARQL to PG translation - run a SPARQL query and get your results back in property graph form.
* Query management API - list and cancel running Sparql queries.
* Bulk Load API for fast setup of new graphs.
* Support for MVCC concurrency model for high-concurrency read access.

## Maven Central Dependency
Blazegraph and the Tinkerpop3 implementation are available on Maven Central.

```
   <dependency>
      <groupId>com.blazegraph</groupId>
      <artifactId>blazegraph-gremlin</artifactId>
      <version>1.0</version>
   </dependency>
```

##Javadocs
[Javadocs](https://blazegraph.github.io/tinkerpop3/apidocs/) 

## Getting Started

To build blazegraph-gremlin:

    > mvn clean install -Dtest=SampleCode

To import blazegraph-gremlin into Eclipse:

    > mvn eclipse:eclipse
    
Then select "File-Import-Existing Projects Into Workspace" from the Eclipse menu and select the root directory of this project.

Continue reading this document and take a look at SampleCode.java provided in blazegraph-gremlin/src/test for information on how to get started writing your TP3 application with Blazegraph.

## Gremlin Console

To install the blazegraph-gremlin plugin in the gremlin console:

	gremlin> :install com.blazegraph blazegraph-gremlin 1.0-SNAPSHOT
	==>Loaded: [com.blazegraph, blazegraph-gremlin, 1.0-SNAPSHOT] - restart the console to use [tinkerpop.blazegraph]

Once you restart the console, activate the blazegraph-gremlin plugin:

	gremlin> :plugin use tinkerpop.blazegraph
	==>tinkerpop.blazegraph activated
	
You can then open a BlazeGraph instance by specifying the location you would like to use for the persistent journal file:

	gremlin> g = BlazeGraphFactory.open("/tmp/blazegraph.jnl")
	BlazeGraph(TM) Graph Engine

                       Flexible
                       Reliable
                      Affordable
          Web-Scale Computing for the Enterprise
          
    Copyright SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.
    ==>blazegraphembedded[vertices:0 edges:0]

## Blazegraph/TP3 Data Model

It's important to understand how Blazegraph organizes property graph data as RDF.  Blazegraph uses the RDF* framework, which is an extension to RDF that provides for an easier RDF reification syntax.  Reification is a means of using an RDF statement as an RDF value in other statements.  The RDF* syntax for an RDF "statement as a value" is as follows:

	# statement
	:john :knows :mary . 
	# "statement as value"
	<<:john :knows :mary>> dc:source <http://johnknowsmary.com> .
	
Blazegraph uses the OpenRDF SAIL API and represents RDF* reified statements as bnodes in that API.  This is important for understanding how to write SPARQL queries against TP3 graphs and how to interpret query results.

Property graph values must be converted into RDF values and vice versa.  Blazegraph provides a BlazeValueFactory interface with a default implementation.  You can extend this interface and provide your own value factory if you prefer a custom look for the RDF values in your property graph.  

Blazegraph accepts user-supplied IDs (strings only) for vertices and edges.  If no id is supplied a UUID will be generated.  By default, TP3 ids and property keys are converted into URIs by prepending a <blaze:> namespace prefix.  Property values are simply converted into datatyped literals.  

Two fixed URIs are used and provided by the BlazeValueFactory to represent element labels (rdf:type by default) and property values for Cardinality.list vertex properties (rdf:value by default).  These can also be overriden as desired.

Property graph elements are represented as follows in Blazegraph:

	# BlazeVertex john = graph.addVertex(T.id, "john", T.label, "person");
	blaze:john rdf:type blaze:person .
	
	# BlazeEdge knows = graph.addEdge(john, mary, "knows", T.id, "k01");
	blaze:john blaze:k01 blaze:mary .
	<<blaze:john blaze:k01 blaze:mary>> rdf:type blaze:knows .
	
Vertices requires one statement, edges require two.

Edge properties are simply attached to the reified edge statement:

	# BlazeProperty p = knows.property("acl", "private");
	<<blaze:john blaze:k01 blaze:mary>> blaze:acl "private" .

Representation of vertex properties depends on the cardinality of the key.  Cardinality.single and .set look the same, .list is represented differently.  Vertices can mix and match cardinalities for different keys.  All three cardinalities are supported, but Cardinality.set is the one most closely aligned with RDF and as such will provide the best performance of the three.  User supplied ids are NOT supported for vertex properties.

Cardinality.set and Cardinality.single are modeled the same way:
	
	# VertexProperty set = john.property(Cardinality.set, "age", 25, "acl", "public");
	blaze:john blaze:age "25"^^xsd:int .
	<<blaze:john blaze:age "25"^^xsd:int>> blaze:acl "public" .
	
Cardinality.list is modeled differently:
	
	# VertexProperty list = john.property(Cardinality.list, "city", "salt lake city", "acl", "public");
	blaze:john blaze:city "12765"^^bg:listIndex .
	<<blaze:john blaze:city "12765"^^bg:listIndex>> rdf:value "salt lake city" .
	<<blaze:john blaze:city "12765"^^bg:listIndex>> blaze:acl "public" .
	
Cardinality.list uses a specially datatyped and monotonically increasing internal identifier to represent the vertex property (the actual datatype is `<http://www.blazegraph.com/rdf/datatype#listIndex>`).  This identifier serves to manage duplicate list values and ordering of list items.  It's important to note this difference as different cardinalities will require different SPARQL queries.

#### Putting it all together: The Crew

Here is how the Tinkerpop3 "Crew" dataset looks when loaded into Blazegraph.  Human-friendly IDs have been assigned to vertices and edge UUIDs have been abbreviated to 5 characters for brevity.

    blaze:tinkergraph rdf:type blaze:software ;
                      blaze:name "tinkergraph" .
                
    blaze:gremlin rdf:type blaze:software ;
                  blaze:name "gremlin" ;
                  blaze:48f63 blaze:tinkergraph .
                
    <<blaze:gremlin blaze:48f63 blaze:tinkergraph>> rdf:type blaze:traverses .                
                
    blaze:daniel rdf:type blaze:person ;
                 blaze:name "daniel" ;
                 blaze:81056 blaze:tinkergraph ;
                 blaze:e09ac blaze:gremlin ;
                 blaze:location "spremberg" ;
                 blaze:location "kaiserslautern" ;
                 blaze:location "aachen" .
                
    <<blaze:daniel blaze:81056 blaze:tinkergraph>>   rdf:type blaze:uses ;
                                                     blaze:skill "3"^^xsd:int .
    <<blaze:daniel blaze:e09ac blaze:gremlin>>       rdf:type blaze:uses ;
                                                     blaze:skill "5"^^xsd:int .
    <<blaze:daniel blaze:location "spremberg">>      blaze:startTime "1982"^^xsd:int ;
                                                     blaze:endTime "2005"^^xsd:int .
    <<blaze:daniel blaze:location "kaiserslautern">> blaze:startTime "2005"^^xsd:int ;
                                                     blaze:endTime "2009"^^xsd:int .
    <<blaze:daniel blaze:location "aachen">>         blaze:startTime "2009"^^xsd:int .
                
    blaze:marko rdf:type blaze:person ;
                blaze:name "marko" ;
                blaze:42af2 blaze:gremlin ;
                blaze:4edec blaze:gremlin ;
                blaze:61d50 blaze:tinkergraph ;
                blaze:68c12 blaze:tinkergraph ;
                blaze:location "san diego" ;
                blaze:location "santa cruz" ;
                blaze:location "brussels" ;
                blaze:location "santa fe" .

    <<blaze:marko blaze:42af2 blaze:gremlin>>     rdf:type blaze:develops ;
                                                  blaze:since "2009"^^xsd:int .
    <<blaze:marko blaze:4edec blaze:gremlin>>     rdf:type blaze:uses ;
                                                  blaze:skill "4"^^xsd:int .
    <<blaze:marko blaze:61d50 blaze:tinkergraph>> rdf:type blaze:develops ;
                                                  blaze:since "2010"^^xsd:int .
    <<blaze:marko blaze:68c12 blaze:tinkergraph>> rdf:type blaze:uses ;
                                                  blaze:skill "5"^^xsd:int .
    <<blaze:marko blaze:location "san diego">>    blaze:startTime "1997"^^xsd:int ;
                                                  blaze:endTime "2001"^^xsd:int .
    <<blaze:marko blaze:location "santa cruz">>   blaze:startTime "2001"^^xsd:int ;
                                                  blaze:endTime "2004"^^xsd:int .
    <<blaze:marko blaze:location "brussels">>     blaze:startTime "2004"^^xsd:int ;
                                                  blaze:endTime "2005"^^xsd:int .
    <<blaze:marko blaze:location "santa fe">>     blaze:startTime "2005"^^xsd:int .
    
    blaze:stephen rdf:type blaze:person ;
                  blaze:name "stephen" ;
                  blaze:15869 blaze:tinkergraph ;
                  blaze:1e9c3 blaze:gremlin ;
                  blaze:bf48d blaze:tinkergraph ;
                  blaze:bff3c blaze:gremlin ;
                  blaze:location "centreville" ;
                  blaze:location "dulles" ;
                  blaze:location "purcellville" .

    <<blaze:stephen blaze:15869 blaze:tinkergraph>> rdf:type blaze:develops ;
                                                    blaze:since "2011"^^xsd:int .
    <<blaze:stephen blaze:1e9c3 blaze:gremlin>>     rdf:type blaze:develops ;
                                                    blaze:since "2010"^^xsd:int .
    <<blaze:stephen blaze:bf48d blaze:tinkergraph>> rdf:type blaze:uses ;
                                                    blaze:skill "4"^^xsd:int .
    <<blaze:stephen blaze:bff3c blaze:gremlin>>     rdf:type blaze:uses ;
                                                    blaze:skill "5"^^xsd:int .
    <<blaze:stephen blaze:location "centreville">>  blaze:startTime "1990"^^xsd:int ;
                                                    blaze:endTime "2000"^^xsd:int .
    <<blaze:stephen blaze:location "dulles">>       blaze:startTime "2000"^^xsd:int ;
                                                    blaze:endTime "2006"^^xsd:int .
    <<blaze:stephen blaze:location "purcellville">> blaze:startTime "2006"^^xsd:int .
    
    blaze:matthias rdf:type blaze:person ;
                blaze:name "matthias" ;
                blaze:7373e blaze:gremlin ;
                blaze:e5a5d blaze:gremlin ;
                blaze:ef89a blaze:tinkergraph ;
                blaze:location "bremen" ;
                blaze:location "baltimore" ;
                blaze:location "oakland" ;
                blaze:location "seattle" .
    
    <<blaze:matthias blaze:7373e blaze:gremlin>>     rdf:type blaze:develops ;
                                                     blaze:since "2012"^^xsd:int .
    <<blaze:matthias blaze:e5a5d blaze:gremlin>>     rdf:type blaze:uses ;
                                                     blaze:skill "3"^^xsd:int .
    <<blaze:matthias blaze:ef89a blaze:tinkergraph>> rdf:type blaze:uses ;
                                                     blaze:skill "3"^^xsd:int .
    <<blaze:matthias blaze:location "bremen">>       blaze:startTime "2004"^^xsd:int ;
                                                     blaze:endTime "2007"^^xsd:int .
    <<blaze:matthias blaze:location "baltimore">>    blaze:startTime "2007"^^xsd:int ;
                                                     blaze:endTime "2011"^^xsd:int .
    <<blaze:matthias blaze:location "oakland">>      blaze:startTime "2011"^^xsd:int ;
                                                     blaze:endTime "2014"^^xsd:int .
    <<blaze:matthias blaze:location "seattle">>      blaze:startTime "2014"^^xsd:int .
        
## Getting up and running with Blazegraph/TP3

Currently **BlazeGraphEmbedded** is the only concrete implementation of the Blazegraph Tinkerpop3 API.  BlazeGraphEmbedded is backed by an embedded (same JVM) instance of Blazegraph.  This puts the enterprise features of Blazegraph (high-availability, scale-out, etc.) out of reach for the 1.0 version of the TP3 integration, since those features are accessed via Blazegraph's client/server API.  A TP3 integration with the client/server version of Blazegraph is reserved for a future blazegraph-tinkerpop release.

BlazeGraphEmbedded is instantiated by providing an open and initialized Blazegraph RDF repository (OpenRDF SAIL).  There a numerous resources available at [blazegraph.com](http://wiki.blazegraph.com) on how to configure a Blazegraph SAIL, however blazegraph-gremlin comes with a quick start factory that will allow you to get up and running with Blazegraph with a reasonable set of defaults for the Tinkerpop3 API.  BasicRepositoryProvider in blazegraph-gremlin/src/main allows you to create or open an RDF repository backed by a persistent journal file at a specified location.  This RDF repository can then be used to open a BlazeGraphEmbedded instance:

    /*
     * A journal file is the persistence mechanism for an embedded 
     * Blazegraph instance.
     */
    String journal = file.getAbsolutePath();
    
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
    BigdataSailRepository repo = BasicRepositoryProvider.open(journal);
    
    /*
     * Open a BlazeGraphEmbedded instance with no additional configuration.
     * See BlazeGraphEmbedded.Options for additional configuration options.
     */
    BlazeGraphEmbedded graph = BlazeGraphEmbedded.open(repo);

There is even a convenience factory that combines those last two steps:

    /*
     * Open a BlazeGraphEmbedded instance using the specified journal file.
     */
    BlazeGraphEmbedded graph = BlazeGraphFactory.open(journal);

## Beyond the Tinkerpop3 Graph API

Blazegraph/TP3 has a number of features that go beyond the standard Tinkerpop3 Graph API.

### Bulk Load API

The bulk load API provides a means of fast unchecked loading of data into Blazegraph.  By default, Blazegraph/TP3 is in "incremental update" mode.  Incremental update does strict checking on vertex and edge id re-use and enforcement of property key cardinality.  Both of these validations require a read against the database indices.  Blazegraph benefits greatly from buffering and batch inserting statements into the database indices.  Buffering and batch insert are defeated by interleaving reads and removes into the loading process, which is what happens with the normal validation steps in incremental update mode.  The bulk load API is a means of bypassing the strict validation that normally occurs in incremental update mode.  The bulk load API is suitable for use in loading "known-good" datasets - datasets that have already been validated for consistency errors.

The bulk load API can be used in several ways:

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
    
Be careful not to introduce consistency errors while in bulk load mode. 
     
    graph.bulkLoad(() -> {
        final BlazeVertex e = graph.addVertex(T.id, "e", T.label, "foo");
        e.property(Cardinality.single, "someKey", "v1");
        e.property(Cardinality.single, "someKey", "v2");
        /*
         * Consistency error - cardinality enforcement has been bypassed,
         * resulting in two values for Cardinality.single.
         */
        assertEquals(2, e.properties("someKey").count());
        
        graph.addVertex(T.id, "e", T.label, "bar");
        /*
         * Consistency error - we've created a new vertex with the same id
         * as an existing one.
         */
        assertEquals(2, graph.vertices("e").count());
    });
    graph.tx().rollback();


### Search API

The search API lets you use Blazegraph's built-in full text index to perform Lucene-style searches against your graph.  Searches are done against property values - the search results are an iterator of Property objects (connected to the graph elements to which they belong).  The search API will find both properties (on vertices and edges) and meta-properties (on vertex properties).  To use the API, specify a search string and a match behavior.  The search string can consist of one or more tokens to find.  The match behavior tells the search engine how to look for the tokens - find any of them (or), find all of them (and), or find only exact matches for the search string.  "Exact" is somewhat expensive relative to "All" - it's usually better to use "All" and live with slightly less precision in the search results.  If a "*" appears at the end of a token, the search engine will use prefix matching instead of token matching.  Prefix match is either on or off for the entire search, it is not done on a token by token basis.

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


### Listener API

This API lets you attach listeners to the graph to track graph elements and properties as they are added or removed and to receive notifications of commits and rollbacks.  The listener API leverages an internal Blazegraph API that notifies listeners of change events in the RDF statement indices.  RDF breaks down nicely into atomic graph units - statements - and the Blazegraph change API notifies listeners of add or remove events for individual statements.  Blazegraph provides a corresponding atomic graph unit for property graphs called the BlazeGraphAtom.  There are four types of atoms - VertexAtom, EdgeAtom, PropertyAtom, and VertexPropertyAtom.  Each atom references the element id to which it belongs - VertexPropertyAtom references both the vertex id and the internal vertex property id.  VertexAtom and EdgeAtom contain the element label, PropertyAtom and VertexPropertyAtom contain a key/value pair.  These atoms represent the smallest unit of information about the property graph and actually correspond one-to-one with RDF statements in the Blazegraph/TP3 data model.  BlazeGraphAtoms are wrapped inside BlazeGraphEdits, which describe the action taken on the atom (add or remove).

The listener API look as follows:

    @FunctionalInterface
    public interface BlazeGraphListener {
    
        /**
         * Notification of an edit to the graph.
         * 
         * @param edit
         *          the {@link BlazeGraphEdit}
         * @param raw
         *          toString() version of the raw RDF mutation
         */
        void graphEdited(BlazeGraphEdit edit, String rdfEdit);
    
        /**
         * Notification of a transaction committed.
         * 
         * @param commitTime
         *          the timestamp on the commit
         */
        default void transactionCommited(long commitTime) {
            // noop default impl
        }
    
        /**
         * Notification of a transaction abort.
         */
        default void transactionAborted() {
            // noop default impl
        }
        
    }

Sample usage of this API can be found in SampleCode.demonstrateListenerAPI().

### History API

The history API is closely related to the listener API in that the unit of information is the same - BlazeGraphEdits.  With the history API, you can look back in time on the history of the graph - when vertices and edges were added or removed or what the property history looks like for individual elements.  With the history API you specify the element ids of interest and get back an iterator of edits for those ids.  The history information is also modeled using RDF*, meaning the history is part of the raw RDF graph itself.  Thus the history information can also be accessed via Sparql.

Sample usage of this API can be found in SampleCode.demonstrateHistoryAPI(), which performs the following operations:

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

...and results in the following six edits:

	BlazeGraphEdit [action=Add, atom=VertexAtom [id=a, label=vertex], timestamp=5247]
	BlazeGraphEdit [action=Add, atom=VertexPropertyAtom [vertexId=a, key=key, val=foo], timestamp=5305]
	BlazeGraphEdit [action=Add, atom=VertexPropertyAtom [vertexId=a, key=key, val=bar], timestamp=5355]
	BlazeGraphEdit [action=Remove, atom=VertexPropertyAtom [vertexId=a, key=key, val=foo], timestamp=5355]
	BlazeGraphEdit [action=Remove, atom=VertexPropertyAtom [vertexId=a, key=key, val=bar], timestamp=5400]
	BlazeGraphEdit [action=Remove, atom=VertexAtom [id=a, label=vertex], timestamp=5400]

### Transaction and Concurrency API

Blazegraph's concurrency model is MVCC, which more or less lines up with Tinkerpop's Transaction model.  When you open a BlazeGraphEmbedded instance, you are working with the unisolated (writer) view of the database.  This view supports Tinkerpop Transactions, and reads are done against the unisolated connection, so uncommitted changes will be visible.  A BlazeGraphEmbedded can be shared across multiple threads, but only one thread can have a Tinkerpop Transaction open at a time (other threads will be blocked until the transaction is closed).  A TP3 Transaction is automatically opened on any read or write operation, and automatically closed on any commit or rollback operation.  The Transaction can also be closed manually, which you will need to do after read operations to unblock other waiting threads.

BlazegraphGraphEmbedded's database operations are thus single-threaded, but Blazegraph/MVCC allows for many concurrent readers in parallel with both the single writer and other readers.  This is possible by opening a read-only view that will read against the last commit point on the database.  The read-only view can be be accessed in parallel to the writer without any of the restrictions described above.  To get a read-only snapshot, use the following pattern:

	final BlazeGraphEmbedded unisolated = ...;
	final BlazeGraphReadOnly readOnly = unisolated.readOnlyConnection();
	try {
		// read operations against readOnly
	} finally {
		readOnly.close();
	}

BlazeGraphReadOnly extends BlazeGraphEmbedded and thus offers all the same operations, except write operations will not be permitted (BlazeGraphReadOnly.tx() will throw an exception).  You can open as many read-only views as you like, but we recommend you use a connection pool so as not to overtax system resources.  Applications should be written with the one-writer many-readers paradigm front of mind.

**Important: Make sure to close the read-only view when you are done with it.**

Sample usage of this API can be found in SampleCode.demonstrateConcurrencyAPI().

### Sparql API

Since it is an RDF graph database at its core, Blazegraph supports the full Sparql 1.1 and Sparql Update 1.1 feature set.  Furthermore, Sparql query results are automatically translated back into property graph form (element ids, labels, property keys and values) using the BlazeValueFactory.  Thus Sparql queries run through the blazegraph-gremlin API will result in PG binding sets instead of RDF binding sets (for select queries) and BlazeGraphAtoms instead of RDF statements (for construct queries).

Blazegraph has one of the fastest native query engines of any graph database.  Currently blazegraph-gremlin does not provide a native TraversalStrategy implementation for Gremlin.  Gremlin traversals will thus be executed via the graph API rather than compiled down into Sparql queries and run natively.  As such it is highly recommended that for the 1.0 release of this module you write any application queries or traversals with high performance requirements in Sparql rather than Gremlin.  Future releases will include a native Blazegraph TraversalStrategy that will execute Gremlin traversals directly against Blazegraph's ultra-fast query engine.  Please contact us for developer support if you would like help expressing your application's queries as Sparql in the most optimized fashion for Blazegraph.

Sample usage of this API can be found in SampleCode.demonstrateSparqlAPI().  This example demonstrates how to formulate one of the gremlin queries from the Tinkerpop3 documentation as a Sparql query:

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
            "    <<?c_id ?x ?lop>> rdf:type <blaze:created> . " +
                 // whose age is 29
            "    ?c_id <blaze:age> \"29\"^^xsd:int . " +
                 // created by ?a
            "    <<?a_id ?y ?lop>> rdf:type <blaze:created> . " +
                 // gather names
            "    ?a_id <blaze:name> ?a . " +
            "    ?c_id <blaze:name> ?c . " +
            "}";

    /*
     * Run the query, auto-translate RDF values to PG values.
     */
    final List<BlazeBindingSet> results = graph.select(sparql).collect();

Which results in the following three binding sets:

	[c=marko, a=marko]
	[c=marko, a=josh]
	[c=marko, a=peter]


