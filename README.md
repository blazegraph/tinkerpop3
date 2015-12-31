# blazegraph-gremlin

Welcome to the Blazegraph/Tinkerpop3 project.  The TP3 implementation has some significant differences from the TP2 version.  The data model has been changed to use RDF*, an RDF reification framework described [here](https://wiki.blazegraph.com/wiki/index.php/Reification_Done_Right).

The concept behind blazegraph-gremlin is that property graph (PG) data can be loaded and accessed via the Tinkerpop3 API, but underneath the hood the data will be stored as RDF using the PG data model described in this document.  Once PG data has been loaded you can interact with it just like you would interact with ordinary RDF - you can run SPARQL queries or interact with the data via the SAIL API.  It just works.  The PG data model is also customizable via a round-tripping interface called the BlazeValueFactory, also described in detail in this document.

Some interesting features of the Blazegraph/TP3 implementation include:

* Listener API - subscribe to notifications about updates to the graph (adds and removes of vertices/edges/properties, commits, rollbacks, etc.)
* History API - capture full or partial history of edits to the graph.
* Built-in full text index and search API to find graph elements.
* Automatic SPARQL to PG translation - run a SPARQL query and get your results back in property graph form.
* Bulk Load API for fast setup of new graphs.
* Support for MVCC concurrency model for high-concurrency read access.

## Getting Started

To build blazegraph-gremlin:

    > mvn clean install

To import blazegraph-gremlin into Eclipse:

    > mvn eclipse:eclipse
    
Then select "File-Import-Existing Projects Into Workspace" from the Eclipse menu and select the root directory of this project.

Take a look at the sample code provided in XXX for an example of how to get started writing your TP3 application with Blazegraph.

Tinkerpop3 is setup to build with Jenkins CI for PRs and the master branch.

## Blazegraph/TP3 Data Model

It's important to understand how Blazegraph organizes property graph data as RDF.  Blazegraph uses the RDF* framework, which is an extension to RDF that provides for an easier RDF reification syntax.  Reification is a means of using an RDF statement as an RDF value in other statements.  The RDF* syntax for an RDF "statement as a value" is as follows:

	# statement
	:john :knows :mary . 
	# "statement as value"
	<<:john :knows :mary>> dc:source <http://johnknowsmary.com> .
	
Blazegraph uses the SAIL API and represents RDF* reified statements as bnodes in that API.  This is important for understanding how to write SPARQL queries against TP3 graphs and how to interpret query results.

Property graph values must be converted into RDF values and vice versa.  Blazegraph provides a BlazeValueFactory interface with a default implementation.  You can extend this interface and provide your own value factory if you prefer a custom look for the RDF values in your property graph.  

Blazegraph accepts user-supplied IDs (strings only) for vertices and edges.  If no id is supplied a UUID will be generated.  By default, TP3 ids and property keys are converted into URIs by prepending a <blaze:> namespace prefix.  Property values are simply converted into datatyped literals.  

Two fixed URIs are used and provided by the BlazeValueFactory to represent element labels (rdfs:label by default) and property values for Cardinality.list vertex properties (rdf:value by default).  These can also be overriden as desired.

Property graph elements are represented as follows in Blazegraph:

	# BlazeVertex john = graph.addVertex(T.id, "john", T.label, "person");
	blaze:john rdfs:label "person" .
	
	# BlazeEdge knows = graph.addEdge(john, mary, "knows", T.id, "k01");
	blaze:john blaze:k01 blaze:mary .
	<<blaze:john blaze:k01 blaze:mary>> rdfs:label "knows" .
	
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
	blaze:john blaze:city "12765"^^internal:packedLong .
	<<blaze:john blaze:city "12765"^^internal:packedLong>> rdf:value "salt lake city" .
	<<blaze:john blaze:city "12765"^^internal:packedLong>> blaze:acl "public" .
	
Cardinality.list uses a specially datatyped and monotonically increasing internal identifier to represent the vertex property (the actual datatype is `<http://www.bigdata.com/rdf/datatype#packedLong>`).  This identifier serves to manage duplicate list values and ordering of list items.  It's important to note this difference as different cardinalities will require different SPARQL queries.

#### Putting it all together: The Crew

Here is how the Tinkerpop3 "Crew" dataset looks when loaded into Blazegraph.  Human-friendly IDs have been assigned to vertices and edge UUIDs have been abbreviated to 5 characters for brevity.

    blaze:tinkergraph rdfs:label "software" ;
                      blaze:name "tinkergraph" .
                
    blaze:gremlin rdfs:label "software" ;
                  blaze:name "gremlin" ;
                  blaze:48f63 blaze:tinkergraph .
                
    <<blaze:gremlin blaze:48f63 blaze:tinkergraph>> rdfs:label "traverses" .                
                
    blaze:daniel rdfs:label "person" ;
                 blaze:name "daniel" ;
                 blaze:81056 blaze:tinkergraph ;
                 blaze:e09ac blaze:gremlin ;
                 blaze:location "spremberg" ;
                 blaze:location "kaiserslautern" ;
                 blaze:location "aachen" .
                
    <<blaze:daniel blaze:81056 blaze:tinkergraph>>   rdfs:label "uses" ;
                                                     blaze:skill "3"^^xsd:int .
    <<blaze:daniel blaze:e09ac blaze:gremlin>>       rdfs:label "uses" ;
                                                     blaze:skill "5"^^xsd:int .
    <<blaze:daniel blaze:location "spremberg">>      blaze:startTime "1982"^^xsd:int ;
                                                     blaze:endTime "2005"^^xsd:int .
    <<blaze:daniel blaze:location "kaiserslautern">> blaze:startTime "2005"^^xsd:int ;
                                                     blaze:endTime "2009"^^xsd:int .
    <<blaze:daniel blaze:location "aachen">>         blaze:startTime "2009"^^xsd:int .
                
    blaze:marko rdfs:label "person" ;
                blaze:name "marko" ;
                blaze:42af2 blaze:gremlin ;
                blaze:4edec blaze:gremlin ;
                blaze:61d50 blaze:tinkergraph ;
                blaze:68c12 blaze:tinkergraph ;
                blaze:location "san diego" ;
                blaze:location "santa cruz" ;
                blaze:location "brussels" ;
                blaze:location "santa fe" .

    <<blaze:marko blaze:42af2 blaze:gremlin>>     rdfs:label "develops" ;
                                                  blaze:since "2009"^^xsd:int .
    <<blaze:marko blaze:4edec blaze:gremlin>>     rdfs:label "uses" ;
                                                  blaze:skill "4"^^xsd:int .
    <<blaze:marko blaze:61d50 blaze:tinkergraph>> rdfs:label "develops" ;
                                                  blaze:since "2010"^^xsd:int .
    <<blaze:marko blaze:68c12 blaze:tinkergraph>> rdfs:label "uses" ;
                                                  blaze:skill "5"^^xsd:int .
    <<blaze:marko blaze:location "san diego">>    blaze:startTime "1997"^^xsd:int ;
                                                  blaze:endTime "2001"^^xsd:int .
    <<blaze:marko blaze:location "santa cruz">>   blaze:startTime "2001"^^xsd:int ;
                                                  blaze:endTime "2004"^^xsd:int .
    <<blaze:marko blaze:location "brussels">>     blaze:startTime "2004"^^xsd:int ;
                                                  blaze:endTime "2005"^^xsd:int .
    <<blaze:marko blaze:location "santa fe">>     blaze:startTime "2005"^^xsd:int .
    
    blaze:stephen rdfs:label "person" ;
                  blaze:name "stephen" ;
                  blaze:15869 blaze:tinkergraph ;
                  blaze:1e9c3 blaze:gremlin ;
                  blaze:bf48d blaze:tinkergraph ;
                  blaze:bff3c blaze:gremlin ;
                  blaze:location "centreville" ;
                  blaze:location "dulles" ;
                  blaze:location "purcellville" .

    <<blaze:stephen blaze:15869 blaze:tinkergraph>> rdfs:label "develops" ;
                                                    blaze:since "2011"^^xsd:int .
    <<blaze:stephen blaze:1e9c3 blaze:gremlin>>     rdfs:label "develops" ;
                                                    blaze:since "2010"^^xsd:int .
    <<blaze:stephen blaze:bf48d blaze:tinkergraph>> rdfs:label "uses" ;
                                                    blaze:skill "4"^^xsd:int .
    <<blaze:stephen blaze:bff3c blaze:gremlin>>     rdfs:label "uses" ;
                                                    blaze:skill "5"^^xsd:int .
    <<blaze:stephen blaze:location "centreville">>  blaze:startTime "1990"^^xsd:int ;
                                                    blaze:endTime "2000"^^xsd:int .
    <<blaze:stephen blaze:location "dulles">>       blaze:startTime "2000"^^xsd:int ;
                                                    blaze:endTime "2006"^^xsd:int .
    <<blaze:stephen blaze:location "purcellville">> blaze:startTime "2006"^^xsd:int .
    
    blaze:matthias rdfs:label "person" ;
                blaze:name "matthias" ;
                blaze:7373e blaze:gremlin ;
                blaze:e5a5d blaze:gremlin ;
                blaze:ef89a blaze:tinkergraph ;
                blaze:location "bremen" ;
                blaze:location "baltimore" ;
                blaze:location "oakland" ;
                blaze:location "seattle" .
    
    <<blaze:matthias blaze:7373e blaze:gremlin>>     rdfs:label "develops" ;
                                                     blaze:since "2012"^^xsd:int .
    <<blaze:matthias blaze:e5a5d blaze:gremlin>>     rdfs:label "uses" ;
                                                     blaze:skill "3"^^xsd:int .
    <<blaze:matthias blaze:ef89a blaze:tinkergraph>> rdfs:label "uses" ;
                                                     blaze:skill "3"^^xsd:int .
    <<blaze:matthias blaze:location "bremen">>       blaze:startTime "2004"^^xsd:int ;
                                                     blaze:endTime "2007"^^xsd:int .
    <<blaze:matthias blaze:location "baltimore">>    blaze:startTime "2007"^^xsd:int ;
                                                     blaze:endTime "2011"^^xsd:int .
    <<blaze:matthias blaze:location "oakland">>      blaze:startTime "2011"^^xsd:int ;
                                                     blaze:endTime "2014"^^xsd:int .
    <<blaze:matthias blaze:location "seattle">>      blaze:startTime "2014"^^xsd:int .	
## Transactions and Concurrency in BlazeGraphEmbedded

Currently BlazeGraphEmbedded is the only concrete implementation of the Blazegraph Tinkerpop3 API.  BlazeGraphEmbedded is backed by an embedded (same JVM) instance of Blazegraph.  This puts the enterprise features of Blazegraph (high-availability, scale-out, etc.) out of reach for the 1.0 version of the TP3 integration, since those features are accessed via Blazegraph's client/server API.  A TP3 integration with the client/server version of Blazegraph is reserved for a future blazegraph-tinkerpop release.

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

**Important: Make sure to close the read-only view as soon as you are done with it.**

