package com.blazegraph.gremlin.structure;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20151210;
import com.blazegraph.gremlin.util.Code;

public class EmbeddedBlazeGraphProvider extends AbstractGraphProvider {

    public static interface Options {
        
        String REPOSITORY_NAME = EmbeddedBlazeGraphProvider.class.getName() + ".repositoryName";
        
    }
    
    @Override
    public String convertId(Object id, Class<? extends Element> c) {
        return id instanceof String ? (String) id : id.toString();
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) 
            throws Exception {
        if (graph != null) {
            final BlazeGraphEmbedded blazeGraph = (BlazeGraphEmbedded) graph;
            blazeGraph.close();
            blazeGraph.__tearDownUnitTest();
        }
    }

    public static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
        add(BlazeEdge.class);
        add(BlazeGraph.class);
        add(BlazeProperty.class);
        add(BlazeVertex.class);
        add(BlazeVertexProperty.class);
    }};

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATIONS;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(
            final String graphName, final Class<?> test, final String testMethodName,
            final GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Options.REPOSITORY_NAME, graphName+"-"+test.getName()+"-"+testMethodName);
//            put(LocalBlazeGraph.Options.REPOSITORY, getRepository(graphName+"-"+test.getName()+"-"+testMethodName));
            put(BlazeGraph.Options.READ_FROM_WRITE_CXN, true);
            put(Graph.GRAPH, BlazeGraphEmbedded.class.getName());
        }};
    }
    
    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (graph != null) {
            final BlazeGraphEmbedded blazeGraph = (BlazeGraphEmbedded) graph;
            blazeGraph.bulkLoad(() -> super.loadGraphData(graph, loadGraphWith, testClass, testName));
        }
    }
    
//    public static LocalBlazeGraph open(final Configuration config) {
//        final String name = config.getString(Options.REPOSITORY_NAME);
//        config.addProperty(LocalBlazeGraph.Options.REPOSITORY, getRepository(name));
//        return new LocalBlazeGraph(config);
//    }
    
    /**
     * Used by TestBlazeGraph
     */
    public static BlazeGraphEmbedded open() {
        return BlazeGraphEmbedded.open(new BaseConfiguration() {{
            this.setProperty(BlazeGraphEmbedded.Options.REPOSITORY, getRepository());
        }});
    }
    
    private static final Map<String,String> repos = new HashMap<>();
    private static synchronized BigdataSailRepository getRepository(final String name) {
        final String journal;
        if (repos.containsKey(name)) {
            journal = repos.get(name);
        } else {
            repos.put(name, journal = journal());
        }
        final Properties props = getProperties(journal);
        return getRepository(props);
    }

    private static String journal() {
        final File file = Code.wrapThrow(() -> File.createTempFile("EmbeddedBlazeGraphProvider", ".jnl"));
        file.deleteOnExit();
        return file.getAbsolutePath();
    }
    
    private static BigdataSailRepository getRepository() {
        final Properties props = getProperties(journal());
        return getRepository(props);
    }
    
    private static BigdataSailRepository getRepository(final Properties props) {
        final BigdataSail sail = new BigdataSail(props);
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        Code.wrapThrow(() -> repo.initialize());
        return repo;
    }
    
    public static Properties getProperties(final String journalFile) {
        
        final Properties props = new Properties();
         
        props.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS, "false");
        props.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS, "false");
        props.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);
        
        // transient means that there is nothing to delete after the test.
//        props.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Disk.toString());

        // journal file
        props.setProperty(Journal.Options.FILE, journalFile);

        // no inference
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");
        
        // yes text index
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "true");
        
        // sids mode
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "true");
        
        // we will manage the grounding of sids manually
        props.setProperty(AbstractTripleStore.Options.COMPUTE_CLOSURE_FOR_SIDS, "false");

//        props.setProperty(BigdataGraph.Options.READ_FROM_WRITE_CONNECTION, "true");

        // vocabulary and extensions
        props.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, BigdataCoreVocabulary_v20151210.class.getName());
//        props.setProperty(AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, CompressedTimestampExtensionFactory.class.getName());
        
        /*
         * Inline string literals up to 5 characters.
         */
        props.setProperty(AbstractTripleStore.Options.INLINE_TEXT_LITERALS, "true");
        props.setProperty(AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH, "5");

        return props;
        
    }
    

}
