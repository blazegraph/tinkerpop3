package com.blazegraph.gremlin.structure;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.internal.InlineURIFactory;
import com.bigdata.rdf.internal.MultipurposeIDHandler;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.RDRHistory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.BaseVocabularyDecl;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20151210;
import com.blazegraph.gremlin.util.Code;

public class RepositoryProvider {

    public static String journal() {
        final File file = Code.wrapThrow(() -> File.createTempFile("EmbeddedBlazeGraphProvider", ".jnl"));
        file.deleteOnExit();
        return file.getAbsolutePath();
    }
    
    /**
     * Open with default configuration.
     */
    public static BigdataSailRepository open() {
        final Properties props = getProperties(journal());
        return open(props);
    }
    
    public static BigdataSailRepository open(final String journalFile) {
        return open(new HashMap<String,String>() {{
            put(Journal.Options.FILE, journalFile);
        }});
    }
    
    public static BigdataSailRepository open(final Map<String,String> overrides) {
        final Properties props = getProperties(journal());
        overrides.entrySet().forEach(e -> {
            if (e.getValue() != null) {
                props.setProperty(e.getKey(), e.getValue());
            } else {
                props.remove(e.getKey());
            }
        });
        return open(props);
    }
    
    public static BigdataSailRepository open(final Properties props) {
        final BigdataSail sail = new BigdataSail(props);
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        Code.wrapThrow(() -> repo.initialize());
        return repo;
    }
    
    public static Properties getProperties(final String journalFile) {
        
        final Properties props = new Properties();

        // journal file
        props.setProperty(Journal.Options.FILE, journalFile);
 
        props.setProperty(Journal.Options.COLLECT_PLATFORM_STATISTICS, "false");
        props.setProperty(Journal.Options.COLLECT_QUEUE_STATISTICS, "false");
        props.setProperty(Journal.Options.HTTPD_PORT, "-1"/* none */);
        
        // transient means that there is nothing to delete after the test.
//        props.setProperty(Options.BUFFER_MODE,BufferMode.Transient.toString());
        props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.Disk.toString());

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

        /*
         * Inline string literals up to 10 characters.
         */
        props.setProperty(AbstractTripleStore.Options.INLINE_TEXT_LITERALS, "true");
        props.setProperty(AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH, "10");
        
        /*
         * Inline PG URIs up to 10 characters.
         */
        props.setProperty(AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS,
                CustomInlineURIFactory.class.getName());
        
        // vocabulary
        props.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, CustomCoreVocab.class.getName());
        
        /*
         * Turn on history by default.
         */
        props.setProperty(AbstractTripleStore.Options.RDR_HISTORY_CLASS, RDRHistory.class.getName());

        return props;
        
    }
    
    public static class CustomInlineURIFactory extends InlineURIFactory {
        
        public CustomInlineURIFactory() {
            addHandler(new MultipurposeIDHandler(BlazeValueFactory.Defaults.NAMESPACE, 10));
        }
        
    }
    
    public static class CustomCoreVocab extends BigdataCoreVocabulary_v20151210 {

        /**
         * De-serialization ctor.
         */
        public CustomCoreVocab() {
            
            super();
            
        }
        
        /**
         * Used by {@link AbstractTripleStore#create()}.
         * 
         * @param namespace
         *            The namespace of the KB instance.
         */
        public CustomCoreVocab(final String namespace) {

            super(namespace);
            
        }

        @Override
        protected void addValues() {

            super.addValues();

            /*
             * Some new URIs for graph and RDR management.
             */
            addDecl(new BaseVocabularyDecl(
                    BlazeValueFactory.Defaults.NAMESPACE));

        }

    }
    
    

}
