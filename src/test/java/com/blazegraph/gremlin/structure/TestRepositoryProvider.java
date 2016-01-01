package com.blazegraph.gremlin.structure;

import java.io.File;
import java.util.Map;
import java.util.Properties;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BasicRepositoryProvider;
import com.blazegraph.gremlin.util.Code;

public class TestRepositoryProvider extends BasicRepositoryProvider {

    /**
     * Create a tmp journal file for test cases.
     * 
     * @return
     *      absolute path of journal file
     */
    public static String tmpJournal() {
        final File file = Code.wrapThrow(() -> File.createTempFile("EmbeddedBlazeGraphProvider", ".jnl"));
        file.deleteOnExit();
        return file.getAbsolutePath();
    }
    
    /**
     * Open with default configuration and a tmp journal file.
     * 
     * @return
     *          an open and initialized Blaze repository
     */
    public static BigdataSailRepository open() {
        final Properties props = getProperties(tmpJournal());
        return open(props);
    }
    
    /**
     * Open with default configuration and a tmp journal file, overriding the
     * default config as specified.
     * 
     * @return
     *          an open and initialized Blaze repository
     */
    public static BigdataSailRepository open(final Map<String,String> overrides) {
        final Properties props = getProperties(tmpJournal());
        overrides.entrySet().forEach(e -> {
            if (e.getValue() != null) {
                props.setProperty(e.getKey(), e.getValue());
            } else {
                props.remove(e.getKey());
            }
        });
        return open(props);
    }
    
}
