/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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
package com.blazegraph.gremlin.embedded;

import java.util.Properties;

import com.bigdata.journal.BufferMode;
import com.bigdata.journal.Journal;
import com.bigdata.rdf.axioms.NoAxioms;
import com.bigdata.rdf.sail.BigdataSail;
import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.RDRHistory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.blazegraph.gremlin.internal.Tinkerpop3CoreVocab_v10;
import com.blazegraph.gremlin.internal.Tinkerpop3ExtensionFactory;
import com.blazegraph.gremlin.internal.Tinkerpop3InlineURIFactory;
import com.blazegraph.gremlin.util.Code;

/**
 * This utilty class is provided as a convenience for getting started with
 * Blazegraph.  You will eventually want to create your own custom configuration
 * specific to your application.  Visit our online 
 * <a href="http://wiki.blazegraph.com">user guide</a> for more information
 * on configuration and performance optimization, or contact us for more direct
 * developer support.
 *  
 * @author mikepersonick
 */
public class BasicRepositoryProvider {

    /**
     * Open and initialize a BigdataSailRepository using the supplied journal
     * file location.  If the file does not exist or is empty, the repository
     * will be created using the default property configuration below. 
     * 
     * @param journalFile
     *          absolute path of the journal file
     * @return
     *          an open and initialized repository
     */
    public static BigdataSailRepository open(final String journalFile) {
        return open(getProperties(journalFile));
    }
    
    /**
     * Open and initialize a BigdataSailRepository using the supplied config
     * properties.  You must specify a journal file in the properties.
     * 
     * @param props
     *          config properties
     * @return
     *          an open and initialized repository
     */
    public static BigdataSailRepository open(final Properties props) {
        if (props.getProperty(Journal.Options.FILE) == null) {
            throw new IllegalArgumentException();
        }
        
        final BigdataSail sail = new BigdataSail(props);
        final BigdataSailRepository repo = new BigdataSailRepository(sail);
        Code.wrapThrow(() -> repo.initialize());
        return repo;
    }
    
    /**
     * Grab the default properties and set the location of the journal file.
     * 
     * @param journalFile
     *          absolute path of the journal file
     * @return
     *          config properties
     */
    public static Properties getProperties(final String journalFile) {
        
        final Properties props = getDefaultProperties();

        // journal file
        props.setProperty(Journal.Options.FILE, journalFile);
 
        return props;
        
    }
    
    /**
     * Some reasonable defaults to get us up and running. Visit our online 
     * <a href="http://wiki.blazegraph.com">user guide</a> for more information
     * on configuration and performance optimization, or contact us for more direct
     * developer support.
     * 
     * @return
     *      config properties
     */
    public static Properties getDefaultProperties() {
        
        final Properties props = new Properties();

        /*
         * Use the RW store for persistence.
         */
        props.setProperty(Journal.Options.BUFFER_MODE, BufferMode.DiskRW.toString());

        /*
         * Turn off all RDFS/OWL inference.
         */
        props.setProperty(BigdataSail.Options.AXIOMS_CLASS, NoAxioms.class.getName());
        props.setProperty(BigdataSail.Options.TRUTH_MAINTENANCE, "false");
        props.setProperty(BigdataSail.Options.JUSTIFY, "false");

        /*
         * Turn on the text index.
         */
        props.setProperty(BigdataSail.Options.TEXT_INDEX, "true");

        /*
         * Turn off quads and turn on statement identifiers.
         */
        props.setProperty(BigdataSail.Options.QUADS, "false");
        props.setProperty(BigdataSail.Options.STATEMENT_IDENTIFIERS, "true");
        
        /*
         * We will manage the grounding of sids manually.
         */
        props.setProperty(AbstractTripleStore.Options.COMPUTE_CLOSURE_FOR_SIDS, "false");

        /*
         * Inline string literals up to 10 characters (avoids dictionary indices
         * for short strings).
         */
        props.setProperty(AbstractTripleStore.Options.INLINE_TEXT_LITERALS, "true");
        props.setProperty(AbstractTripleStore.Options.MAX_INLINE_TEXT_LENGTH, "10");
        
        /*
         * Custom core Tinkerpop3 vocabulary.  Applications will probably want
         * to extend this.
         */
        props.setProperty(AbstractTripleStore.Options.VOCABULARY_CLASS, 
                Tinkerpop3CoreVocab_v10.class.getName());
        
        /*
         * Use a multi-purpose inline URI factory that will auto-inline URIs
         * in the <blaze:> namespace.
         */
        props.setProperty(AbstractTripleStore.Options.INLINE_URI_FACTORY_CLASS,
                Tinkerpop3InlineURIFactory.class.getName());
        
        /*
         * Custom Tinkerpop3 extension factory for the ListIndexExtension IV,
         * used for Cardinality.list.
         */
        props.setProperty(AbstractTripleStore.Options.EXTENSION_FACTORY_CLASS, 
                Tinkerpop3ExtensionFactory.class.getName());
        
        /*
         * Turn on history.  You can turn history off by not setting this
         * property.
         */
        props.setProperty(AbstractTripleStore.Options.RDR_HISTORY_CLASS, 
                RDRHistory.class.getName());

        return props;
        
    }
    
}
