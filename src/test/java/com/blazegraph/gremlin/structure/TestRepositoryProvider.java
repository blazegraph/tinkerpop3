/**
Copyright (C) SYSTAP, LLC 2006-2016.  All rights reserved.

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

import java.io.File;
import java.util.Map;
import java.util.Properties;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BasicRepositoryProvider;
import com.blazegraph.gremlin.util.Code;

/**
 * Extension of BasicRepositoryProvider for test cases.
 * 
 * @author mikepersonick
 */
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
