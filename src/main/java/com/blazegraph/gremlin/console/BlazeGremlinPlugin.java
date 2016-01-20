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
package com.blazegraph.gremlin.console;

import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.groovy.plugin.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.groovy.plugin.IllegalEnvironmentException;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.PluginInitializationException;

/**
 * Plugin for gremlin console.
 *  
 * @author mikepersonick
 */
public final class BlazeGremlinPlugin extends AbstractGremlinPlugin {

    private static final Set<String> IMPORTS = new HashSet<String>() {{
        add(IMPORT_SPACE + "com.blazegraph.gremlin.embedded" + DOT_STAR);
        add(IMPORT_SPACE + "com.blazegraph.gremlin.listener" + DOT_STAR);
        add(IMPORT_SPACE + "com.blazegraph.gremlin.structure" + DOT_STAR);
        add(IMPORT_SPACE + "com.blazegraph.gremlin.util" + DOT_STAR);
    }};

    @Override
    public String getName() {
        return "tinkerpop.blazegraph";
    }

    @Override
    public void pluginTo(final PluginAcceptor pluginAcceptor) 
            throws PluginInitializationException, IllegalEnvironmentException {
        pluginAcceptor.addImports(IMPORTS);
    }

    @Override
    public void afterPluginTo(final PluginAcceptor pluginAcceptor) 
            throws IllegalEnvironmentException, PluginInitializationException {
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
    
}
