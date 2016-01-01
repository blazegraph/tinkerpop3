/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;

/**
 * Tinkerpop GraphProvider for the TP3 test suites.
 * 
 * @author mikepersonick
 */
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

    private static final Set<Class> IMPLEMENTATIONS = new HashSet<Class>() {{
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
            final String name = graphName+"-"+test.getName()+"-"+testMethodName;
            put(Options.REPOSITORY_NAME, name);
            put(Graph.GRAPH, EmbeddedBlazeGraphProvider.class.getName());
        }};
    }
    
    @Override
    public void loadGraphData(final Graph graph, final LoadGraphWith loadGraphWith, final Class testClass, final String testName) {
        if (graph != null) {
            final BlazeGraphEmbedded blazeGraph = (BlazeGraphEmbedded) graph;
            blazeGraph.bulkLoad(() -> super.loadGraphData(graph, loadGraphWith, testClass, testName));
        }
    }
    
    public static BlazeGraphEmbedded open(final Configuration config) {
        final String name = config.getString(Options.REPOSITORY_NAME);
        final BigdataSailRepository repo = getRepository(name);
        return BlazeGraphEmbedded.open(repo, config);
    }
    

    private static final Map<String,String> repos = new HashMap<>();
    private static synchronized BigdataSailRepository getRepository(final String name) {
        final String journal;
        if (repos.containsKey(name)) {
            journal = repos.get(name);
        } else {
            repos.put(name, journal = TestRepositoryProvider.tmpJournal());
        }
        return TestRepositoryProvider.open(journal);
    }

}
