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
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import com.bigdata.rdf.store.AbstractTripleStore;
import com.blazegraph.gremlin.structure.BlazeGraph.Match;
import com.blazegraph.gremlin.util.LambdaLogger;

public class TestSearch extends TestBlazeGraph {

    private final static transient LambdaLogger log = LambdaLogger.getLogger(TestSearch.class);

//    @Override
//    protected Map<String,String> overrides() {
//        return new HashMap<String,String>() {{
//            put(AbstractTripleStore.Options.RDR_HISTORY_CLASS, null);
//        }};
//    }

    /**
     * Make sure we can find all five kinds of properties: Edge, 
     * Vertex (single/set), Vertex (list), VertexProperty (single/set), and
     * VertexProperty (list).
     */
    public void testBasicSearch() {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeEdge e = graph.addEdge(a, b, Edge.DEFAULT_LABEL, T.id, "e");
        
        final Property<String> ep = e.property("ep", "foo 1");
        final VertexProperty<String> vps = a.property(Cardinality.single, "vps", "foo 2");
        final VertexProperty<String> vpl = a.property(Cardinality.list, "vpl", "foo 3");
        final Property<String> vpsp = vps.property("vpsp", "foo 4");
        final Property<String> vplp = vpl.property("vplp", "foo 5");
        graph.commit();
        
        final Map<String,Property<String>> expected = 
                new HashMap<String,Property<String>>() {{
            put("foo 1", ep);
            put("foo 2", vps);
            put("foo 3", vpl);
            put("foo 4", vpsp);
            put("foo 5", vplp);
        }};
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        final List<Property<String>> results = 
                graph.<String>search("foo", Match.ANY).collect();
        log.debug(() -> results.stream());
        assertEquals(5, results.size());
        results.stream().forEach(p -> {
            assertEquals(expected.get(p.value()), p);
        });
        
    }

}
