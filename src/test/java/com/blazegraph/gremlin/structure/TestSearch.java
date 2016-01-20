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
package com.blazegraph.gremlin.structure;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

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
     * Make sure we can find all five kinds of properties:
     * <p>
     * <pre>
     * Edge -> Property
     * Vertex -> VertexProperty(single/set)
     * Vertex -> VertexProperty(list)
     * VertexProperty(single/set) -> Property
     * VertexProperty(list) -> Property
     * </pre>
     * </p>
     */
    public void testAllFiveKinds() {
        
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

    /**
     * Test for Match.ANY/ALL/EXACT.
     */
    public void testMatchEnum() {
        
        final VertexProperty<String> any = graph.addVertex(T.id, "any").property(Cardinality.set, "key", "foo");
        final VertexProperty<String> all = graph.addVertex(T.id, "all").property(Cardinality.set, "key", "foo bar");
        final VertexProperty<String> exact = graph.addVertex(T.id, "exact").property(Cardinality.set, "key", "bar foo");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());

        {
            final List<Property<String>> results = 
                    graph.<String>search("bar foo", Match.ANY).collect();
            log.debug(() -> results.stream());
            assertEquals(3, results.size());
            assertTrue(results.contains(any));
            assertTrue(results.contains(all));
            assertTrue(results.contains(exact));
        }
        
        {
            final List<Property<String>> results = 
                    graph.<String>search("bar foo", Match.ALL).collect();
            log.debug(() -> results.stream());
            assertEquals(2, results.size());
            assertTrue(results.contains(all));
            assertTrue(results.contains(exact));
        }
        
        {
            final List<Property<String>> results = 
                    graph.<String>search("bar foo", Match.EXACT).collect();
            log.debug(() -> results.stream());
            assertEquals(1, results.size());
            assertTrue(results.contains(exact));
        }
        
    }
    
}
