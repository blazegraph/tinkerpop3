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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import com.bigdata.rdf.sail.RDRHistory;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;
import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.util.LambdaLogger;

import junit.framework.TestCase;

public class TestHistory extends TestBlazeGraph {

    private final static transient LambdaLogger log = LambdaLogger.getLogger(TestHistory.class);
    
    public void testVertexHistory() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(4, graph.statementCount());
        assertEquals(2, graph.vertexCount());
        countEdits(2, "a", "b");
        
        a.remove();
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(5, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        countEdits(2, "a");
        
        b.remove();
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(6, graph.statementCount());
        assertEquals(0, graph.vertexCount());
        countEdits(2, "b");
        
        final List<BlazeGraphEdit> edits = graph.history("a", "b").collect();
        log.debug(() -> edits.stream());
        assertEquals(4, edits.size());
        
    }

    public void testVertexPropertyHistory() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertexProperty<Integer> _1 = a.property(Cardinality.set, "foo", 1, "acl", "public");
        final BlazeVertexProperty<Integer> _2 = a.property(Cardinality.set, "foo", 2, "acl", "private");
//        final BlazeVertex b = graph.addVertex(T.id, "b");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(10, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(2, a.properties().count());
        assertEquals(1, _1.properties().count());
        assertEquals(1, _2.properties().count());

        _1.properties().forEachRemaining(p -> p.remove());
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(11, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(2, a.properties().count());
        assertEquals(0, _1.properties().count());
        assertEquals(1, _2.properties().count());
        
        countEdits(3, "a");
        
        try {
            graph.history(_1.id(), _2.id());
            fail("History not supported for VertexProperty elements");
        } catch (IllegalArgumentException ex) {}
        
        _2.remove();
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(13, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(1, a.properties().count());
        countEdits(4, "a");
        
        a.remove();
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(15, graph.statementCount());
        assertEquals(0, graph.vertexCount());
        countEdits(6, "a");
        
    }
    
    public void testEdgeHistory() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeEdge e = graph.addEdge(a, b, Edge.DEFAULT_LABEL, T.id, "e", "key", "val");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(10, graph.statementCount());
        countEdits(1, a.id());
        countEdits(1, b.id());
        countEdits(2, e.id());
        
        e.properties().forEachRemaining(p -> p.remove());
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(11, graph.statementCount());
        countEdits(1, a.id());
        countEdits(1, b.id());
        countEdits(3, e.id());

        e.remove();
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(13, graph.statementCount());
        countEdits(1, a.id());
        countEdits(1, b.id());
        countEdits(4, e.id());
        
    }
    
    private void countEdits(final int expected, final String... ids) {
        final List<BlazeGraphEdit> edits = graph.history(ids).collect();
        log.debug(() -> edits.stream());
        assertEquals(expected, edits.size());
    }

}
