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

import java.util.LinkedList;
import java.util.List;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.util.LambdaLogger;

public class TestListeners extends TestBlazeGraph {

    private final static transient LambdaLogger log = LambdaLogger.getLogger(TestListeners.class);
    
    public void testVertices() throws Exception {
        
        final List<BlazeGraphEdit> edits = new LinkedList<>();
        graph.addListener((edit,rdfEdit) -> edits.add(edit));
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(2, graph.statementCount());
        assertEquals(2, graph.vertexCount());
        assertEquals(2, edits.size());

        a.remove();
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(1, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(3, edits.size());
        
        b.remove();
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(0, graph.statementCount());
        assertEquals(0, graph.vertexCount());
        assertEquals(4, edits.size());
        
        
    }

    public void testVertexProperties() throws Exception {
        
        final List<BlazeGraphEdit> edits = new LinkedList<>();
        graph.addListener((edit,rdfEdit) -> edits.add(edit));
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertexProperty<Integer> _1 = a.property(Cardinality.set, "foo", 1, "acl", "public");
        final BlazeVertexProperty<Integer> _2 = a.property(Cardinality.set, "foo", 2, "acl", "private");
//        final BlazeVertex b = graph.addVertex(T.id, "b");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(5, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(2, a.properties().count());
        assertEquals(1, _1.properties().count());
        assertEquals(1, _2.properties().count());
        assertEquals(5, edits.size());

        _1.properties().forEachRemaining(p -> p.remove());
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(4, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(2, a.properties().count());
        assertEquals(0, _1.properties().count());
        assertEquals(1, _2.properties().count());
        assertEquals(6, edits.size());
        
        _2.remove();
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(2, graph.statementCount());
        assertEquals(1, graph.vertexCount());
        assertEquals(1, a.properties().count());
        assertEquals(8, edits.size());
        
        a.remove();
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(0, graph.statementCount());
        assertEquals(0, graph.vertexCount());
        assertEquals(10, edits.size());
        
    }
    
    public void testEdges() throws Exception {
        
        final List<BlazeGraphEdit> edits = new LinkedList<>();
        graph.addListener((edit,rdfEdit) -> {
            log.debug(() -> rdfEdit);
            edits.add(edit);
        });
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeEdge e = graph.addEdge(a, b, Edge.DEFAULT_LABEL, T.id, "e", "key", "val");
        graph.commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(5, graph.statementCount());
        assertEquals(4, edits.size());
        
        e.properties().forEachRemaining(p -> p.remove());
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(4, graph.statementCount());
        assertEquals(5, edits.size());

        e.remove();
        graph.commit();
        log.debug(() -> "\n"+graph.dumpStore());
        log.debug(() -> edits.stream());
//        assertEquals(2, graph.statementCount());
        assertEquals(6, edits.size());
        
    }
    
}
