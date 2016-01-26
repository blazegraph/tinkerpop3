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

import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;

import com.blazegraph.gremlin.listener.BlazeGraphEdit;
import com.blazegraph.gremlin.listener.BlazeGraphEdit.Action;

/**
 * Bulk load API tests.
 * 
 * @author mikepersonick
 */
public class TestBulkLoad extends TestBlazeGraph {

    public void testBulkLoad1() {
        
        final List<BlazeGraphEdit> edits = new LinkedList<>();
        graph.addListener((edit,rdfEdit) -> edits.add(edit));
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        graph.bulkLoad(() -> {
            /*
             * In incremental update mode Cardinality.single would clean old
             * values when a new value is set.  If bulk load is working we
             * should see three values and no removes.  Breaks the semantics
             * of Cardinality.single which is why bulk load should be used with
             * care.
             */
            a.property(Cardinality.single, "key", "v1");
            a.property(Cardinality.single, "key", "v2");
            a.property(Cardinality.single, "key", "v3");
        });
        graph.commit();
        
        // bulk load should be off again
        assertFalse(graph.isBulkLoad());
        // three properties
        assertEquals(3, a.properties().count());
        // zero removes
        assertEquals(0, edits.stream().filter(e -> e.getAction() == Action.Remove).count());
        
    }

    public void testBulkLoad2() {
        
        final List<BlazeGraphEdit> edits = new LinkedList<>();
        graph.addListener((edit,rdfEdit) -> edits.add(edit));
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        graph.setBulkLoad(true);
        /*
         * In incremental update mode Cardinality.single would clean old
         * values when a new value is set.  If bulk load is working we
         * should see three values and no removes.  Breaks the semantics
         * of Cardinality.single which is why bulk load should be used with
         * care.
         */
        a.property(Cardinality.single, "key", "v1");
        a.property(Cardinality.single, "key", "v2");
        a.property(Cardinality.single, "key", "v3");
        graph.commit();
        graph.setBulkLoad(false);
        
        // bulk load should be off again
        assertFalse(graph.isBulkLoad());
        // three properties
        assertEquals(3, a.properties().count());
        // zero removes
        assertEquals(0, edits.stream().filter(e -> e.getAction() == Action.Remove).count());
        
    }

}
