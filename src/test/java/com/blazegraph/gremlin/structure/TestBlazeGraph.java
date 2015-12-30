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

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.LambdaLogger;
import com.blazegraph.gremlin.util.Streams;

import junit.framework.TestCase;

public class TestBlazeGraph extends TestCase {

    private final static transient LambdaLogger log = LambdaLogger.getLogger(TestBlazeGraph.class);
    
    private BlazeGraphEmbedded graph = null;
    
    @Override
    public void setUp() throws Exception {
        this.graph = EmbeddedBlazeGraphProvider.open();
    }
    
    @Override
    public void tearDown() throws Exception {
        this.graph.close();
        this.graph.__tearDownUnitTest();
    }
    
    public void testSimpleVertex() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a", T.label, "Foo");
        graph.tx().commit();
        
        {
            final List<BlazeVertex> list = collect(graph.vertices(), BlazeVertex.class);
            assertEquals(1, list.size());
            assertEquals(a.id(), list.get(0).id());
        }
        {
            final List<BlazeVertex> list = collect(graph.vertices(a), BlazeVertex.class);
            assertEquals(1, list.size());
            assertEquals(a.id(), list.get(0).id());
        }
        {
            final List<BlazeVertex> list = collect(graph.vertices(a.id()), BlazeVertex.class);
            assertEquals(1, list.size());
            assertEquals(a.id(), list.get(0).id());
        }
        
        a.property("foo", "bar").property("created", "now");
        graph.tx().commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        {
            final List<BlazeVertexProperty> vps = collect(a.properties(), BlazeVertexProperty.class);
            assertEquals(1, vps.size());
            final BlazeVertexProperty vp = vps.get(0);
            assertEquals("foo", vp.key());
            assertEquals("bar", vp.value());
            final List<BlazeProperty> props = collect(vp.properties(), BlazeProperty.class);
            assertEquals(1, props.size());
            final BlazeProperty prop = props.get(0);
            assertEquals("created", prop.key());
            assertEquals("now", prop.value());
        }
        
        {
            final List<BlazeVertexProperty> vps = collect(a.properties("foo"), BlazeVertexProperty.class);
            assertEquals(1, vps.size());
            final BlazeVertexProperty vp = vps.get(0);
            assertEquals("foo", vp.key());
            assertEquals("bar", vp.value());
            final List<BlazeProperty> props = collect(vp.properties("created"), BlazeProperty.class);
            assertEquals(1, props.size());
            final BlazeProperty prop = props.get(0);
            assertEquals("created", prop.key());
            assertEquals("now", prop.value());
        }
        
        {
            final List<BlazeProperty> props = collect(a.properties("xyz"), BlazeProperty.class);
            assertEquals(0, props.size());
        }
        
    }
    
    public void testSimpleEdge() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeEdge e = graph.addEdge(a, b, Edge.DEFAULT_LABEL, T.id, "e");
        graph.tx().commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        {
            final List<BlazeVertex> list = collect(graph.vertices(), BlazeVertex.class);
            assertEquals(2, list.size());
        }
        {
            final List<BlazeVertex> list = collect(graph.vertices(a, b), BlazeVertex.class);
            assertEquals(2, list.size());
        }
        {
            final List<BlazeVertex> list = collect(graph.vertices(a.id(), b.id()), BlazeVertex.class);
            assertEquals(2, list.size());
        }
        {
            final List<BlazeEdge> list = collect(graph.edges(), BlazeEdge.class);
            assertEquals(1, list.size());
            assertEquals(e.id(), list.get(0).id());
        }
        {
            final List<BlazeEdge> list = collect(graph.edges(e), BlazeEdge.class);
            assertEquals(1, list.size());
            assertEquals(e.id(), list.get(0).id());
        }
        {
            final List<BlazeEdge> list = collect(graph.edges(e.id()), BlazeEdge.class);
            assertEquals(1, list.size());
            assertEquals(e.id(), list.get(0).id());
        }
        
        e.property("foo", "bar");
        graph.tx().commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        {
            final List<Property> props = collect(e.properties(), Property.class);
            log.debug(() -> props.stream());
            assertEquals(1, props.size());
            assertEquals("foo", props.get(0).key());
            assertEquals("bar", props.get(0).value());
        }
        
//        {
//            final List<Property> props = collect(e.properties("foo"), Property.class);
//            assertEquals(1, props.size());
//            assertEquals("foo", props.get(0).key());
//            assertEquals("bar", props.get(0).value());
//        }
//        
//        {
//            final List<Property> props = collect(e.properties("xyz"), Property.class);
//            assertEquals(0, props.size());
//        }

    }
    
    public void testVerticesFromEdge() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeVertex c = graph.addVertex(T.id, "c");
        final BlazeVertex d = graph.addVertex(T.id, "d");
        final BlazeEdge x = graph.addEdge(a, b, "foo", T.id, "x");
        final BlazeEdge y = graph.addEdge(a, c, "bar", T.id, "y");
        final BlazeEdge z = graph.addEdge(c, d, "bar", T.id, "z");
        graph.tx().commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        {
            final List<BlazeVertex> vertices = 
                    collect(x.vertices(Direction.BOTH), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(2, vertices.size());
            assertTrue(vertices.contains(a));
            assertTrue(vertices.contains(b));
        }
        
        {
            final List<BlazeVertex> vertices = 
                    collect(x.vertices(Direction.OUT), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(1, vertices.size());
            assertTrue(vertices.contains(a));
        }
        
        {
            final List<BlazeVertex> vertices = 
                    collect(x.vertices(Direction.IN), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(1, vertices.size());
            assertTrue(vertices.contains(b));
        }
        
    }
    
    
    public void testEdgesFromVertex() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeVertex c = graph.addVertex(T.id, "c");
        final BlazeVertex d = graph.addVertex(T.id, "d");
        final BlazeEdge x = graph.addEdge(a, b, "foo", T.id, "x");
        final BlazeEdge y = graph.addEdge(a, c, "bar", T.id, "y");
        final BlazeEdge z = graph.addEdge(c, d, "bar", T.id, "z");
        graph.tx().commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        {
            final List<BlazeEdge> edges = 
                    collect(a.edges(Direction.OUT), BlazeEdge.class);
            log.debug(() -> edges.stream());
            assertEquals(2, edges.size());
            assertTrue(edges.contains(x));
            assertTrue(edges.contains(y));
            
            final List<BlazeVertex> vertices = 
                    collect(a.vertices(Direction.OUT), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(2, vertices.size());
            assertTrue(vertices.contains(b));
            assertTrue(vertices.contains(c));
        }
        
        {
            final List<BlazeEdge> edges = 
                    collect(c.edges(Direction.IN), BlazeEdge.class);
            log.debug(() -> edges.stream());
            assertEquals(1, edges.size());
            assertTrue(edges.contains(y));
            
            final List<BlazeVertex> vertices = 
                    collect(c.vertices(Direction.IN), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(1, vertices.size());
            assertTrue(vertices.contains(a));
        }
        
        {
            final List<BlazeEdge> edges = 
                    collect(c.edges(Direction.OUT), BlazeEdge.class);
            log.debug(() -> edges.stream());
            assertEquals(1, edges.size());
            assertTrue(edges.contains(z));
            
            final List<BlazeVertex> vertices = 
                    collect(c.vertices(Direction.OUT), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(1, vertices.size());
            assertTrue(vertices.contains(d));
        }
        
        {
            final List<BlazeEdge> edges = 
                    collect(c.edges(Direction.BOTH), BlazeEdge.class);
            log.debug(() -> edges.stream());
            assertEquals(2, edges.size());
            assertTrue(edges.contains(y));
            assertTrue(edges.contains(z));
            
            final List<BlazeVertex> vertices = 
                    collect(c.vertices(Direction.BOTH), BlazeVertex.class);
            log.debug(() -> vertices.stream());
            assertEquals(2, vertices.size());
            assertTrue(vertices.contains(a));
            assertTrue(vertices.contains(d));
        }
        
    }
    
    public void testMultiProperties() throws Exception {
        final BlazeVertex v = graph.addVertex("name", "marko", "age", 34);
        graph.tx().commit();
        
        v.property("name").property("acl", "public");
        v.property("age").property("acl", "private");
        v.property("name").property("acl", "public");
        v.property("age").property("acl", "private");
        v.property("age").property("acl", "public");
        v.property("age").property("changeDate", 2014);
        graph.tx().commit();
        
        log.debug(() -> "\n"+graph.dumpStore());
        
        assertEquals(2, count(v.properties()));
        assertEquals(1, count(v.property("name").properties()));
        assertEquals(2, count(v.property("age").properties()));
        
        
    }
    
    public void testRemove() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        final BlazeVertex b = graph.addVertex(T.id, "b");
        final BlazeVertex c = graph.addVertex(T.id, "c");
        final BlazeVertex d = graph.addVertex(T.id, "d");
        final BlazeEdge x = graph.addEdge(a, b, "foo", T.id, "x");
        final BlazeEdge y = graph.addEdge(a, c, "bar", T.id, "y");
        final BlazeEdge z = graph.addEdge(c, d, "bar", T.id, "z");
        graph.tx().commit();
        
        assertEquals(4, count(graph.vertices()));
        assertEquals(3, count(graph.edges()));
        
        graph.remove(a);
        graph.tx().commit();
        
        assertEquals(3, count(graph.vertices()));
        assertEquals(1, count(graph.edges()));
        assertEquals(0, count(b.edges(Direction.BOTH)));
        assertEquals(1, count(c.edges(Direction.BOTH)));
        assertEquals(1, count(d.edges(Direction.BOTH)));
        
        graph.remove(z);
        graph.tx().commit();
        
        assertEquals(3, count(graph.vertices()));
        assertEquals(0, count(graph.edges()));
        assertEquals(0, count(b.edges(Direction.BOTH)));
        assertEquals(0, count(c.edges(Direction.BOTH)));
        assertEquals(0, count(d.edges(Direction.BOTH)));
        
    }
    
    public void testRemoveVertexProperties() throws Exception {
        
        final BlazeValueFactory vf = graph.valueFactory();
        
        {
            final BlazeVertex a = graph.addVertex(T.id, "a");
            a.property(Cardinality.single, "foo", "bar", "acl", "public");
            graph.tx().commit();

            a.remove();
            graph.tx().commit();

            log.debug(() -> "\n"+graph.dumpStore());

            final URI p = vf.propertyURI("acl");
            final Literal o = vf.toLiteral("public");
            assertFalse(hasStatement(null, p, o));
        }
        
        {
            final BlazeVertex b = graph.addVertex(T.id, "b");
            b.property(Cardinality.single, "foo", "bar", "acl", "public");
            graph.tx().commit();

            b.property(Cardinality.single, "foo", "baz");
            graph.tx().commit();
            
            log.debug(() -> "\n"+graph.dumpStore());
            
            final URI p = vf.propertyURI("acl");
            final Literal o = vf.toLiteral("public");
            assertFalse(hasStatement(null, p, o));
        }
        
    }
    
    public void testCardinalityList() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        a.property(Cardinality.list, "foo", "bar", "acl", "public");
        a.property(Cardinality.list, "foo", "baz", "acl", "public");
        a.property(Cardinality.list, "foo", "bar", "acl", "private");
        graph.tx().commit();

        log.debug(() -> "\n"+graph.dumpStore());

        final List<VertexProperty<Object>> vps = a.properties("foo").collect();
        log.debug(() -> vps.stream());
        assertEquals(3, vps.size());
        
        vps.get(0).remove();
        graph.tx().commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(2, a.properties().count());
        
        vps.get(1).remove();
        graph.tx().commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(1, a.properties().count());
        
        a.remove();
        graph.tx().commit();
        log.debug(() -> "\n"+graph.dumpStore());
        assertEquals(0, a.properties().count());
        
    }
    
    public void testSwitchCardinality() throws Exception {
        
        final BlazeVertex a = graph.addVertex(T.id, "a");
        a.property(Cardinality.list, "foo", "bar", "acl", "public");
        a.property(Cardinality.list, "foo", "baz", "acl", "public");
        a.property(Cardinality.list, "foo", "bar", "acl", "private");
        graph.tx().commit();

        log.debug(() -> "\n"+graph.dumpStore());

        a.property(Cardinality.single, "foo", "single", "acl", "public");
        graph.tx().commit();

        log.debug(() -> "\n"+graph.dumpStore());
        
        assertEquals(1, a.properties().count());
        
    }
    
    public void testRollback() throws Exception {
        
        final Vertex v1 = graph.addVertex("name", "marko");
        final Edge e1 = v1.addEdge("l", v1, "name", "xxx");
        graph.tx().commit();
        
        assertEquals(v1.id(), graph.vertices(v1.id()).next().id());
        assertEquals(e1.id(), graph.edges(e1.id()).next().id());
        assertEquals("marko", v1.<String>value("name"));
        assertEquals("xxx", e1.<String>value("name"));

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", graph.vertices(v1.id()).next().<String>value("name"));

        v1.property(VertexProperty.Cardinality.single, "name", "stephen");

        graph.readFromWriteCxn(() -> {
            assertEquals("stephen", v1.<String>value("name"));
            assertEquals("stephen", graph.vertices(v1.id()).next().<String>value("name"));
        });

        graph.tx().rollback();

        assertEquals("marko", v1.<String>value("name"));
        assertEquals("marko", graph.vertices(v1.id()).next().<String>value("name"));

    }
    
    private boolean hasStatement(final Resource s, final URI p, final Literal o) throws Exception {
        final BigdataSailRepositoryConnection cxn = graph.readCxn();
        try {
            return cxn.hasStatement(null, p, o, true);
        } finally {
            cxn.close();
        }
    }
    
    private <E> List<E> collect(final CloseableIterator<?> it, final Class<E> cls) {
        try {
            return Streams.of(it).map(cls::cast).collect(toList());
        } finally {
            it.close();
        }
    }
    
    private long count(final CloseableIterator<?> it) {
        try {
            return Streams.of(it).count();
        } finally {
            it.close();
        }
    }
    
}
