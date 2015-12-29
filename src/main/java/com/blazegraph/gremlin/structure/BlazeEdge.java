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

import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.blazegraph.gremlin.util.CloseableIterator;

public class BlazeEdge extends AbstractBlazeElement implements Edge, BlazeReifiedElement {

    private final BigdataBNode sid;
    
//    private final Memoizer<Vertices> vertices;
    private final Vertices vertices;
    
//    public BlazeEdge(final BlazeGraph graph, final BigdataStatement stmt, 
//            final Literal label) {
//        this(graph, stmt, label, null);
//    }
    
    public BlazeEdge(final BlazeGraph graph, final BigdataStatement stmt, 
            final Literal label, final BlazeVertex from, final BlazeVertex to) {
        this(graph, stmt, label, new Vertices(from, to));
    }
    
    private BlazeEdge(final BlazeGraph graph, final BigdataStatement stmt, 
            final Literal label, Vertices vertices) {
        super(graph, stmt.getPredicate(), label);
        final BigdataValueFactory rdfvf = graph.rdfValueFactory();
        this.sid = rdfvf.createBNode(stmt);
//        this.vertices = new Memoizer<>(verticesCompute, vertices);
        this.vertices = vertices;
    }
    
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
    
    @Override
    public BigdataBNode rdfId() {
        return sid;
    }
    
    @Override
    public void remove() {
//        throw Edge.Exceptions.edgeRemovalNotSupported();
        graph.remove(this);
    }

    @Override
    public <V> BlazeProperty<V> property(String key, V val) {
        return BlazeReifiedElement.super.property(key, val);
    }

    @Override
    public <V> CloseableIterator<Property<V>> properties(final String... keys) {
        return BlazeReifiedElement.super.properties(keys);
    }
    
    /**
     * Get the outgoing/tail vertex of this edge.
     *
     * @return the outgoing vertex of the edge
     */
    @Override
    public Vertex outVertex() {
        return vertices.from;
    }

    /**
     * Get the incoming/head vertex of this edge.
     *
     * @return the incoming vertex of the edge
     */
    @Override
    public Vertex inVertex() {
        return vertices.to;
    }

    /**
     * Get both the outgoing and incoming vertices of this edge.
     * The first vertex in the iterator is the outgoing vertex.
     * The second vertex in the iterator is the incoming vertex.
     *
     * @return an iterator of the two vertices of this edge
     */
    @Override
    public CloseableIterator<Vertex> bothVertices() {
        return this.vertices(Direction.BOTH);
    }

    @Override
    public CloseableIterator<Vertex> vertices(final Direction direction) {
//        if (removed) return CloseableIterators.emptyIterator();
//        final Vertices vertices = this.vertices.get();
        final BlazeVertex from = vertices.from;
        final BlazeVertex to = vertices.to;
        final Stream<BlazeVertex> stream;
        switch (direction) {
            case OUT:
                stream = Stream.of(from); break;
            case IN:
                stream = Stream.of(to); break;
            default:
                stream = Stream.of(from, to); break;
        }
        return CloseableIterator.of(stream.map(Vertex.class::cast));
    }
    
//    private final Callable<Vertices> verticesCompute = () -> {
//        final BigdataStatement stmt = this.rdfId().getStatement();
//        final URI s = (URI) stmt.getSubject();
//        final URI o = (URI) stmt.getObject();
//        if (s.equals(o)) {
//            final BlazeVertex v = graph.vertex(vf.elementId(s)).get();
//            return new Vertices(v, v);
//        } else {
//            final List<BlazeVertex> vertices = 
//                    Streams.of(graph.vertices(vf.elementId(s), vf.elementId(o)))
//                           .map(BlazeVertex.class::cast)
//                           .collect(toList());
//            final BlazeVertex from, to;
//            if (vertices.get(0).rdfId().equals(s)) {
//                from = vertices.get(0); to = vertices.get(1);
//            } else {
//                from = vertices.get(1); to = vertices.get(0);
//            }
//            return new Vertices(from, to);
//        }
//    };

    private static class Vertices {
        private final BlazeVertex from, to;
        public Vertices(final BlazeVertex from, final BlazeVertex to) {
            this.from = from;
            this.to = to;
        }
    }
    
}
