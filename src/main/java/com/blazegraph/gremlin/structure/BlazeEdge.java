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

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.blazegraph.gremlin.structure.BlazeGraphFeatures.Graph;
import com.blazegraph.gremlin.util.CloseableIterator;

/**
 * Concrete edge implementation for Blazegraph.
 * <p/>
 * Edge existence is represented as two triples as follows:
 * <p/>
 * <pre>
 *     :fromId :edgeId :toId .
 *     <<:fromId :edgeId :toId>> rdf:type :label .
 * </pre>
 * <p/>
 * Edge properties are represented as follows:
 * <p/>
 * <pre>
 *     <<:fromId :edgeId :toId>> :key "val" .
 * </pre>
 * 
 * @author mikepersonick
 */
public class BlazeEdge extends AbstractBlazeElement implements Edge, BlazeReifiedElement {

    /**
     * The sid (reified statement) for this edge.
     */
    private final BigdataBNode sid;

    /**
     * The from and to vertices.
     */
    private final Vertices vertices;
    
    /**
     * Construct an instance.
     */
    BlazeEdge(final BlazeGraph graph, final BigdataStatement stmt, 
            final BigdataURI label, final BlazeVertex from, final BlazeVertex to) {
        super(graph, stmt.getPredicate(), label);
        final BigdataValueFactory rdfvf = graph.rdfValueFactory();
        this.sid = rdfvf.createBNode(stmt);
        this.vertices = new Vertices(from, to);
    }
    
    /**
     * Construct an instance without vertices.  Used by 
     * {@link BlazeGraph#bulkLoad(Graph)}
     */
    BlazeEdge(final BlazeGraph graph, final BigdataStatement stmt, 
            final BigdataURI label) {
        super(graph, stmt.getPredicate(), label);
        final BigdataValueFactory rdfvf = graph.rdfValueFactory();
        this.sid = rdfvf.createBNode(stmt);
        this.vertices = null;
    }
    
    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
    
    /**
     * Return the sid (reified statement) for this edge.
     */
    @Override
    public BigdataBNode rdfId() {
        return sid;
    }
    
    /**
     * @see {@link Edge#remove()}
     * @see {@link BlazeGraph#remove(BlazeReifiedElement)}
     */
    @Override
    public void remove() {
        graph.remove(this);
    }

    /**
     * Strengthen return type to {@link BlazeProperty}.
     */
    @Override
    public <V> BlazeProperty<V> property(String key, V val) {
        return BlazeReifiedElement.super.property(key, val);
    }

    /**
     * Strength return type to {@link CloseableIterator}.  You MUST close this
     * iterator when finished.
     */
    @Override
    public <V> CloseableIterator<Property<V>> properties(final String... keys) {
        return BlazeReifiedElement.super.properties(keys);
    }
    
    /**
     * @see {@link Edge#outVertex()} 
     */
    @Override
    public Vertex outVertex() {
        return vertices.from;
    }

    /**
     * @see {@link Edge#inVertex()} 
     */
    @Override
    public Vertex inVertex() {
        return vertices.to;
    }

    /**
     * @see {@link Edge#bothVertices()} 
     */
    @Override
    public Iterator<Vertex> bothVertices() {
        return this.vertices(Direction.BOTH);
    }

    /**
     * @see {@link Edge#vertices(Direction)} 
     */
    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
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
        return stream.map(Vertex.class::cast).iterator();
    }

    private static class Vertices {
        private final BlazeVertex from, to;
        public Vertices(final BlazeVertex from, final BlazeVertex to) {
            this.from = from;
            this.to = to;
        }
    }
    
}
