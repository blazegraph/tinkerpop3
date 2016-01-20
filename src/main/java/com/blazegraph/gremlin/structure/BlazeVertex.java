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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataURI;
import com.blazegraph.gremlin.util.CloseableIterator;

/**
 * Concrete vertex implementation for BlazeGraph.
 * <p/>
 * Vertex existence is represented as one triples as follows:
 * <p/>
 * <pre>
 *     :vertexId rdf:type :label .
 * </pre>
 * <p/>
 * Vertex properties are represented as follows:
 * <p/>
 * <pre>
 *     :vertexId :key "val" .
 * </pre>
 * 
 * @author mikepersonick
 */
public class BlazeVertex extends AbstractBlazeElement implements Vertex, BlazeElement {

    /**
     * Construct an instance.
     */
    BlazeVertex(final BlazeGraph graph, final BigdataURI uri, 
            final BigdataURI label) {
        super(graph, uri, label);
    }
    
    /**
     * Strengthen return type. Vertex RDF id is its URI.
     */
    @Override
    public BigdataURI rdfId() {
        return uri;
    }
    
    /**
     * Pass through to {@link StringFactory#vertexString(Vertex)}
     */
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
    
    /**
     * Strengthen return type to {@link BlazeVertexProperty}.
     * 
     * @see {@link Vertex#property(String)}
     * @see {@link BlazeVertex#properties(String...)}
     */
    @Override
    public <V> BlazeVertexProperty<V> property(final String key) {
        try (CloseableIterator<VertexProperty<V>> it = this.properties(key)) {
            if (it.hasNext()) {
                final VertexProperty<V> property = it.next();
                if (it.hasNext())
                    throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
                else
                    return (BlazeVertexProperty<V>) property;
            } else {
                return EmptyBlazeVertexProperty.instance();
            }
        }
    }
    
    /**
     * Strengthen return type to {@link BlazeVertexProperty} and use 
     * Cardinality.single by default.
     */
    @Override
    public <V> BlazeVertexProperty<V> property(final String key, final V val) {
        return property(Cardinality.single, key, val);
    }

    /**
     * Strengthen return type to {@link BlazeVertexProperty}.
     * 
     * @see {@link BlazeGraph#vertexProperty(BlazeVertex, Cardinality, String, Object, Object...)}
     */
    @Override
    public <V> BlazeVertexProperty<V> property(final Cardinality cardinality, 
            final String key, final V val, final Object... kvs) {
        ElementHelper.validateProperty(key, val);
        if (ElementHelper.getIdValue(kvs).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        
        return graph.vertexProperty(this, cardinality, key, val, kvs);
    }
    
    /**
     * Strength return type to {@link CloseableIterator}.  You MUST close this
     * iterator when finished.
     */
    @Override
    public <V> CloseableIterator<VertexProperty<V>> properties(String... keys) {
        return graph.properties(this, keys);
    }

    /**
     * @see {@link Vertex#addEdge(String, Vertex, Object...)}
     * @see {@link BlazeGraph#addEdge(BlazeVertex, BlazeVertex, String, Object...)}
     */
    @Override
    public BlazeEdge addEdge(final String label, final Vertex to, final Object... kvs) {
        if (to == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        return graph.addEdge(this, (BlazeVertex) to, label, kvs);
    }

    /**
     * Strength return type to {@link CloseableIterator}.  You MUST close this
     * iterator when finished.
     *
     * @see {@link Vertex#edges(Direction, String...)}
     * @see {@link BlazeGraph#edgesFromVertex(BlazeVertex, Direction, String...)}
     */
    @Override
    public CloseableIterator<Edge> edges(final Direction direction, 
            final String... edgeLabels) {
        return graph.edgesFromVertex(this, direction, edgeLabels);
    }
    
    /**
     * Strength return type to {@link CloseableIterator}.  You MUST close this
     * iterator when finished.
     * 
     * @see {@link Vertex#vertices(Direction, String...)}
     */
    @Override
    public CloseableIterator<Vertex> vertices(final Direction direction, 
            final String... edgeLabels) {
        return CloseableIterator.of(edges(direction, edgeLabels)
                    .stream()
                    .map(e -> {
                        final BlazeEdge be = (BlazeEdge) e;
                        return be.outVertex().equals(this) ? be.inVertex() : be.outVertex();
                    }));
    }
    
    /**
     * @see {@link Vertex#remove()}
     * @see {@link BlazeGraph#remove(BlazeVertex)}
     */
    @Override
    public void remove() {
        graph.remove(this);
    }
    
}
