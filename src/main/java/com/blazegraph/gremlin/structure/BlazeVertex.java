package com.blazegraph.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyVertexProperty;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataURI;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.CloseableIterators;

public class BlazeVertex extends AbstractBlazeElement implements Vertex, BlazeElement {

    public BlazeVertex(final BlazeGraph graph, final BigdataURI uri, final Literal label) {
        super(graph, uri, label);
    }
    
    @Override
    public BigdataURI rdfId() {
        return uri;
    }
    
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
    
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
    
    @Override
    public <V> BlazeVertexProperty<V> property(final String key, final V val) {
        return property(Cardinality.single, key, val);
    }

    @Override
    public <V> BlazeVertexProperty<V> property(final Cardinality cardinality, 
            final String key, final V val, final Object... kvs) {
        ElementHelper.validateProperty(key, val);
        if (ElementHelper.getIdValue(kvs).isPresent())
            throw Vertex.Exceptions.userSuppliedIdsNotSupported();
        
        final BlazeVertexProperty<V> prop = graph.vertexProperty(this, key, val, cardinality);
        ElementHelper.attachProperties(prop, kvs);
        return prop;
    }
    
//    private boolean clean(final Cardinality cardinality) {
//        switch(cardinality) {
//        case single: return true; 
//        case set: 
//        case list: return false;
//        default: throw new IllegalArgumentException("Cardinality not supported: " + cardinality);
//        }
//    }

    @Override
    public <V> CloseableIterator<VertexProperty<V>> properties(String... keys) {
        return graph.properties(this, keys);
    }
    
    @Override
    public BlazeEdge addEdge(final String label, final Vertex to, final Object... kvs) {
        if (to == null) throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        return graph.addEdge(this, (BlazeVertex) to, label, kvs);
    }

    
    @Override
    public CloseableIterator<Edge> edges(final Direction direction, 
            final String... edgeLabels) {
        return graph.edgesFromVertex(this, direction, edgeLabels);
    }
    
    @Override
    public CloseableIterator<Vertex> vertices(final Direction direction, 
            final String... edgeLabels) {
        final CloseableIterator<Edge> edges = edges(direction, edgeLabels);
        return CloseableIterators.of(edges, e -> {
            final BlazeEdge be = (BlazeEdge) e;
            return be.outVertex().equals(this) ? be.inVertex() : be.outVertex();
        });
    }
    
    @Override
    public void remove() {
        graph.remove(this);
    }
    
}
