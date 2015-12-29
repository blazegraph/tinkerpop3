package com.blazegraph.gremlin.structure;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;

import com.bigdata.rdf.model.BigdataResource;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.CloseableIterators;

public interface BlazeElement extends Element {

    @Override
    BlazeGraph graph();
    
    BigdataResource rdfId();
    
    Literal rdfLabel();
    
//    @Override
//    String id();
    
    /**
     * Strengthen return type to {@link CloseableIterator}.
     */
    @Override
    <V> CloseableIterator<? extends Property<V>> properties(String... keys);

    /**
     * Strengthen return type to {@link CloseableIterator}.
     */
    @Override
    public default <V> CloseableIterator<V> values(final String... keys) {
//        return CloseableIterators.of(this.<V>properties(keys), Property::value);
        return CloseableIterator.of(this.<V>properties(keys).stream().map(Property::value));
    }

    @Override
    public default Set<String> keys() {
        try (CloseableIterator<? extends Property<?>> it = this.properties()) {
            final Set<String> keys = new HashSet<>();
            it.forEachRemaining(property -> keys.add(property.key()));
            return Collections.unmodifiableSet(keys);
        }
    }

}
