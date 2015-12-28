package com.blazegraph.gremlin.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.bigdata.rdf.model.BigdataBNode;
import com.blazegraph.gremlin.util.CloseableIterator;

public abstract interface BlazeReifiedElement extends BlazeElement {

    @Override
    BigdataBNode rdfId();
    

//    default <V> Property<V> property(final String key, final V val) {
//        final BlazeGraph graph = graph();
//        final BlazeValueFactory vf = graph.valueFactory();
//        final Resource s = resource();
//        final URI p = vf.propertyURI(key);
//        final Literal o = vf.toLiteral(val);
//        
//        Code.unchecked(() -> {
//            final RepositoryConnection cxn = graph.writeConnection();
//            // << stmt >> <key> "val" .
//            cxn.add(s, p, o);
//            
//            /*
//             * Do I need to remove the old value?  What is the expected 
//             * cardinality on edge properties in TP3?
//             */
//        });
//        
//        final BlazeProperty<V> prop = new BlazeProperty<V>(graph, this, p, o);
//        return prop;
//    }
//

    /**
     * Get a {@link Property} for the {@code Element} given its key.
     * The default implementation calls the raw {@link Element#properties}.
     */
    @Override
    default <V> BlazeProperty<V> property(final String key) {
        try (CloseableIterator<? extends Property<V>> it = this.<V>properties(key)) {
            return it.hasNext() ? (BlazeProperty<V>) it.next() : EmptyBlazeProperty.instance();
        }
    }
    
    @Override
    default <V> CloseableIterator<Property<V>> properties(final String... keys) {
        return graph().properties(this, keys);
    }

    default <V> BlazeProperty<V> property(final String key, final V val) {
        ElementHelper.validateProperty(key, val);
        return graph().property(this, key, val);
    }
    
}
