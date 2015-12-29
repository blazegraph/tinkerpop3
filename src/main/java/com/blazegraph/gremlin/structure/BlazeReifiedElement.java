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
