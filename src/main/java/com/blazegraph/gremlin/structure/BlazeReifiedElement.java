/**
Copyright (C) SYSTAP, LLC 2006-2016.  All rights reserved.

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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import com.bigdata.rdf.model.BigdataBNode;
import com.blazegraph.gremlin.util.CloseableIterator;

/**
 * Common interface for {@link BlazeEdge} and {@link BlazeVertexProperty}, 
 * both of which use a sid (reified statement) as their RDF id for attaching
 * labels (BlazeEdge) and properties (both).
 *   
 * @author mikepersonick
 */
public interface BlazeReifiedElement extends BlazeElement {

    /**
     * Strengthen return type.
     * 
     * @see {@link BlazeElement#rdfId()}
     */
    @Override
    BigdataBNode rdfId();
    
    /**
     * Safer default implementation that closes the iterator from properties().
     */
    @Override
    default <V> BlazeProperty<V> property(final String key) {
        try (CloseableIterator<? extends Property<V>> it = this.<V>properties(key)) {
            return it.hasNext() ? (BlazeProperty<V>) it.next() : EmptyBlazeProperty.instance();
        }
    }
    
    /**
     * Pass through to {@link BlazeGraph#properties(BlazeReifiedElement, String...)}
     */
    @Override
    default <V> CloseableIterator<Property<V>> properties(final String... keys) {
        return graph().properties(this, keys);
    }

    /**
     * Pass through to {@link BlazeGraph#property(BlazeReifiedElement, String, Object)}
     */
    default <V> BlazeProperty<V> property(final String key, final V val) {
        ElementHelper.validateProperty(key, val);
        return graph().property(this, key, val);
    }
    
}
