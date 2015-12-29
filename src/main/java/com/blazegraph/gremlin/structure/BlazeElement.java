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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataResource;
import com.blazegraph.gremlin.util.CloseableIterator;

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
