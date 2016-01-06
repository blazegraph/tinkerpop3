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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataResource;
import com.blazegraph.gremlin.util.CloseableIterator;

/**
 * Extends the Tinkerpop3 interface to strengthen return types from Iterator
 * to {@link CloseableIterator} and to expose RDF values for id and label.
 * 
 * @author mikepersonick
 */
public interface BlazeElement extends Element {

    /**
     * Strengthen return type to {@link BlazeGraph}.
     */
    @Override
    BlazeGraph graph();
    
    /**
     * Return RDF value for element id (URI for Vertex, BNode for Edge and
     * VertexProperty).
     */
    BigdataResource rdfId();

    /**
     * Return RDF literal for element label.
     */
    URI rdfLabel();
    
    /**
     * Strengthen return type to {@link CloseableIterator}. You MUST close this
     * iterator when finished.
     */
    @Override
    <V> CloseableIterator<? extends Property<V>> properties(String... keys);

    /**
     * Strengthen return type to {@link CloseableIterator}. You MUST close this
     * iterator when finished.
     */
    @Override
    public default <V> CloseableIterator<V> values(final String... keys) {
        return CloseableIterator.of(this.<V>properties(keys).stream().map(Property::value));
    }

    /**
     * Provide safer default implementation (closes properties() iterator used
     * to calculate keys).
     */
    @Override
    public default Set<String> keys() {
        try (CloseableIterator<? extends Property<?>> it = this.properties()) {
            final Set<String> keys = new HashSet<>();
            it.forEachRemaining(property -> keys.add(property.key()));
            return Collections.unmodifiableSet(keys);
        }
    }

}
