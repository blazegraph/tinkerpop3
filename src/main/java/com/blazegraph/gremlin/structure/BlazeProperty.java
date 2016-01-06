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

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

/**
 * Concrete property implementation for Blazegraph.  BlazeProperties are
 * attached to {@link BlazeEdge}s and {@link BlazeVertexProperty}s 
 * (meta-properties).
 * 
 * @author mikepersonick
 */
public class BlazeProperty<V> implements Property<V> {

    /**
     * {@link BlazeGraph} instance this property belongs to.
     */
    protected final BlazeGraph graph;

    /**
     * The {@link BlazeValueFactory} provided by the graph for round-tripping
     * values.
     */
    protected final BlazeValueFactory vf;
    
    /**
     * The {@link BlazeElement} this property belongs to.
     */
    protected final BlazeElement element;
    
    /**
     * RDF representation of the property key.
     */
    protected final URI key;
    
    /**
     * RDF representation of the property value.
     */
    protected final Literal val;
    
    /**
     * Solely for {@link EmptyBlazeProperty}.
     */
    protected BlazeProperty() {
        this.graph = null;
        this.vf = null;
        this.element = null;
        this.key = null;
        this.val = null;
    }
    
    BlazeProperty(final BlazeGraph graph, final BlazeElement element,
            final URI key, final Literal val) {
        this.graph = graph;
        this.vf = graph.valueFactory();
        this.element = element;
        this.key = key;
        this.val = val;
    }

    public BlazeGraph graph() {
        return graph;
    }
    
    /**
     * RDF representation of the property key.
     */
    public URI rdfKey() {
        return key;
    }

    /**
     * RDF representation of the property value.
     */
    public Literal rdfValue() {
        return val;
    }

    /**
     * The {@link BlazeElement} this property belongs to.
     */
    @Override
    public BlazeElement element() {
        return element;
    }

    /**
     * @see {@link Property#key()}
     */
    @Override
    public String key() {
        return vf.fromURI(key);
    }

    /**
     * @see {@link Property#value()}
     */
    @Override
    @SuppressWarnings("unchecked")
    public V value() throws NoSuchElementException {
        return (V) vf.fromLiteral(val);
    }
    
    /**
     * @see {@link Property#isPresent()}
     */
    @Override
    public boolean isPresent() {
        return true;
    }

    /**
     * @see {@link Property#remove()}
     * @see {@link BlazeGraph#remove(BlazeProperty)}
     */
    @Override
    public void remove() {
        graph.remove(this);
    }
    
    /**
     * Pass through to {@link ElementHelper#areEqual(Property, Object)}
     */
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * Pass through to {@link ElementHelper#hashCode(Property)}
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
    
    /**
     * Pass through to {@link StringFactory#propertyString(Property)}
     */
    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

}
