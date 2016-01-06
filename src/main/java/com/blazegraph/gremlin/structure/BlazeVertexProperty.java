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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataBNode;
import com.blazegraph.gremlin.util.CloseableIterator;

/**
 * Concrete vertex property implementation for Blazegraph.
 * <p/>
 * Vertex properties can be represented in one of two ways depending on the
 * cardinality of the key.  Cardinality.single and Cardinality.set are
 * represented the same way, as follows:
 * <p/>
 * <pre>
 *     :vertexId :key "val" .
 * </pre>
 * <p/>
 * For Cardinality.list, a list index literal with a special datatype is used 
 * to manage duplicate list items and ordering.  Cardinality.list requires two 
 * triples instead of one:
 * <p/>
 * <pre>
 *     :vertexId :key "0"^^blazegraph:listIndex .
 *     <<:vertexId :key "0"^^blazegraph:listIndex>> rdf:value "val" .
 * </pre>
 * <p/>
 * In either case, meta-properties can be attached to the reified vertex
 * property triple: 
 * <p/>
 * <pre>
 *     # for Cardinality.single and Cardinality.set
 *     <<:vertexId :key "val">> :metaKey "metaVal" .
 *     # for Cardinality.list
 *     <<:vertexId :key "0"^^blazegraph:listIndex>> :metaKey "metaVal" .
 * </pre>
 * 
 * @author mikepersonick
 *
 * @param <V>
 */
public class BlazeVertexProperty<V>  
        implements VertexProperty<V>, BlazeReifiedElement {

    /**
     * Delegation pattern, since a vertex property is a property.
     */
    private final BlazeProperty<V> prop;

    /**
     * The reified vertex property triple.
     */
    private final BigdataBNode sid;
    
    /**
     * The RDF id - an internally generated string representation of the 
     * reified vertex property triple.
     */
    private final String id;

    /**
     * Solely for {@link EmptyBlazeVertexProperty}.
     */
    protected BlazeVertexProperty() {
        this.prop = null;
        this.id = null;
        this.sid = null;
    }
    
    /**
     * Construct an instance.
     */
    BlazeVertexProperty(final BlazeProperty<V> prop, 
            final String id, final BigdataBNode sid) {
        this.prop = prop;
        this.sid = sid;
        this.id = id;
    }

    /**
     * The reified vertex property triple.
     */
    @Override
    public BigdataBNode rdfId() {
        return sid;
    }

    /**
     * The internally generated element id for this vertex property.
     */
    @Override
    public String id() {
        return id;
    }

    /**
     * The {@link BlazeGraph} instance.
     */
    @Override
    public BlazeGraph graph() {
        return prop.graph();
    }

    /**
     * The vertex to which this property belongs.
     */
    @Override 
    public BlazeVertex element() {
        return (BlazeVertex) prop.element();
    }

    /**
     * Property key.
     */
    @Override
    public String key() {
        return prop.key();
    }

    /**
     * Property value.
     */
    @Override
    public V value() throws NoSuchElementException {
        return prop.value();
    }

    /**
     * @see {@link Property#isPresent()}}
     */
    @Override
    public boolean isPresent() {
        return prop.isPresent();
    }

    /**
     * @see {@link Property#remove()}
     * @see {@link BlazeGraph#remove(BlazeReifiedElement)}
     */
    @Override
    public void remove() {
        graph().remove(this);
    }

    /**
     * Strengthen return type to {@link BlazeProperty}.
     */
    @Override
    public <U> BlazeProperty<U> property(final String key, final U val) {
        return BlazeReifiedElement.super.property(key, val);
    }

    /**
     * Strength return type to {@link CloseableIterator}.  You MUST close this
     * iterator when finished.
     */
    @Override
    public <U> CloseableIterator<Property<U>> properties(final String... keys) {
        return BlazeReifiedElement.super.properties(keys);
    }

    /**
     * Pass through to {@link ElementHelper#hashCode(Element)}
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    /**
     * Pass through to {@link ElementHelper#areEqual(VertexProperty, Object)}
     */
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    /**
     * Pass through to {@link StringFactory#propertyString(Property)}
     */
    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    /**
     * The element label, which is the property key.
     */
    @Override
    public String label() {
        return key();
    }
    
    /**
     * The RDF representation of the element label (required by 
     * {@link BlazeElement}).
     */
    @Override
    public URI rdfLabel() {
        return graph().valueFactory().typeURI(label()); 
    }
    

}
