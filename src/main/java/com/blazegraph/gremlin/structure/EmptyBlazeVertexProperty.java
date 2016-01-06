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
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyVertexProperty;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataBNode;
import com.blazegraph.gremlin.util.CloseableIterator;

/**
 * Wanted to extend TP3's {@link EmptyVertexProperty}, but it's final.
 * 
 * @author mikepersonick
 */
public class EmptyBlazeVertexProperty<V> extends BlazeVertexProperty<V> {

    public static final EmptyBlazeVertexProperty INSTANCE = new EmptyBlazeVertexProperty<>();
    public static <V> BlazeVertexProperty<V> instance() {
        return INSTANCE;
    }

    private EmptyBlazeVertexProperty() {
        super();
    }

    @Override
    public BigdataBNode rdfId() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public URI rdfLabel() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public String label() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public int hashCode() {
        return 987654321;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof EmptyBlazeVertexProperty;
    }

    @Override
    public BlazeVertex element() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public String id() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public BlazeGraph graph() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public <U> BlazeProperty<U> property(String key) {
        return EmptyBlazeProperty.<U>instance();
    }

    @Override
    public <U> BlazeProperty<U> property(String key, U value) {
        return EmptyBlazeProperty.<U>instance();
    }

    @Override
    public String key() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public <U> CloseableIterator<Property<U>> properties(String... propertyKeys) {
        return CloseableIterator.emptyIterator();
    }

}
