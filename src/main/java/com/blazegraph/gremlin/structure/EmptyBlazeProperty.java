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

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

public class EmptyBlazeProperty<V> extends BlazeProperty<V> {

    public static final EmptyBlazeProperty INSTANCE = new EmptyBlazeProperty<>();
    public static <V> BlazeProperty<V> instance() {
        return INSTANCE;
    }

    private EmptyBlazeProperty() {
        super();
    }

    @Override
    public Resource s() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public URI p() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public Literal o() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public String key() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public BlazeElement element() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyBlazeProperty;
    }

    public int hashCode() {
        return 123456789; 
    }

}
