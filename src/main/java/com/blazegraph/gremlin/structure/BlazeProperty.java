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
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

public class BlazeProperty<V> implements Property<V> {

    protected final BlazeGraph graph;
    
    protected final BlazeValueFactory vf;
    
    protected final BlazeElement element;
    
//    protected final BigdataResource subject;
    
    protected final URI key;
    
    protected final Literal val;
    
//    private transient volatile boolean removed = false;
    
    /**
     * Solely for EmptyBlazeProperty
     */
    protected BlazeProperty() {
        this.graph = null;
        this.vf = null;
        this.element = null;
        this.key = null;
        this.val = null;
    }
    
    public BlazeProperty(final BlazeGraph graph, final BlazeElement element,
            /*final BigdataResource subject,*/ final URI key, final Literal val) {
        this.graph = graph;
        this.vf = graph.valueFactory();
        this.element = element;
//        this.subject = subject;
        this.key = key;
        this.val = val;
    }

    public BlazeGraph graph() {
        return graph;
    }
    
    public Resource s() {
        return element.rdfId();
    }
    
    public URI p() {
        return key;
    }
    
    public Literal o() {
        return val;
    }
    
    @Override
    public BlazeElement element() {
        return element;
    }

    @Override
    public String key() {
        return vf.fromURI(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public V value() throws NoSuchElementException {
        return (V) vf.fromLiteral(val);
    }
    
    public Literal rdfValue() {
        return val;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public void remove() {
        graph.remove(this);
    }
    
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
    
    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

}
