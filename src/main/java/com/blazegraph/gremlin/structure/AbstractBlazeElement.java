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

import java.util.Objects;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataURI;

public abstract class AbstractBlazeElement implements BlazeElement {

    protected final BlazeGraph graph;
    
    protected final BigdataURI uri;
    
    protected final Literal label;
    
    protected final BlazeValueFactory vf;
    
//    protected transient volatile boolean removed = false;
    
//    protected transient Map<String, ? extends Property<?>> cachedProps = null;
    
    AbstractBlazeElement(final BlazeGraph graph, final BigdataURI uri, 
            final Literal label) {
        Objects.requireNonNull(graph);
        Objects.requireNonNull(uri);
        Objects.requireNonNull(label);
        
        this.graph = graph;
        this.uri = uri;
        this.label = label;
        this.vf = graph.valueFactory();
    }
    
    @Override
    public String id() {
        return vf.fromURI(uri);
    }
    
    @Override
    public String label() {
        return (String) vf.fromLiteral(label);
    }
    
    @Override
    public Literal rdfLabel() {
        return label;
    }

    @Override
    public BlazeGraph graph() {
        return graph;
    }
    
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    
}
