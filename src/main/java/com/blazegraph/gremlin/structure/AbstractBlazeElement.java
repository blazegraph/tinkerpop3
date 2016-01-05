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

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.openrdf.model.URI;

import com.bigdata.rdf.model.BigdataURI;

/**
 * Abstract base class for {@link BlazeVertex} and {@link BlazeEdge}.
 * 
 * @author mikepersonick
 */
abstract class AbstractBlazeElement implements BlazeElement {

    /**
     * {@link BlazeGraph} instance this element belongs to.
     */
    protected final BlazeGraph graph;

    /**
     * The {@link BlazeValueFactory} provided by the graph for round-tripping
     * values.
     */
    protected final BlazeValueFactory vf;
    
    /**
     * The URI representation of the element id.
     */
    protected final BigdataURI uri;
    
    /**
     * The Literal representation of the element label.
     */
    protected final BigdataURI label;
    
    AbstractBlazeElement(final BlazeGraph graph, final BigdataURI uri, 
            final BigdataURI label) {
        Objects.requireNonNull(graph);
        Objects.requireNonNull(uri);
        Objects.requireNonNull(label);
        
        this.graph = graph;
        this.uri = uri;
        this.label = label;
        this.vf = graph.valueFactory();
    }
    
    /**
     * Return element id. Tinkerpop3 interface method.
     * 
     * @see {@link Element#id()}
     */
    @Override
    public String id() {
        return vf.fromURI(uri);
    }
    
    /**
     * Return element label. Tinkerpop3 interface method.
     * 
     * @see {@link Element#label()}
     */
    @Override
    public String label() {
        return (String) vf.fromURI(label);
    }
    
    /**
     * Return the RDF representation of the label.
     * 
     * @see {@link BlazeElement#rdfLabel()}
     */
    @Override
    public URI rdfLabel() {
        return label;
    }

    /**
     * Return the {@link BlazeGraph} instance.
     */
    @Override
    public BlazeGraph graph() {
        return graph;
    }
    
    /**
     * Delegates to {@link ElementHelper#hashCode(Element)}
     */
    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    /**
     * Delegates to {@link ElementHelper#areEqual(Element, Object)}
     */
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }
    
}
