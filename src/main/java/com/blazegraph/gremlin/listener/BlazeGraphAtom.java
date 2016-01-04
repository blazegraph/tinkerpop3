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
package com.blazegraph.gremlin.listener;

/**
 * An atomic unit of information about a property graph.  Analogous to an RDF 
 * statement- the atomic unit of information about an RDF graph.  This is the
 * unit of graph information used by the listener and history APIs.
 * 
 * @author mikepersonick
 */
public abstract class BlazeGraphAtom {

    /**
     * All atoms refer back to an element (Vertex, Edge, or VertexProperty).
     */
    protected final String id;

    /**
     * All atoms refer back to an element (Vertex, Edge, or VertexProperty).
     */
    protected BlazeGraphAtom(final String id) {
        this.id = id;
    }

    /**
     * The element id (Vertex, Edge, or VertexProperty) this atom refers to.
     */
    public String elementId() {
        return id;
    }

    /**
     * Abstract base class for VertexAtom and EdgeAtom.  Adds the element label.
     * 
     * @author mikepersonick
     */
    private static abstract class ElementAtom extends BlazeGraphAtom {

        /**
         * Element label.
         */
        protected final String label;
        
        private ElementAtom(final String id, final String label) {
            super(id);
            
            this.label = label;
        }
        
        /**
         * Element label.
         */
        public String label() {
            return label;
        }
        
    }
    
    /**
     * Vertex atom describes a vertex - id and label.
     *  
     * @author mikepersonick
     */
    public static class VertexAtom extends ElementAtom {
        
        /**
         * Full construct a vertex atom.
         */
        public VertexAtom(final String id, final String label) {
            super(id, label);
        }
        
        /**
         * Return the vertex id.
         */
        public String vertexId() {
            return id;
        }
        
        @Override
        public String toString() {
            return "VertexAtom [id=" + id + ", label=" + label + "]";
        }
        
    }
    
    /**
     * Edge atom describes an edge - id, label, and from/to (vertex ids).
     *  
     * @author mikepersonick
     */
    public static class EdgeAtom extends ElementAtom {
        
        /**
         * From vertex id.
         */
        private final String fromId;
        
        /**
         * To vertex id.
         */
        private final String toId;
        
        /**
         * Fully construct an edge atom.
         */
        public EdgeAtom(final String id, final String label,
                final String fromId, final String toId) {
            super(id, label);
            
            this.fromId = fromId;
            this.toId = toId;
        }
        
        /**
         * Return the edge id.
         */
        public String edgeId() {
            return id;
        }
        
        /**
         * From vertex id.
         */
        public String fromId() {
            return fromId;
        }

        /**
         * To vertex id.
         */
        public String toId() {
            return toId;
        }

        @Override
        public String toString() {
            return "EdgeAtom [id=" + id + ", label=" + label + ", from=" + fromId + ", to=" + toId + "]";
        }
        
    }
    
    /**
     * Property atom describes a property on an Edge or VertexProperty - 
     * element id, key, and value.
     *   
     * @author mikepersonick
     */
    public static class PropertyAtom extends BlazeGraphAtom {
    
        /**
         * Property key (name).
         */
        protected final String key;
        
        /**
         * Property value (primitive).
         */
        protected final Object val;

        /**
         * Fully construct a property atom.
         */
        public PropertyAtom(final String id, final String key, final Object val) {
            super(id);
            
            this.key = key;
            this.val = val;
        }
        
        /**
         * Property key (name).
         */
        public String getKey() {
            return key;
        }

        /**
         * Property value (primitive).
         */
        public Object getVal() {
            return val;
        }

        @Override
        public String toString() {
            return "PropertyAtom [elementId=" + id + ", key=" + key + ", val=" + val + "]";
        }

    }
    
    /**
     * VertexProperty atom describes a property on a Vertex - vertex id, vertex
     * property id, key, value.
     *  
     * @author mikepersonick
     */
    public static class VertexPropertyAtom extends PropertyAtom {
        
        /**
         * Vertex property id.
         */
        private final String vpId;
        
        /**
         * Fully construct a vertex property atom.
         */
        public VertexPropertyAtom(final String vertexId,  
                final String key, final Object val, final String vpId) {
            super(vertexId, key, val);
            
            this.vpId = vpId;
        }

        /**
         * Return the vertex id.
         */
        public String vertexId() {
            return id;
        }

        /**
         * Return the vertex property id.
         */
        public String vertexPropertyId() {
            return vpId;
        }
        
        @Override
        public String toString() {
            return "VertexPropertyAtom [vertexId=" + id + ", key=" + key + ", val=" + val + ", vertexPropertyId=" + vpId + "]";
        }

    }
    
}
