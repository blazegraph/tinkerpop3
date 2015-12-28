package com.blazegraph.gremlin.listener;

/**
 * An atomic unit of information about a property graph.  Analogous to an RDF 
 * statement- the atomic unit of information about an RDF graph.
 * 
 * @author mikepersonick
 */
public abstract class BlazeGraphAtom {

    /**
     * The element id.
     */
    protected final String id;

    protected BlazeGraphAtom(final String id) {
        this.id = id;
    }
    
    public String elementId() {
        return id;
    }

    private static class ElementAtom extends BlazeGraphAtom {
     
        protected final String label;
        
        private ElementAtom(final String id, final String label) {
            super(id);
            
            this.label = label;
        }
        
        public String label() {
            return label;
        }
        
    }
    
    
    public static class VertexAtom extends ElementAtom {
        
        public VertexAtom(final String id, final String label) {
            super(id, label);
        }
        
        @Override
        public String toString() {
            return "VertexAtom [id=" + id + ", label=" + label + "]";
        }
        
    }
    
    public static class EdgeAtom extends ElementAtom {
        
        /**
         * Edge from id.
         */
        private final String fromId;
        
        /**
         * Edge to id.
         */
        private final String toId;
        
        public EdgeAtom(final String id, final String label,
                final String fromId, final String toId) {
            super(id, label);
            
            this.fromId = fromId;
            this.toId = toId;
        }
        
        public String fromId() {
            return fromId;
        }

        public String toId() {
            return toId;
        }

        @Override
        public String toString() {
            return "EdgeAtom [id=" + id + ", label=" + label + ", from=" + fromId + ", to=" + toId + "]";
        }
        
    }
    
    public static class PropertyAtom extends BlazeGraphAtom {
    
        /**
         * Property key (name).
         */
        protected final String key;
        
        /**
         * Property value (primitive).
         */
        protected final Object val;

        public PropertyAtom(final String id, final String key, final Object val) {
            super(id);
            
            this.key = key;
            this.val = val;
        }
        
        public String getKey() {
            return key;
        }

        public Object getVal() {
            return val;
        }

        @Override
        public String toString() {
            return "PropertyAtom [elementId=" + id + ", key=" + key + ", val=" + val + "]";
        }

    }
    
    public static class VertexPropertyAtom extends PropertyAtom {
        
        private final String vpId;
        
        public VertexPropertyAtom(final String vertexId,  
                final String key, final Object val, final String vpId) {
            super(vertexId, key, val);
            
            this.vpId = vpId;
        }
        
        @Override
        public String toString() {
            return "VertexPropertyAtom [vertexId=" + id + ", key=" + key + ", val=" + val + ", vpId=" + vpId + "]";
        }

    }
    
}
