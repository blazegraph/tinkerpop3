package com.blazegraph.gremlin.structure;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.RDFS;

import com.bigdata.rdf.internal.XSD;

/**
 * Factory for converting Tinkerpop data (element ids, property names/values)
 * to RDF values (URIs, Literals) and back again. 
 * 
 * @author mikepersonick
 */
public interface BlazeValueFactory {

    public interface Defaults {
        
        /**
         * Namespace for RDF URIs.
         */
        String NAMESPACE = "blaze:";
        
        /**
         * URI used for labeling elements.
         */
        URI LABEL = RDFS.LABEL;
        
        /**
         * URI used for list item values.
         */
        URI VALUE = RDF.VALUE;
        
//        /**
//         * Default label for vertices.
//         */
//        String VERTEX_LABEL = "Vertex";
        
//        /**
//         * URI used to represent a Vertex.
//         */
//        URI VERTEX = new URIImpl("blaze:Vertex");
        
//        /**
//         * Default label for edges.
//         */
//        String EDGE_LABEL = "Edge";
        
//        /**
//         * URI used to represent a Edge.
//         */
//        URI EDGE = new URIImpl("blaze:Edge");
        
//        /**
//         * Template for stamping vertex URIs (blaze:vertex:type:id).
//         */
//        String VERTEX_URI_TEMPLATE = NAMESPACE + ":vertex:%type:%id";
//
//        /**
//         * Template for stamping edge URIs (blaze:edge:type:id).
//         */
//        String EDGE_URI_TEMPLATE = NAMESPACE + ":edge:%type:%id";
//
//        /**
//         * Template for stamping type URIs (blaze:type:type).
//         */
//        String TYPE_URI_TEMPLATE = NAMESPACE + ":type:%type";

        /**
         * Template for stamping type element URIs (blaze:id).
         */
        String ELEMENT_URI_TEMPLATE = NAMESPACE + "%s";

        /**
         * Template for stamping type property URIs (blaze:key).
         */
        String PROPERTY_URI_TEMPLATE = ELEMENT_URI_TEMPLATE;
        
        /**
         * Default RDF value factory.
         */
        ValueFactory VF = new ValueFactoryImpl();

    }
    
    default URI labelURI() {
        return Defaults.LABEL;
    }
    
    default URI valueURI() {
        return Defaults.VALUE;
    }
    
//    default URI getVertexURI() {
//        return Defaults.VERTEX;
//    }
    
//    default String getVertexLabel() {
//        return Defaults.VERTEX_LABEL;
//    }
    
//    default URI getEdgeURI() {
//        return Defaults.EDGE;
//    }
    
//    default String getEdgeLabel() {
//        return Defaults.EDGE_LABEL;
//    }
    
//    default URI toVertexURI(final String id, final String type) {
//        return new URIImpl(String.format(Defaults.VERTEX_URI_TEMPLATE, type, id));
//    }
//    
//    default URI toEdgeURI(final String id, final String type) {
//        return new URIImpl(String.format(Defaults.EDGE_URI_TEMPLATE, type, id));
//    }
    
//    default URI elementURI(final String id, final String label) {
//        return new URIImpl(String.format(Defaults.ELEMENT_URI_TEMPLATE, label, id));
//    }
    
    default URI elementURI(final String id) {
        return new URIImpl(String.format(Defaults.ELEMENT_URI_TEMPLATE, id));
    }
    
    default URI propertyURI(final String key) {
        return new URIImpl(String.format(Defaults.PROPERTY_URI_TEMPLATE, key));
    }
    
    default String fromURI(final URI uri) {
        final String s = uri.stringValue();
        return s.substring(s.lastIndexOf(':')+1);
    }
    
//    default URI toTypeURI(final String type) {
//        return new URIImpl(String.format(Defaults.TYPE_URI_TEMPLATE, type));
//    }
    
    /**
     * Create a datatyped literal from a blueprints property value.
     * <p>
     * Supports: Float, Double, Integer, Long, Boolean, Short, Byte, and String.
     */
    default Literal toLiteral(final Object value) {

//      /*
//       * Need to handle this better.
//       */
//      if (value instanceof Collection) {
//          return vf.createLiteral(Arrays.toString(((Collection<?>) value).toArray()));
//      }
        
        final ValueFactory vf = Defaults.VF;
        
        if (value instanceof Float) {
            return vf.createLiteral((Float) value);
        } else if (value instanceof Double) {
            return vf.createLiteral((Double) value);
        } else if (value instanceof Integer) {
            return vf.createLiteral((Integer) value);
        } else if (value instanceof Long) {
            return vf.createLiteral((Long) value);
        } else if (value instanceof Boolean) {
            return vf.createLiteral((Boolean) value);
        } else if (value instanceof Short) {
            return vf.createLiteral((Short) value);
        } else if (value instanceof Byte) {
            return vf.createLiteral((Byte) value);
        } else if (value instanceof String) { // treat as string by default
            return vf.createLiteral((String) value);
        } else {
            throw new IllegalArgumentException(String.format("not supported: %s", value));
        }
        
    }
    
    /**
     * Create a blueprints property value from a datatyped literal.
     * <p>
     * Return a graph property from a datatyped literal using its
     * XSD datatype.
     * <p>
     * Supports: Float, Double, Integer, Long, Boolean, Short, Byte, and String.
     */
    default Object fromLiteral(final Literal l) {
        
        final URI datatype = l.getDatatype();
        
        if (datatype == null) {
            return l.getLabel();
        } else if (datatype.equals(XSD.FLOAT)) {
            return l.floatValue();
        } else if (datatype.equals(XSD.DOUBLE)) {
            return l.doubleValue();
        } else if (datatype.equals(XSD.INT)) {
            return l.intValue();
        } else if (datatype.equals(XSD.LONG)) {
            return l.longValue();
        } else if (datatype.equals(XSD.BOOLEAN)) {
            return l.booleanValue();
        } else if (datatype.equals(XSD.SHORT)) {
            return l.shortValue();
        } else if (datatype.equals(XSD.BYTE)) {
            return l.byteValue();
        } else {
            return l.getLabel();
        }
        
    }
    
}
