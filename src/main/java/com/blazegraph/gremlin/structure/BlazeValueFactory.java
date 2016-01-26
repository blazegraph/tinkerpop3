/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.model.vocabulary.RDF;

import com.bigdata.rdf.internal.XSD;
import com.bigdata.rdf.sail.RDRHistory;
import com.blazegraph.gremlin.internal.ListIndexExtension;

/**
 * Factory for converting Tinkerpop data (element ids, property names/values)
 * to RDF values (URIs, Literals) and back again.  This interface comes with
 * reasonable default behavior for all methods.  It can be overridden to give
 * an application a custom look and feel for the RDF values used to represent
 * the property graph.
 * 
 * @author mikepersonick
 */
public interface BlazeValueFactory {

    /**
     * Default instance.
     */
    public static final BlazeValueFactory INSTANCE = new BlazeValueFactory() {};

    /**
     * Some default constants.
     * 
     * @author mikepersonick
     */
    public interface Defaults {
        
        /**
         * Namespace for RDF URIs.
         */
        String NAMESPACE = "blaze:";
        
        /**
         * URI used for typing elements.
         */
        URI TYPE = RDF.TYPE;
        
        /**
         * URI used for list item values.
         */
        URI VALUE = RDF.VALUE;
        
        /**
         * Datatype URI for list index for Cardinality.list vertex properties. 
         */
        URI LI_DATATYPE = ListIndexExtension.DATATYPE;
        
        /**
         * Template for stamping element URIs (blaze:id).
         */
        String ELEMENT_URI_TEMPLATE = NAMESPACE + "%s";

        /**
         * Template for stamping property key URIs (blaze:key).
         */
        String PROPERTY_URI_TEMPLATE = ELEMENT_URI_TEMPLATE;
        
        /**
         * Template for stamping type URIs (blaze:type).
         */
        String TYPE_URI_TEMPLATE = ELEMENT_URI_TEMPLATE;
        
        /**
         * Default RDF value factory.
         */
        ValueFactory VF = new ValueFactoryImpl();

    }
    
    /**
     * URI used for element labels (typing).
     * 
     * @see Defaults#TYPE
     * 
     * @return
     *          URI used for element labels.
     */
    default URI type() {
        return Defaults.TYPE;
    }
    
    /**
     * URI used for Cardinality.list property values.
     * 
     * @see Defaults#VALUE
     * 
     * @return
     *          URI used for Cardinality.list property values.
     */
    default URI value() {
        return Defaults.VALUE;
    }
    
    /**
     * URI used for Cardinality.list list index datatype.
     * 
     * @see Defaults#LI_DATATYPE
     * 
     * @return
     *          URI used for Cardinality.list list index datatype.
     */
    default URI liDatatype() {
        return Defaults.LI_DATATYPE;
    }
    
    /**
     * URI used for history.  Only reason to override this is if a different
     * history implementation is used.
     * 
     * @see RDRHistory
     * 
     * @return
     *          URI used for history.
     */
    default URI historyAdded() {
        return RDRHistory.Vocab.ADDED;
    }
    
    /**
     * URI used for history.  Only reason to override this is if a different
     * history implementation is used.
     * 
     * @see RDRHistory
     * 
     * @return
     *          URI used for history.
     */
    default URI historyRemoved() {
        return RDRHistory.Vocab.REMOVED;
    }
    
    /**
     * Convert an element id into an RDF URI.
     * <p>
     * Default behavior is to prepend the {@code <blaze:>} namespace to the id.
     * </p>
     * 
     * @param id
     *          property graph element id
     * @return
     *          RDF URI representation
     */
    default URI elementURI(final String id) {
        return new URIImpl(String.format(Defaults.ELEMENT_URI_TEMPLATE, id));
    }
    
    /**
     * Convert an property key into an RDF URI.
     * <p>
     * Default behavior is to prepend the {@code <blaze:>} namespace to the key.
     * </p> 
     * @param key
     *          property graph property key
     * @return
     *          RDF URI representation
     */
    default URI propertyURI(final String key) {
        return new URIImpl(String.format(Defaults.PROPERTY_URI_TEMPLATE, key));
    }
    
    /**
     * Convert an element label (type) into an RDF URI.
     * <p>
     * Default behavior is to prepend the {@code <blaze:>} namespace to the label.
     * </p> 
     * @param label  property graph element label
     * @return URI RDF URI representation
     * 
     */
    default URI typeURI(final String label) {
        return new URIImpl(String.format(Defaults.TYPE_URI_TEMPLATE, label));
    }
    
    /**
     * Convert an RDF URI (element id/label or property key) back into a string.
     * 
     * @param uri
     *          RDF representation of an element id/label or property key
     * @return
     *          property graph (string) representation
     */
    default String fromURI(final URI uri) {
        final String s = uri.stringValue();
        return s.substring(s.lastIndexOf(':')+1);
    }
    
    /**
     * Create a datatyped literal from a blueprints property value.
     * <p>
     * Supports: Float, Double, Integer, Long, Boolean, Short, Byte, and String.
     */
    default Literal toLiteral(final Object value) {

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
        } else if (value instanceof String) { 
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
