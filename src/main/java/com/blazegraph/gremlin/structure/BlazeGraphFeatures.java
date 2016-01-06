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

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * The blazegraph-tinkerpop3 feature set.
 * 
 * @author mikepersonick
 */
public class BlazeGraphFeatures implements Features {
    
    public static final BlazeGraphFeatures INSTANCE = new BlazeGraphFeatures();
    
    private GraphFeatures graphFeatures = new Graph();
    private VertexFeatures vertexFeatures = new Vertex();
    private EdgeFeatures edgeFeatures = new Edge();

    private BlazeGraphFeatures() {
    }
    
    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }

    public static class Graph implements GraphFeatures {

        public static final boolean SUPPORTS_PERSISTENCE = true;
        public static final boolean SUPPORTS_TRANSACTIONS = true;
        public static final boolean SUPPORTS_THREADED_TRANSACTIONS = false;
        public static final boolean SUPPORTS_CONCURRENT_ACCESS = true;
        public static final boolean SUPPORTS_COMPUTER = false;
        
        private VariableFeatures variableFeatures = new Variable();

        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        public boolean supportsPersistence() {
            return SUPPORTS_PERSISTENCE;
        }

        @Override
        public boolean supportsTransactions() {
            return SUPPORTS_TRANSACTIONS;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return SUPPORTS_THREADED_TRANSACTIONS;
        }
        
        /**
         * One writer, many readers
         */
        @Override
        public boolean supportsConcurrentAccess() {
            return SUPPORTS_CONCURRENT_ACCESS;
        }

        @Override
        public boolean supportsComputer() {
            return SUPPORTS_COMPUTER;
        }

    }
    
    public static class Variable extends Datatype 
            implements VariableFeatures {

        public static final boolean SUPPORTS_VARIABLES = false;
        public static final boolean SUPPORTS_BOOLEAN_VALUES = false;
        public static final boolean SUPPORTS_BYTE_VALUES = false;
        public static final boolean SUPPORTS_DOUBLE_VALUES = false;
        public static final boolean SUPPORTS_FLOAT_VALUES = false;
        public static final boolean SUPPORTS_INTEGER_VALUES = false;
        public static final boolean SUPPORTS_LONG_VALUES = false;
        public static final boolean SUPPORTS_STRING_VALUES = false;

        /**
         * TODO FIXME
         */
        @Override
        public boolean supportsVariables() {
            return SUPPORTS_VARIABLES;
        }
        
        @Override
        public boolean supportsBooleanValues() {
            return SUPPORTS_BOOLEAN_VALUES;
        }

        @Override
        public boolean supportsByteValues() {
            return SUPPORTS_BYTE_VALUES;
        }

        @Override
        public boolean supportsDoubleValues() {
            return SUPPORTS_DOUBLE_VALUES;
        }

        @Override
        public boolean supportsFloatValues() {
            return SUPPORTS_FLOAT_VALUES;
        }

        @Override
        public boolean supportsIntegerValues() {
            return SUPPORTS_INTEGER_VALUES;
        }

        @Override
        public boolean supportsLongValues() {
            return SUPPORTS_LONG_VALUES;
        }

        @Override
        public boolean supportsStringValues() {
            return SUPPORTS_STRING_VALUES;
        }

    }

    public static class Vertex extends Element implements VertexFeatures {

        public static final boolean SUPPORTS_META_PROPERTIES = true;
        public static final boolean SUPPORTS_MULTI_PROPERTIES = true;
        public static final boolean SUPPORTS_USER_SUPPLIED_IDS = true;
        public static final boolean SUPPORTS_ADD_VERTICES = true;
        public static final boolean SUPPORTS_REMOVE_VERTICES = true;

        private final VertexPropertyFeatures vertexPropertyFeatures = new VertexProperty();

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMetaProperties() {
            return SUPPORTS_META_PROPERTIES;
        }

        @Override
        public boolean supportsMultiProperties() {
            return SUPPORTS_MULTI_PROPERTIES;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return SUPPORTS_USER_SUPPLIED_IDS;
        }

        /**
         * Really you would need to perform a Sparql query to ascertain this,
         * unless you have a fixed application schema.  Returns Cardinality.set
         * by default since that is the closest match to RDF.
         */
        @Override
        public Cardinality getCardinality(final String key) {
            return Cardinality.set;
        }
        
        @Override
        public boolean supportsAddVertices() {
            return SUPPORTS_ADD_VERTICES;
        }

        @Override
        public boolean supportsRemoveVertices() {
            return SUPPORTS_REMOVE_VERTICES;
        }

    }

    public static class Edge extends Element implements EdgeFeatures {

        public static final boolean SUPPORTS_ADD_EDGES = true;
        public static final boolean SUPPORTS_REMOVE_EDGES = true;
        public static final boolean SUPPORTS_REMOVE_PROPERTY = true;

        private final EdgePropertyFeatures edgePropertyFeatures = new EdgeProperty();

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }

        @Override
        public boolean supportsAddEdges() {
            return SUPPORTS_ADD_EDGES;
        }

        @Override
        public boolean supportsRemoveEdges() {
            return SUPPORTS_REMOVE_EDGES;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return SUPPORTS_REMOVE_PROPERTY;
        }

    }

    public static class Element implements ElementFeatures {

        public static final boolean SUPPORTS_USER_SUPPLIED_IDS = true;
        public static final boolean SUPPORTS_STRING_IDS = true;
        public static final boolean SUPPORTS_ADD_PROPERTY = true;
        public static final boolean SUPPORTS_REMOVE_PROPERTY = true;
        public static final boolean SUPPORTS_UUID_IDS = false;
        public static final boolean SUPPORTS_ANY_IDS = false;
        public static final boolean SUPPORTS_CUSTOM_IDS = false;
        public static final boolean SUPPORTS_NUMERIC_IDS = false;
        
        @Override
        public boolean supportsUserSuppliedIds() {
            return SUPPORTS_USER_SUPPLIED_IDS;
        }

        @Override
        public boolean supportsStringIds() {
            return SUPPORTS_STRING_IDS;
        }

        @Override
        public boolean supportsAddProperty() {
            return SUPPORTS_ADD_PROPERTY;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return SUPPORTS_REMOVE_PROPERTY;
        }

        @Override
        public boolean supportsUuidIds() {
            return SUPPORTS_UUID_IDS;
        }

        @Override
        public boolean supportsAnyIds() {
            return SUPPORTS_ANY_IDS;
        }

        @Override
        public boolean supportsCustomIds() {
            return SUPPORTS_CUSTOM_IDS;
        }
        
        @Override
        public boolean supportsNumericIds() {
            return SUPPORTS_NUMERIC_IDS;
        }

    }

    public static class VertexProperty extends Datatype 
            implements VertexPropertyFeatures {

        public static final boolean SUPPORTS_USER_SUPPLIED_IDS = false;
        public static final boolean SUPPORTS_STRING_IDS = true;
        public static final boolean SUPPORTS_ADD_PROPERTY = true;
        public static final boolean SUPPORTS_REMOVE_PROPERTY = true;
        public static final boolean SUPPORTS_UUID_IDS = false;
        public static final boolean SUPPORTS_ANY_IDS = false;
        public static final boolean SUPPORTS_CUSTOM_IDS = false;
        public static final boolean SUPPORTS_NUMERIC_IDS = false;
        
        @Override
        public boolean supportsUserSuppliedIds() {
            return SUPPORTS_USER_SUPPLIED_IDS;
        }

        @Override
        public boolean supportsStringIds() {
            return SUPPORTS_STRING_IDS;
        }

        @Override
        public boolean supportsAddProperty() {
            return SUPPORTS_ADD_PROPERTY;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return SUPPORTS_REMOVE_PROPERTY;
        }

        @Override
        public boolean supportsUuidIds() {
            return SUPPORTS_UUID_IDS;
        }

        @Override
        public boolean supportsAnyIds() {
            return SUPPORTS_ANY_IDS;
        }

        @Override
        public boolean supportsCustomIds() {
            return SUPPORTS_CUSTOM_IDS;
        }
        
        @Override
        public boolean supportsNumericIds() {
            return SUPPORTS_NUMERIC_IDS;
        }

    }

    public static class EdgeProperty extends Datatype 
            implements EdgePropertyFeatures {

    }
    
    /**
     * Supports all primitives, no lists.
     * 
     * @author mikepersonick
     */
    public static class Datatype implements DataTypeFeatures {

        public static final boolean SUPPORTS_BOOLEAN_VALUES = true;
        public static final boolean SUPPORTS_BYTE_VALUES = true;
        public static final boolean SUPPORTS_DOUBLE_VALUES = true;
        public static final boolean SUPPORTS_FLOAT_VALUES = true;
        public static final boolean SUPPORTS_INTEGER_VALUES = true;
        public static final boolean SUPPORTS_LONG_VALUES = true;
        public static final boolean SUPPORTS_STRING_VALUES = true;
        public static final boolean SUPPORTS_SERIALIZABLE_VALUES = false;
        public static final boolean SUPPORTS_ARRAY_VALUES = false;
        public static final boolean SUPPORTS_LIST_VALUES = false;
        public static final boolean SUPPORTS_MAP_VALUES = false;

        /**
         * Supports setting of a boolean value.
         */
        @Override
        public boolean supportsBooleanValues() {
            return SUPPORTS_BOOLEAN_VALUES;
        }

        /**
         * Supports setting of a byte value.
         */
        @Override
        public boolean supportsByteValues() {
            return SUPPORTS_BYTE_VALUES;
        }

        /**
         * Supports setting of a double value.
         */
        @Override
        public boolean supportsDoubleValues() {
            return SUPPORTS_DOUBLE_VALUES;
        }

        /**
         * Supports setting of a float value.
         */
        @Override
        public boolean supportsFloatValues() {
            return SUPPORTS_FLOAT_VALUES;
        }

        /**
         * Supports setting of a integer value.
         */
        @Override
        public boolean supportsIntegerValues() {
            return SUPPORTS_INTEGER_VALUES;
        }

        /**
         * Supports setting of a long value.
         */
        @Override
        public boolean supportsLongValues() {
            return SUPPORTS_LONG_VALUES;
        }

        /**
         * Supports setting of a string value.
         */
        @Override
        public boolean supportsStringValues() {
            return SUPPORTS_STRING_VALUES;
        }

        @Override
        public boolean supportsSerializableValues() {
            return SUPPORTS_SERIALIZABLE_VALUES;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return SUPPORTS_ARRAY_VALUES;
        }

        @Override
        public boolean supportsMapValues() {
            return SUPPORTS_MAP_VALUES;
        }

        @Override
        public boolean supportsMixedListValues() {
            return SUPPORTS_LIST_VALUES;
        }

        @Override
        public boolean supportsUniformListValues() {
            return SUPPORTS_LIST_VALUES;
        }
        
    }
}
