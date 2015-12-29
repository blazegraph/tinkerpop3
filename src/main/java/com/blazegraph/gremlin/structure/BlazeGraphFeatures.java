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

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class BlazeGraphFeatures implements Features {
    
    public static final BlazeGraphFeatures INSTANCE = new BlazeGraphFeatures();
    
    protected GraphFeatures graphFeatures = new BlazeGraphGraphFeatures();
    protected VertexFeatures vertexFeatures = new BlazeVertexFeatures();
    protected EdgeFeatures edgeFeatures = new BlazeEdgeFeatures();

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

    public class BlazeGraphGraphFeatures implements GraphFeatures {

        private VariableFeatures variableFeatures = new BlazeVariableFeatures();

        private BlazeGraphGraphFeatures() {
        }
        
        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        public boolean supportsPersistence() {
            return true;
        }

        @Override
        public boolean supportsTransactions() {
            return true;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }
        
        /**
         * One writer, many readers
         */
        @Override
        public boolean supportsConcurrentAccess() {
            return true;
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

    }
    
    public class BlazeVariableFeatures extends BlazeDatatypeFeatures 
            implements VariableFeatures {

        private BlazeVariableFeatures() {
        }
        
        /**
         * TODO FIXME
         */
        @Override
        public boolean supportsVariables() {
            return false;
        }
        
        @Override
        public boolean supportsBooleanValues() {
            return false;
        }

        @Override
        public boolean supportsByteValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleValues() {
            return false;
        }

        @Override
        public boolean supportsFloatValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerValues() {
            return false;
        }

        @Override
        public boolean supportsLongValues() {
            return false;
        }

        @Override
        public boolean supportsStringValues() {
            return false;
        }

    }

    public class BlazeVertexFeatures extends BlazeElementFeatures implements VertexFeatures {

        private final VertexPropertyFeatures vertexPropertyFeatures = new BlazeVertexPropertyFeatures();

        private BlazeVertexFeatures() {
        }
        
        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        public boolean supportsMetaProperties() {
            return true;
        }

        @Override
        public boolean supportsMultiProperties() {
            return true;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return Cardinality.set;
        }
        
        @Override
        public boolean supportsAddVertices() {
            return true;
        }

        @Override
        public boolean supportsRemoveVertices() {
            return true;
        }

    }

    public class BlazeEdgeFeatures extends BlazeElementFeatures implements EdgeFeatures {

        private final EdgePropertyFeatures edgePropertyFeatures = new BlazeEdgePropertyFeatures();

        private BlazeEdgeFeatures() {
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }

        @Override
        public boolean supportsAddEdges() {
            return true;
        }

        @Override
        public boolean supportsRemoveEdges() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

    }

    public class BlazeElementFeatures implements ElementFeatures {

        private BlazeElementFeatures() {
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return true;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }
        
        @Override
        public boolean supportsAddProperty() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

    }

    public class BlazeVertexPropertyFeatures extends BlazeDatatypeFeatures 
            implements VertexPropertyFeatures {

        private BlazeVertexPropertyFeatures() {
        }
        
        @Override
        public boolean supportsAddProperty() {
            return true;
        }

        @Override
        public boolean supportsRemoveProperty() {
            return true;
        }

        @Override
        @FeatureDescriptor(name = Graph.Features.VertexPropertyFeatures.FEATURE_USER_SUPPLIED_IDS)
        public boolean supportsUserSuppliedIds() {
            return false;
        }
        
        @Override
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsNumericIds() {
            return false;
        }

        @Override
        public boolean supportsStringIds() {
            return true;
        }

        @Override
        public boolean supportsUuidIds() {
            return false;
        }

    }

    public class BlazeEdgePropertyFeatures extends BlazeDatatypeFeatures 
            implements EdgePropertyFeatures {

        private BlazeEdgePropertyFeatures() {
        }
        
    }
    
    /**
     * Supports all primitives, no lists.
     * 
     * @author mikepersonick
     */
    public class BlazeDatatypeFeatures implements DataTypeFeatures {

        private BlazeDatatypeFeatures() {
        }
        
        /**
         * Supports setting of a boolean value.
         */
        @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
        public boolean supportsBooleanValues() {
            return true;
        }

        /**
         * Supports setting of a byte value.
         */
        @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
        public boolean supportsByteValues() {
            return true;
        }

        /**
         * Supports setting of a double value.
         */
        @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
        public boolean supportsDoubleValues() {
            return true;
        }

        /**
         * Supports setting of a float value.
         */
        @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
        public boolean supportsFloatValues() {
            return true;
        }

        /**
         * Supports setting of a integer value.
         */
        @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
        public boolean supportsIntegerValues() {
            return true;
        }

        /**
         * Supports setting of a long value.
         */
        @FeatureDescriptor(name = FEATURE_LONG_VALUES)
        public boolean supportsLongValues() {
            return true;
        }

        /**
         * Supports setting of a string value.
         */
        @FeatureDescriptor(name = FEATURE_STRING_VALUES)
        public boolean supportsStringValues() {
            return true;
        }

        @Override
        public boolean supportsBooleanArrayValues() {
            return false;
        }

        @Override
        public boolean supportsByteArrayValues() {
            return false;
        }

        @Override
        public boolean supportsDoubleArrayValues() {
            return false;
        }

        @Override
        public boolean supportsFloatArrayValues() {
            return false;
        }

        @Override
        public boolean supportsIntegerArrayValues() {
            return false;
        }

        @Override
        public boolean supportsLongArrayValues() {
            return false;
        }

        @Override
        public boolean supportsMapValues() {
            return false;
        }

        @Override
        public boolean supportsMixedListValues() {
            return false;
        }

        @Override
        public boolean supportsSerializableValues() {
            return false;
        }

        @Override
        public boolean supportsStringArrayValues() {
            return false;
        }

        @Override
        public boolean supportsUniformListValues() {
            return false;
        }
        
    }
}
