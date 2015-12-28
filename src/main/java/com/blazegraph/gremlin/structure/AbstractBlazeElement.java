package com.blazegraph.gremlin.structure;

import java.util.Objects;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;

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
