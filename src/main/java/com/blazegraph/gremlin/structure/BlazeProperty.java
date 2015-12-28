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
