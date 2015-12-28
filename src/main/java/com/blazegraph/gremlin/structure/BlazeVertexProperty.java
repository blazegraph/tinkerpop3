package com.blazegraph.gremlin.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataBNode;
import com.blazegraph.gremlin.util.CloseableIterator;

public class BlazeVertexProperty<V>  
        implements VertexProperty<V>, BlazeReifiedElement {

    private final BlazeProperty<V> prop;
    
    private final String id;

    private final BigdataBNode sid;
    
    /**
     * Solely for EmptyBlazeVertexProperty
     */
    protected BlazeVertexProperty() {
        this.prop = null;
        this.id = null;
        this.sid = null;
    }
    
    public BlazeVertexProperty(final BlazeProperty<V> prop, final String id, final BigdataBNode sid) {
        this.prop = prop;
        this.id = id;
        this.sid = sid;
    }
    
    BlazeProperty<V> prop() {
        return prop;
    }
    
    @Override
    public BigdataBNode rdfId() {
        return sid;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public String label() {
        return key();
    }
    
    @Override
    public Literal rdfLabel() {
        return graph().valueFactory().toLiteral(label()); 
    }
    
    @Override
    public BlazeGraph graph() {
        return prop.graph();
    }

    @Override 
    public BlazeVertex element() {
        return (BlazeVertex) prop.element();
    }

    @Override
    public String key() {
        return prop.key();
    }

    @Override
    public V value() throws NoSuchElementException {
        return prop.value();
    }

    @Override
    public boolean isPresent() {
        return prop.isPresent();
    }

    @Override
    public void remove() {
        graph().remove(this);
    }

    @Override
    public <U> BlazeProperty<U> property(final String key, final U val) {
        return BlazeReifiedElement.super.property(key, val);
    }

    @Override
    public <U> CloseableIterator<Property<U>> properties(final String... keys) {
        return BlazeReifiedElement.super.properties(keys);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode((Element) this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }


}
