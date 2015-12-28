package com.blazegraph.gremlin.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;

import com.bigdata.rdf.model.BigdataBNode;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.CloseableIterators;

public class EmptyBlazeVertexProperty<V> extends BlazeVertexProperty<V> {

    public static final EmptyBlazeVertexProperty INSTANCE = new EmptyBlazeVertexProperty<>();
    public static <V> BlazeVertexProperty<V> instance() {
        return INSTANCE;
    }

    private EmptyBlazeVertexProperty() {
        super();
    }

    @Override
    public BigdataBNode rdfId() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public Literal rdfLabel() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public String label() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public int hashCode() {
        return 987654321;
    }

    @Override
    public boolean equals(Object object) {
        return object instanceof EmptyBlazeVertexProperty;
    }

    @Override
    public BlazeVertex element() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public String id() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public BlazeGraph graph() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public <U> BlazeProperty<U> property(String key) {
        return EmptyBlazeProperty.<U>instance();
    }

    @Override
    public <U> BlazeProperty<U> property(String key, U value) {
        return EmptyBlazeProperty.<U>instance();
    }

    @Override
    public String key() {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Property.Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public <U> CloseableIterator<Property<U>> properties(String... propertyKeys) {
        return CloseableIterators.emptyIterator();
    }

}
