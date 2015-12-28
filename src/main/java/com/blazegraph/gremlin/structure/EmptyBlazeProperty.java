package com.blazegraph.gremlin.structure;

import java.util.NoSuchElementException;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

public class EmptyBlazeProperty<V> extends BlazeProperty<V> {

    public static final EmptyBlazeProperty INSTANCE = new EmptyBlazeProperty<>();
    public static <V> BlazeProperty<V> instance() {
        return INSTANCE;
    }

    private EmptyBlazeProperty() {
        super();
    }

    @Override
    public Resource s() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public URI p() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public Literal o() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public String key() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public V value() throws NoSuchElementException {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public boolean isPresent() {
        return false;
    }

    @Override
    public BlazeElement element() {
        throw Exceptions.propertyDoesNotExist();
    }

    @Override
    public void remove() {

    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyBlazeProperty;
    }

    public int hashCode() {
        return 123456789; 
    }

}
