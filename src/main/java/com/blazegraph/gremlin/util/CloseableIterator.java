package com.blazegraph.gremlin.util;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {
    
    /**
     * You MUST close this iterator or auto-close within a try-with-resources.
     */
    @Override
    void close();
    
    /**
     * You MUST close this stream or auto-close within a try-with-resources.
     */
    default Stream<E> stream() {
        return Streams.of(this).onClose(() -> close());
    }
    
    /**
     * Collect the elements into a list.  This will close the iterator.
     */
    default List<E> collect() {
        try {
            return Streams.of(this).collect(Collectors.toList());
        } finally {
            close();
        }
    }
    
    /**
     * Count the elements.  This will close the iterator.
     */
    default long count() {
        try {
            return Streams.of(this).count();
        } finally {
            close();
        }
    }
    
    /**
     * Count the distinct elements.  This will close the iterator.
     */
    default long countDistinct() {
        try {
            return Streams.of(this).distinct().count();
        } finally {
            close();
        }
    }
    
    public static <T> CloseableIterator<T> of(final Stream<T> stream) {
        return of(stream.iterator(), () -> stream.close());
    }
    
    public static <T> 
    CloseableIterator<T> of(final Iterator<T> it, final Runnable close) {
        return new CloseableIterator<T>() {

            @Override
            public boolean hasNext() { 
                return it.hasNext(); 
            }

            @Override
            public T next() { 
                return it.next(); 
            }

            @Override
            public void close() { 
                close.run(); 
            }
            
        };
    }
    
}
