package com.blazegraph.gremlin.util;

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {
    
    /**
     * You MUST close this iterator or auto-close with a try-with-resources.
     */
    @Override
    void close();
    
    /**
     * You MUST close this stream or auto-close with a try-with-resources.
     */
    default Stream<E> stream() {
        return Streams.of(this).onClose(() -> close());
    }
    
    /**
     * Collect the elements into a list.  This will close the iterator.
     */
    default List<E> collect() {
        try (Stream<E> s = stream()) {
            return s.collect(toList());
        }
    }
    
    /**
     * Count the elements.  This will close the iterator.
     */
    default long count() {
        try (Stream<E> s = stream()) {
            return s.count();
        }
//        try {
//            return Streams.of(this).count();
//        } finally {
//            close();
//        }
    }
    
    /**
     * Count the distinct elements.  This will close the iterator.
     */
    default long countDistinct() {
        try (Stream<E> s = stream()) {
            return s.distinct().count();
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
            public void remove() {
                it.remove();
            }
            
            @Override
            public void close() { 
                close.run(); 
            }
            
            @Override
            public void forEachRemaining(Consumer<? super T> action) {
                it.forEachRemaining(action);
            }

        };
    }
    
    public static <T> CloseableIterator<T> emptyIterator() {
        final Iterator<T> it = Collections.<T>emptyIterator();
        return of(it, () -> {});
    }

}
