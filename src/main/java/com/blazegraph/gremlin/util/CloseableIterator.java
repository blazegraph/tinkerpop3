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
package com.blazegraph.gremlin.util;

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Interface that combines Iterator and AutoCloseable.  Why does Java 8 not
 * support this natively?!?
 * 
 * @author mikepersonick
 */
public interface CloseableIterator<E> extends Iterator<E>, AutoCloseable {
    
    /**
     * You MUST close this iterator or auto-close with a try-with-resources.
     */
    @Override
    void close();
    
    /**
     * Perform some action on each remaining element and then close the itertor.
     */
    @Override
    default void forEachRemaining(Consumer<? super E> action) {
        try {
            Iterator.super.forEachRemaining(action);
        } finally {
            close();
        }
    }

    /**
     * You MUST close this stream or auto-close in a try-with-resources.
     */
    default Stream<E> stream() {
        return Streams.of(this).onClose(() -> close());
    }
    
    /**
     * Collect the elements into a list. This will close the iterator.
     */
    default List<E> collect() {
        try (Stream<E> s = stream()) {
            return s.collect(toList());
        }
    }
    
    /**
     * Count the elements. This will close the iterator.
     */
    default long count() {
        try (Stream<E> s = stream()) {
            return s.count();
        }
    }
    
    /**
     * Count the distinct elements.  This will close the iterator.
     */
    default long countDistinct() {
        try (Stream<E> s = stream()) {
            return s.distinct().count();
        }
    }
    
    /**
     * Construct an instance from a stream using stream.close() as the close
     * behavior for the iterator.
     */
    public static <T> CloseableIterator<T> of(final Stream<T> stream) {
        return of(stream.iterator(), () -> stream.close());
    }
    
    /**
     * Construct an instance from the supplied iterator and the supplied
     * onClose behavior.
     */
    public static <T> 
    CloseableIterator<T> of(final Iterator<T> it, final Runnable onClose) {
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
                onClose.run(); 
            }
            
            @Override
            public void forEachRemaining(Consumer<? super T> action) {
                it.forEachRemaining(action);
            }

        };
    }
    
    /**
     * Empty instance.
     */
    public static <T> CloseableIterator<T> emptyIterator() {
        final Iterator<T> it = Collections.<T>emptyIterator();
        return of(it, () -> {});
    }

}
