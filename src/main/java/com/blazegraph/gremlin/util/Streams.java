package com.blazegraph.gremlin.util;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Streams {

    public static final <T> Stream<T> of(final Iterator<T> it) {
        return StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
    }
    
    public static final <T> Stream<T> of(final CloseableIterator<T> it) {
        return of((Iterator<T>) it).onClose(() -> it.close());
    }
    
}
