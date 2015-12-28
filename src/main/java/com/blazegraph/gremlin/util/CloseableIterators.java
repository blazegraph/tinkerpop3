package com.blazegraph.gremlin.util;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

public class CloseableIterators {

    @SuppressWarnings("unchecked")
    public static <T> CloseableIterator<T> emptyIterator() {
        return (CloseableIterator<T>) CloseableEmptyIterator.EMPTY_ITERATOR;
    }

    private static class CloseableEmptyIterator<E> implements CloseableIterator<E> {
        static final CloseableEmptyIterator<Object> EMPTY_ITERATOR
            = new CloseableEmptyIterator<>();

        public boolean hasNext() { return false; }
        public E next() { throw new NoSuchElementException(); }
        public void remove() { throw new IllegalStateException(); }
        public void close() { throw new IllegalStateException(); }
        @Override
        public void forEachRemaining(Consumer<? super E> action) {
            Objects.requireNonNull(action);
        }
    }

    public static <T,R> CloseableIterator<R> of(
            CloseableIterator<T> it, Function<? super T, ? extends R> mapper) {
        return new CloseableWrappedIterator<T,R>(it, mapper);
    }
    
    private static class 
    CloseableWrappedIterator<T,R> implements CloseableIterator<R> {
        private final CloseableIterator<T> it;
        private final Function<? super T, ? extends R> mapper;
        
        private CloseableWrappedIterator(
                CloseableIterator<T> it, Function<? super T, ? extends R> mapper) {
            this.it = it;
            this.mapper = mapper;
        }
        
        @Override
        public boolean hasNext() {
            return it.hasNext();
        }

        @Override
        public R next() {
            final T t = it.next();
            return mapper.apply(t);
        }

        @Override
        public void close() {
            it.close();
        }
    }
    
    public static <T> CloseableIterator<T> of(Stream<T> stream) {
        return new CloseableWrappedStreamIterator<T>(stream);
    }
    
    private static class 
    CloseableWrappedStreamIterator<T> implements CloseableIterator<T> {
        private final Stream<T> stream;
        private final Iterator<T> it;
        
        private CloseableWrappedStreamIterator(Stream<T> stream) {
            this.stream = stream;
            this.it = stream.iterator();
        }
        
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
            stream.close();
        }
    }
    
    

}
