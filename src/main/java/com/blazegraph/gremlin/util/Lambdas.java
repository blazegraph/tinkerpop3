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

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * Lambda helpers.
 * 
 * @author mikepersonick
 */
public interface Lambdas {

    /**
     * Create a "distinct by property" predicate for filtering.
     * 
     * Usage:
     * Stream distinct Person objects by name:
     * persons.stream().filter(distinctByKey(p -> p.getName());
     * 
     * @return predicate (thread-safe)
     */
    public static <T> Predicate<T> distinctByKey(
            final Function<? super T,Object> keyExtractor) {
        final Map<Object,Boolean> seen = new ConcurrentHashMap<>();
        return t -> seen.putIfAbsent(keyExtractor.apply(t), Boolean.TRUE) == null;
    }
    
    /**
     * Collect an Map.Entry stream into a Map.
     * 
     * @param <K> the type of key in the incoming entry stream
     * @param <V> the type of value in the incoming entry stream
     */
    public static <K, V> Collector<Entry<K,V>, ?, Map<K,V>> toMap() {
        return Collectors.toMap(Entry::getKey, Entry::getValue);
    }
    
    /**
     * Collect an Map.Entry stream into a Map using the specified map supplier.
     * 
     * @param <K> the type of key in the incoming entry stream
     * @param <V> the type of value in the incoming entry stream
     * @param <M> the type of the resulting {@code Map}
     */
    public static <K, V, M extends Map<K,V>> 
    Collector<Entry<K,V>, ?, M> toMap(Supplier<M> mapSupplier) {
        return Collectors.toMap(
                Entry::getKey, 
                Entry::getValue,
                /*
                 * We should never need to merge values (assuming the entry 
                 * stream has distinct keys).
                 */
                (a, b) -> { throw new IllegalStateException(
                                    String.format("Duplicate key %s", a)); },
                mapSupplier
                );
    }
    
    /**
     * This is the missing toMap() method on the Collectors class - specify
     * the key mapper, the value mapper, and the map supplier without having
     * to specify the value merge function.
     * 
     * @param <T> the type of the input elements
     * @param <K> the output type of the key mapping function
     * @param <V> the output type of the value mapping function
     * @param <M> the type of the resulting {@code Map}
     */
    public static <T, K, V, M extends Map<K, V>>
    Collector<T, ?, M> toMap(Function<? super T, ? extends K> keyMapper,
                             Function<? super T, ? extends V> valueMapper,
                             Supplier<M> mapSupplier) {
        return Collectors.toMap(
                keyMapper, 
                valueMapper,
                (a,b) -> { throw new IllegalStateException(
                                    String.format("Duplicate key %s", a)); },
                mapSupplier
                );
    }

}
