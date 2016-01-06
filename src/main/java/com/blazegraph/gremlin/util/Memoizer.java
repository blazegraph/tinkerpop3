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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Safe lazy-initialization pattern for Java 8.
 * 
 * @author mikepersonick
 */
public class Memoizer<V> {

    private final AtomicReference<Future<V>> cache = new AtomicReference<>(null);
    
    private final Callable<V> compute;

    /**
     * Lazy initializer.
     * 
     * @param compute
     *          Callable to compute the value to cache.
     */
    public Memoizer(final Callable<V> compute) {
        this(compute, null);
    }
    
    /**
     * Eager initializer.
     * 
     * @param compute
     *          Callable to compute the value to cache.
     * @param val
     *          Pre-computed value to cache (if non-null).
     */
    public Memoizer(final Callable<V> compute, final V val) {
        this.compute = compute;
        
        if (val != null) {
            final FutureTask<V> ft = new FutureTask<>(() -> val);
            ft.run();
            cache.set(ft);
        }
    }
    
    /**
     * Get the cached value (compute if necessary).
     * 
     * @return
     *      the cached value
     */
    public V get() {
        if (cache.get() == null) {
            final FutureTask<V> ft = new FutureTask<>(compute);
            if (cache.compareAndSet(null, ft)) {
                ft.run();
            }
        }
        final Future<V> f = cache.get();
        if (f == null) {
            /*
             * Cleared by another thread, re-compute
             */
            return get();
        }
        return Code.wrapThrow(() -> f.get());
    }
    
    /**
     * Clear the cache, return the old value (or null if it had not been 
     * computed yet).
     * 
     * @return
     *      the previously cached value
     */
    public V clear() {
        final Future<V> f = cache.getAndSet(null);
        return f == null ? null : Code.wrapThrow(() -> f.get());
    }
    
}
