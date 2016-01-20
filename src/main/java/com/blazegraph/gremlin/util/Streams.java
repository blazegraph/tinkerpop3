/**
Copyright (C) SYSTAP, LLC DBA Blazegraph 2006-2016.  All rights reserved.

Contact:
     SYSTAP, LLC DBA Blazegraph
     2501 Calvert ST NW #106
     Washington, DC 20008
     licenses@blazegraph.com

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

import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Additional Java 8 stream helpers.
 * 
 * @author mikepersonick
 */
public class Streams {

    /**
     * Obtain a Java 8 stream from an iterator.  If the iterator happens to
     * implement AutoCloseable (e.g. {@link CloseableIterator}), the stream's 
     * onClose behavior will close the iterator.  Thus it is important to
     * always close the returned stream, or use within a try-with-resources:
     * <p/>
     * <pre>
     * try (Stream<Object> s = Streams.of(it)) {
     *     // do something with s
     * } // auto-close
     * </pre>
     *
     */
    public static final <T> Stream<T> of(final Iterator<T> it) {
        final Stream<T> s = StreamSupport.stream(
                Spliterators.spliteratorUnknownSize(it, Spliterator.ORDERED), false);
        if (it instanceof AutoCloseable) {
            final AutoCloseable closeable = (AutoCloseable) it;
            s.onClose(() -> Code.wrapThrow(() -> closeable.close()));
        }
        return s;
    }
    
}
