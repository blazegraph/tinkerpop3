/**
Copyright (C) SYSTAP, LLC 2006-2015.  All rights reserved.

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
package com.blazegraph.gremlin.embedded;

import org.apache.commons.configuration.Configuration;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.blazegraph.gremlin.util.Code;

/**
 * BlazeGraphReadOnly extends BlazeGraphEmbedded and thus offers all the same
 * operations, except write operations will not be permitted
 * (BlazeGraphReadOnly.tx() will throw an exception). You can open as many
 * read-only views as you like, but we recommend you use a connection pool so as
 * not to overtax system resources. Applications should be written with the
 * one-writer many-readers paradigm front of mind.
 * <p/>
 * Important: Make sure to close the read-only view as soon as you are done with
 * it.
 *
 * @author mikepersonick
 */
public class BlazeGraphReadOnly extends BlazeGraphEmbedded {

    /**
     * Unlike the unisolated version (superclass), the read-only version
     * holds the read connection open for the duration of its lifespan, until
     * close() is called.
     */
    private final BigdataSailRepositoryConnection cxn;
    
    /**
     * Never publicly instantiated - only by another {@link BlazeGraphEmbedded} 
     * instance.
     */
    BlazeGraphReadOnly(final BigdataSailRepository repo,
            final BigdataSailRepositoryConnection cxn,
            final Configuration config) {
        super(repo, config);
        
        this.cxn = cxn;
    }
    
    /**
     * Write operations not supported by this class.
     */
    @Override
    public BlazeTransaction tx() {
        throw new UnsupportedOperationException("Transactions not allowed on read-only view");
    }
    
    /**
     * Return the read-only SAIL connection.
     */
    @Override
    public BigdataSailRepositoryConnection cxn() {
        if (closed) throw new IllegalStateException();
        
        return cxn;
    }
    
    /**
     * Close the read-only SAIL connection.
     */
    @Override
    public synchronized void close() {
        if (closed)
            return;
        
        Code.wrapThrow(() -> cxn.close());
        closed = true;
    }


}
