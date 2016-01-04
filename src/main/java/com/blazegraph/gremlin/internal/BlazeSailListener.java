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
package com.blazegraph.gremlin.internal;

import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;

/**
 * Turn IChangeLog (Java 7) into a Java 8 functional interface.
 * 
 * @author mikepersonick
 */
@FunctionalInterface
public interface BlazeSailListener extends IChangeLog {

    @Override
    void changeEvent(IChangeRecord changeRecord);

    /**
     * Default no-op.
     */
    @Override
    default void transactionAborted() {
    }

    /**
     * Default no-op.
     */
    @Override
    default void transactionBegin() {
    }

    /**
     * Default no-op.
     */
    @Override
    default void transactionCommited(long commitTime) {
    }

    /**
     * Default no-op.
     */
    @Override
    default void transactionPrepare() {
    }
    
    /**
     * Default no-op.
     */
    @Override
    default void close() {
    }

}
