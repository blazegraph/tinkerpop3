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
package com.blazegraph.gremlin.listener;

/**
 * An edit consists of an edit action (add or remove), the atomic unit of graph
 * information that was edited ({@link BlazeGraphAtom}), and the commit time of
 * the edit. This is the unit of graph information used by the listener and
 * history APIs.  Listeners will get edits with no timestamp information, that
 * is because the edits are happening prior to a commit.  Listeners will 
 * eventually get a commit notification with a timestamp (or a rollback).
 * The history API will provide timestamps, since it is dealing with committed
 * edits that happened in the past.
 * 
 * @author mikepersonick
 */
public class BlazeGraphEdit { 

    /**
     * Edit action - add or remove.
     * 
     * @author mikepersonick
     */
    public static enum Action {

        /**
         * Graph atom added.
         */
        Add, 
        
        /**
         * Graph atom removed.
         */
        Remove;
        
    }

    /**
     * Edit action.
     */
    private final Action action;

    /**
     * Atomic unit of graph information edited.
     */
    private final BlazeGraphAtom atom;
    
    /**
     * The commit time of the edit action.
     */
    private final long timestamp;
    
    /**
     * Construct an edit with an unknown commit time (listener API).
     */
    public BlazeGraphEdit(final Action action, final BlazeGraphAtom atom) {
        this(action, atom, 0l);
    }

    /**
     * Construct an edit with an known commit time (history API).
     */
    public BlazeGraphEdit(final Action action, final BlazeGraphAtom atom, 
            final long timestamp) {
        this.action = action;
        this.atom = atom;
        this.timestamp = timestamp;
    }

    /**
     * Return the edit action.
     */
    public Action getAction() {
        return action;
    }

    /**
     * Return the atomic unit of graph information edited.
     */
    public BlazeGraphAtom getAtom() {
        return atom;
    }
    
    /**
     * Return the commit time of the edit action or 0l if this is an 
     * uncommitted edit.
     */
    public long getTimestamp() {
        return timestamp;
    }
    
    @Override
    public String toString() {
        return "BlazeGraphEdit [action=" + action + ", atom=" + atom
                + (timestamp > 0 ? ", timestamp=" + timestamp : "") + "]";
    }

}

