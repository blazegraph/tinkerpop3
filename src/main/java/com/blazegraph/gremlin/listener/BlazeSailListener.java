package com.blazegraph.gremlin.listener;

import com.bigdata.rdf.changesets.IChangeLog;
import com.bigdata.rdf.changesets.IChangeRecord;

/**
 * Provide default no-ops for IChangeLog.
 * 
 * @author mikepersonick
 */
public interface BlazeSailListener extends IChangeLog {

    @Override
    default void changeEvent(IChangeRecord changeRecord) {
    }

    @Override
    default void close() {
    }

    @Override
    default void transactionAborted() {
    }

    @Override
    default void transactionBegin() {
    }

    @Override
    default void transactionCommited(long commitTime) {
    }

    @Override
    default void transactionPrepare() {
    }
    
}
