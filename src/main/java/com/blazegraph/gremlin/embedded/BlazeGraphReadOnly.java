package com.blazegraph.gremlin.embedded;

import org.apache.commons.configuration.Configuration;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.blazegraph.gremlin.util.Code;

public class BlazeGraphReadOnly extends BlazeGraphEmbedded {

    private final BigdataSailRepositoryConnection cxn;
    
    public BlazeGraphReadOnly(final BigdataSailRepository repo,
            final BigdataSailRepositoryConnection cxn,
            final Configuration config) {
        super(repo, config);
        
        this.cxn = cxn;
    }
    
    @Override
    public BlazeTransaction tx() {
        throw new UnsupportedOperationException("Transactions not allowed on read-only view");
    }
    
    @Override
    public BigdataSailRepositoryConnection cxn() {
        if (closed) throw new IllegalStateException();
        
        return cxn;
    }
    
    @Override
    public synchronized void close() {
        if (closed)
            return;
        
        Code.wrapThrow(() -> cxn.close());
        closed = true;
    }


}
