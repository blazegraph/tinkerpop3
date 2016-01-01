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
package com.blazegraph.gremlin.structure;

import static java.util.stream.Collectors.toList;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;

import com.bigdata.rdf.sail.BigdataSailRepository;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.blazegraph.gremlin.embedded.BlazeGraphEmbedded;
import com.blazegraph.gremlin.util.CloseableIterator;
import com.blazegraph.gremlin.util.LambdaLogger;

import junit.framework.TestCase;

public abstract class TestBlazeGraph extends TestCase {

    private final static transient LambdaLogger log = LambdaLogger.getLogger(TestBlazeGraph.class);
    
    protected BlazeGraphEmbedded graph = null;
    
    protected Map<String,String> overrides() {
        return Collections.emptyMap();
    }
    
    @Override
    public void setUp() throws Exception {
        final BigdataSailRepository repo = TestRepositoryProvider.open(overrides()); 
        final Configuration config = new BaseConfiguration();
//        this.mgr = new BlazeGraphManager(repo, config);
//        this.graph = mgr.unisolatedConnection();
        this.graph = BlazeGraphEmbedded.open(repo, config);
    }
    
    @Override
    public void tearDown() throws Exception {
        this.graph.close();
        this.graph.__tearDownUnitTest();
    }
    
    protected boolean hasStatement(final Resource s, final URI p, final Literal o) throws Exception {
        final BigdataSailRepositoryConnection cxn = graph.cxn();
        return cxn.hasStatement(null, p, o, true);
    }
    
    protected <E> List<E> collect(final CloseableIterator<?> it, final Class<E> cls) {
        try (Stream<?> s = it.stream()) {
            return s.map(cls::cast).collect(toList());
        }
    }
    
    protected long count(final CloseableIterator<?> it) {
        return it.count();
    }
    
}
