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

import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.vocab.BaseVocabularyDecl;
import com.bigdata.rdf.vocab.core.BigdataCoreVocabulary_v20151210;
import com.blazegraph.gremlin.structure.BlazeValueFactory;

/**
 * Custom core Tinkerpop3 vocabulary.  Applications should extend this
 * vocabulary to include known constants that can be inlined into the 
 * vocabulary, avoiding the dictionary indices for higher performance.  A good
 * place to start for identifying these constants are known property keys in
 * the application space.
 */
public class Tinkerpop3CoreVocab_v10 extends BigdataCoreVocabulary_v20151210 {

    /**
     * Tinkerpop3 URIs to be inlined.
     */
    private static final Object[] uris = new Object[]{
            BlazeValueFactory.Defaults.NAMESPACE,
            ListIndexExtension.DATATYPE,
    };

    /**
     * De-serialization ctor.
     */
    public Tinkerpop3CoreVocab_v10() {
        super();
    }
    
    /**
     * Used by {@link AbstractTripleStore#create()}.
     * 
     * @param namespace
     *            The namespace of the KB instance.
     */
    public Tinkerpop3CoreVocab_v10(final String namespace) {
        super(namespace);
    }

    @Override
    protected void addValues() {
        super.addValues();
        addDecl(new BaseVocabularyDecl(uris));
    }
    
}
