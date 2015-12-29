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

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.internal.impl.extensions.CompressedTimestampExtension;
import com.bigdata.rdf.internal.impl.literal.PackedLongIV;
import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sail.RDRHistory;
import com.bigdata.rdf.sparql.ast.QueryHints;

public class SparqlGenerator {

    private static interface Templates {
        
        String VALUES = UUID.randomUUID().toString() + "\n";
        
        String VERTICES =
                "select ?id ?label where {\n" +
                     VALUES +
                "    ?id <LABEL> ?label . filter(isURI(?id)) .\n" +
                "}";
        
        String VERTEX_COUNT =
                "select (count(?id) as ?count) where {\n" +
                "    ?id <LABEL> ?label . filter(isURI(?id)) .\n" +
                "}";
        
        String EDGES =
                "select ?sid ?label ?fromLabel ?toLabel where {\n" +
                     VALUES +
                "    bind(<<?from ?id ?to>> as ?sid) .\n" +
                "    ?sid <LABEL> ?label . filter(!isURI(?sid)) .\n" +
                "    ?from <LABEL> ?fromLabel .\n" +
                "    ?to <LABEL> ?toLabel .\n" +
                "}";

        String EDGE_COUNT =
                "select (count(?sid) as ?count) where {\n" +
                "    ?sid <LABEL> ?label . filter(!isURI(?sid)) .\n" +
                "}";
        
        String PROPERTIES =
                "select ?p ?o where {\n" +
                     VALUES + 
                "    ?s ?p ?o .\n" +
                "    filter(isLiteral(?o)) .\n" +
                "    filter(?p not in(<LABEL>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "}";
        
        String VERTEX_PROPERTIES =
                "select ?vp ?p ?o where {\n" +
                     VALUES + 
                "    {\n" +
                "        bind(<<?s ?p ?o>> as ?vp) .\n" +
                "        filter(isLiteral(?o)) .\n" +
                "        filter(datatype(?o) != <LI>) .\n" +
                "        filter(?p != <LABEL>) .\n" +
                "    } union {\n" +
                "        bind(<<?s ?p ?order>> as ?vp) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <LI>) .\n" +
                "        ?vp <VALUE> ?o .\n" +
                "    }\n" +
                "} order by ?order";
        
        String DIRECTION = UUID.randomUUID().toString() + "\n";
        
        String EDGES_FROM_VERTEX =
                "select ?sid ?label ?fromLabel ?toLabel where {\n" +
                     VALUES + 
                     DIRECTION +
                "    ?sid <LABEL> ?label . filter(!isURI(?sid)) .\n" +
                "}";
        
        String FORWARD =
                "    bind(<<?src ?id ?to>> as ?sid) .\n" +
                "    ?src <LABEL> ?fromLabel .\n" +
                "    ?to <LABEL> ?toLabel .\n";
        
        String REVERSE =
                "    bind(<<?from ?id ?src>> as ?sid) .\n" +
                "    ?src <LABEL> ?toLabel .\n" +
                "    ?from <LABEL> ?fromLabel .\n";
        
        String BOTH =
                "    {\n"+
                         FORWARD.replace("    ", "        ") +
                "    } union {\n" +
                         REVERSE.replace("    ", "        ") +
                "    }\n";
        
        /**
         * Used by Cardinality.single to remove all old vertex properties
         * except exact matches for the new rdf:value.
         */
        String CLEAN_VERTEX_PROPS =
                "delete {\n" +
                "    <VERTEX> <KEY> ?val .\n" +
                "    <VERTEX> <KEY> ?li .\n" +
                "    ?sid ?pp ?oo .\n" +
                "} where {\n" +
                "    {\n" +
                         // remove any non-li vps with different value
                "        bind(<< <VERTEX> <KEY> ?val >> as ?sid) .\n" +
                "        filter(datatype(?val) != <LI>) .\n" +
                "        filter(?val != VAL) .\n" +
                "        optional {\n" +
                "           ?sid ?pp ?oo .\n" +
                "        }\n" +
                "    } union {\n" +
                         // remove all li vps regardless of rdf:value
                "        bind(<< <VERTEX> <KEY> ?li >> as ?sid) .\n" +
                "        filter(datatype(?li) = <LI>) .\n" +
                "        ?sid ?pp ?oo .\n" +
                "    }\n" +
                "}";
        
        /**
         * Sparql template for history query.
         * 
         * TODO FIXME Does not show history of VertexProperties.  Need to
         * verify that RDR syntax works in values clause.
         */
        String HISTORY =
                "prefix hint: <"+QueryHints.NAMESPACE+">\n" +
                "select ?sid ?action ?time where {\n"+
                     VALUES + 
                "    {\n" +
                         // vertices
                "        bind(<< ?id ?p ?o >> as ?sid) .\n" +
                "        hint:Prior hint:history true .\n" +
                "        ?sid ?action ?time .\n" +
                "        filter(?action in (<ADDED>,<REMOVED>)) .\n" +
                "    } union {\n" +
                         // edges
                "        bind(<< ?from ?id ?to >> as ?edge) .\n" +
                "        bind(<< ?edge ?p ?o >> as ?sid) .\n" +
                "        ?sid ?action ?time .\n" +
                "        filter(?action in (<ADDED>,<REMOVED>)) .\n" +
                "    }\n" +
                "    hint:Query hint:history true .\n" +
                "} order by ?time ?action";

         
    }

    private final String VERTICES;
    
    private final String VERTEX_COUNT;
    
    private final String EDGES;
    
    private final String EDGE_COUNT;
    
    private final String PROPERTIES;
    
    private final String VERTEX_PROPERTIES;
    
    private final String EDGES_FROM_VERTEX;
    
    private final String FORWARD;
    
    private final String REVERSE;
    
    private final String BOTH;
    
    private final String CLEAN_VERTEX_PROPS;
    
    private final String HISTORY;
    
    public SparqlGenerator(final BlazeValueFactory vf) {
        final String label = vf.label().stringValue();
        final String value = vf.value().stringValue();
        final String li = vf.liDatatype().stringValue();
        final String added = vf.historyAdded().stringValue();
        final String removed = vf.historyRemoved().stringValue();
        final Function<String, String> f = template -> {
            return template.replace("LABEL", label)
                           .replace("VALUE", value)
                           .replace("LI", li)
                           .replace("ADDED", added)
                           .replace("REMOVED", removed);
        };
        
        this.VERTICES = f.apply(Templates.VERTICES);
        this.VERTEX_COUNT = f.apply(Templates.VERTEX_COUNT);
        this.EDGES = f.apply(Templates.EDGES);
        this.EDGE_COUNT = f.apply(Templates.EDGE_COUNT);
        this.PROPERTIES = f.apply(Templates.PROPERTIES);
        this.VERTEX_PROPERTIES = f.apply(Templates.VERTEX_PROPERTIES);
        this.EDGES_FROM_VERTEX = f.apply(Templates.EDGES_FROM_VERTEX);
        this.FORWARD = f.apply(Templates.FORWARD);
        this.REVERSE = f.apply(Templates.REVERSE);
        this.BOTH = f.apply(Templates.BOTH);
        this.CLEAN_VERTEX_PROPS = f.apply(Templates.CLEAN_VERTEX_PROPS);
        this.HISTORY = f.apply(Templates.HISTORY);
    }
    
    public String vertices(final List<URI> uris) {
        return elements(VERTICES, uris);
    }
    
    public String vertexCount() {
        return VERTEX_COUNT;
    }
    
    public String edges(final List<URI> uris) {
        return elements(EDGES, uris);
    }
    
    public String edgeCount() {
        return EDGE_COUNT;
    }
    
    private String elements(final String template, 
            final List<URI> uris) {
        final StringBuilder sb = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(sb, "?id", uris);
        }
        final String queryStr = template.replace(Templates.VALUES, sb.toString());
        return queryStr;
    }
    
    public String properties(final BlazeElement element, final List<URI> uris) {
        final StringBuilder vc = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(vc, "?p", uris);
        }
        final StringBuilder s = new StringBuilder();
        final Resource resource = element.rdfId();
        if (resource instanceof URI) {
            s.append(sparql(resource));
        } else {
            final BigdataBNode sid = (BigdataBNode) resource;
            final BigdataStatement stmt = sid.getStatement();
            s.append("<<")
                .append(sparql(stmt.getSubject())).append(' ')
                .append(sparql(stmt.getPredicate())).append(' ')
                .append(sparql(stmt.getObject()))
                .append(">>");
        }
        final String template = element instanceof BlazeVertex ?
                VERTEX_PROPERTIES : PROPERTIES;
        final String queryStr = 
                template.replace("?s", s.toString())
                        .replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    public String edgesFromVertex(final BlazeVertex src, final Direction dir,
            final List<Literal> edgeLabels) {
        final URI id = src.rdfId();
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?src", asList(id));
        if (!edgeLabels.isEmpty()) {
            buildValuesClause(vc, "?label", edgeLabels);
        }
        final String DIRECTION = dir == Direction.OUT ? FORWARD 
                                    : dir == Direction.IN ? REVERSE : BOTH;
        final String queryStr = 
                EDGES_FROM_VERTEX.replace(Templates.VALUES, vc.toString())
                                 .replace(Templates.DIRECTION, DIRECTION);
        return queryStr;
    }
    
    public String cleanVertexProps(final URI vertex, final URI key, 
            final Literal val) {
        return CLEAN_VERTEX_PROPS
                .replace("VERTEX", vertex.toString())
                .replace("KEY", key.toString())
                .replace("VAL", val.toString());
    }
    
    public String history(final List<URI> uris) {
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?id", uris);
        final String queryStr = 
                HISTORY.replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    private StringBuilder buildValuesClause(final StringBuilder sb, 
            final String var, final List<? extends Value> vals) {
        sb.append("    values (").append(var).append(") {");
        vals.stream().forEach(val -> {
            sb.append("\n        (").append(sparql(val)).append(")");
        });
        sb.append("\n    }\n");
        return sb;
    }

    private StringBuilder buildValuesClause(final StringBuilder sb, 
            final List<String> vars, final List<List<? extends Value>> vals) {
        sb.append("    values (");
        sb.append(vars.stream().collect(joining(" ")));
        sb.append(") {");
        vals.stream().forEach(val -> {
            sb.append("\n        (");
            sb.append(val.stream().map(this::sparql).collect(joining(" ")));
            sb.append(")");
        });
        sb.append("\n    }\n");
        return sb;
    }

    public final String sparql(final Value val) {
        if (val instanceof Literal) {
            return val.toString();
        } else if (val instanceof URI) {
            return '<' + val.stringValue() + '>';
        } else {
            throw new IllegalArgumentException();
        }
    }
    


}
