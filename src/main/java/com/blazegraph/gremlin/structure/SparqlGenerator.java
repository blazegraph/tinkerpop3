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

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.BDS;
import com.blazegraph.gremlin.structure.BlazeGraph.Match;

public class SparqlGenerator {

    private static interface Templates {
        
        String VALUES = UUID.randomUUID().toString() + "\n";
        
        String VERTICES =
                "select ?vertex ?label where {\n" +
                     VALUES +
                "    ?vertex <LABEL> ?label .\n" +
                "    filter(isURI(?vertex)) .\n" +
                "}";
        
        String VERTEX_COUNT =
                "select (count(?vertex) as ?count) where {\n" +
                "    ?vertex <LABEL> ?label . filter(isURI(?vertex)) .\n" +
                "}";
        
        String EDGES =
                "select ?edge ?label ?fromLabel ?toLabel where {\n" +
                     VALUES +
                "    bind(<<?from ?eid ?to>> as ?edge) .\n" +
                "    filter(isURI(?from) && isURI(?to)) .\n" +
                "    optional {\n" +
                "        ?edge <LABEL> ?label .\n" +
                "        ?from <LABEL> ?fromLabel .\n" +
                "        ?to <LABEL> ?toLabel .\n" +
                "    }" +
                "}";

        String EDGE_COUNT =
                "select (count(?edge) as ?count) where {\n" +
                "    ?edge <LABEL> ?label . filter(!isURI(?edge)) .\n" +
                "}";
        
        String PROPERTIES =
                "select ?key ?val where {\n" +
                     VALUES + 
                "    ?s ?key ?val .\n" +
                "    filter(isLiteral(?val)) .\n" +
                "    filter(?key not in(<LABEL>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "}";
        
        String VERTEX_PROPERTIES =
                "select ?vp ?val where {\n" +
                     VALUES + 
                "    {\n" +
                "        bind(<<?s ?key ?val>> as ?vp) .\n" +
                "        filter(isLiteral(?val)) .\n" +
                "        filter(datatype(?val) != <LI>) .\n" +
                "        filter(?key != <LABEL>) .\n" +
                "    } union {\n" +
                "        bind(<<?s ?key ?order>> as ?vp) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <LI>) .\n" +
                "        ?vp <VALUE> ?val .\n" +
                "    }\n" +
                "} order by ?order";
        
        String DIRECTION = UUID.randomUUID().toString() + "\n";
        
        String EDGES_FROM_VERTEX =
                "select ?edge ?label ?fromLabel ?toLabel where {\n" +
                     VALUES + 
                     DIRECTION +
                "    ?edge <LABEL> ?label .\n" +
                "    filter(!isURI(?edge)) .\n" +
                "}";
        
        String FORWARD =
                "    bind(<<?src ?id ?to>> as ?edge) .\n" +
                "    filter(isURI(?src) && isURI(?to)) .\n" +
                "    optional {\n" +
                "        ?src <LABEL> ?fromLabel .\n" +
                "        ?to <LABEL> ?toLabel .\n" +
                "    }\n";
        
        String REVERSE =
                "    bind(<<?from ?id ?src>> as ?edge) .\n" +
                "    filter(isURI(?src) && isURI(?from)) .\n" +
                "    optional {\n" +
                "        ?src <LABEL> ?toLabel .\n" +
                "        ?from <LABEL> ?fromLabel .\n" +
                "    }\n";
        
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
                         /*
                          * Remove any card.set/single vps with different value.
                          */
                "        bind(<< <VERTEX> <KEY> ?val >> as ?sid) .\n" +
                "        filter(isLiteral(?val)) .\n" +
                "        filter(datatype(?val) != <LI>) .\n" +
                "        filter(?val != VAL) .\n" +
                "        optional {\n" +
                            /*
                             * Look for vpps, optional because there might not
                             * be any. Skip history statements.
                             */
                "           ?sid ?pp ?oo .\n" +
                "           filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
                "        }\n" +
                "    } union {\n" +
                         /*
                          * Remove all card.list vps regardless of rdf:value,
                          * since this is only used with card.single.
                          */
                "        bind(<< <VERTEX> <KEY> ?order >> as ?sid) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <LI>) .\n" +
                         /*
                          * This part does not need to be optional, since
                          * there will always be an rdfs:value statement.
                          * Skip history statements.
                          */
                "        ?sid ?pp ?oo .\n" +
                "        filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
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

        String REMOVE_VERTEX =
                "delete {\n" +
                "    ?id ?p ?o .\n" +
                "    ?s ?p ?id .\n" +
                "    ?sid ?pp ?oo .\n" +
                "} where {\n" +
                     VALUES +
                "    {\n" +
                "        bind(<< ?id ?p ?o >> as ?sid) .\n" +
                "        optional {\n" +
                "            ?sid ?pp ?oo .\n" +
                "            filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
                "        }\n" +
                "    } union {\n" +
                "        bind(<< ?s ?p ?id >> as ?sid) .\n" +
                "        optional {\n" +
                "            ?sid ?pp ?oo .\n" +
                "            filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
                "        }\n" +
                "    }\n" +
                "}";
        
        
        String REMOVE_REIFIED_ELEMENT =
                "delete {\n" +
                "    ?s ?p ?o .\n" +
                "    ?sid ?pp ?oo .\n" +
                "} where {\n" +
                     VALUES +
                "    bind(<< ?s ?p ?o >> as ?sid) .\n" +
                "    optional {\n" +
                "        ?sid ?pp ?oo .\n" +
                "        filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
                "    }\n" +
                "}";
        
        String TEXT_SEARCH =
                "select ?vertex ?edge ?label ?fromLabel ?toLabel ?vp ?vpVal ?key ?val \n" +
                "where {\n" +
                "    ?val <"+BDS.SEARCH+"> ?search .\n" +
                "    ?val <"+BDS.MATCH_ALL_TERMS+"> ?matchAll .\n" +
                "    ?val <"+BDS.MATCH_EXACT+"> ?matchExact .\n" +
                "    {\n" +
                         /*
                          * Edge property
                          */
                "        bind(<<?from ?eid ?to>> as ?edge) .\n" +
                "        filter(isURI(?from) && isURI(?to)) .\n" +
                "        ?edge ?key ?val .\n" +
                "        filter(?key not in (<LABEL>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        optional {\n" +
                "            ?edge <LABEL> ?label .\n" +
                "            ?from <LABEL> ?fromLabel .\n" +
                "            ?to <LABEL> ?toLabel .\n" +
                "        }" +
                "    } union {\n" +
                         /*
                          * VertexProperty property
                          */
                "        bind(<<?vertex ?vpKey ?vpVal>> as ?vp) .\n" +
                "        filter(isURI(?vertex)) .\n" +
                "        filter(isLiteral(?vpVal)) .\n" +
                "        filter(!isURI(?vp)) .\n" +
                "        ?vp ?key ?val .\n" +
                "        filter(?key not in (<LABEL>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        optional { ?vertex <LABEL> ?label . }\n" +
                "    } union {\n" +
                         /*
                          * Vertex property (Cardinality.set/single)
                          */
                "        bind(<<?vertex ?vpKey ?val>> as ?vp) .\n" +
                "        filter(isURI(?vertex)) .\n" +
                "        filter(?vpKey not in (<LABEL>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        optional { ?vertex <LABEL> ?label . }\n" +
                "    } union {\n" +
                         /*
                          * Vertex property (Cardinality.list)
                          */
                "        bind(<<?vertex ?vpKey ?order>> as ?vp) .\n" +
                "        filter(isURI(?vertex)) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <LI>) .\n" +
                "        filter(?vpKey not in (<LABEL>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        ?vp <VALUE> ?val.\n" +
                "        optional { ?vertex <LABEL> ?label . }\n" +
                "    }\n" +
                "}";
         
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
    
    private final String REMOVE_VERTEX;

    private final String REMOVE_REIFIED_ELEMENT;
    
    private final String TEXT_SEARCH;

    
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
        this.REMOVE_VERTEX = f.apply(Templates.REMOVE_VERTEX);
        this.REMOVE_REIFIED_ELEMENT = f.apply(Templates.REMOVE_REIFIED_ELEMENT);
        this.TEXT_SEARCH = f.apply(Templates.TEXT_SEARCH);
    }
    
    public String vertices(final List<URI> uris) {
        final StringBuilder sb = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(sb, "?vertex", uris);
        }
        final String queryStr = VERTICES.replace(Templates.VALUES, sb.toString());
        return queryStr;
    }
    
    public String vertexCount() {
        return VERTEX_COUNT;
    }
    
    public String edges(final List<URI> uris) {
        final StringBuilder sb = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(sb, "?eid", uris);
        }
        final String queryStr = EDGES.replace(Templates.VALUES, sb.toString());
        return queryStr;
    }
    
    public String edgeCount() {
        return EDGE_COUNT;
    }
    
    public String properties(final BlazeReifiedElement element, final List<URI> uris) {
        final StringBuilder vc = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(vc, "?key", uris);
        }
        final StringBuilder s = new StringBuilder();
        final BigdataBNode sid = element.rdfId();
        final BigdataStatement stmt = sid.getStatement();
        s.append("<<")
            .append(sparql(stmt.getSubject())).append(' ')
            .append(sparql(stmt.getPredicate())).append(' ')
            .append(sparql(stmt.getObject()))
            .append(">>");
        final String queryStr = 
                PROPERTIES.replace("?s", s.toString())
                          .replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    public String vertexProperties(final BlazeVertex vertex, final List<URI> uris) {
        final StringBuilder vc = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(vc, "?key", uris);
        }
        final URI uri = vertex.rdfId();
        final String queryStr = 
                VERTEX_PROPERTIES.replace("?s", sparql(uri))
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
    
    public String removeVertex(final BlazeVertex v) {
        final URI id = v.rdfId();
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?id", asList(id));
        final String queryStr =
                REMOVE_VERTEX.replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    public String removeReifiedElement(final BlazeReifiedElement e) {
        final BigdataBNode sid = e.rdfId();
        final BigdataStatement stmt = sid.getStatement();
        final Resource s = stmt.getSubject();
        final URI p = stmt.getPredicate();
        final Value o = stmt.getObject();
        final StringBuilder vc = buildValuesClause(new StringBuilder(), 
                new String[] { "?s", "?p", "?o" }, 
                new Value[]  { s, p, o });
        final String queryStr =
                REMOVE_REIFIED_ELEMENT.replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    public String search(final String search, final Match match) {
        final Boolean matchAll = match != Match.ANY;
        final Boolean matchExact = match == Match.EXACT;
        final String queryStr = TEXT_SEARCH
                .replace("?search", "\""+search+"\"")
                .replace("?matchAll", matchAll.toString())
                .replace("?matchExact", matchExact.toString());
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
            final String[] vars, final Value[] vals) {
        return buildValuesClause(sb, Arrays.asList(vars), Arrays.asList(Arrays.asList(vals)));
    }
    
    private StringBuilder buildValuesClause(final StringBuilder sb, 
            final List<String> vars, final List<List<? extends Value>> vals) {
        sb.append("    values (");
        sb.append(vars.stream().collect(joining(" ")));
        sb.append(") {");
        vals.stream().forEach(val -> {
            sb.append("\n        (");
            sb.append(val.stream().map(SparqlGenerator::sparql).collect(joining(" ")));
            sb.append(")");
        });
        sb.append("\n    }\n");
        return sb;
    }

    public static final String sparql(final Value val) {
        if (val instanceof Literal) {
            return val.toString();
        } else if (val instanceof URI) {
            return '<' + val.stringValue() + '>';
        } else {
            throw new IllegalArgumentException();
        }
    }
    


}
