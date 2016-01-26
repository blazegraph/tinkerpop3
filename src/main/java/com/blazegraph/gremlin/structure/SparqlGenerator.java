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
package com.blazegraph.gremlin.structure;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

import com.bigdata.rdf.model.BigdataBNode;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.sparql.ast.QueryHints;
import com.bigdata.rdf.store.BDS;
import com.blazegraph.gremlin.structure.BlazeGraph.Match;

/**
 * Generate Sparql queries according the blazegraph-tinkerpop3 PG/RDF data model.
 * 
 * @author mikepersonick
 */
class SparqlGenerator {

    /**
     * Sparql query templates to be fully materialized with a 
     * {@link BlazeValueFactory}.
     * 
     * @author mikepersonick
     */
    private static interface Templates {
        
        /**
         * Find and replace for values clauses.
         */
        String VALUES = UUID.randomUUID().toString() + "\n";
        
        /**
         * @see {@link BlazeGraph#vertices(Object...)}
         */
        String VERTICES =
                "select ?vertex ?label where {\n" +
                     VALUES +
                "    ?vertex <TYPE> ?label .\n" +
                "    filter(isURI(?vertex)) .\n" +
                "}";
        
        /**
         * @see {@link BlazeGraph#vertexCount()}
         */
        String VERTEX_COUNT =
                "select (count(?vertex) as ?count) where {\n" +
                "    ?vertex <TYPE> ?label . filter(isURI(?vertex)) .\n" +
                "}";
        
        /**
         * @see {@link BlazeGraph#edges(Object...)}
         */
        String EDGES =
                "select ?edge ?label ?fromLabel ?toLabel where {\n" +
                     VALUES +
                "    bind(<<?from ?eid ?to>> as ?edge) .\n" +
                "    filter(?eid != <TYPE>) .\n" +
                "    filter(isURI(?from) && isURI(?to)) .\n" +
                "    optional {\n" +
                "        ?edge <TYPE> ?label .\n" +
                "        ?from <TYPE> ?fromLabel .\n" +
                "        ?to <TYPE> ?toLabel .\n" +
                "    }" +
                "}";

        /**
         * @see {@link BlazeGraph#edgeCount()}
         */
        String EDGE_COUNT =
                "select (count(?edge) as ?count) where {\n" +
                "    ?edge <TYPE> ?label . filter(!isURI(?edge)) .\n" +
                "}";
        
        /**
         * @see {@link BlazeGraph#properties(BlazeReifiedElement, String...)}
         */
        String PROPERTIES =
                "select ?key ?val where {\n" +
                     VALUES + 
                "    ?s ?key ?val .\n" +
                "    filter(isLiteral(?val)) .\n" +
                "    filter(?key not in(<TYPE>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "}";
        
        /**
         * @see {@link BlazeGraph#properties(BlazeVertex, String...)}
         */
        String VERTEX_PROPERTIES =
                "select ?vp ?val where {\n" +
                "    {\n" +
                         VALUES + 
                "        bind(<<?s ?key ?val>> as ?vp) .\n" +
                "        filter(isLiteral(?val)) .\n" +
                "        filter(datatype(?val) != <LI>) .\n" +
                "        filter(?key != <TYPE>) .\n" +
                "    } union {\n" +
                         VALUES + 
                "        bind(<<?s ?key ?order>> as ?vp) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <LI>) .\n" +
                "        ?vp <VALUE> ?val .\n" +
                "        hint:Prior hint:runLast true .\n" +
                "    }\n" +
                "} order by ?order";
        
        /**
         * Find and replace for the edge direction.
         */
        String DIRECTION = UUID.randomUUID().toString() + "\n";
        
        /**
         * @see {@link BlazeGraph#edgesFromVertex(BlazeVertex, Direction, String...)}
         * @see {@link BlazeVertex#edges(Direction, String...)}
         */
        String FORWARD =
                "    bind(<<?src ?id ?to>> as ?edge) .\n" +
                "    filter(?id != <TYPE>) .\n" +
                "    filter(isURI(?src) && isURI(?to)) .\n" +
//                "    optional {\n" +
                "        ?edge <TYPE> ?label .\n" +
                "        ?src <TYPE> ?fromLabel .\n" +
                "        ?to <TYPE> ?toLabel .\n" +
//                "    }\n";
                "";
        
        /**
         * @see {@link BlazeGraph#edgesFromVertex(BlazeVertex, Direction, String...)}
         * @see {@link BlazeVertex#edges(Direction, String...)}
         */
        String REVERSE =
                "    bind(<<?from ?id ?src>> as ?edge) .\n" +
                "    filter(?id != <TYPE>) .\n" +
                "    filter(isURI(?src) && isURI(?from)) .\n" +
//                "    optional {\n" +
                "        ?edge <TYPE> ?label .\n" +
                "        ?src <TYPE> ?toLabel .\n" +
                "        ?from <TYPE> ?fromLabel .\n" +
//                "    }\n";
                "";
        
        /**
         * @see {@link BlazeGraph#edgesFromVertex(BlazeVertex, Direction, String...)}
         * @see {@link BlazeVertex#edges(Direction, String...)}
         */
        String EDGES_FROM_VERTEX =
                "select ?edge ?label ?fromLabel ?toLabel where {\n" +
                     VALUES + 
                     DIRECTION +
                "}";
        
        /**
         * @see {@link BlazeGraph#edgesFromVertex(BlazeVertex, Direction, String...)}
         * @see {@link BlazeVertex#edges(Direction, String...)}
         */
        String EDGES_FROM_VERTEX_BOTH =
                "select ?edge ?label ?fromLabel ?toLabel where {\n" +
                "    {\n"+
                         VALUES + 
                         FORWARD.replace("    ", "        ") +
                "    } union {\n" +
                         VALUES + 
                         REVERSE.replace("    ", "        ") +
                "    }\n" +
                "}";
        
        /**
         * Used by Cardinality.single to remove all old vertex properties
         * except exact matches for the new rdf:value.
         * 
         * @see {@link BlazeGraph#vertexProperty(BlazeVertex, Cardinality, String, Object, Object...)}
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
         * @see {@link BlazeGraph#history(List)}
         * 
         * TODO FIXME Does not show history of VertexProperties.  Need to
         * verify that RDR syntax works in values clause.
         */
        String HISTORY =
                "prefix hint: <"+QueryHints.NAMESPACE+">\n" +
                "select ?sid ?action ?time where {\n"+
                "    {\n" +
                         VALUES + 
                         // vertices
                "        bind(<< ?id ?p ?o >> as ?sid) .\n" +
                "        hint:Prior hint:history true .\n" +
                "        ?sid ?action ?time .\n" +
                "        hint:Prior hint:runLast true .\n" +
                "        filter(?action in (<ADDED>,<REMOVED>)) .\n" +
                "    } union {\n" +
                         VALUES + 
                         // edges
                "        bind(<< ?from ?id ?to >> as ?edge) .\n" +
                "        filter(?id != <TYPE>) .\n" +
                "        bind(<< ?edge ?p ?o >> as ?sid) .\n" +
                "        ?sid ?action ?time .\n" +
                "        hint:Prior hint:runLast true .\n" +
                "        filter(?action in (<ADDED>,<REMOVED>)) .\n" +
                "    }\n" +
                "    hint:Query hint:history true .\n" +
                "} order by ?time ?action";

        /**
         * @see {@link BlazeGraph#remove(BlazeVertex)}
         */
        String REMOVE_VERTEX =
                "delete {\n" +
                "    ?id ?p ?o .\n" +
                "    ?s ?p ?id .\n" +
                "    ?sid ?pp ?oo .\n" +
                "} where {\n" +
                "    {\n" +
                         VALUES +
                "        bind(<< ?id ?p ?o >> as ?sid) .\n" +
                "        optional {\n" +
                "            ?sid ?pp ?oo .\n" +
                "            filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
                "        }\n" +
                "    } union {\n" +
                         VALUES +
                "        bind(<< ?s ?p ?id >> as ?sid) .\n" +
                "        optional {\n" +
                "            ?sid ?pp ?oo .\n" +
                "            filter(?pp not in(<ADDED>,<REMOVED>)) .\n" +
                "        }\n" +
                "    }\n" +
                "}";
        
        /**
         * @see {@link BlazeGraph#remove(BlazeReifiedElement)
         */
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
        
        /**
         * @see {@link BlazeGraph#search(String, Match)}
         */
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
                "        filter(?key not in (<TYPE>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        optional {\n" +
                "            ?edge <TYPE> ?label .\n" +
                "            ?from <TYPE> ?fromLabel .\n" +
                "            ?to <TYPE> ?toLabel .\n" +
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
                "        filter(?key not in (<TYPE>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        optional { ?vertex <TYPE> ?label . }\n" +
                "    } union {\n" +
                         /*
                          * Vertex property (Cardinality.set/single)
                          */
                "        bind(<<?vertex ?vpKey ?val>> as ?vp) .\n" +
                "        filter(isURI(?vertex)) .\n" +
                "        filter(?vpKey not in (<TYPE>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        optional { ?vertex <TYPE> ?label . }\n" +
                "    } union {\n" +
                         /*
                          * Vertex property (Cardinality.list)
                          */
                "        bind(<<?vertex ?vpKey ?order>> as ?vp) .\n" +
                "        filter(isURI(?vertex)) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <LI>) .\n" +
                "        filter(?vpKey not in (<TYPE>,<VALUE>,<ADDED>,<REMOVED>)) .\n" +
                "        ?vp <VALUE> ?val.\n" +
                "        optional { ?vertex <TYPE> ?label . }\n" +
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
    
    private final String EDGES_FROM_VERTEX_BOTH;
    
    private final String FORWARD;
    
    private final String REVERSE;
    
    private final String CLEAN_VERTEX_PROPS;
    
    private final String HISTORY;
    
    private final String REMOVE_VERTEX;

    private final String REMOVE_REIFIED_ELEMENT;
    
    private final String TEXT_SEARCH;

    /**
     * Create materialized versions of the Sparql templates using the supplied
     * {@link BlazeValueFactory}.
     */
    SparqlGenerator(final BlazeValueFactory vf) {
        final String type = vf.type().stringValue();
        final String value = vf.value().stringValue();
        final String li = vf.liDatatype().stringValue();
        final String added = vf.historyAdded().stringValue();
        final String removed = vf.historyRemoved().stringValue();
        final Function<String, String> f = template -> {
            return template.replace("TYPE", type)
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
        this.EDGES_FROM_VERTEX_BOTH = f.apply(Templates.EDGES_FROM_VERTEX_BOTH);
        this.FORWARD = f.apply(Templates.FORWARD);
        this.REVERSE = f.apply(Templates.REVERSE);
        this.CLEAN_VERTEX_PROPS = f.apply(Templates.CLEAN_VERTEX_PROPS);
        this.HISTORY = f.apply(Templates.HISTORY);
        this.REMOVE_VERTEX = f.apply(Templates.REMOVE_VERTEX);
        this.REMOVE_REIFIED_ELEMENT = f.apply(Templates.REMOVE_REIFIED_ELEMENT);
        this.TEXT_SEARCH = f.apply(Templates.TEXT_SEARCH);
    }
    
    /**
     * @see {@link Templates#VERTICES}
     */
    String vertices(final List<URI> uris) {
        final StringBuilder sb = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(sb, "?vertex", uris);
        }
        final String queryStr = VERTICES.replace(Templates.VALUES, sb.toString());
        return queryStr;
    }
    
    /**
     * @see {@link Templates#VERTEX_COUNT}
     */
    String vertexCount() {
        return VERTEX_COUNT;
    }
    
    /**
     * @see {@link Templates#EDGES}
     */
    String edges(final List<URI> uris) {
        final StringBuilder sb = new StringBuilder();
        if (!uris.isEmpty()) {
            buildValuesClause(sb, "?eid", uris);
        }
        final String queryStr = EDGES.replace(Templates.VALUES, sb.toString());
        return queryStr;
    }
    
    /**
     * @see {@link Templates#EDGE_COUNT}
     */
    String edgeCount() {
        return EDGE_COUNT;
    }
    
    /**
     * @see {@link Templates#PROPERTIES}
     */
    String properties(final BlazeReifiedElement element, final List<URI> uris) {
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
    
    /**
     * @see {@link Templates#VERTEX_PROPERTIES}
     */
    String vertexProperties(final BlazeVertex vertex, final List<URI> uris) {
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
    
    /**
     * @see {@link Templates#EDGES_FROM_VERTEX}
     */
    String edgesFromVertex(final BlazeVertex src, final Direction dir,
            final List<URI> edgeLabels) {
        final URI id = src.rdfId();
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?src", asList(id));
        if (!edgeLabels.isEmpty()) {
            buildValuesClause(vc, "?label", edgeLabels);
        }
        final String queryStr;
        if (dir == Direction.BOTH) {
            queryStr = EDGES_FROM_VERTEX_BOTH.replace(Templates.VALUES, vc.toString());
        } else {
            final String DIRECTION = dir == Direction.OUT ? FORWARD : REVERSE;
            queryStr = EDGES_FROM_VERTEX.replace(Templates.VALUES, vc.toString())
                                        .replace(Templates.DIRECTION, DIRECTION);
        }
        return queryStr;
    }
    
    /**
     * @see {@link Templates#CLEAN_VERTEX_PROPS}
     */
    String cleanVertexProps(final URI vertex, final URI key, 
            final Literal val) {
        return CLEAN_VERTEX_PROPS
                .replace("VERTEX", vertex.toString())
                .replace("KEY", key.toString())
                .replace("VAL", val.toString());
    }
    
    /**
     * @see {@link Templates#HISTORY}
     */
    String history(final List<URI> uris) {
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?id", uris);
        final String queryStr = 
                HISTORY.replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    /**
     * @see {@link Templates#REMOVE_VERTEX}
     */
    String removeVertex(final BlazeVertex v) {
        final URI id = v.rdfId();
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?id", asList(id));
        final String queryStr =
                REMOVE_VERTEX.replace(Templates.VALUES, vc.toString());
        return queryStr;
    }
    
    /**
     * @see {@link Templates#REMOVE_REIFIED_ELEMENT}
     */
    String removeReifiedElement(final BlazeReifiedElement e) {
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
    
    /**
     * @see {@link Templates#TEXT_SEARCH}
     */
    String search(final String search, final Match match) {
        final Boolean matchAll = match != Match.ANY;
        final Boolean matchExact = match == Match.EXACT;
        final String queryStr = TEXT_SEARCH
                .replace("?search", "\""+search+"\"")
                .replace("?matchAll", matchAll.toString())
                .replace("?matchExact", matchExact.toString());
        return queryStr;
    }
    
    private String sparql(final Value val) {
        if (val instanceof Literal) {
            return val.toString();
        } else if (val instanceof URI) {
            return '<' + val.stringValue() + '>';
        } else {
            throw new IllegalArgumentException();
        }
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
            sb.append(val.stream().map(this::sparql).collect(joining(" ")));
            sb.append(")");
        });
        sb.append("\n    }\n");
        return sb;
    }



}
