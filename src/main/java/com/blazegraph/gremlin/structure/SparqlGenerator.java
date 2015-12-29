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
                "    filter(?p not in(<LABEL>,<VALUE>)) .\n" +
                "}";
        
        String VERTEX_PROPERTIES =
                "select ?vp ?p ?o where {\n" +
                     VALUES + 
                "    {\n" +
                "        bind(<<?s ?p ?o>> as ?vp) .\n" +
                "        filter(isLiteral(?o)) .\n" +
                "        filter(datatype(?o) != <"+CompressedTimestampExtension.COMPRESSED_TIMESTAMP+">) .\n" +
                "        filter(?p != <LABEL>) .\n" +
                "    } union {\n" +
                "        bind(<<?s ?p ?order>> as ?vp) .\n" +
                "        filter(isLiteral(?order)) .\n" +
                "        filter(datatype(?order) = <"+CompressedTimestampExtension.COMPRESSED_TIMESTAMP+">) .\n" +
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
        
        String CLEAN_VERTEX_PROPS =
                "delete {\n" +
                "    <S> <P> ?o .\n" +
                "    ?sid ?pp ?oo .\n" +
                "} where {\n" +
                "    {\n" +
                "        <S> <P> ?o . " +
                "        filter(?o != O) .\n" +
                "        filter(datatype(?o) != <"+PackedLongIV.PACKED_LONG+">) .\n" +
                "        optional {\n" +
                "           bind(<<<S> <P> ?o>> as ?sid) .\n" +
                "           ?sid ?pp ?oo .\n" +
                "        }\n" +
                "    } union {\n" +
                "        <S> <P> ?vp . " +
                "        filter(datatype(?vp) = <"+PackedLongIV.PACKED_LONG+">) .\n" +
                "        optional {\n" +
                "           bind(<<<S> <P> ?o>> as ?sid) .\n" +
                "           ?sid ?pp ?oo .\n" +
                "        }\n" +
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
                "        bind(<< ?id ?p ?o >> as ?sid) .\n" + // vertices
                "        hint:Prior hint:history true .\n" +
                "    } union {\n" +
                "        bind(<< ?from ?id ?to >> as ?edge) .\n" + // edges
                "        bind(<< ?edge ?p ?o >> as ?sid) .\n" +
                "    }\n" +
                "    ?sid ?action ?time ." +
                "    filter(?action in (<"+RDRHistory.Vocab.ADDED+">," +
                                       "<"+RDRHistory.Vocab.REMOVED+">)) .\n" +
                "    hint:Query hint:history true .\n" +
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
    
    public SparqlGenerator(final BlazeValueFactory vf) {
        final String label = vf.labelURI().stringValue();
        final String value = vf.valueURI().stringValue();
        final Function<String, String> replacer = template -> {
            return template.replace("LABEL", label)
                           .replace("VALUE", value);
        };
        
        this.VERTICES = replacer.apply(Templates.VERTICES);
        this.VERTEX_COUNT = replacer.apply(Templates.VERTEX_COUNT);
        this.EDGES = replacer.apply(Templates.EDGES);
        this.EDGE_COUNT = replacer.apply(Templates.EDGE_COUNT);
        this.PROPERTIES = replacer.apply(Templates.PROPERTIES);
        this.VERTEX_PROPERTIES = replacer.apply(Templates.VERTEX_PROPERTIES);
        this.EDGES_FROM_VERTEX = replacer.apply(Templates.EDGES_FROM_VERTEX);
        this.FORWARD = replacer.apply(Templates.FORWARD);
        this.REVERSE = replacer.apply(Templates.REVERSE);
        this.BOTH = replacer.apply(Templates.BOTH);
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
//        final List<URI> uris = validateIds(elementIds);
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
    
    public String cleanVertexProps(final URI s, final URI p, final Literal o) {
        return Templates.CLEAN_VERTEX_PROPS
                .replace("S", s.toString())
                .replace("P", p.toString())
                .replace("O", o.toString());
    }
    
    public String history(final List<URI> uris) {
        final StringBuilder vc = buildValuesClause(
                new StringBuilder(), "?s", uris);
        final String queryStr = 
                Templates.HISTORY.replace(Templates.VALUES, vc.toString());
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
