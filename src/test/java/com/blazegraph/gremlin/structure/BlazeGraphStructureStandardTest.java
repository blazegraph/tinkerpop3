package com.blazegraph.gremlin.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;

import junit.framework.TestCase;

@RunWith(StructureModifiedSuite.class)
@GraphProviderClass(provider = LocalBlazeGraphProvider.class, graph = BlazeGraph.class)
public class BlazeGraphStructureStandardTest extends TestCase {}
