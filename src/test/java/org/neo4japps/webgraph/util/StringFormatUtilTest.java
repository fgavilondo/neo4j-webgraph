package org.neo4japps.webgraph.util;

import junit.framework.TestCase;

public class StringFormatUtilTest extends TestCase {

    public void testNodesPerSecondFormattedCorrectly() {
        assertEquals("0.00", StringFormatUtil.formatNodesPerSecond(0.00));
        assertEquals("1.00", StringFormatUtil.formatNodesPerSecond(1.00));
        assertEquals("1.11", StringFormatUtil.formatNodesPerSecond(1.111));
        assertEquals("1.26", StringFormatUtil.formatNodesPerSecond(1.256));
        assertEquals("1.26", StringFormatUtil.formatNodesPerSecond(1.264));
        assertEquals("1111.26", StringFormatUtil.formatNodesPerSecond(1111.264));
    }
}
