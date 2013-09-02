package org.neo4japps.webgraph.importer;

import junit.framework.TestCase;

import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.testutils.MockNodeFactory;

public class PageNodeTest extends TestCase {

    private MockNodeFactory nodeFactory = new MockNodeFactory();

    @Override
    protected void setUp() throws Exception {
        nodeFactory.clear();
    }

    public void testGettersAndSetters() {
        Node page = nodeFactory.newNode();

        assertFalse(PageNode.hasUrlProperty(page));
        assertFalse(PageNode.hasDomainProperty(page));
        assertFalse(PageNode.hasTypeProperty(page));
        assertFalse(PageNode.hasContentProperty(page));

        assertEquals("", PageNode.getUrl(page));
        assertEquals("", PageNode.getDomain(page));
        assertEquals("", PageNode.getType(page));
        assertEquals("", PageNode.getContent(page));

        assertEquals(0, PageNode.getNumberOfOutgoingLinks(page));
        assertEquals(0, PageNode.getNumberOfIncomingLinks(page));
        assertEquals(-1, PageNode.getFacebookTotalCount(page));
        assertEquals(-1, PageNode.getTwitterCount(page));

        PageNode.setUrl(page, "a");
        PageNode.setDomain(page, "b");
        PageNode.setType(page, "c");
        PageNode.setContent(page, "d");
        PageNode.setOutgoingLinks(page, 1);
        PageNode.setIncomingLinks(page, 2);
        PageNode.setFacebookTotalCount(page, 100);
        PageNode.setTwitterCount(page, 101);

        assertEquals("a", PageNode.getUrl(page));
        assertEquals("b", PageNode.getDomain(page));
        assertEquals("c", PageNode.getType(page));
        assertEquals("d", PageNode.getContent(page));
        assertEquals(1, PageNode.getNumberOfOutgoingLinks(page));
        assertEquals(2, PageNode.getNumberOfIncomingLinks(page));
        assertEquals(100, PageNode.getFacebookTotalCount(page));
        assertEquals(101, PageNode.getTwitterCount(page));

        PageNode.incrementOutgoingLinks(page);
        PageNode.incrementIncomingLinks(page);
        assertEquals(2, PageNode.getNumberOfOutgoingLinks(page));
        assertEquals(3, PageNode.getNumberOfIncomingLinks(page));
    }

    public void testToString() {
        Node page = nodeFactory.newNode();
        PageNode.setUrl(page, "a");
        PageNode.setDomain(page, "b");
        PageNode.setType(page, "c");
        PageNode.setContent(page, "d");

        assertEquals("id: 0, url: a, domain: b, type: c", PageNode.toString(page));
    }
}
