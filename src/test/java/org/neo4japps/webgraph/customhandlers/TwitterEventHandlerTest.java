package org.neo4japps.webgraph.customhandlers;

import java.util.Properties;

import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.importer.AbstractObservableGraphImporter;
import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.PageNode;
import org.neo4japps.webgraph.importer.TransactionalGraphImporter;

import junit.framework.TestCase;

public class TwitterEventHandlerTest extends TestCase {

    private AbstractObservableGraphImporter graphImporter;
    private TwitterEventHandler handler;

    @Override
    protected void setUp() throws Exception {
        graphImporter = TransactionalGraphImporter.createImpermanentInstance("http://homepage.com/",
                System.currentTimeMillis(), 100, 500);
        handler = new TwitterEventHandler();
        handler.configure(new ApplicationConfiguration(new String[] { "-t" }, new Properties()));
        graphImporter.addObserver(handler);
    }

    @Override
    protected void tearDown() {
        handler.shutdown();
        graphImporter.shutdown();
    }

    public void testThatNodeIsUpdatedByHandlerWhenUrlImported() throws Exception {
        handler.setTwitterClient(new SocialMediaClientStub(42));
        Node page = graphImporter.addPage("http://mydomain.com/", "random content");
        assertTrue(PageNode.hasTwitterCountProperty(page));
        assertEquals(42, PageNode.getTwitterCount(page));
        assertTrue(handler.shouldIgnore(page, graphImporter));
    }

    public void testThatNodeIsNotUpdatedWhenSocialCallFails() throws Exception {
        handler.setTwitterClient(new SocialMediaClientStub(-1));
        Node page = graphImporter.addPage("http://mydomain.com/", "random content");
        assertFalse(PageNode.hasTwitterCountProperty(page));
        assertEquals(-1, PageNode.getTwitterCount(page));
        assertFalse(handler.shouldIgnore(page, graphImporter));
    }
}
