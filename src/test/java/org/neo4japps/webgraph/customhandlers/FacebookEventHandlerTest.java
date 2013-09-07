package org.neo4japps.webgraph.customhandlers;

import java.util.Properties;

import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.importer.AbstractObservableGraphImporter;
import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.PageNode;

public class FacebookEventHandlerTest extends EventHandlerTest {

    private AbstractObservableGraphImporter graphImporter;
    private FacebookEventHandler handler;

    @Override
    protected void setUp() throws Exception {
        graphImporter = createImpermanentGraphImporter("http://homepage.com/", System.currentTimeMillis(), 100, 500);
        handler = new FacebookEventHandler();
        handler.configure(new ApplicationConfiguration(new String[] { "-t" }, new Properties()));
        graphImporter.addObserver(handler);
    }

    @Override
    protected void tearDown() {
        handler.shutdown();
        graphImporter.shutdown();
    }

    public void testThatNodeIsUpdatedByHandlerWhenUrlImported() throws Exception {
        handler.setFacebookClient(new SocialMediaClientStub(42));
        Node page = graphImporter.addPage("http://mydomain.com/", "random content");
        assertTrue(PageNode.hasFacebookTotalCountProperty(page));
        assertEquals(42, PageNode.getFacebookTotalCount(page));
        assertTrue(handler.shouldIgnore(page, graphImporter));
    }

    public void testThatNodeIsNotUpdatedWhenSocialCallFails() throws Exception {
        handler.setFacebookClient(new SocialMediaClientStub(-1));
        Node page = graphImporter.addPage("http://mydomain.com/", "random content");
        assertFalse(PageNode.hasFacebookTotalCountProperty(page));
        assertEquals(-1, PageNode.getFacebookTotalCount(page));
        assertFalse(handler.shouldIgnore(page, graphImporter));
    }
}
