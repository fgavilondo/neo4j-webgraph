package org.neo4japps.webgraph.customhandlers;

import junit.framework.TestCase;
import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.importer.AbstractObservableGraphImporter;
import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.PageNode;
import org.neo4japps.webgraph.importer.TransactionalGraphImporter;

import java.util.Properties;

public class FacebookAndTwitterEventHandlerTest extends TestCase {

    private AbstractObservableGraphImporter graphImporter;
    private FacebookAndTwitterEventHandler handler;

    @Override
    protected void setUp() throws Exception {
        graphImporter = TransactionalGraphImporter.createImpermanentInstance("http://homepage.com/",
                System.currentTimeMillis(), 100, 500);
        handler = new FacebookAndTwitterEventHandler();
        handler.configure(new ApplicationConfiguration(new String[]{"-t"}, new Properties()));
        graphImporter.addObserver(handler);
    }

    @Override
    protected void tearDown() {
        handler.shutdown();
        graphImporter.shutdown();
    }

    public void testThatNodeIsUpdatedByHandlerWhenUrlImported() throws Exception {
        handler.setFacebookClient(new SocialMediaClientStub(42));
        handler.setTwitterClient(new SocialMediaClientStub(42));

        Node page = graphImporter.addPage("http://mydomain.com/", "random content");

        assertTrue(PageNode.hasFacebookTotalCountProperty(page));
        assertEquals(42, PageNode.getFacebookTotalCount(page));

        assertTrue(PageNode.hasTwitterCountProperty(page));
        assertEquals(42, PageNode.getTwitterCount(page));

        assertTrue(handler.shouldIgnore(page, graphImporter));
    }

    public void testThatNodeIsNotUpdatedWhenAllSocialCallFail() throws Exception {
        handler.setFacebookClient(new SocialMediaClientStub(-1));
        handler.setTwitterClient(new SocialMediaClientStub(-1));

        Node page = graphImporter.addPage("http://mydomain.com/", "random content");

        assertFalse(PageNode.hasFacebookTotalCountProperty(page));
        assertEquals(-1, PageNode.getFacebookTotalCount(page));

        assertFalse(PageNode.hasTwitterCountProperty(page));
        assertEquals(-1, PageNode.getTwitterCount(page));

        assertFalse(handler.shouldIgnore(page, graphImporter));
    }

    public void testThatNodeIsUpdatedCorrectlyWhenOnlyTwitterCallFails() throws Exception {
        handler.setFacebookClient(new SocialMediaClientStub(42));
        handler.setTwitterClient(new SocialMediaClientStub(-1));

        Node page = graphImporter.addPage("http://mydomain.com/", "random content");

        assertTrue(PageNode.hasFacebookTotalCountProperty(page));
        assertEquals(42, PageNode.getFacebookTotalCount(page));

        assertFalse(PageNode.hasTwitterCountProperty(page));
        assertEquals(-1, PageNode.getTwitterCount(page));

        assertFalse(handler.shouldIgnore(page, graphImporter));
    }

    public void testThatNodeIsUpdatedCorrectlyWhenOnlyFacebookCallFails() throws Exception {
        handler.setFacebookClient(new SocialMediaClientStub(-1));
        handler.setTwitterClient(new SocialMediaClientStub(42));

        Node page = graphImporter.addPage("http://mydomain.com/", "random content");

        assertFalse(PageNode.hasFacebookTotalCountProperty(page));
        assertEquals(-1, PageNode.getFacebookTotalCount(page));

        assertTrue(PageNode.hasTwitterCountProperty(page));
        assertEquals(42, PageNode.getTwitterCount(page));

        assertFalse(handler.shouldIgnore(page, graphImporter));
    }
}
