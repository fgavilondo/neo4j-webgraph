package org.neo4japps.webgraph.importer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

public abstract class GraphImporterTestBase {

    static class TestGraphObserver extends GraphObserver {
        volatile String url = "";
        volatile String content = "";

        @Override
        protected int getTransactionSize() {
            return 1;
        }

        @Override
        protected boolean shouldIgnore(Node page, GraphImporter graphImporter) {
            return false;
        }

        @Override
        protected Node updatePage(Node page, GraphImporter graphImporter) throws Exception {
            url = PageNode.getUrl(page);
            content = PageNode.getContent(page);
            // System.out.println("Update (" + eventCounter + "): " + url);
            return page;
        }
    }

    protected static final String ROOT_URL = "http://homepage.com/";

    protected static final void assertThatImporterIsCorrectlyInitialized(AbstractObservableGraphImporter theImporter) {
        assertNull(theImporter.getRootPage());
        assertEquals(0, theImporter.getNumberOfPageNodes());
        assertEquals(0, theImporter.getNumberOfPagesOfType(PageNode.HOME_PAGE));
        assertEquals(0, theImporter.getNumberOfPagesOfType(PageNode.LEAF_PAGE));
        assertEquals(0, theImporter.getNumberOfPagesForDomain("homepage"));
        assertEquals(0, theImporter.countObservers());
    }

    protected static final void assertRootPageIsCreatedCorrectly(AbstractObservableGraphImporter theImporter) {
        Node rootPage = theImporter.getRootPage();

        assertNotNull(rootPage);
        assertEquals(-1, PageNode.getFacebookTotalCount(rootPage));
        assertEquals(-1, PageNode.getTwitterCount(rootPage));
        assertEquals(0, PageNode.getNumberOfIncomingLinks(rootPage));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(rootPage));
        assertEquals(ROOT_URL, PageNode.getUrl(rootPage));
        assertEquals("homepage", PageNode.getDomain(rootPage));
        assertEquals("root content", PageNode.getContent(rootPage));
        assertEquals(PageNode.HOME_PAGE, PageNode.getType(rootPage));

        assertEquals(1, theImporter.getNumberOfPageNodes());
        assertEquals(1, theImporter.getNumberOfPagesOfType(PageNode.HOME_PAGE));
        assertEquals(0, theImporter.getNumberOfPagesOfType(PageNode.LEAF_PAGE));
        assertEquals(1, theImporter.getNumberOfPagesForDomain("homepage"));
    }

    protected abstract AbstractObservableGraphImporter getImporter();

    @Test
    public void testThatSameUrlCannotBeImportedTwiceButIsUpdated() {
        final int initialNumberOfPageNodes = getImporter().getNumberOfPageNodes();

        Node page = getImporter().addPage("http://sub1.homepage.com/somepage", "");
        assertEquals(1 + initialNumberOfPageNodes, getImporter().getNumberOfPageNodes());
        assertEquals("", PageNode.getContent(page));

        page = getImporter().addPage("http://sub1.homepage.com/somepage", "updated content");
        assertEquals(1 + initialNumberOfPageNodes, getImporter().getNumberOfPageNodes());
        assertEquals("updated content", PageNode.getContent(page));
    }

    @Test
    public void testThatNodesAreCreatedCorrectly() {
        final int initialNumberOfPageNodes = getImporter().getNumberOfPageNodes();
        final int initialNumberOfHomePages = getImporter().getNumberOfPagesOfType(PageNode.HOME_PAGE);
        final int initialNumberOfLeafPages = getImporter().getNumberOfPagesOfType(PageNode.LEAF_PAGE);

        Node page1 = getImporter().addPage("http://sub2.homepage.com/", "content1");

        assertEquals("sub2", PageNode.getDomain(page1));
        assertEquals("http://sub2.homepage.com/", PageNode.getUrl(page1));
        assertEquals("content1", PageNode.getContent(page1));
        assertEquals(PageNode.HOME_PAGE, PageNode.getType(page1));
        assertEquals(-1, PageNode.getFacebookTotalCount(page1));
        assertEquals(-1, PageNode.getTwitterCount(page1));
        assertEquals(0, PageNode.getNumberOfIncomingLinks(page1));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(page1));
        assertEquals(1 + initialNumberOfPageNodes, getImporter().getNumberOfPageNodes());
        assertEquals(1 + initialNumberOfHomePages, getImporter().getNumberOfPagesOfType(PageNode.HOME_PAGE));
        assertEquals(0 + initialNumberOfLeafPages, getImporter().getNumberOfPagesOfType(PageNode.LEAF_PAGE));
        assertEquals(1, getImporter().getNumberOfPagesForDomain("sub2"));

        Node page2 = getImporter().addPage("http://sub2.homepage.com/page2", "content2");

        assertEquals("sub2", PageNode.getDomain(page2));
        assertEquals("http://sub2.homepage.com/page2", PageNode.getUrl(page2));
        assertEquals("content2", PageNode.getContent(page2));
        assertEquals(PageNode.LEAF_PAGE, PageNode.getType(page2));
        assertEquals(-1, PageNode.getFacebookTotalCount(page2));
        assertEquals(-1, PageNode.getTwitterCount(page2));
        assertEquals(0, PageNode.getNumberOfIncomingLinks(page2));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(page2));
        assertEquals(2 + initialNumberOfPageNodes, getImporter().getNumberOfPageNodes());
        assertEquals(1 + initialNumberOfHomePages, getImporter().getNumberOfPagesOfType(PageNode.HOME_PAGE));
        assertEquals(1 + initialNumberOfLeafPages, getImporter().getNumberOfPagesOfType(PageNode.LEAF_PAGE));
        assertEquals(2, getImporter().getNumberOfPagesForDomain("sub2"));
    }

    @Test
    public void testThatLinksAreCreatedCorrectly() {
        final int initialNumberOfPageNodes = getImporter().getNumberOfPageNodes();

        Node rootPage = getImporter().getRootPage();

        Relationship link = getImporter().addLinks(rootPage,
                Collections.singletonList("http://sub3.homepage.com/linked1")).get(0);
        assertNotNull(link);
        assertEquals(1 + initialNumberOfPageNodes, getImporter().getNumberOfPageNodes());
        assertEquals(rootPage, link.getStartNode());
        Node linkedPage1 = link.getEndNode();
        assertEquals("http://sub3.homepage.com/linked1", PageNode.getUrl(linkedPage1));
        assertEquals(PageNode.UNKNOWN_PAGE_CONTENT, PageNode.getContent(linkedPage1));
        assertEquals(0, PageNode.getNumberOfIncomingLinks(rootPage));
        assertEquals(1, PageNode.getNumberOfOutgoingLinks(rootPage));
        assertEquals(1, PageNode.getNumberOfIncomingLinks(linkedPage1));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(linkedPage1));

        link = getImporter().addLinks(rootPage, Collections.singletonList("http://sub3.homepage.com/linked2")).get(0);
        assertNotNull(link);
        assertEquals(rootPage, link.getStartNode());
        assertEquals(2 + initialNumberOfPageNodes, getImporter().getNumberOfPageNodes());
        assertEquals(0, PageNode.getNumberOfIncomingLinks(rootPage));
        assertEquals(2, PageNode.getNumberOfOutgoingLinks(rootPage));

        Node linkedPage2 = getImporter().getPage("http://sub3.homepage.com/linked2");
        assertEquals(1, PageNode.getNumberOfIncomingLinks(linkedPage2));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(linkedPage2));
        assertEquals(PageNode.UNKNOWN_PAGE_CONTENT, PageNode.getContent(linkedPage2));
        // check that first linked page wasn't affected by the new link
        assertEquals(1, PageNode.getNumberOfIncomingLinks(linkedPage1));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(linkedPage1));

        // re-adding a link that already exists shouldn't change anything
        link = getImporter().addLinks(rootPage, Collections.singletonList("http://sub3.homepage.com/linked2")).get(0);
        assertNotNull(link);
        assertEquals(rootPage, link.getStartNode());
        assertEquals(linkedPage2, link.getEndNode());
        assertEquals(0, PageNode.getNumberOfIncomingLinks(rootPage));
        assertEquals(2, PageNode.getNumberOfOutgoingLinks(rootPage));
        assertEquals(1, PageNode.getNumberOfIncomingLinks(linkedPage1));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(linkedPage1));
        assertEquals(1, PageNode.getNumberOfIncomingLinks(linkedPage2));
        assertEquals(0, PageNode.getNumberOfOutgoingLinks(linkedPage2));

        // re-adding a page as a link shouldn't modify the previously imported page
        link = getImporter().addLinks(linkedPage1, Collections.singletonList("http://homepage.com/")).get(0);
        // now page 2 is linking back to the root page

        assertEquals(ROOT_URL, PageNode.getUrl(rootPage));
        assertEquals("homepage", PageNode.getDomain(rootPage));
        assertEquals("root content", PageNode.getContent(rootPage));
        assertEquals(PageNode.HOME_PAGE, PageNode.getType(rootPage));
        assertEquals(1, PageNode.getNumberOfIncomingLinks(rootPage));
        assertEquals(2, PageNode.getNumberOfOutgoingLinks(rootPage));
        assertEquals(1, PageNode.getNumberOfIncomingLinks(linkedPage1));
        assertEquals(1, PageNode.getNumberOfOutgoingLinks(linkedPage1));

        // and finally, adding a url that was previously added as link as a proper page should update the page content
        Node linkedPage1AddedAsPage = getImporter().addPage("http://sub3.homepage.com/linked1", "the content");
        assertEquals("the content", PageNode.getContent(linkedPage1AddedAsPage));
        assertEquals(PageNode.getUrl(linkedPage1), PageNode.getUrl(linkedPage1AddedAsPage));
    }

    @Test
    public void testThatEventHandlersAreInvokedCorrectly() throws InterruptedException {
        TestGraphObserver testObserver = new TestGraphObserver();
        getImporter().addObserver(testObserver);

        assertEquals(0, testObserver.getNumberOfUpdatedPageNodes());
        assertEquals("", testObserver.url);
        assertEquals("", testObserver.content);

        Node page = getImporter().addPage("http://sub4.homepage.com/", "content1");
        getImporter().waitForImportToFinish();

        assertEquals(1, testObserver.getNumberOfUpdatedPageNodes());
        assertEquals("http://sub4.homepage.com/", testObserver.url);
        assertEquals("content1", testObserver.content);

        // updating the page should broadcast another event
        page = getImporter().addPage("http://sub4.homepage.com/", "content1-updated");
        getImporter().waitForImportToFinish();
        assertEquals(2, testObserver.getNumberOfUpdatedPageNodes());
        assertEquals("http://sub4.homepage.com/", testObserver.url);
        assertEquals("content1-updated", testObserver.content);

        getImporter().addLinks(page, Collections.singletonList("http://sub4.homepage.com/page5"));
        getImporter().waitForImportToFinish();
        assertEquals(3, testObserver.getNumberOfUpdatedPageNodes());
        assertEquals("http://sub4.homepage.com/page5", testObserver.url);
        assertEquals(PageNode.UNKNOWN_PAGE_CONTENT, testObserver.content);

        getImporter().addLinks(page, createUrlList(2));
        getImporter().waitForImportToFinish();
        assertEquals(5, testObserver.getNumberOfUpdatedPageNodes());

        page = getImporter().addPage("http://sub4.homepage.com/page6", "content6");
        getImporter().waitForImportToFinish();
        assertEquals(6, testObserver.getNumberOfUpdatedPageNodes());
        assertEquals("http://sub4.homepage.com/page6", testObserver.url);
        assertEquals("content6", testObserver.content);
    }

    protected final List<String> createUrlList(int size) {
        final List<String> list = new ArrayList<String>(size);
        for (int i = 0; i < size; i++) {
            list.add("http://listsubdomain" + i + ".homepage.com/");
        }
        return list;
    }
}
