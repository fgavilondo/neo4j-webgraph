package org.neo4japps.webgraph.importer;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.test.TestGraphDatabaseFactory;

public class TransactionalGraphImporterTest extends GraphImporterTestBase {
    private AbstractObservableGraphImporter importer;

    @Override
    protected AbstractObservableGraphImporter getImporter() {
        return importer;
    }

    @Before
    public void initImporter() {
        createGraphImporter();
        assertThatImporterIsCorrectlyInitialized(getImporter());

        importRootPage();
        assertRootPageIsCreatedCorrectly(getImporter());
    }

    private void createGraphImporter() {
        createGraphImporter(500, false);
    }

    private void createGraphImporter(int transactionSize, boolean eventHandlersExecuteTheirOwnTransactions) {
        importer = new TransactionalGraphImporter(new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
                .newGraphDatabase(), ROOT_URL, System.currentTimeMillis(), 100, transactionSize);
    }

    private Node importRootPage() {
        return getImporter().addPage(ROOT_URL, "root content");
    }

    @After
    public void shutdownImporter() {
        if (getImporter() != null) {
            getImporter().shutdown();
        }
    }

    @Test
    public void testLinksAreChunkedCorrectly() {
        getImporter().shutdown();
        final int transactionSize = 5;
        for (int numberOfLinks = 0; numberOfLinks <= transactionSize + 1; numberOfLinks++) {
            // System.out.println("testLinksAreChunkedCorrectly(" + numberOfLinks + ")");
            doTestLinksAreChunkedCorrectly(numberOfLinks, transactionSize);
        }
    }

    private void doTestLinksAreChunkedCorrectly(int numberOfLinks, int transactionSize) {
        try {
            createGraphImporter(transactionSize, true);
            Node rootPage = importRootPage();

            TestGraphObserver testObserver = new TestGraphObserver();
            getImporter().addObserver(testObserver);

            getImporter().addLinks(rootPage, createUrlList(numberOfLinks));

            if (numberOfLinks == 0) {
                assertEquals(0, testObserver.getNumberOfReceivedEvents());
            } else {
                assertEquals(1, testObserver.getNumberOfReceivedEvents());
            }
        } finally {
            getImporter().shutdown();
        }
    }
}
