package org.neo4japps.webgraph.importer;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.neo4japps.webgraph.util.DirectoryUtil;

public class BatchGraphImporterTest extends GraphImporterTestBase {

    private static AbstractObservableGraphImporter importer;

    @Override
    protected AbstractObservableGraphImporter getImporter() {
        return importer;
    }

    @BeforeClass
    public static void initImporter() throws Exception {
        deleteDbDir("initImporter()");

        createGraphImporter();
        assertThatImporterIsCorrectlyInitialized(importer);

        importRootPage();
        assertRootPageIsCreatedCorrectly(importer);
    }

    private static void deleteDbDir(String callingMethod) throws IOException {
        final int maxRetries = 3;

        boolean deleted = false;
        int counter = 0;

        while (!deleted) {
            try {
                DirectoryUtil.deleteDir(ApplicationConfiguration.DEFAULT_DB_DIR_LOCATION);
                deleted = true;
            } catch (IOException e) {
                // we're having trouble deleting Lucene index files on Windows sometimes
                if (counter < maxRetries) {
                    counter++;
                    System.err.println("BatchGraphImporterTest." + callingMethod + ": failed to delete "
                            + ApplicationConfiguration.DEFAULT_DB_DIR_LOCATION + ". Retrying...");
                    sleep(1000);
                } else {
                    throw e;
                }
            }
        }
    }

    private static void sleep(int millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignore) {
        }
    }

    private static void createGraphImporter() {
        importer = new BatchGraphImporter(ApplicationConfiguration.DEFAULT_DB_DIR_LOCATION, ROOT_URL,
                System.currentTimeMillis(), 100, 1);
    }

    private static void importRootPage() {
        importer.addPage(ROOT_URL, "root content");
    }

    @AfterClass
    public static void shutdownImporter() {
        if (importer != null) {
            importer.shutdown();
        }
    }
}
