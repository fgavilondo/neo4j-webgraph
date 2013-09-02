package org.neo4japps.webgraph.importer;

import junit.framework.TestCase;

public class AppTest extends TestCase {

    public void testAppDoesNotRunWithInvalidConfiguration() throws Exception {
        App app = new App(new String[] { "-c", "0" }, true);
        app.run();
        assertFalse(app.isSuccessfulImport());

        app = new App(new String[] { "c" }, true);
        app.run();
        assertFalse(app.isSuccessfulImport());
    }

    public void testAppDoesNotRunWhenHelpRequested() throws Exception {
        App app = new App(new String[] { "-h", }, true);
        app.run();
        assertFalse(app.isSuccessfulImport());
    }
}
