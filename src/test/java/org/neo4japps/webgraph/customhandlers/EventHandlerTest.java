package org.neo4japps.webgraph.customhandlers;

import junit.framework.TestCase;

import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4japps.webgraph.importer.TransactionalGraphImporter;

public abstract class EventHandlerTest extends TestCase {

    protected TransactionalGraphImporter createImpermanentGraphImporter(String rootUrl, long startTimeInMillis,
        int importProgressReportFrequency, int transactionSize) {
        return new TransactionalGraphImporter(new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder()
            .newGraphDatabase(), rootUrl, startTimeInMillis, importProgressReportFrequency, transactionSize);
    }

}
