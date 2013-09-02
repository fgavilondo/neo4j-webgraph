package org.neo4j.unsafe.batchinsert;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Work-around to be able to get hold of a batch DB's inserter which we need to create node indices.
 */
public final class BatchDbWorkAround {

    public static BatchInserterIndexProvider createLuceneBatchInserterIndexProvider(GraphDatabaseService batchDb) {
        // we can only call this from the same package
        BatchInserter inserter = ((BatchGraphDatabaseImpl) batchDb).getBatchInserter();

        return new LuceneBatchInserterIndexProvider(inserter);
    }
}
