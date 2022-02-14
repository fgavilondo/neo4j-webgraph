package org.neo4japps.webgraph.importer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.DeadlockDetectedException;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Utility class to execute arbitrary code wrapped in a Neo4j transaction. The template reduces the need for having to
 * write transaction boiler plate code every time we need to modify the graph. This class also deals with possible
 * deadlocks.
 */
public class GraphTransactionTemplate {

    private static final int DEFAULT_SLEEP_MILLIS = 1000;
    private static final int DEFAULT_MAX_RETRIES = 10;

    private final Logger logger = LogManager.getLogger(this.getClass());

    private final AtomicInteger retriedTransactions = new AtomicInteger();
    private final AtomicInteger failedTransactions = new AtomicInteger();

    private final Object owner;

    public GraphTransactionTemplate(Object owner) {
        this.owner = owner;
    }

    public int getNumberOfRetriedTransactions() {
        return retriedTransactions.get();
    }

    public int getNumberOfFailedTransactions() {
        return failedTransactions.get();
    }

    /**
     * Execute the given task wrapped in a Neo4j transaction. In the case of deadlock retry the transaction up to 10
     * times with 1 second pauses in-between.
     *
     * @param task          the task to execute
     * @param graphImporter the graph importer used to create the transaction
     * @return whatever the task returns
     * @throws Exception if unable to compute a result
     */
    public Object execute(Callable<Object> task, GraphImporter graphImporter) throws Exception {
        return execute(task, graphImporter, DEFAULT_MAX_RETRIES, DEFAULT_SLEEP_MILLIS);
    }

    /**
     * Execute the given task wrapped in a Neo4j transaction. In the case of deadlock retry the transaction up to
     * 'maxRetries' times with 'sleepMillis' long pauses in-between.
     *
     * @param task          the task to execute
     * @param graphImporter the graph importer used to create the transaction
     * @param maxRetries    max retries
     * @param sleepMillis   time to sleep between retries
     * @return whatever the task returns
     * @throws Exception if unable to compute a result
     */
    public Object execute(Callable<Object> task, GraphImporter graphImporter, int maxRetries, int sleepMillis)
            throws Exception {

        int retryCounter = 0;
        Object result = null;
        boolean transactionSucceeded = false;

        while (!transactionSucceeded && retryCounter <= maxRetries) {
            try {
                result = executeInTransaction(task, graphImporter);
                transactionSucceeded = true;
            } catch (DeadlockDetectedException exception) {
                retryCounter++;
                if (retryCounter <= maxRetries) {
                    retriedTransactions.incrementAndGet();
                    logger.warn(ownerString() + "Deadlock executing task " + task.toString() + " - Retrying ... "
                            + retryCounter);
                    sleep(sleepMillis);
                } else {
                    failedTransactions.incrementAndGet();
                    logger.error(
                            ownerString() + "Too many deadlocks executing task " + task.toString() + " - Giving up.");
                }
            }
        }

        return result;
    }

    private String ownerString() {
        return "(" + owner.toString() + ") ";
    }

    private Object executeInTransaction(Callable<Object> task, GraphImporter graphImporter) throws Exception {
        Transaction tx = graphImporter.beginDbTransaction();
        try {
            Object result = task.call();
            tx.success();
            return result;
        } finally {
            tx.finish();
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            logger.warn(ownerString() + "sleep() interrupted unexpectedly", e);
            // Restore the interrupted status
            Thread.currentThread().interrupt();
        }
    }
}
