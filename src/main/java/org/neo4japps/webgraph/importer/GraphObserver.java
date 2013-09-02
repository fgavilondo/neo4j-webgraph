package org.neo4japps.webgraph.importer;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.Node;
import org.neo4japps.webgraph.util.ListChunker;

/**
 * Observers can be invoked by multiple threads concurrently so they must be thread-safe, and ideally stateless.
 * 
 * If an observer is intended to be used in conjunction with a {@link TransactionalGraphImporter} you can extend
 * {@link TransactionCapableGraphObserver} which provides transactional scaffolding.
 * 
 * If an observer is intended to be used in conjunction with a {@link BatchGraphImporter} and multiple crawler threads
 * care must be taken to use the lock provided by {@link GraphImporter#getLock()} when accessing and modifying the graph
 * in your observer.
 */
public abstract class GraphObserver {

    protected final Logger logger = Logger.getLogger(this.getClass());

    private int reportFrequency = 100;

    private final AtomicBoolean isShutdown = new AtomicBoolean();

    private final AtomicInteger numberOfReceivedEvents = new AtomicInteger();
    private final AtomicInteger numberOfNotifiedPages = new AtomicInteger();
    private final AtomicInteger numberOfUpdatedPages = new AtomicInteger();
    private final AtomicInteger numberOfIgnoredPages = new AtomicInteger();
    private final AtomicInteger numberOfFailedUpdates = new AtomicInteger();
    private final AtomicInteger numberOfPagesPendingProcessing = new AtomicInteger();

    private final GraphTransactionTemplate transactionTemplate = new GraphTransactionTemplate(this);

    private boolean useTransactions = false;

    public int getNumberOfReceivedEvents() {
        return numberOfReceivedEvents.get();
    }

    public int getNumberOfNotifiedPageNodes() {
        return numberOfNotifiedPages.get();
    }

    public final int getNumberOfUpdatedPageNodes() {
        return numberOfUpdatedPages.get();
    }

    public final int getNumberOfIgnoredPageNodes() {
        return numberOfIgnoredPages.get();
    }

    public final int getNumberOfFailedUpdates() {
        return numberOfFailedUpdates.get();
    }

    public final int getNumberOfPageNodesPendingProcessing() {
        return numberOfPagesPendingProcessing.get();
    }

    protected final void decrementNumberOfPageNodesPendingProcessing() {
        numberOfPagesPendingProcessing.decrementAndGet();
    }

    protected final int incrementNumberOfIgnoredPageNodes() {
        return numberOfIgnoredPages.incrementAndGet();
    }

    protected final int incrementNumberOfUpdatedPages() {
        return numberOfUpdatedPages.incrementAndGet();
    }

    protected final int incrementNumberOfFailedUpdates() {
        return numberOfFailedUpdates.incrementAndGet();
    }

    public final int getNumberOfRetriedTransactions() {
        return transactionTemplate.getNumberOfRetriedTransactions();
    }

    public final int getNumberOfFailedTransactions() {
        return transactionTemplate.getNumberOfFailedTransactions();
    }

    public void configure(ApplicationConfiguration config) {
        this.useTransactions = config.isUseTransactions();
        this.reportFrequency = config.getImportProgressReportFrequency();
    }

    public void shutdown() {
        isShutdown.set(true);
    }

    public final void update(ConcurrentObservable source, PageNodesModificationEvent event) {
        numberOfReceivedEvents.incrementAndGet();

        if (event.getPages().isEmpty()) {
            logger.warn("Event contains no page nodes " + event);
            return;
        }

        numberOfNotifiedPages.addAndGet(event.getPages().size());

        if (isShutdown.get()) {
            // ignore all events after shutdown
            numberOfIgnoredPages.addAndGet(event.getPages().size());
            return;
        }

        numberOfPagesPendingProcessing.addAndGet(event.getPages().size());

        try {
            doUpdate((GraphImporter) source, event);
        } catch (Exception e) {
            logger.warn("Error processing " + event.toString(), e);
        }
    }

    private Object doUpdate(GraphImporter graphImporter, PageNodesModificationEvent event) throws Exception {

        if (useTransactions) {
            doUpdateUsingTransactions(graphImporter, event);
        } else {
            doUpdateWithoutTransaction(graphImporter, event);
        }

        return null;
    }

    private void doUpdateWithoutTransaction(GraphImporter graphImporter, PageNodesModificationEvent event) {
        final List<Node> pages = event.getPages();
        for (Node page : pages) {
            updateSinglePageWithoutTransaction(graphImporter, page);
        }
    }

    private Object updateSinglePageWithoutTransaction(GraphImporter graphImporter, Node page) {
        decrementNumberOfPageNodesPendingProcessing();

        if (shouldIgnore(page, graphImporter)) {
            int counter = incrementNumberOfIgnoredPageNodes();
            reportProgress(counter, "ignored");
            return null;
        }

        try {
            Node updatedPage = updatePage(page, graphImporter);
            int counter = incrementNumberOfUpdatedPages();
            reportProgress(counter, "updated");
            return updatedPage;
        } catch (Exception e) {
            logger.warn("Failure updating page node " + PageNode.getUrl(page), e);
            incrementNumberOfFailedUpdates();
            return null;
        }
    }

    private void doUpdateUsingTransactions(GraphImporter graphImporter, PageNodesModificationEvent event)
            throws Exception {

        final List<Node> pages = event.getPages();
        final int transactionSize = getTransactionSize();
        if (transactionSize == 1) {
            // Shortcut: no need for chunking
            for (Node page : pages) {
                updateSinglePageInTransaction(page, graphImporter);
            }
        } else {
            ListChunker<Node> chunker = new ListChunker<Node>(pages, transactionSize);
            while (chunker.hasMore()) {
                updateChunkInTransaction(chunker.getNextChunk(), graphImporter);
            }
        }
    }

    protected Object updateSinglePageInTransaction(final Node page, final GraphImporter graphImporter) throws Exception {
        decrementNumberOfPageNodesPendingProcessing();

        if (shouldIgnore(page, graphImporter)) {
            int counter = incrementNumberOfIgnoredPageNodes();
            reportProgress(counter, "ignored");
            return null;
        }

        Callable<Object> task = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                try {
                    Node updatedPage = updatePage(page, graphImporter);
                    int counter = incrementNumberOfUpdatedPages();
                    reportProgress(counter, "updated");
                    return updatedPage;
                } catch (Exception e) {
                    logger.warn("Failure updating page " + PageNode.getUrl(page), e);
                    incrementNumberOfFailedUpdates();
                    return null;
                }
            }
        };

        return transactionTemplate.execute(task, graphImporter);
    }

    protected Object updateChunkInTransaction(final List<Node> chunk, final GraphImporter graphImporter)
            throws Exception {

        Callable<Object> task = new Callable<Object>() {
            @Override
            public Object call() throws Exception {
                for (Node page : chunk) {
                    decrementNumberOfPageNodesPendingProcessing();

                    if (shouldIgnore(page, graphImporter)) {
                        int counter = incrementNumberOfIgnoredPageNodes();
                        reportProgress(counter, "ignored");
                        continue;
                    }

                    try {
                        updatePage(page, graphImporter);
                        int counter = incrementNumberOfUpdatedPages();
                        reportProgress(counter, "updated");
                    } catch (Exception e) {
                        logger.warn("Failure updating page node " + PageNode.getUrl(page), e);
                        incrementNumberOfFailedUpdates();
                    }
                }

                return null;
            }
        };

        // increase deadlock retry timeout as this is potentially a big transaction
        return transactionTemplate.execute(task, graphImporter, 10, 5000);
    }

    private void reportProgress(int numberOfPages, String action) {
        if (numberOfPages % reportFrequency != 0) {
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info(numberOfPages + " page nodes " + action + ".");
        }
    }

    protected abstract int getTransactionSize();

    protected abstract boolean shouldIgnore(Node page, GraphImporter graphImporter);

    protected abstract Node updatePage(Node page, GraphImporter graphImporter) throws Exception;
}
