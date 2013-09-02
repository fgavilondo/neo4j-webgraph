package org.neo4japps.webgraph.importer;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchDbWorkAround;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4japps.webgraph.util.ListChunker;
import org.neo4japps.webgraph.util.UrlUtil;

/**
 * This importer drops support for transactions and concurrency in favor of insertion speed. Only one thread at a time
 * may work against the underlying batch database/inserter.
 * 
 * Note that if if the JVM/machine crashes or we fail to invoke {@link GraphDatabaseService.shutdown()} before the JVM
 * exits the Neo4j store can be considered being in an inconsistent state and the insertion has to be re-done from
 * scratch.
 */
public class BatchGraphImporter extends AbstractObservableGraphImporter {
    class EventQueueConsumer implements Runnable {
        private final AtomicBoolean isProcessing;
        final String name;

        public EventQueueConsumer(String name) {
            this.isProcessing = new AtomicBoolean();
            this.name = name;
        }

        @Override
        public void run() {
            logger.info(name + " started");
            try {
                while (true) {
                    if (isStopped.get()) {
                        break;
                    }
                    consume(eventQueue.take());
                }
            } catch (InterruptedException e) {
                logger.warn(name, e);
                Thread.currentThread().interrupt();
            }
            logger.info(name + " stopping");
        }

        void consume(PageNodesModificationEvent event) {
            isProcessing.set(true);
            try {
                notifyObservers(event);
            } catch (Exception e) {
                // Make sure to catch all exceptions to prevent event handler code
                // to kill the consumer thread.
                // Handlers themselves should attempt to handle all exceptions in
                // their update() method though.
                // Otherwise a misbehaving handler would prevent subsequent handlers
                // from executing.
                logger.warn(name + " error handling event " + event.toString(), e);
            } finally {
                isProcessing.set(false);
            }
        }

        public boolean isProcessingEvent() {
            return isProcessing.get();
        }
    }

    private final Lock graphMutex = new ReentrantLock();

    // private final BatchInserter inserter;
    private final GraphDatabaseService graphDb;

    private final BatchInserterIndexProvider indexProvider;
    private final BatchInserterIndex pageIndex;
    private final BatchInserterIndex categoryIndex;

    final BlockingQueue<PageNodesModificationEvent> eventQueue;
    final List<EventQueueConsumer> eventConsumers;

    public BatchGraphImporter(String storeDir, String rootUrl, long startTimeInMillis,
            int importProgressReportFrequency, int numberOfEventHandlerThreads) {
        super(rootUrl, startTimeInMillis, importProgressReportFrequency);

        // TODO Maybe use inserter for better performance
        // inserter = BatchInserters.inserter(storeDir);
        graphDb = BatchInserters.batchDatabase(storeDir);

        // Need this workaround because we are using a batch db rather than a batch inserter
        indexProvider = BatchDbWorkAround.createLuceneBatchInserterIndexProvider(graphDb);

        pageIndex = indexProvider.nodeIndex(PAGE_INDEX_NAME, MapUtil.stringMap("type", "exact"));
        // This will slow down inserts but we need to cache pages by URL to use in the getPage() method
        // so that we don't create duplicate nodes with the same URL
        // TODO Make cache capacity configurable, but the value below should be sufficient for most graphs
        pageIndex.setCacheCapacity(PageNode.URL_KEY, 150000);

        // this index will only contain a couple of entries
        categoryIndex = indexProvider.nodeIndex(CATEGORY_INDEX_NAME, MapUtil.stringMap("type", "exact"));

        // keep the queue size manageable, don't let the crawling get too far ahead of the importing
        int eventQueueCapacity = numberOfEventHandlerThreads / 5;
        if (eventQueueCapacity < 5) {
            eventQueueCapacity = 5;
        }
        eventQueue = new ArrayBlockingQueue<PageNodesModificationEvent>(eventQueueCapacity);

        eventConsumers = Collections.synchronizedList(new ArrayList<EventQueueConsumer>());

        for (int i = 1; i <= numberOfEventHandlerThreads; i++) {
            final String name = EventQueueConsumer.class.getSimpleName() + "-" + i;
            EventQueueConsumer consumer = new EventQueueConsumer(name);
            eventConsumers.add(consumer);
            Thread thread = new Thread(consumer, name);
            thread.setDaemon(true);
            thread.start();
        }
    }

    @Override
    public Lock getLock() {
        return graphMutex;
    }

    @Override
    public Transaction beginDbTransaction() {
        return graphDb.beginTx();
    }

    @Override
    public Node createNode() {
        return graphDb.createNode();
    }

    @Override
    public Node addPage(String url, String content) {
        if (isStopped.get())
            return null;

        Node page = addPageWithoutBroadcasting(url, content);

        broadcastModification(page);

        return page;
    }

    private Node addPageWithoutBroadcasting(String url, String content) {
        if (isStopped.get())
            return null;

        String domain = null;
        String type = null;

        try {
            URL urlObject = new URL(url);
            domain = UrlUtil.extractDomain(urlObject);
            type = UrlUtil.isHomePage(urlObject) ? PageNode.HOME_PAGE : PageNode.LEAF_PAGE;
        } catch (MalformedURLException e) {
            logger.warn("Ignoring malformed URL " + url, e);
            return null;
        }

        return addOrModifyPage(url, content, domain, type);
    }

    private Node addOrModifyPage(String url, String content, String domain, String type) {
        Node page = null;

        graphMutex.lock();
        try {

            page = getPage(url);
            if (page == null) {
                page = addNewPage(url, content, domain, type);
            } else {
                // Sometimes pages are parsed after the corresponding node has been
                // created, e.g. when the node was created as a link from another page earlier on.
                // In that case we need to update the page content because we did
                // not have it earlier.
                updatePageContent(page, content);
            }

        } finally {
            graphMutex.unlock();
        }

        reportProgress();

        return page;
    }

    private Node addNewPage(String url, String content, String domain, String type) {
        Node page = createNode();

        populateNewPageNode(page, url, domain, type, content);
        updatePageIndex(page);

        numberOfPageNodes.incrementAndGet();

        if (logger.isDebugEnabled()) {
            logger.trace("Created page node: " + PageNode.toString(page));
        }

        return page;
    }

    @Override
    protected void updatePageIndex(Node page) {
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(PageNode.URL_KEY, PageNode.getUrl(page));
        properties.put(PageNode.DOMAIN_KEY, PageNode.getDomain(page));
        properties.put(PageNode.TYPE_KEY, PageNode.getType(page));
        pageIndex.add(page.getId(), properties);
    }

    @Override
    public void addCategoryNodeToIndex(Node node) {
        final Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("name", node.getProperty("category"));
        categoryIndex.add(node.getId(), properties);
    }

    @Override
    protected void notifyObserversAndHandleExceptions(PageNodesModificationEvent event) {
        // TODO should chunk size be configurable/calculated?
        final int chunkSize = 20;
        List<PageNodesModificationEvent> chunks = chunkUp(event, chunkSize);

        try {
            for (PageNodesModificationEvent chunk : chunks) {
                if (eventQueue.remainingCapacity() == 0) {
                    logger.trace("Event queue is full. Waiting for importer threads to become available.");
                }
                eventQueue.put(chunk);
            }
        } catch (InterruptedException e) {
            logger.warn(e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Chunk up the list of pages in broadcasted events so that we can parallelize better, i.e 5 consumer threads can
     * process 20 pages each rather than 1 thread processing all 100 pages
     */
    private List<PageNodesModificationEvent> chunkUp(PageNodesModificationEvent event, int chunkSize) {
        List<PageNodesModificationEvent> events = new ArrayList<PageNodesModificationEvent>();

        ListChunker<Node> chunker = new ListChunker<Node>(event.getPages(), chunkSize);
        while (chunker.hasMore()) {
            final List<Node> chunk = chunker.getNextChunk();
            events.add(new PageNodesModificationEvent(event.getSource(), chunk));
        }

        return events;
    }

    @Override
    public List<Relationship> addLinks(Node fromPage, List<String> toUrls) {
        final List<Relationship> links = new ArrayList<Relationship>();

        if (isStopped.get()) {
            return links;
        }

        if (toUrls == null || toUrls.isEmpty()) {
            return links;
        }

        final List<Node> nodes = new ArrayList<Node>();

        graphMutex.lock();
        try {
            for (String toUrl : toUrls) {
                Node linkedPage = getPage(toUrl);
                if (linkedPage == null) {
                    linkedPage = addPageWithoutBroadcasting(toUrl, PageNode.UNKNOWN_PAGE_CONTENT);
                    if (linkedPage != null) {
                        nodes.add(linkedPage);
                    }
                }
                final Relationship link = addLink(fromPage, linkedPage);
                if (link != null) {
                    links.add(link);
                }
            }
        } finally {
            graphMutex.unlock();
        }

        broadcastModifications(nodes);

        return links;
    }

    @Override
    public Node getReferenceNode() {
        graphMutex.lock();
        try {
            return graphDb.getReferenceNode();
        } finally {
            graphMutex.unlock();
        }
    }

    @Override
    public Node getRootPage() {
        graphMutex.lock();
        try {
            Relationship rel = graphDb.getReferenceNode().getSingleRelationship(RelTypes.ROOT_PAGE_REFERENCE,
                    Direction.OUTGOING);
            return rel == null ? null : rel.getEndNode();
        } finally {
            graphMutex.unlock();
        }
    }

    @Override
    public Node getPage(String url) {
        graphMutex.lock();
        try {
            Long id = getNodeId(url);
            return (id == null) ? null : graphDb.getNodeById(id);
        } finally {
            graphMutex.unlock();
        }
    }

    private Long getNodeId(String url) {
        IndexHits<Long> indexHits = pageIndex.get(PageNode.URL_KEY, url);
        return indexHits.getSingle();
    }

    @Override
    public Iterator<Node> getAllPagesForDomain(String domain) {
        graphMutex.lock();
        try {
            makePageIndexChangesVisibleForReading();
            IndexHits<Long> ids = pageIndex.get(PageNode.DOMAIN_KEY, domain);
            return getNodesByIds(ids).iterator();
        } finally {
            graphMutex.unlock();
        }
    }

    @Override
    public int getNumberOfPagesForDomain(String domain) {
        graphMutex.lock();
        try {
            makePageIndexChangesVisibleForReading();
            return pageIndex.get(PageNode.DOMAIN_KEY, domain).size();
        } finally {
            graphMutex.unlock();
        }
    }

    @Override
    public Iterator<Node> getAllPagesOfType(String type) {
        graphMutex.lock();
        try {
            makePageIndexChangesVisibleForReading();
            IndexHits<Long> ids = pageIndex.get(PageNode.TYPE_KEY, type);
            return getNodesByIds(ids).iterator();
        } finally {
            graphMutex.unlock();
        }
    }

    @Override
    public int getNumberOfPagesOfType(String type) {
        graphMutex.lock();
        try {
            makePageIndexChangesVisibleForReading();
            return pageIndex.get(PageNode.TYPE_KEY, type).size();
        } finally {
            graphMutex.unlock();
        }
    }

    private void makePageIndexChangesVisibleForReading() {
        pageIndex.flush();
    }

    private List<Node> getNodesByIds(IndexHits<Long> ids) {
        final List<Node> nodes = new ArrayList<Node>(ids.size());
        for (Long id : ids) {
            nodes.add(graphDb.getNodeById(id));
        }
        return nodes;
    }

    @Override
    public void waitForImportToFinish() throws InterruptedException {
        while (eventQueueNotEmpty()) {
            Thread.sleep(500);
        }

        int activeConsumers = 0;

        while ((activeConsumers = getNumberOfActiveConsumers()) > 0) {
            String message = activeConsumers == 1 ? " event consumer thread is still processing "
                    : " event consumer threads are still processing ";
            logger.info(activeConsumers + message + getNumberOfUnprocessedPageNodesByObservers() + " page nodes");

            Thread.sleep(2000);
        }
    }

    private boolean eventQueueNotEmpty() {
        final int size = eventQueue.size();
        if (size > 0) {
            logger.info(size + " queued events remaining");
        }
        return size > 0;
    }

    private int getNumberOfActiveConsumers() {
        int activeConsumers = 0;

        for (EventQueueConsumer consumer : eventConsumers) {
            if (consumer.isProcessingEvent()) {
                activeConsumers++;
            }
        }

        return activeConsumers;
    }

    @Override
    public void stop() {
        super.stop();
        eventQueue.clear();
    }

    @Override
    public void shutdown() {
        graphMutex.lock();
        try {
            doShutdown();
        } finally {
            graphMutex.unlock();
        }
    }

    private void doShutdown() {
        logger.trace("Start importer shut down");
        stop();

        logger.trace("Flushing caches");
        pageIndex.flush();
        categoryIndex.flush();

        logger.trace("Shutting down index provider");
        indexProvider.shutdown();

        logger.trace("Shutting down database");
        // inserter.shutdown();
        graphDb.shutdown();

        logger.trace("Finished importer shut down");
    }
}
