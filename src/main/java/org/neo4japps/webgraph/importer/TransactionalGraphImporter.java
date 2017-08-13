package org.neo4japps.webgraph.importer;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.UniqueFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4japps.webgraph.util.ListChunker;
import org.neo4japps.webgraph.util.UrlUtil;

/**
 * This importer uses transactions for graph modifications and can be accessed by multiple threads.
 */
public final class TransactionalGraphImporter extends AbstractObservableGraphImporter {

    private static class UniqueUrlNodeFactory extends UniqueFactory.UniqueNodeFactory {
        private static final String INDEX_NAME = PageNode.URL_KEY;

        public UniqueUrlNodeFactory(GraphDatabaseService graphDb) {
            // this creates an ancillary index called 'url' in addition to the 'pages' index.
            super(graphDb, INDEX_NAME);
        }

        @Override
        protected void initialize(Node created, Map<String, Object> properties) {
            PageNode.setUrl(created, (String) properties.get(INDEX_NAME));
        }

        public Node getOrCreate(String url) {
            return getOrCreate(INDEX_NAME, url);
        }
    }

    private final GraphTransactionTemplate transactionTemplate = new GraphTransactionTemplate(this);

    private final Lock nullLock = new Lock() {

        @Override
        public void lock() {
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
        }

        @Override
        public boolean tryLock() {
            return true;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            return true;
        }

        @Override
        public void unlock() {
        }

        @Override
        public Condition newCondition() {
            return null;
        }
    };

    private final GraphDatabaseService graphDb;
    private final Index<Node> pageIndex;
    private final Index<Node> categoryIndex;
    private final UniqueUrlNodeFactory nodeFactory;

    private final int transactionSize;

    /**
     * For unit testing
     */
    public static TransactionalGraphImporter createImpermanentInstance(String rootUrl, long startTimeInMillis,
            int importProgressReportFrequency, int transactionSize) {
        return new TransactionalGraphImporter(
                new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().newGraphDatabase(), rootUrl,
                startTimeInMillis, importProgressReportFrequency, transactionSize);
    }

    public TransactionalGraphImporter(GraphDatabaseService graphDb, String rootUrl, long startTimeInMillis,
            int importProgressReportFrequency, int transactionSize) {

        super(rootUrl, startTimeInMillis, importProgressReportFrequency);

        if (transactionSize < 1) {
            throw new IllegalArgumentException("Value for transactionSize must be greater than 0");
        }
        this.transactionSize = transactionSize;

        this.graphDb = graphDb;

        this.nodeFactory = new UniqueUrlNodeFactory(graphDb);
        this.pageIndex = graphDb.index().forNodes(PAGE_INDEX_NAME);
        this.categoryIndex = graphDb.index().forNodes(CATEGORY_INDEX_NAME);
    }

    public int getNumberOfRetriedTransactions() {
        return transactionTemplate.getNumberOfRetriedTransactions();
    }

    public int getNumberOfFailedTransactions() {
        return transactionTemplate.getNumberOfFailedTransactions();
    }

    @Override
    public Lock getLock() {
        return nullLock;
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

        Node page = addPageWithoutBroadcasting(url, content, true);

        // make sure to broadcast the event once the transaction has completed
        broadcastModification(page);

        return page;
    }

    Node addPageWithoutBroadcasting(String url, String content, boolean useTransaction) {
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

        Node node = null;
        if (useTransaction) {
            node = addOrModifyPageInTransaction(url, domain, type, content);
        } else {
            node = addOrModifyPage(url, domain, type, content);
        }

        return node;
    }

    private Node addOrModifyPageInTransaction(final String url, final String domain, final String type,
            final String content) {
        try {
            Callable<Object> task = new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    return addOrModifyPage(url, domain, type, content);
                }
            };
            return (Node) transactionTemplate.execute(task, this);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    Node addOrModifyPage(String url, String domain, String type, String content) {
        Node page = createPage(url);

        if (isNewlyCreatedPageNode(page)) {
            populateNewPageNode(page, url, domain, type, content);
            updatePageIndex(page);
            numberOfPageNodes.incrementAndGet();
            logger.trace("Created page node: " + PageNode.toString(page));
            reportProgress();
        } else {
            // Sometimes pages are parsed after the corresponding node has been
            // created, e.g.
            // when the node was created as a link from another page earlier on.
            // In that case we need to update the page content because we did
            // not have it earlier.
            updatePageContent(page, content);
        }

        return page;
    }

    private Node createPage(String url) {
        // By using a UniqueNodeFactory instead of graphDb.createNode() we
        // ensure that a node with the same URL does
        // not get created twice, even when multiple (crawler) threads are
        // modifying the graph at the same time.
        return nodeFactory.getOrCreate(url);
    }

    private boolean isNewlyCreatedPageNode(Node node) {
        // Default isolation level is READ_COMMITTED, so the graph keeps keeps
        // write locks on the node until the
        // end of the transaction. Threads reusing a node should never see a
        // half-constructed node.
        return !node.hasProperty(PageNode.DOMAIN_KEY);
    }

    @Override
    protected void updatePageIndex(Node page) {
        pageIndex.add(page, PageNode.URL_KEY, PageNode.getUrl(page));
        pageIndex.add(page, PageNode.DOMAIN_KEY, PageNode.getDomain(page));
        pageIndex.add(page, PageNode.TYPE_KEY, PageNode.getType(page));
    }

    @Override
    public void addCategoryNodeToIndex(Node node) {
        categoryIndex.add(node, "name", node.getProperty("category"));
    }

    @Override
    protected void notifyObserversAndHandleExceptions(PageNodesModificationEvent event) {
        try {
            notifyObservers(event);
        } catch (Exception e) {
            // Make sure to catch all exceptions to prevent event handler code
            // to kill the calling crawler thread.
            // Handlers themselves should attempt to handle all exceptions in
            // their update() method though.
            // Otherwise a misbehaving handler would prevent subsequent handlers
            // from executing.
            logger.warn("Error in event handling code for " + event.toString(), e);
        }
    }

    /**
     * Note that deadlocks are possible if 2 threads try to create the opposite links at the same time (A-->B, B-->A).
     * In that case this method will sleep for one second and retry up to 10 times before giving up (the other thread's
     * transaction will release the locks eventually).
     * 
     * @param fromPage
     * @param toUrls
     */
    @Override
    public List<Relationship> addLinks(Node fromPage, List<String> toUrls) {
        if (isStopped.get())
            return new ArrayList<Relationship>();

        NodesAndRelationshipsTuple tuple = doAddLinksInTransaction(fromPage, toUrls);

        // make sure to broadcast the event once the transaction has completed
        broadcastModifications(tuple.nodes);

        return tuple.relationships;
    }

    class NodesAndRelationshipsTuple {
        final List<Node> nodes;
        final List<Relationship> relationships;

        public NodesAndRelationshipsTuple() {
            this.nodes = new ArrayList<Node>();
            this.relationships = new ArrayList<Relationship>();
        }

        public NodesAndRelationshipsTuple(List<Node> nodes, List<Relationship> relationships) {
            this.nodes = nodes;
            this.relationships = relationships;
        }

        public void combine(NodesAndRelationshipsTuple chunk) {
            nodes.addAll(chunk.nodes);
            relationships.addAll(chunk.relationships);
        }
    }

    private NodesAndRelationshipsTuple doAddLinksInTransaction(final Node fromPage, final List<String> toUrls) {
        NodesAndRelationshipsTuple result = new NodesAndRelationshipsTuple();

        if (toUrls == null || toUrls.isEmpty()) {
            return result;
        }

        ListChunker<String> chunker = new ListChunker<String>(toUrls, transactionSize);
        while (chunker.hasMore()) {
            final List<String> chunk = chunker.getNextChunk();
            NodesAndRelationshipsTuple chunkResult = doAddLinksChunkInTransaction(fromPage, chunk);
            if (chunkResult != null) {
                result.combine(chunkResult);
            }
        }

        return result;
    }

    protected NodesAndRelationshipsTuple doAddLinksChunkInTransaction(final Node fromPage, final List<String> toUrls) {
        try {
            Callable<Object> task = new Callable<Object>() {
                final List<Node> nodes = new ArrayList<Node>(toUrls.size());
                final List<Relationship> links = new ArrayList<Relationship>(toUrls.size());

                @Override
                public Object call() throws Exception {
                    for (final String toUrl : toUrls) {
                        Node linkedPage = getPage(toUrl);
                        if (linkedPage == null) {
                            linkedPage = addPageWithoutBroadcasting(toUrl, PageNode.UNKNOWN_PAGE_CONTENT, false);
                            if (linkedPage != null) {
                                nodes.add(linkedPage);
                            }
                        }
                        final Relationship link = addLink(fromPage, linkedPage);
                        if (link != null) {
                            links.add(link);
                        }
                    }
                    return new NodesAndRelationshipsTuple(nodes, links);
                }

                @Override
                public String toString() {
                    return "Creating " + toUrls.size() + " links from " + PageNode.getUrl(fromPage);
                }
            };

            // increase deadlock retry timeout as this is potentially a big
            // transaction
            return (NodesAndRelationshipsTuple) transactionTemplate.execute(task, this, 10, 5000);
        } catch (Exception e) {
            logger.error(e);
            return null;
        }
    }

    @Override
    public Node getReferenceNode() {
        return graphDb.getReferenceNode();
    }

    @Override
    public Node getRootPage() {
        Relationship rel = getReferenceNode().getSingleRelationship(RelTypes.ROOT_PAGE_REFERENCE, Direction.OUTGOING);
        return rel == null ? null : rel.getEndNode();
    }

    @Override
    public Node getPage(String url) {
        return pageIndex.get(PageNode.URL_KEY, url).getSingle();
    }

    @Override
    public Iterator<Node> getAllPagesForDomain(String domain) {
        return pageIndex.get(PageNode.DOMAIN_KEY, domain).iterator();
    }

    @Override
    public Iterator<Node> getAllPagesOfType(String type) {
        return pageIndex.get(PageNode.TYPE_KEY, type).iterator();
    }

    @Override
    public int getNumberOfPagesForDomain(String domain) {
        return pageIndex.get(PageNode.DOMAIN_KEY, domain).size();
    }

    @Override
    public int getNumberOfPagesOfType(String type) {
        return pageIndex.get(PageNode.TYPE_KEY, type).size();
    }

    @Override
    public void waitForImportToFinish() {
        // no-op, this importer is synchronous
    }

    @Override
    public void shutdown() {
        logger.trace("Start importer shut down");
        stop();
        logger.trace("Shutting down database");
        graphDb.shutdown();
        logger.trace("Finished importer shut down");
    }
}
