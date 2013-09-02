package org.neo4japps.webgraph.importer;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4japps.webgraph.util.StringFormatUtil;

public abstract class AbstractObservableGraphImporter extends ConcurrentObservable implements GraphImporter {

    protected static final String PAGE_INDEX_NAME = "pages";
    protected static final String CATEGORY_INDEX_NAME = "categories";

    protected final Logger logger = Logger.getLogger(this.getClass());

    protected final String rootUrl;
    protected final int reportFrequency;
    protected final long startTimeInMillis;

    protected final AtomicInteger numberOfPageNodes = new AtomicInteger();
    protected final AtomicInteger numberOfLinks = new AtomicInteger();

    protected final AtomicBoolean isStopped = new AtomicBoolean();

    public AbstractObservableGraphImporter(String rootUrl, long startTimeInMillis, int importProgressReportFrequency) {
        if (importProgressReportFrequency < 1) {
            throw new IllegalArgumentException("Value for importProgressReportFrequency must be greater than 0");
        }

        this.rootUrl = rootUrl;
        this.startTimeInMillis = startTimeInMillis;
        this.reportFrequency = importProgressReportFrequency;
    }

    // This index can be used in Cypher queries to find pages by url/type/domain
    protected abstract void updatePageIndex(Node page);

    protected abstract void notifyObserversAndHandleExceptions(PageNodesModificationEvent event);

    protected final int getNumberOfUnprocessedPageNodesByObservers() {
        int ret = 0;
        for (GraphObserver observer : getObservers()) {
            ret += observer.getNumberOfPageNodesPendingProcessing();
        }
        return ret;
    }

    @Override
    public final int getNumberOfPageNodes() {
        return numberOfPageNodes.get();
    }

    @Override
    public final int getNumberOfLinks() {
        return numberOfLinks.get();
    }

    @Override
    public void stop() {
        boolean wasStopped = isStopped.getAndSet(true);
        if (wasStopped) {
            logger.trace("Already stopped");
        } else {
            logger.trace("Stopped");
        }
    }

    protected final void populateNewPageNode(Node node, String url, String domain, String type, String content) {
        if (!PageNode.hasUrlProperty(node)) {
            PageNode.setUrl(node, url);
        }
        PageNode.setDomain(node, domain);
        PageNode.setOutgoingLinks(node, 0);
        PageNode.setIncomingLinks(node, 0);
        PageNode.setContent(node, content);

        // instead of using a 'type' property we could use the 'category node' approach as explained
        // at http://py2neo.org/tutorials/tables_to_graphs to achieve typing.
        // I.e. we would have a 'homes' node and a 'leaf pages' node that link to the appropriate pages.
        // (home)-[:HOME]->(page)
        // (leaf)-[:LEAF]->(page)
        PageNode.setType(node, type);

        if (url.equals(rootUrl)) {
            getReferenceNode().createRelationshipTo(node, RelTypes.ROOT_PAGE_REFERENCE);
        }
    }

    protected final void updatePageContent(Node page, String content) {
        if (PageNode.UNKNOWN_PAGE_CONTENT.equals(content)) {
            // don't override genuine page content when a page gets linked after it's been added
            return;
        }

        if (logger.isDebugEnabled()) {
            logger.trace("Updating page content: " + PageNode.toString(page));
        }
        PageNode.setContent(page, content);
    }

    protected final Relationship addLink(Node fromPage, Node toPage) {
        if (isStopped.get()) {
            return null;
        }

        if (fromPage == null || toPage == null) {
            return null;
        }

        // only create once, otherwise reuse
        Iterable<Relationship> existingLinks = fromPage.getRelationships(RelTypes.LINKS_TO, Direction.OUTGOING);
        for (Relationship existingLink : existingLinks) {
            if (existingLink.getEndNode().equals(toPage)) {
                return existingLink;
            }
        }

        Relationship newLink = fromPage.createRelationshipTo(toPage, RelTypes.LINKS_TO);
        numberOfLinks.incrementAndGet();
        PageNode.incrementOutgoingLinks(fromPage);
        PageNode.incrementIncomingLinks(toPage);

        if (logger.isDebugEnabled()) {
            logger.trace("Created link: " + PageNode.getUrl(fromPage) + " ("
                    + PageNode.getNumberOfOutgoingLinks(fromPage) + ") --> " + PageNode.getUrl(toPage) + " ("
                    + PageNode.getNumberOfIncomingLinks(toPage) + ")");
        }

        return newLink;
    }

    protected final void broadcastModification(Node page) {
        if (!isStopped.get() && countObservers() > 0 && page != null) {
            notifyObserversAndHandleExceptions(new PageNodesModificationEvent(this, page));
        }
    }

    protected final void broadcastModifications(List<Node> pages) {
        if (!isStopped.get() && countObservers() > 0 && pages != null && !pages.isEmpty()) {
            notifyObserversAndHandleExceptions(new PageNodesModificationEvent(this, pages));
        }
    }

    protected final void reportProgress() {
        int numberOfCreatedNodes = getNumberOfPageNodes();
        if (numberOfCreatedNodes % reportFrequency == 0) {
            final long endMillis = System.currentTimeMillis();
            final double elapsedSeconds = (endMillis - startTimeInMillis) / 1000;
            // avoid division by zero!
            final double nodesPerSecond = (elapsedSeconds == 0.00) ? numberOfCreatedNodes
                    : (numberOfCreatedNodes / elapsedSeconds);
            if (logger.isInfoEnabled()) {
                logger.info("Nodes imported so far: " + numberOfCreatedNodes + ". That's "
                        + StringFormatUtil.formatNodesPerSecond(nodesPerSecond) + " nodes per sec.");
            }
        }
    }
}
