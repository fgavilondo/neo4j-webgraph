package org.neo4japps.webgraph.importer;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public interface GraphImporter {

    public abstract Lock getLock();

    public abstract Transaction beginDbTransaction();

    public abstract Node createNode();

    public abstract void addCategoryNodeToIndex(Node node);

    public abstract Node addPage(String url, String content);

    public abstract List<Relationship> addLinks(Node fromPage, List<String> toUrls);

    public abstract Node getReferenceNode();

    public abstract Node getRootPage();

    public abstract Node getPage(String url);

    public abstract int getNumberOfPageNodes();

    public abstract int getNumberOfLinks();

    public abstract Iterator<Node> getAllPagesForDomain(String domain);

    public abstract int getNumberOfPagesForDomain(String domain);

    public abstract Iterator<Node> getAllPagesOfType(String type);

    public abstract int getNumberOfPagesOfType(String type);

    public abstract void waitForImportToFinish() throws InterruptedException;

    public abstract void stop();

    public abstract void shutdown();
}
