package org.neo4japps.webgraph.importer;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;

public interface GraphImporter {

    Lock getLock();

    Transaction beginDbTransaction();

    Node createNode();

    void addCategoryNodeToIndex(Node node);

    Node addPage(String url, String content);

    List<Relationship> addLinks(Node fromPage, List<String> toUrls);

    Node getReferenceNode();

    Node getRootPage();

    Node getPage(String url);

    int getNumberOfPageNodes();

    int getNumberOfLinks();

    Iterator<Node> getAllPagesForDomain(String domain);

    int getNumberOfPagesForDomain(String domain);

    Iterator<Node> getAllPagesOfType(String type);

    int getNumberOfPagesOfType(String type);

    void waitForImportToFinish() throws InterruptedException;

    void stop();

    void shutdown();
}
