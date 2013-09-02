package org.neo4japps.webgraph.importer;

import java.util.Collections;
import java.util.EventObject;
import java.util.List;

import org.neo4j.graphdb.Node;

/**
 * Event that gets broadcasted during the graph import process when a number of page node are created or modified.
 */
public final class PageNodesModificationEvent extends EventObject {

    private static final long serialVersionUID = 1L;

    private final List<Node> pages;

    public PageNodesModificationEvent(Object source, List<Node> pages) {
        super(source);
        if (pages == null) {
            throw new IllegalArgumentException("null pages");
        }
        this.pages = Collections.unmodifiableList(pages);
    }

    public PageNodesModificationEvent(Object source, Node page) {
        this(source, Collections.singletonList(page));
    }

    public List<Node> getPages() {
        return pages;
    }

    @Override
    public String toString() {
        if (pages.size() == 1) {
            return "PageNodesModificationEvent [" + pageToString(pages.get(0)) + "]";
        }
        return "PageNodesModificationEvent [" + pages.size() + " pages]";
    }

    private String pageToString(Node page) {
        try {
            return PageNode.toString(page);
        } catch (Exception e) {
            return "Node [" + page.getId() + "]";
        }
    }
}
