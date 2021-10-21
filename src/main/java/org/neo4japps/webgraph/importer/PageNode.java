package org.neo4japps.webgraph.importer;

import org.neo4j.graphdb.Node;

import java.util.concurrent.locks.Lock;

/**
 * Static wrapper for easy access to common page node properties.
 */
public class PageNode {

    public static final String URL_KEY = "url";
    public static final String DOMAIN_KEY = "domain";
    public static final String TYPE_KEY = "type";
    public static final String INCOMING_LINKS_KEY = "incomingLinks";
    public static final String OUTGOING_LINKS_KEY = "outgoingLinks";
    public static final String CONTENT_KEY = "content";

    public static final String FACEBOOK_TOTAL_COUNT_KEY = "facebookTotalCount";
    public static final String TWITTER_COUNT_KEY = "twitterCount";

    public static final String LEAF_PAGE = "page";
    public static final String HOME_PAGE = "home";

    public static final String UNKNOWN_PAGE_CONTENT = "Page not yet fetched. Content unknown.";

    protected PageNode() {
        // non instantiable
    }

    // "getters"

    public static String getUrl(Node page) {
        return page == null ? null : (String) page.getProperty(URL_KEY, "");
    }

    public static String getUrl(Node page, Lock lock) {
        lock.lock();
        try {
            return getUrl(page);
        } finally {
            lock.unlock();
        }
    }

    public static String getDomain(Node page) {
        return page == null ? null : (String) page.getProperty(DOMAIN_KEY, "");
    }

    public static String getType(Node page) {
        return page == null ? null : (String) page.getProperty(TYPE_KEY, "");
    }

    public static int getNumberOfOutgoingLinks(Node page) {
        return page == null ? 0 : (Integer) page.getProperty(OUTGOING_LINKS_KEY, 0);
    }

    public static int getNumberOfIncomingLinks(Node page) {
        return page == null ? 0 : (Integer) page.getProperty(INCOMING_LINKS_KEY, 0);
    }

    public static String getContent(Node page) {
        return page == null ? null : (String) page.getProperty(CONTENT_KEY, "");
    }

    public static boolean hasNoContent(Node page) {
        final String content = getContent(page);
        return content == null || content.isEmpty() || content.equals(UNKNOWN_PAGE_CONTENT);
    }

    public static int getFacebookTotalCount(Node page) {
        return page == null ? 0 : (Integer) page.getProperty(FACEBOOK_TOTAL_COUNT_KEY, -1);
    }

    public static int getFacebookTotalCount(Node page, Lock lock) {
        lock.lock();
        try {
            return getFacebookTotalCount(page);
        } finally {
            lock.unlock();
        }
    }

    public static int getTwitterCount(Node page) {
        return page == null ? 0 : (Integer) page.getProperty(TWITTER_COUNT_KEY, -1);
    }

    public static int getTwitterCount(Node page, Lock lock) {
        lock.lock();
        try {
            return getTwitterCount(page);
        } finally {
            lock.unlock();
        }
    }

    // "setters"

    public static void setUrl(Node page, String url) {
        if (page != null)
            page.setProperty(URL_KEY, url);
    }

    public static void setDomain(Node page, String domain) {
        if (page != null)
            page.setProperty(DOMAIN_KEY, domain);
    }

    public static void setType(Node page, String type) {
        if (page != null)
            page.setProperty(TYPE_KEY, type);
    }

    public static void setOutgoingLinks(Node page, int count) {
        if (page != null)
            page.setProperty(OUTGOING_LINKS_KEY, count);
    }

    public static void setIncomingLinks(Node page, int count) {
        if (page != null)
            page.setProperty(INCOMING_LINKS_KEY, count);
    }

    public static void setContent(Node page, String content) {
        if (page != null) {
            page.setProperty(CONTENT_KEY, (content == null) ? UNKNOWN_PAGE_CONTENT : content);
        }
    }

    public static void setFacebookTotalCount(Node page, int count) {
        if (page != null)
            page.setProperty(FACEBOOK_TOTAL_COUNT_KEY, count);
    }

    public static void setFacebookTotalCount(Node page, int count, Lock lock) {
        lock.lock();
        try {
            setFacebookTotalCount(page, count);
        } finally {
            lock.unlock();
        }
    }

    public static void setTwitterCount(Node page, int count) {
        if (page != null)
            page.setProperty(TWITTER_COUNT_KEY, count);
    }

    public static void setTwitterCount(Node page, int count, Lock lock) {
        lock.lock();
        try {
            setTwitterCount(page, count);
        } finally {
            lock.unlock();
        }
    }

    // "has"

    public static boolean hasProperty(Node page, String key) {
        return page != null && page.hasProperty(key);
    }

    public static boolean hasProperty(Node page, String key, Lock lock) {
        lock.lock();
        try {
            return hasProperty(page, key);
        } finally {
            lock.unlock();
        }
    }

    public static boolean hasUrlProperty(Node page) {
        return hasProperty(page, URL_KEY);
    }

    public static boolean hasDomainProperty(Node page) {
        return hasProperty(page, DOMAIN_KEY);
    }

    public static boolean hasTypeProperty(Node page) {
        return hasProperty(page, TYPE_KEY);
    }

    public static boolean hasNumberOfOutgoingLinksProperty(Node page) {
        return hasProperty(page, OUTGOING_LINKS_KEY);
    }

    public static boolean hasNumberOfIncomingLinksProperty(Node page) {
        return hasProperty(page, INCOMING_LINKS_KEY);
    }

    public static boolean hasContentProperty(Node page) {
        return hasProperty(page, CONTENT_KEY);
    }

    public static boolean hasFacebookTotalCountProperty(Node page) {
        return hasProperty(page, FACEBOOK_TOTAL_COUNT_KEY);
    }

    public static boolean hasFacebookTotalCountProperty(Node page, Lock lock) {
        return hasProperty(page, FACEBOOK_TOTAL_COUNT_KEY, lock);
    }

    public static boolean hasTwitterCountProperty(Node page) {
        return hasProperty(page, TWITTER_COUNT_KEY);
    }

    public static boolean hasTwitterCountProperty(Node page, Lock lock) {
        return hasProperty(page, TWITTER_COUNT_KEY, lock);
    }

    // other utilities

    public static void incrementIncomingLinks(Node page) {
        if (page == null)
            return;
        int links = PageNode.getNumberOfIncomingLinks(page);
        PageNode.setIncomingLinks(page, links + 1);
    }

    public static void incrementOutgoingLinks(Node page) {
        if (page == null)
            return;
        int links = PageNode.getNumberOfOutgoingLinks(page);
        PageNode.setOutgoingLinks(page, links + 1);
    }

    public static String toString(Node page) {
        if (page == null)
            return "null";

        return "id: " + page.getId() + ", " + URL_KEY + ": " + getUrl(page) + ", " + DOMAIN_KEY + ": " + getDomain(page)
                + ", " + TYPE_KEY + ": " + getType(page);
    }
}
