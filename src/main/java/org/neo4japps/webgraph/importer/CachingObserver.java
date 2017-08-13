package org.neo4japps.webgraph.importer;

/**
 * If an event handler implements this interface these numbers will be added to the execution report printed before
 * program exit.
 */
public interface CachingObserver {
    boolean isCachingEnabled();

    long getCacheHits();

    long getCacheMisses();

    long getCacheUpdates();
}
