package org.neo4japps.webgraph.customhandlers;

import org.neo4japps.webgraph.importer.ApplicationConfiguration;
import org.neo4japps.webgraph.importer.CachingObserver;
import org.neo4japps.webgraph.importer.GraphObserver;
import org.neo4japps.webgraph.util.SimpleHttpClient;

public abstract class SocialMediaEventHandler extends GraphObserver implements CachingObserver {

    // we should never request the same URL twice, so no need for caching
    private int maxCacheEntries = 0;

    protected final SimpleHttpClient httpClient = new SimpleHttpClient(this, maxCacheEntries);

    @Override
    public void configure(ApplicationConfiguration config) {
        super.configure(config);

        httpClient.configProxy(config.getProxyHost(), config.getProxyPort(), config.getProxyUsername(),
                config.getProxyPassword());
    }

    @Override
    public final boolean isCachingEnabled() {
        return maxCacheEntries > 0;
    }

    @Override
    public final long getCacheHits() {
        return httpClient.getCacheHits();
    }

    @Override
    public final long getCacheMisses() {
        return httpClient.getCacheMisses();
    }

    @Override
    public final long getCacheUpdates() {
        return httpClient.getCacheUpdates();
    }

    @Override
    public void shutdown() {
        super.shutdown();
        httpClient.shutdown();
    }
}
