package org.neo4japps.webgraph.util;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleHttpClient {
    private static final AtomicInteger instanceCounter = new AtomicInteger();

    private final Logger logger;
    private final AtomicBoolean isShutdown;

    private final DefaultHttpClient defaultClient;
    private final CachingHttpClient cachingClient;
    private final HttpClient theClient;

    /**
     * Creates a HTTP client.
     *
     * @param parent          Owning object. Used for logging.
     * @param maxCacheEntries the maximum number of cache entries the cache will retain. Set to 0 for no caching.
     */
    public SimpleHttpClient(Object parent, int maxCacheEntries) {
        logger = LogManager.getLogger(this.getClass().getName() + "-" + instanceCounter.incrementAndGet() + " ("
                + parent.getClass().getSimpleName() + ")");

        isShutdown = new AtomicBoolean();

        PoolingClientConnectionManager conman = new PoolingClientConnectionManager();
        // increase max number of connections per host and total, defaults are too low to take advantage of multiple
        // threads
        conman.setDefaultMaxPerRoute(50); // default is 2
        conman.setMaxTotal(100); // default is 20
        defaultClient = new DefaultHttpClient(conman);

        if (maxCacheEntries > 0) {
            CacheConfig cacheConfig = new CacheConfig();
            cacheConfig.setSharedCache(false);
            cacheConfig.setMaxCacheEntries(maxCacheEntries);
            cachingClient = new CachingHttpClient(defaultClient, cacheConfig);
            theClient = cachingClient;
        } else {
            cachingClient = null;
            theClient = defaultClient;
        }

        logger.info("Created");
    }

    public void configProxy(String proxyHost, int proxyPort, String proxyUsername, String proxyPassword) {
        if (isShutdown.get()) {
            return;
        }

        if (proxyHost == null) {
            return;
        }

        theClient.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, new HttpHost(proxyHost, proxyPort, "http"));
        logger.info("Configured proxy host " + proxyHost);

        if (proxyUsername != null) {
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(new AuthScope(proxyHost, proxyPort),
                    new UsernamePasswordCredentials(proxyUsername, proxyPassword));
            defaultClient.setCredentialsProvider(credsProvider);
            logger.info("Configured proxy user " + proxyUsername);
        }
    }

    /**
     * Retrieves the textual content of a HTTP response triggered by a HTTP GET request to the specified URI. The
     * content itself can be HTML, XML, JSON,... whatever.
     *
     * @param uri The URI to get
     * @return the content of the HTTP response, or null if the response cannot be read
     */
    public String httpGet(String uri) {
        if (isShutdown.get()) {
            return "";
        }

        final HttpGet request = new HttpGet(uri);

        try {
            HttpResponse response = theClient.execute(request);
            HttpEntity entity = response.getEntity();
            if (entity != null) {
                return getEntityContent(entity, uri);
            } else {
                logger.warn(String.format("Cannot read response for URI %s. It has no content.", uri));
            }
        } catch (Exception e) {
            logger.warn("Cannot read response for URI " + uri, e);
            request.abort();
        }

        return "";
    }

    private String getEntityContent(HttpEntity entity, String uri) throws IOException {
        String content = "";

        long len = entity.getContentLength();
        if (len != -1 && len < 2048) {
            // the use of EntityUtils is strongly discouraged unless the response entities originate
            // from a trusted HTTP server and are known to be of limited length.
            content = EntityUtils.toString(entity);
        } else {
            logger.warn("Cannot read response for URI " + uri + ". Incorrect response length " + len);
        }

        // ensure the connection gets released to the manager
        EntityUtils.consume(entity);

        return content;
    }

    public void shutdown() {
        if (isShutdown.get()) {
            return;
        }

        isShutdown.set(true);

        logger.info("Shutting down");
        theClient.getConnectionManager().shutdown();
    }

    public long getCacheHits() {
        return cachingClient == null ? 0 : cachingClient.getCacheHits();
    }

    public long getCacheMisses() {
        return cachingClient == null ? 0 : cachingClient.getCacheMisses();
    }

    public long getCacheUpdates() {
        return cachingClient == null ? 0 : cachingClient.getCacheUpdates();
    }
}
